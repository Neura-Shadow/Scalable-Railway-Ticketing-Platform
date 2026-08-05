# ADR 051: Durable Ticket Issuance

- Status: Accepted for Milestone 6 implementation
- Date: 2026-08-05

## Context

A captured payment must produce exactly one durable ticket for each reserved
seat, even when workers retry, replicas race, control finalization fails, or the
train run is migrating. Issuing from control PostgreSQL or directly from a
provider callback would separate ticket state from the authoritative reservation
and seat masks on the physical shard.

## Decision

Ticket issuance is a single transaction on the reservation's current
authoritative physical shard. It begins only after the control plane has durably
recorded a consistent captured provider operation with the exact reservation
amount and currency. Captured proof is necessary but not sufficient: the shard
command independently validates local reservation and payment facts.

The issuance coordinator resolves the current reservation directory and train-
run assignment immediately before dispatch and sends an immutable, globally
unique issuance command to exactly one approved shard. The command does not bind
the payment intent permanently to a shard. The selected shard validates its
storage identity and expected monotonic generation fence as required by ADR 037
and ADR 043.

In one local transaction the shard:

1. locks the reservation and verifies it is `payment_pending` or
   `payment_review` in an issuable state;
2. verifies owner-independent internal identity, payment intent, captured
   operation, integer minor-unit amount, and currency snapshots;
3. acquires a unique payment-command receipt and ticket-issuance receipt;
4. changes the reservation to `confirmed`;
5. changes the ticket order through `payment_captured` and
   `issuance_pending` to `issued` as one committed result;
6. creates or activates exactly one ticket for each reservation-seat row;
7. assigns each ticket an opaque, globally collision-resistant, non-sequential
   code containing no passenger PII;
8. records issued count, ticket order, command and operation identities in the
   issuance receipt;
9. appends shard-local reservation, order, ticket, and issuance outbox intent;
   and
10. commits all rows together.

Unique constraints cover issuance identity, payment intent and reservation,
ticket order per reservation, ticket per reservation-seat identity, ticket
code, and the relevant command receipts. A same-command/same-fingerprint retry
returns the original order and tickets. A different fingerprint or financial
snapshot conflicts. A receipt cannot claim issuance without the matching
confirmed reservation, issued order, exact ticket set, and outbox intent.

Shard-local outbox behavior follows ADR 041. Publication may be retried at least
once, but consumer receipts prevent duplicate global projections. Event delivery
is not ticket authority and the system does not claim exactly-once distributed
processing.

After the shard commit, the control plane loads and verifies the issuance
receipt and local result, then conditionally marks the saga and payment intent
completed and updates any global directory or projection. If that control
transaction fails, the authoritative tickets remain valid. Retry or
reconciliation finalizes control from the same receipt and never reissues.

Transient issuance failures before local commit retry the same issuance command.
An ambiguous shard response is resolved by reading the same command/issuance
receipt on the assigned shard, not by creating a new command or scanning all
shards. An irrecoverable issuance failure after capture begins the compensation
and full-refund decision in ADR 052.

## Invariants

- No ticket becomes active before captured status and matching amount/currency
  are durably known.
- Exactly one ticket exists per reservation seat, and exactly one issued ticket
  order exists per reservation.
- Reservation confirmation, order issuance, ticket activation, issuance
  receipt, and local outbox intent commit atomically on one shard.
- Control finalization, cache population, and event publication are not required
  for an already committed ticket to remain durable.
- Ticket codes are opaque and globally unique; offline signed boarding
  credentials are outside Milestone 6.

## Consequences

- Shard-local atomicity prevents confirmed reservations without tickets and
  tickets without the corresponding occupied seats.
- A control outage can temporarily hide a valid ticket from global projections,
  requiring owner reads to route through the authoritative directory and shard.
- Payment fields, order/ticket state, receipts, and outbox expectations must be
  included in physical-shard migration copy, journal, validation, and reverse.
- Permanent post-capture failure requires financial compensation rather than
  silently cancelling or releasing the seat.

## Rejected alternatives

- Issue in control PostgreSQL: rejected because it cannot atomically validate
  or transition authoritative shard-local seats and reservation state.
- Issue from a webhook handler: rejected because webhook delivery is unordered,
  duplicated, and independent from the shard transaction.
- Mark control complete before shard issuance: rejected because a customer could
  observe completion without durable tickets.
- Create tickets before capture: rejected because it can activate unpaid travel
  rights and complicates safe recovery.
- Repair missing tickets with direct reconciliation writes: rejected because it
  bypasses the issuance receipt, fence, and atomic local command.
