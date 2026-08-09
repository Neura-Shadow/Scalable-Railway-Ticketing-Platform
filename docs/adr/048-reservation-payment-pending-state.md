# ADR 048: Reservation Payment-Pending State

- Status: Accepted for Milestone 6 implementation
- Date: 2026-08-05

## Context

The existing shard-local reservation state machine has `held`, `confirmed`,
`cancelled`, and `expired`. Its normal hold expirer releases an expired `held`
reservation. Payment adds intervals in which authorization, capture, or refund
may have succeeded even though the platform has not yet received a conclusive
response. Releasing a seat during that uncertainty could leave a charged
customer with no inventory and allow a second customer to acquire the seat.

## Decision

Extend the physical-shard reservation state machine with `payment_pending`,
`payment_review`, and `refund_pending`. Reservation state remains authoritative
on the physical shard because it is coupled to seat masks and ticket lifecycle,
as required by ADR 037. Control payment state does not mutate or override it.

The normal begin-payment command resolves the current assignment, validates the
database-local generation fence, locks the reservation, verifies the owner and
unexpired `held` state, revalidates the server-derived amount and currency,
rejects a conflicting active payment, and atomically:

- changes `held` to `payment_pending`;
- records the immutable payment intent ID without a cross-database foreign key;
- sets a bounded payment grace deadline;
- creates or updates the ticket order as `payment_pending`;
- writes a unique payment-command receipt; and
- appends shard-local outbox intent under ADR 041.

Allowed reservation transitions are:

- `held` to `payment_pending`, `expired`, or `cancelled`;
- `payment_pending` to `confirmed`, `cancelled`, or `payment_review`;
- `payment_review` to `payment_pending`, `confirmed`, `refund_pending`, or
  `cancelled`;
- `confirmed` to `refund_pending`; and
- `refund_pending` to `cancelled` after a proven full refund.

Expired or cancelled reservations cannot begin payment or become confirmed.
The normal hold expirer continues to select and mutate only `held` rows. It
never expires `payment_pending`, `payment_review`, or `refund_pending` rows.
Payment reconciliation owns those states and requires provider evidence before
confirming, voiding, refunding, or releasing inventory.

The payment grace deadline is an observation and alert threshold, not automatic
proof of payment failure. When it elapses, reconciliation queries the provider.
A proven absence, expiration, void, or permanent pre-capture failure permits an
idempotent shard command to cancel and release the seat. A timeout, unknown
capture result, contradictory provider evidence, or unavailable shard moves the
reservation toward `payment_review` and conservatively retains inventory.

Conservative retention is bounded operationally rather than by unsafe automatic
release. Age, retry, and manual-review metrics alert operators; reconciliation
checkpoints and cases identify every overdue item. No case may silently remain
without a next attempt or manual owner.

After payment begins, reservation amount and currency are immutable. Capture,
issuance, void, refund, and compensation commands carry and verify the same
integer minor-unit snapshot. A customer-provided amount is never accepted.

## Cancellation behavior

Before provider authorization, cancellation cancels the checkout/operation and
then uses the shard command to cancel the reservation. After authorization but
before capture, it first establishes a successful void. After capture, it enters
the full-refund saga and `refund_pending`; the seat remains occupied until the
refund result is durable. An uncertain void or refund remains in review and
does not trigger release.

## Invariants

- The normal hold expirer mutates only `held`; it never expires a payment or
  refund state.
- One reservation has at most one active payment intent, immutable amount, and
  immutable currency.
- A transition that releases inventory requires durable proof that capture did
  not occur or that a captured amount was fully refunded.
- Control state, a grace deadline, or an unavailable provider cannot directly
  confirm, cancel, expire, or release a shard-local reservation.
- Every payment-state mutation validates the current shard fence and commits its
  receipt, inventory/ticket effects, journal, and outbox intent atomically.

## Consequences

- The normal expiration path cannot sell inventory that may already be paid.
- Provider or database uncertainty can temporarily reduce availability, but it
  preserves the stronger no-charged-customer-without-seat invariant.
- Reservation, ticket-order, receipt, inventory, journal, and outbox transitions
  remain one local shard transaction and migrate together under ADR 043.
- Payment-state age and manual-review operations become required production
  signals rather than optional cleanup.

## Rejected alternatives

- Extend the existing hold deadline during payment: rejected because a generic
  hold expiry still cannot distinguish proven failure from unknown capture.
- Let the normal hold expirer inspect control payment state: rejected because it
  creates a synchronous cross-database correctness dependency.
- Release on provider timeout: rejected because timeout does not prove absence
  of authorization or capture.
- Store reservation payment state only in control PostgreSQL: rejected because
  seat release, confirmation, and ticket transitions would lose local atomicity.
