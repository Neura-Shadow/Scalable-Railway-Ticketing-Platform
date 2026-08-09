# ADR 047: Payment Intent and Saga State Machine

- Status: Accepted for Milestone 6 implementation
- Date: 2026-08-05

## Context

Payment progresses across provider calls, verified events, control commits, and
physical-shard commands. Retries, worker crashes, and out-of-order observations
make a single free-form status insufficient. A timeout after an external call
also cannot be classified as ordinary failure, because the provider may have
accepted the request.

## Decision

Use three constrained control-plane state machines: `PaymentIntent` describes
the customer-visible aggregate, `PaymentSaga` describes orchestration progress,
and `PaymentOperation` describes one provider interaction. Every transition is
an allowlisted compare-and-set transition with a bounded domain error for an
invalid source state. Database constraints reject unknown values, and tests
cover every accepted and rejected edge.

`PaymentIntent` states are `created`, `reservation_securing`,
`checkout_pending`, `awaiting_customer`, `authorization_pending`, `authorized`,
`capture_pending`, `captured`, `ticket_issue_pending`, `completed`,
`void_pending`, `voided`, `refund_pending`, `refunded`, `cancelled`, `failed`,
`manual_review`, and `expired`.

`PaymentSaga` states are `created`, `reservation_secured`, `checkout_created`,
`awaiting_provider`, `authorized`, `capturing`, `captured`, `issuing_tickets`,
`completed`, `compensating`, `refunding`, `compensated`, `failed`, and
`manual_review`.

`PaymentOperation` types are `create_checkout`, `query_status`, `authorize`,
`capture`, `void`, and `refund`. Operation states are `pending`, `claimed`,
`in_flight`, `succeeded`, `failed_retryable`, `failed_permanent`, `uncertain`,
and `cancelled`.

The normal path is:

1. create an intent and saga from a server-priced reservation;
2. execute a shard-local begin-payment command and record its receipt;
3. create the hosted checkout and await the provider;
4. durably establish authorization, then claim an idempotent capture operation;
5. durably establish captured amount and currency;
6. execute the shard-local ticket-issuance command; and
7. finalize the control intent and saga from the issuance receipt.

Each claimed saga or operation has a bounded lease, attempt count, and next
attempt time. Claim and finalization transactions are short. Network I/O occurs
after claim commit and before a new conditional finalization transaction.
`SKIP LOCKED` supports bounded multi-replica claims. Lease expiry permits another
worker to inspect durable state; it does not prove that an in-flight provider
operation failed.

An adapter response that proves success or permanent rejection advances the
operation accordingly. A connection failure, deadline after dispatch,
malformed bounded response, crash window, or contradictory event makes the
operation `uncertain`. The next permitted money operation is a `query_status`
using the provider payment and operation identity. The original authorize,
capture, void, or refund must not be blindly retried until the query proves it
absent or the same stable provider idempotency identity is known to be safe.

Provider responses and webhooks are observations of the same provider object.
They can race. State transitions are monotonic with respect to financial facts:
an older authorization event cannot regress a captured intent, and a duplicated
capture observation cannot create another capture operation. Contradictory
amounts, currencies, payload hashes, or terminal states fail closed into a
security conflict or manual review.

A browser success, cancel, or return callback does not transition an intent.
It can prompt a bounded status read only. Verified webhooks are durably stored
before asynchronous processing and still pass the same transition and
reconciliation rules.

The payment saga is a purpose-built module for the bounded payment flow. It is
not a generic workflow language, distributed transaction manager, or reusable
orchestration platform. Its steps and compensations are explicit domain code.

## Invariants

- There is at most one active intent and one active saga for a reservation.
- A state advance never changes the immutable reservation ID, owner, train run,
  amount, currency, request fingerprint, or provider-operation identity.
- `completed` requires a verified shard-local issuance receipt; `refunded`
  requires a verified full-refund result and local compensation receipt.
- `failed` is used only for a proven terminal non-financial failure. Unknown
  capture or refund outcomes remain `uncertain` or `manual_review`.
- Replaying the same command or observation yields the same state and result.

## Consequences

- Partial progress is durable and visible, enabling deterministic crash and
  multi-replica recovery tests.
- Conservative uncertain and manual-review states can delay inventory release,
  so alerting, age metrics, and bounded operator procedures are mandatory.
- Adding a provider requires an explicit mapping from its states and errors into
  these domain transitions; arbitrary provider strings cannot become states.

## Rejected alternatives

- One free-form status column: rejected because it cannot express operation
  uncertainty, compensation, or enforce valid transitions.
- Retry every timed-out call: rejected because a timeout can follow successful
  capture or refund and cause duplicate financial effects.
- Let webhooks set terminal state directly: rejected because signature validity
  proves origin, not ordering, uniqueness, amount consistency, or current state.
- Hold a database transaction around a provider call: rejected because it
  creates long locks without providing atomicity with the provider.
