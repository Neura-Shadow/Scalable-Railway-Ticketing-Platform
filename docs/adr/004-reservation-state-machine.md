# ADR 004: Reservation State Machine and Race Resolution

- Status: Accepted
- Date: 2026-07-15

## Context

Holds expire automatically while customers may confirm or cancel concurrently. Confirmed reservations may later be cancelled, but expiration must never release confirmed inventory. Repeated client commands must be stable.

## Decision

Reservation states are `held`, `confirmed`, `expired`, and `cancelled`.

| Current | Command | Guard | Next | Inventory | Side effects |
|---|---|---|---|---|---|
| held | confirm | `now < expires_at` | confirmed | unchanged | create ticket order/tickets; outbox confirmed |
| held | expire | `now >= expires_at` | expired | release exact masks | outbox expired |
| held | cancel | owner authorized | cancelled | release exact masks | outbox cancelled |
| confirmed | cancel | owner authorized | cancelled | release exact masks | cancel order/tickets; outbox cancelled |
| confirmed | confirm | same completed command | confirmed | unchanged | return stable result |
| cancelled | cancel | same completed command | cancelled | unchanged | return stable result |
| expired | expire | worker repeat | expired | unchanged | no duplicate release/event |

All other transitions are domain errors. In particular, `expired -> confirmed`, `cancelled -> confirmed`, `confirmed -> expired`, and either terminal state back to `held` are impossible.

Every command first locks the reservation row. The winning transaction rechecks status and time while holding that lock, then follows the shared deterministic lock order for reservation seats and inventory. Therefore:

- confirm versus expire: exactly one sees `held`; the loser observes the winner's committed state and cannot release or confirm incorrectly;
- cancel versus confirm: exactly one transition wins; cancellation after committed confirmation is allowed and cancels tickets, while a confirm after committed cancellation fails;
- duplicate worker execution: only one transaction changes `held` to `expired` and emits the event;
- train-run cancellation versus hold: the hold transaction validates/locks authoritative run status before inventory mutation under the documented ordering.

Repeated command stability is enforced by both state-aware behavior and durable command idempotency. A repeated call does not create another ticket order, ticket, release, or outbox event.

The Clock interface is injected into application and worker modules. Production uses UTC wall time; tests use a deterministic clock. Database transition predicates use a transaction-consistent timestamp where needed so application and database decisions cannot silently diverge.

## Consequences

- Reservation status is the first serialization point for lifecycle races.
- Inventory release always derives from locked `reservation_seats.segment_mask` rows.
- HTTP handlers do not encode state-machine rules; they map typed results/errors.
- No payment state exists. Confirmation simulates domain approval only, and payment authorization is deferred.
## Rejected alternatives

- Background expiry that clears masks before locking the reservation: rejected because it can release a concurrent confirmation.
- Soft best-effort idempotency in handlers: rejected because process or Redis loss can duplicate side effects.
- A generic workflow engine: rejected as speculative for four explicit states.
