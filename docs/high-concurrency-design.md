# High-Concurrency Design

## Correctness target

The platform must prevent overlapping active reservations for the same seat and train run while allowing that seat to be reused on non-overlapping route segments. A multi-passenger request receives all seats or none.

## Serialization points

- Train-run row: compatible shared locks allow holds to proceed concurrently while still serializing them against operator status updates.
- Passenger row: deterministic locks serialize the one-active-reservation-per-passenger/train-run rule.
- Seat-inventory row: serializes occupancy changes for one physical seat and train run.
- Reservation row: serializes confirm, cancel, and expire.
- Idempotency unique key/row: serializes retries of one customer command.
- Outbox row: serializes claim/finalize ownership.

## Allocation algorithm

Create-hold uses one Read Committed transaction. It locks the idempotency record, takes a shared train-run lock, and locks canonical owned passenger rows before reading fare and mutating inventory. It rejects a passenger already present in a held/confirmed reservation on that run. A data-modifying CTE selects candidates in `(coach_number, seat_number, seat_id)` order with `FOR UPDATE OF seat_inventory SKIP LOCKED`. The update applies `occupied_segments | requested_mask` only to active-seat candidates whose equal-length intersection is zero.

The returned row count must exactly equal passenger count. Any shortfall returns a bounded availability conflict by rolling back the transaction. Passenger-to-seat assignment follows returned deterministic seat order and canonical passenger order.

`SKIP LOCKED` avoids waiting behind seats another request is deciding. It can conservatively reject a request when enough seats are temporarily locked; it cannot partially commit. This tradeoff is measured under the hot-train k6 scenario.

## Lock ordering

All commands use the ordering in ADR 002. Multi-row inventory operations sort by seat ID before mutation. Outbox workers order by ready time and ID. Lock acquisition never depends on untrusted sort input.

## Isolation and retry

Atomic predicates and row locks are sufficient at Read Committed. The application retries only PostgreSQL `40001` and `40P01`, with a small configured limit, context cancellation, and jitter. It never retries inventory conflicts, invalid states, authorization failures, or changed idempotency fingerprints.

## Deterministic concurrency tests

Tests coordinate starts with barriers/channels and expose transaction hooks at adapter seams. They do not assert timing using sleeps.

Required cases:

1. One seat and 100 overlapping attempts: one success.
2. Ten seats and 100 overlapping attempts: ten successes.
3. Same seat on non-overlapping A-C/C-D intervals: both may succeed; overlapping A-C/B-D may not.
4. Multi-passenger shortfall: no seat remains allocated.
5. Multiple expirer workers: one terminal transition/release/event.
6. Confirm versus expire: one valid state and consistent occupancy.
7. Same idempotency key: one resource.
8. Cancel versus confirm: valid terminal outcome with no leak/double release.
9. Train-run cancellation versus hold: no hold after non-bookable status is authoritative.

Critical cases run repeatedly and under the Go race detector. Reconciliation follows every database concurrency suite.

## Deadlock controls

- One documented order across all lifecycle commands.
- Stable multi-row ordering before locks.
- Short transactions with no network publication or Redis writes inside.
- Small worker batches.
- Bounded deadlock retry and metrics by bounded reason.
- Database statement/lock timeouts configured below request deadlines.

## Honest performance scope

Milestone 1 measures local/single-region behavior only. It does not claim national-scale throughput, unlimited horizontal write scaling, or multi-region active-active writes.
