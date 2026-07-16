# ADR 002: PostgreSQL Owns Authoritative Seat Inventory

- Status: Accepted
- Date: 2026-07-15

## Context

Availability reads are allowed to be stale, but overlapping active reservations must never share the same physical seat. Temporary holds, confirmations, cancellations, expiration workers, train-run status changes, idempotency, and outbox writes race on the same state.

Redis can be fast and highly available, but its eviction, expiry, failover, replication, and cache invalidation behavior does not provide the transaction that must also create reservation rows and outbox events.

## Decision

PostgreSQL is the sole authority for:

- train-run bookable status;
- each seat's occupied route segments;
- held and confirmed reservation state;
- ticket orders and tickets;
- durable command idempotency; and
- transactional outbox events.

The primary inventory key is `(train_run_id, seat_id)`. Every allocation and release uses an atomic SQL predicate inside the reservation transaction. The allocation succeeds only when mask lengths match and the bitwise intersection is zero. The release clears only the reservation seat's exact mask.

Read Committed is the default isolation level because the hot-path `UPDATE` predicates and explicit row locks serialize conflicting rows. Recognized `40001` and `40P01` failures may be retried with a small configured limit and jitter. Business conflicts and validation errors are never retried as transient failures.

Create-hold uses this order:

```text
idempotency record
-> train run
-> owned passengers and selected fare
-> seat_inventory ordered by coach/seat/seat_id
-> reservation and reservation seats
-> idempotency completion
-> outbox insert
```

Confirm, cancel, and expire use:

```text
idempotency record for customer commands
-> reservation
-> reservation seats ordered by seat_id
-> seat_inventory ordered by seat_id
-> ticket order
-> tickets ordered by id
-> idempotency completion
-> outbox insert
```

Create-hold locks the train run before inventory mutation. Train-run status updates lock the same row first, so a non-bookable status and a new hold have an explicit serialization order.

Redis may cache station metadata, searches, availability hints, completed idempotency lookups, rate-limit counters, stream events, processed-event keys, and bounded consumer failure counts. Every cached availability result is explicitly a hint. Booking always rechecks PostgreSQL.

## Validation mechanism

- Inventory initialization creates one zero-valued `VARBIT` row per active seat with length `train_runs.segment_count`.
- Seat inventory stores `segment_count`, checks `bit_length(occupied_segments) = segment_count`, and references the immutable `(train_run_id, segment_count)` pair. A deferred constraint trigger validates reservation-seat mask lengths against the train run.
- Allocation/release SQL uses a `CASE` length guard before bitwise operators because SQL does not promise `AND` evaluation order.
- A reconciliation query compares stored occupancy with the union of masks for `held` and `confirmed` reservations.

## Consequences

- Redis loss can reduce rate-limit/cache functionality but cannot duplicate or oversell inventory.
- PostgreSQL write capacity is the Milestone 1 scaling ceiling; no national-scale claim is made.
- All booking mutations must route through the Booking transaction module.
- Read replicas and regional caches may serve future searches, but never accept authoritative seat writes.

## Rejected alternatives

- Redis locks or counters as inventory authority: rejected because domain state and outbox commits would not be atomic.
- Simple seat-class quantity: rejected because it cannot represent non-overlapping reuse of one seat.
- Publish-before-commit or consumer-owned allocation: rejected because delivery cannot be atomic with the reservation transaction.
