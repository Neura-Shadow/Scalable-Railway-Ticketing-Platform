# Consistency Model

## Authoritative hierarchy

```text
PostgreSQL seat inventory and train-run state
-> reservation transaction
-> durable idempotency and transactional outbox
-> publisher
-> consumers
-> caches and read models
```

No lower layer can override a higher one.

## Strongly consistent commands

Create hold, confirm, cancel, expire, train-run status changes, inventory initialization, durable idempotency, tickets, and outbox insertion run against the PostgreSQL primary in explicit transactions. Atomic predicates and deterministic row locks provide the command serialization needed at Read Committed isolation.

## Eventually consistent reads

Station metadata, train search, availability counts, and event-driven read models may be cached. Responses are point-in-time observations. Create hold rechecks authoritative run status and overlap; a stale positive result can become a conflict but cannot oversell.

## Transaction boundaries

- Create hold: idempotency ownership through reservation, seat mutations, fare snapshots, completion, and outbox.
- Confirm: locked reservation through ticket artifacts, completion, and outbox; masks remain unchanged.
- Cancel/expire: locked reservation through exact releases, terminal state, ticket cancellation when applicable, completion, and outbox.
- Inventory initialization: validate immutable segment count and insert every active seat or none.
- Outbox claim and finalize: separate short transactions around external publication.

## Failure semantics

- Any booking-transaction failure rolls back every changed row.
- Redis failure never changes committed authority. Authentication and
  passenger-profile creation fail closed. A non-hot reservation's existing
  rate limiter may degrade open, but the request still executes every
  authoritative PostgreSQL check. An enabled hot policy is never downgraded:
  waiting-room, token, and hot-reservation admission fail closed with bounded
  retry guidance when Redis or its required continuity state is unavailable.
- A response timeout after commit is resolved through durable idempotency.
- Outbox publish failure retries independently and does not roll back domain state.
- Dead letters and backlog are alertable conditions, not direct readiness failures.

## Isolation and retries

Read Committed is the default. Conditional updates serialize contending inventory rows, reservation locks serialize lifecycle transitions, and consistent ordering reduces deadlocks. Only PostgreSQL `40001` or `40P01` can enter a small bounded retry policy with context-aware jitter. Validation, authorization, state, inventory, and idempotency conflicts return immediately.

## Acceptance invariant

Stored occupancy must equal the active reservation-seat union after every concurrency scenario. This detects mask leaks, double allocation, confirmed-seat release, and partial rollback.
