# ADR 024: Durable Projection Idempotency and Current-State Ordering

- Status: Accepted
- Date: 2026-07-22

## Context

Redis Streams and the transactional outbox deliver events at least once.
Worker failure can redeliver a pending entry, and independent mutations can be
observed out of order. Applying payload fields as patches would allow an older
event to overwrite a newer projection. Redis-only deduplication would be lost
on flush and cannot commit atomically with PostgreSQL projection rows.

## Decision

Create `read_model_event_receipts` with a unique key on
`(consumer_name,event_id)` and bounded event/aggregate metadata. Projection
event handling uses one PostgreSQL transaction:

1. insert the receipt;
2. if the receipt already exists, skip projection rebuild as an idempotent
   success;
3. reload current authoritative source state;
4. rebuild or remove the current projection; and
5. commit the receipt and complete projection change together.

Handlers use the event only to classify work and identify the affected
aggregate. They never treat payload fields as the complete current state.
Consequently, a late older event rebuilds the same newest source view rather
than reverting it.

Cache invalidation is external to PostgreSQL and cannot join that transaction.
After the receipt/projection commit, the worker performs the idempotent logical
version rotation before acknowledging the stream entry. If rotation fails, the
entry remains retryable. A retry with an existing receipt skips rebuild but
retries required invalidation. Repeated rotation is safe; TTL remains the final
staleness bound.

Malformed or unsupported events enter bounded retry and then a dead-letter
stream with safe metadata only. Full payloads are never logged or placed in
metric labels. Event-type labels pass through an allowlist.

## Consequences

- Duplicate events do not repeat effective projection replacement.
- Out-of-order events converge to current PostgreSQL state.
- A crash cannot commit a receipt that claims a projection replacement which
  rolled back.
- A crash after the PostgreSQL commit can repeat cache rotation, favoring safe
  invalidation over hit ratio.
- Receipt retention and table growth require an explicit bounded operational
  policy that does not delete evidence still within the delivery window.

## Rejected alternatives

- Apply event payload patches: rejected because delivery order is not current
  source order and payloads are intentionally minimal.
- Redis-only event deduplication: rejected because cache loss would lose the
  durable projection transaction relationship.
- Global event sequence ordering: rejected because no current producer exposes
  a single authoritative sequence and current-state reload already converges.
