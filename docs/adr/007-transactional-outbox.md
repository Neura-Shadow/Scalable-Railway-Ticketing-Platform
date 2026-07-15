# ADR 007: Transactional Outbox with Claim, Publish, and Finalize

- Status: Accepted
- Date: 2026-07-15

## Context

Reservation and ticket changes must emit events, but publishing to an external system inside a PostgreSQL transaction extends locks and still cannot make the external side effect atomic. Publishing only after commit without a durable record can lose events on process failure.

## Decision

Every domain mutation inserts an outbox row in the same PostgreSQL transaction. Candidate event types are bounded to:

- `reservation.held`;
- `reservation.confirmed`;
- `reservation.expired`;
- `reservation.cancelled`;
- `ticket.created`; and
- `trainrun.cancelled`.

Rows move through `pending`, `processing`, `published`, and `dead_letter` with attempts, next-attempt time, lock time/owner, creation time, and publication time.

The publisher implements:

1. **Claim**: in a short transaction, select ready/stale rows in deterministic order with `FOR UPDATE SKIP LOCKED`, mark them `processing`, set a bounded lease, increment attempts, and commit.
2. **Publish**: outside any database transaction, send a minimal versioned envelope through the configured adapter.
3. **Finalize**: in a short transaction, conditionally change rows still owned by this worker to `published`, retryable `pending`, or `dead_letter`.

Stale `processing` rows are reclaimable after `outbox_processing_timeout_seconds`. Finalization predicates include event ID, `processing` state, and lock owner so an obsolete worker cannot finalize a reclaimed event.

Publisher adapters are `log` and optional `redis_stream`. Redis Streams is disabled by default. Delivery is at least once. Downstream consumers deduplicate by event ID. No payment, email, SMS, or notification consumer is implemented.

Payloads contain only the minimum fields downstream needs. They are validated against bounded event types, size-limited, and never logged in full. Metrics use normalized event type/result/reason values; unknowns collapse to `unknown`.

## Consequences

- Domain commits cannot lose the intent to publish.
- Duplicate delivery is possible and is part of the consumer contract.
- Publication latency is observable and independent from HTTP latency.
- The database outbox can become a throughput/backlog constraint; it does not change readiness merely because backlog or dead letters exist.

## Rejected alternatives

- Publish inside the booking transaction: rejected because it holds locks across network I/O and remains non-atomic.
- Fire-and-forget after commit: rejected because a crash can lose the event.
- Kafka in Milestone 1: rejected as unnecessary infrastructure that would not replace the authoritative transaction.
