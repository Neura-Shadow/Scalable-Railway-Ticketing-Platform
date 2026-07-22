# ADR 020: Event-Driven Projection Rebuild

- Status: Accepted
- Date: 2026-07-22

## Context

The journey projection is derived from station, route, train, coach, seat,
fare, and train-run source tables. Direct synchronous projection writes inside
every source command would enlarge authoritative transactions and couple their
availability to derived state. Purely periodic rebuild would leave avoidable
lag and provide no event-level operational evidence.

The repository already commits bounded domain events through a transactional
outbox and can publish them at least once to a bounded Redis Stream.

## Decision

Maintain the projection asynchronously from the existing transactional-outbox
transport. Railway Offering mutations append minimal versioned events inside
their authoritative PostgreSQL transactions. Reservation events already follow
that rule. The outbox worker publishes committed envelopes; a separate
`read-model-worker` consumes Redis Stream entries in consumer group
`railway-read-model`.

Events identify affected aggregates but never act as a complete current-state
document. Projection handlers reload authoritative PostgreSQL tables and invoke
the same deterministic train-run rebuild used by admin commands. Station,
route, train, coach, seat, or fare changes that affect many train runs are
expanded through stable bounded pages, not one unbounded transaction.

`RebuildTrainRun` atomically replaces one train run's complete projection.
`RebuildAll` iterates a stable resumable cursor with a bounded batch size.
`DeleteTrainRunProjection` removes a permanently absent run. An explicit admin
command is the intentional repair path; public HTTP handlers never expose
rebuild commands.

The worker provides `RunOnce(ctx)`, bounded batch size and retry attempts,
pending-entry recovery, poison-event dead-lettering, a context-driven loop,
graceful shutdown, and private health/metrics. It is disabled by default and
performs no initial pass while disabled.

## Consequences

- Source commands commit without waiting for Redis or projection work.
- Projection delivery is eventually consistent and at least once; lag is
  observable and does not itself fail API readiness.
- Redis Stream loss can require an explicit bounded rebuild from PostgreSQL.
- Producer event coverage becomes part of Railway Offering mutation tests.
- The read-model worker may scale separately as a process while remaining part
  of the modular monolith.

## Rejected alternatives

- Synchronous dual writes to PostgreSQL and Redis: rejected because there is no
  atomic distributed transaction and cache failure must not block source state.
- Trigger-maintained projection: rejected because cross-table fan-out,
  operational retry, and rebuild progress would become opaque database logic.
- Kafka: rejected because the existing bounded transport meets Milestone 3 and
  another platform does not improve seat-authority correctness.
- Periodic full rebuild only: rejected because it creates unbounded work and
  poorer change-to-projection latency.
