# ADR 026: Regional Cache Replication Remains Future Scope

- Status: Accepted
- Date: 2026-07-22

## Context

Milestone 3 adds shared caches and a PostgreSQL journey projection within one
region. Version rotation and event-driven rebuild may suggest a path toward
regional read scaling, but cross-region propagation, partitions, failover,
clock/ordering, residency, and recovery introduce different consistency and
operational questions. No current evidence establishes safe multi-region
active-active booking writes or globally coherent cache invalidation.

## Decision

Keep all Milestone 3 projection workers, cache namespaces, and Redis data in
the single authoritative region. Multiple API replicas in that region share
the same PostgreSQL and Redis dependencies. No regional cache replication,
cross-region cache write, or global version key is implemented.

A future read-only regional model may maintain independent disposable
projections and caches from a durable authoritative event feed. Each region
would own its local cache generations, bound staleness, expose lag, and fall
back according to an explicit regional source/read-replica policy. Booking
would still route to an authoritative train-run writer and recheck PostgreSQL
inventory.

Future adoption requires separate decisions and evidence for event retention,
bootstrap, ordering, region loss/rejoin, version namespace scope, data
residency, read-your-write expectations, failover, monitoring, and reconciliation.
It must not be inferred from Milestone 3's local multi-replica tests.

## Consequences

- Milestone 3 can prove local shared-cache coherence without claiming global
  consistency.
- The current design keeps derived state disposable and therefore compatible
  with a future independent regional rebuild.
- Cross-region latency, failover, and staleness remain unmeasured limitations.

## Rejected alternatives

- Replicate Redis globally now: rejected because cache conflict, eviction,
  failover, and ordering semantics are not defined or tested.
- Multi-region active-active seat writes: rejected because train-run writer
  ownership and cross-region transaction correctness are not implemented.
- Present local replicas as regional evidence: rejected because they share one
  failure domain and do not exercise WAN partitions or regional recovery.
