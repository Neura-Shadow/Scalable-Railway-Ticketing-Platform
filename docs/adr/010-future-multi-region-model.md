# ADR 010: Future Multi-Region Reads Preserve a Single Writer per Train-Run Shard

- Status: Accepted as future direction; not implemented in Milestone 1
- Date: 2026-07-15

## Context

Users may eventually need low-latency search and availability reads across regions. Active-active writes to the same seat inventory would require consensus or deterministic ownership transfer to prevent overlapping allocations during partitions.

## Decision

Milestone 1 remains single-region. PostgreSQL in that region is the sole authoritative writer for every train run.

A future design may add:

- multi-region active-active station, schedule, and search reads;
- regional read models and bounded-TTL availability caches;
- event-driven cache invalidation from the transactional outbox;
- regional failover procedures; and
- assignment of each `train_run_id` shard to exactly one authoritative write region at a time.

All reservation create/confirm/cancel/expire commands for a train-run shard route to its current writer. Ownership changes require a fenced epoch/lease and a verified handoff that prevents old and new writers from accepting overlapping epochs. Cached/read-model availability remains a hint.

The invariant remains:

```text
one authoritative PostgreSQL writer per train-run shard
-> reservation transaction
-> durable outbox
-> asynchronous regional read models and caches
```

This ADR does not select a consensus technology, shard directory, failover RTO/RPO, or cross-region transport. Those decisions require measured load, availability objectives, and operational evidence.

## Consequences

- Milestone 1 makes no multi-region active-active write claim.
- Regional read scaling can evolve without moving seat authority into caches or consumers.
- Failover is not safe until fencing and ownership-transfer verification exist.
- Kafka, service meshes, Kubernetes Operators, and global consensus remain future topics and cannot bypass allocation authority.

## Rejected alternatives

- Independent writers per region with eventual conflict resolution: rejected because a sold seat cannot be safely reconciled after two customers receive it.
- Redis-based global locks: rejected because they do not atomically own PostgreSQL reservation state.
- Premature shard implementation: rejected without load or operational evidence.
