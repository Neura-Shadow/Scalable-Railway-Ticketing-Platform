# Future Multi-Region Design

This document records direction only. Milestone 4 implements none of these
multi-region capabilities. Its logical schemas are an extraction seam, not
physical shards or regional ownership.

## Current truth

- The system is single-region.
- One PostgreSQL primary is authoritative for all train-run seat writes.
- `legacy`, `shard-0`, and `shard-1` are fixed schemas in that one database.
  They share locks, transactions, foreign keys, disk, connections, and the
  physical failure domain.
- One positive monotonic assignment generation and a local PostgreSQL fence
  reject stale logical routes. This is same-database serialization, not
  distributed consensus or a cross-region epoch service.
- Redis availability values are hints.
- Waiting-room ordering, rate limits, inflight admission limits, and continuity
  are coordinated by one regional Redis authority. They are not globally fair
  or continuous across independent regional Redis deployments.
- No multi-region active-active seat writes or global strong consistency exist.

Milestone 4 may run multiple API, admission-worker, and read-model-worker
replicas inside that one region. Redis Lua scripts make policy-generation
operations atomic, while PostgreSQL remains authoritative for routing, fencing,
quotas, idempotency, reservations, and seat inventory. This replica topology
must not be described as regional or physical-shard failover.

## Potential read architecture

Today, API replicas in one region share one Redis cache and one PostgreSQL
projection. There is no regional cache replication. Future regions may serve
station, schedule, train-search, and availability read models from regional
databases/caches. The transactional outbox could drive idempotent regional
updates, but responses would remain observations and booking would recheck the
single fenced authority.

## Authoritative write ownership

Each train-run shard must have exactly one write owner. A global routing directory may map `train_run_id` to an owner region and fenced epoch. All create/confirm/cancel/expire/status commands route to that owner.

Any future regional admission design must route a train run's waiting-room
joins and token lifecycle to the same fenced owner. A token issued under an old
owner epoch cannot be accepted by a new owner merely because its HMAC and TTL
remain valid. Cross-region FIFO, Redis continuity, token handoff, and draining
outstanding leases require a separately reviewed protocol; Milestone 4 supplies
none of them.

Failover requires:

1. stop or fence the old owner;
2. prove no old-epoch writes can commit;
3. catch up durable database/event state;
4. assign a higher ownership epoch; and
5. route new commands to the new owner.

Independent regional allocation with eventual conflict repair is invalid because two confirmed customers cannot be reconciled safely after overselling.

## Physical-shard extraction seam

The next eligible design is a bounded **Physical PostgreSQL Shard Pilot and
Online Rebalancing**, still in one region and on synthetic/selected train runs.
It cannot reuse Milestone 4's same-database atomicity without replacement
protocols for:

- global idempotency-key uniqueness and synchronized expiry;
- active-hold quota claims;
- resource locator creation, owner pagination, cutover, and repair;
- shard-local transactional outbox intent and relay discovery;
- cross-schema foreign keys to identity/offering data;
- monotonic generation issue and stale-writer rejection across catalog/data
  database partitions;
- copy, validation, cutover, rollback, source retention, cleanup, and recovery
  without one transaction; and
- per-shard credentials, pools, topology rollout, backup/PITR, RTO, and RPO.

The pilot must not add source/target dual writes or infer online rebalancing from
the bounded quiesced logical migration. Any coordinator proposal must state its
partial-failure, blocking, restart, and repair behavior explicitly.

## Evidence needed before implementation

- accepted Milestone 4 routing, fencing, migration, failure, and reconciliation
  evidence plus measured single-region bottlenecks and hot-run contention;
- explicit availability, latency, RTO, and RPO objectives;
- sustained multi-replica benchmarks;
- operational ownership and deployment boundaries;
- fencing and failover test results; and
- reconciliation and chaos tooling.

Kafka, service meshes, Kubernetes Operators, or consensus systems may be
evaluated later, but none replaces the authoritative reservation transaction
or supplies a physical-shard consistency protocol by itself.
