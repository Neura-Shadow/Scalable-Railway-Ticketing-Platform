# Future Multi-Region Design

This document records direction only. Milestone 6 implements none of these
multi-region capabilities. Its control database, two physical booking
databases, payment workers, sandbox provider, and webhook ingress are one
bounded single-region topology, not regional ownership or disaster recovery.

## Current truth

- The system is single-region.
- A control PostgreSQL and up to two fixed physical booking PostgreSQL instances
  may be configured. Exactly one database-local fence is writable per train
  run; legacy/logical storage remains compatible.
- One positive monotonic assignment generation and a local PostgreSQL fence
  reject stale logical routes. This is same-database serialization, not
  distributed consensus or a cross-region epoch service.
- Redis availability values are hints.
- Waiting-room ordering, rate limits, inflight admission limits, and continuity
  are coordinated by one regional Redis authority. They are not globally fair
  or continuous across independent regional Redis deployments.
- No multi-region active-active seat writes or global strong consistency exist.
- Payment intent/saga authority, provider operations, webhook deduplication,
  reconciliation checkpoints, and manual review are regional control-plane
  state. There is no cross-region webhook ingress, payment failover, provider
  routing, settlement replication, or demonstrated payment RPO/RTO.

Milestone 6 may run multiple API, admission-worker, read-model-worker, and
payment-worker replicas inside that one region. Redis Lua scripts make policy-generation
operations atomic, while the control database remains authoritative for route
intent and global leases and the selected booking PostgreSQL remains seat
authority. The command protocol is a saga, not a distributed transaction. This
topology must not be described as regional failover or active-active writes.

## Potential read architecture

Today, API replicas in one region share one Redis cache and one PostgreSQL
projection. There is no regional cache replication. Future regions may serve
station, schedule, train-search, and availability read models from regional
databases/caches. The transactional outbox could drive idempotent regional
updates, but responses would remain observations and booking would recheck the
single fenced authority.

## Authoritative write ownership

Each train-run shard must have exactly one write owner. A global routing directory may map `train_run_id` to an owner region and fenced epoch. All create/confirm/cancel/expire/status commands route to that owner.

Any future regional admission and payment design must route a train run's waiting-room
joins and token lifecycle to the same fenced owner. A token issued under an old
owner epoch cannot be accepted by a new owner merely because its HMAC and TTL
remain valid. Cross-region FIFO, Redis continuity, token handoff, and draining
outstanding leases require a separately reviewed protocol; Milestone 6 supplies
none of them.

Payment failover additionally requires fencing old control/worker authority,
provider idempotency continuity, webhook key and event-ID continuity, status
reconciliation before any financial replay, preserved manual-review evidence,
and a proven current booking-shard route before ticket/refund compensation.
Failing over API reads alone must never authorize capture, ticket issuance, or
seat release.

Failover requires:

1. stop or fence the old owner;
2. prove no old-epoch writes can commit;
3. catch up durable database/event state;
4. assign a higher ownership epoch; and
5. route new commands to the new owner.

Independent regional allocation with eventual conflict repair is invalid because two confirmed customers cannot be reconciled safely after overselling.

## Physical-shard extraction seam

Milestone 5 implements a bounded **Physical PostgreSQL Shard Pilot and Online
Rebalancing**, still in one region and on synthetic/selected train runs. It
replaces Milestone 4's same-database atomicity with explicit protocols for:

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

The pilot does not add source/target dual writes. It uses online base copy and
journal catch-up, then a bounded final pause; target-era writes require reverse
migration. These mechanics are not evidence for multi-region consensus,
zero-downtime operation, or production capacity.

## Evidence needed before implementation

- accepted Milestone 6 routing, payment, fencing, migration, failure, and reconciliation
  evidence plus measured single-region bottlenecks and hot-run contention;
- explicit availability, latency, RTO, and RPO objectives;
- sustained multi-replica benchmarks;
- operational ownership and deployment boundaries;
- fencing and failover test results; and
- reconciliation and chaos tooling.

Kafka, service meshes, Kubernetes Operators, or consensus systems may be
evaluated later, but none replaces the authoritative reservation transaction
or supplies a physical-shard consistency protocol by itself.
