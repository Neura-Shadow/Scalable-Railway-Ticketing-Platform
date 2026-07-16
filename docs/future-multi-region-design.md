# Future Multi-Region Design

This document records direction only. Milestone 1 implements none of these capabilities.

## Current truth

- The system is single-region.
- One PostgreSQL primary is authoritative for all train-run seat writes.
- Redis availability values are hints.
- No multi-region active-active seat writes or global strong consistency exist.

## Potential read architecture

Future regions may serve station, schedule, train-search, and availability read models from regional databases/caches. The transactional outbox can drive idempotent cache invalidation and read-model updates. Responses remain observations and booking rechecks authority.

## Authoritative write ownership

Each train-run shard must have exactly one write owner. A global routing directory may map `train_run_id` to an owner region and fenced epoch. All create/confirm/cancel/expire/status commands route to that owner.

Failover requires:

1. stop or fence the old owner;
2. prove no old-epoch writes can commit;
3. catch up durable database/event state;
4. assign a higher ownership epoch; and
5. route new commands to the new owner.

Independent regional allocation with eventual conflict repair is invalid because two confirmed customers cannot be reconciled safely after overselling.

## Evidence needed before implementation

- measured single-region bottlenecks and hot-run contention;
- explicit availability, latency, RTO, and RPO objectives;
- sustained multi-replica benchmarks;
- operational ownership and deployment boundaries;
- fencing and failover test results; and
- reconciliation and chaos tooling.

Kafka, service meshes, Kubernetes Operators, or consensus systems may be evaluated later, but none replaces the authoritative reservation transaction.
