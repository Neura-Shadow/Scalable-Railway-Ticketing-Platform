# ADR 018: Future Train-Run Shards Retain One Fenced Writer

- Status: Accepted as future direction; not implemented in Milestone 2
- Date: 2026-07-18

## Context

Milestone 2 adds multiple API and admission-worker replicas inside one region, but PostgreSQL remains the single authoritative writer for every train run. Redis hash tags group waiting-room state by train run and seat class for atomic admission; they do not assign inventory ownership.

Future scale or regional-read requirements may motivate train-run sharding. Shard ownership must preserve the no-overlap inventory invariant during routing changes and failover. Premature database sharding, cross-region writes, or network-service extraction would add interfaces without measured evidence that they improve the current modular monolith.

This ADR specializes the single-writer direction in ADR 010 for hot-train admission and future train-run shard ownership. It does not supersede ADR 010.

## Decision

Milestone 2 remains a single-region modular monolith:

- one PostgreSQL authority owns train-run status, durable quotas, reservations, and segment seat allocation;
- Redis owns only ephemeral waiting-room and admission control-plane state;
- API, admission-worker, hold-expirer, and outbox-worker executables are deployment roles over shared modules, not independently owned microservices; and
- all reservation writes use the existing Booking module and PostgreSQL transaction seam.

No train-run shard directory, shard migration, ownership lease, regional Redis replication, or cross-region reservation routing is implemented in this milestone.

A future sharded design must assign each `train_run_id` to exactly one authoritative write owner at a time. Every ownership assignment carries a monotonically increasing fenced epoch. Reservation commands and any admission authorization used during a handoff carry the epoch, and the authoritative PostgreSQL write path rejects a command from an old or unrecognized epoch before it can mutate inventory.

An ownership handoff must, in order:

1. stop new admissions and writes for the old epoch;
2. account for or expire outstanding admission and processing leases;
3. verify that the old writer can no longer commit;
4. transfer authoritative train-run state;
5. publish a higher fenced epoch; and
6. enable admissions and writes only on the new owner.

The exact directory, lease protocol, consensus mechanism, migration format, RTO/RPO, and token behavior across a handoff require a later ADR backed by measured load and failure testing. Redis locks or Redis Cluster slot ownership alone cannot provide the PostgreSQL write fence.

Future regions may host read-only station, schedule, search, or availability models, but cached availability remains a hint. Active-active writes to one train-run shard remain prohibited. An admission granted in one location never guarantees inventory and cannot override the shard's authoritative PostgreSQL predicate.

The current module seams are retained until operational evidence justifies another deployment topology. If extraction is later considered, the interface must preserve policy ownership, admission identity, PostgreSQL transaction authority, fenced epochs, idempotency, and outbox ordering. A process boundary is not itself a correctness mechanism.

## Consequences

- Milestone 2 gains horizontal API and worker concurrency without claiming database sharding or multi-region writes.
- Redis key locality prepares policy-scoped atomic operations but does not predetermine future shard placement.
- A future handoff cannot be considered safe without a PostgreSQL-enforced fenced epoch and verified quiescence.
- The modular monolith keeps booking invariants local behind one deep transaction interface.
- Kafka, service meshes, Kubernetes Operators, global consensus, and regional Redis replication remain outside Milestone 2.

## Rejected alternatives

- Active-active writers with eventual seat-conflict reconciliation: rejected because two successful allocations cannot be repaired after sale.
- Redis locks or Cluster ownership as the write fence: rejected because they are not atomic with PostgreSQL inventory mutations.
- Implement train-run sharding in Milestone 2: rejected because this milestone has neither the scope nor the load and recovery evidence.
- Extract admission or booking into microservices now: rejected because network interfaces would increase partial-failure modes without changing the single Redis admission authority or single PostgreSQL inventory authority.
- Route by replica-local ownership state: rejected because stale replicas could accept overlapping ownership epochs.
