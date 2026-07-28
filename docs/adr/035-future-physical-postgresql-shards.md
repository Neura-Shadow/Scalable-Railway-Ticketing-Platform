# ADR 035: Defer Physical PostgreSQL Shards to an Evidence-Gated Pilot

- Status: Accepted as future direction; not implemented in Milestone 4
- Date: 2026-07-23

## Context

Milestone 4 proves an explicit train-run ownership seam with fixed logical
schemas, monotonic database fences, locator-based routing, and bounded quiesced
migration inside one PostgreSQL database. That topology deliberately preserves
the existing booking transaction across shard-local state and public control
relations.

Moving the schemas to separate databases would remove the same-database locks,
foreign keys, and atomic commit on which the proof relies. Treating the logical
PoC as completed physical sharding would therefore hide unresolved consistency,
security, migration, and recovery work rather than eliminate it.

## Decision

Defer physical PostgreSQL shard extraction. Milestone 4 neither provisions a
second booking database nor claims a distributed transaction, transparent
online rebalancing, independent shard RTO/RPO, active-active writes, or
production-scale physical isolation.

Before a physical pilot, make separate decisions for every dependency that is
currently atomic only because all schemas share one database:

- **Cross-schema foreign keys:** shard-local inventory, reservations,
  reservation seats, orders, tickets, and idempotency currently reference
  public train-run, seat, user, and passenger rows. A pilot must choose bounded
  replicated reference snapshots, application validation, or another explicit
  integrity protocol and reconcile drift.
- **Global idempotency uniqueness:** the public key claim and routed local
  completion currently share one transaction and synchronized expiry. A pilot
  needs a durable uniqueness/expiry/replay protocol that cannot diverge from a
  committed shard result during timeout, retry, migration, or coordinator loss.
- **Global reservation quotas:** the public quota-claim ledger and local
  lifecycle transition currently commit atomically. A pilot must choose a
  durable coordinator, change the quota contract, or prove another protocol;
  eventually consistent counters cannot authorize a hold.
- **Global locators and owner lists:** reservation, ticket-order, and ticket
  locators currently appear and move atomically with local resources. A pilot
  must define creation, cutover, stale lookup, pagination, repair, and recovery
  without exposing a resource before it exists or losing its only route.
- **Central outbox:** booking state and `public.outbox_events` currently commit
  together. Physical extraction needs shard-local transactional intent, bounded
  relay discovery, globally unique event identity, consumer deduplication, and
  migration behavior that does not lose or incorrectly duplicate intent.
- **Catalog and fencing:** row locks across public assignment, local fences,
  migration state, and target-write evidence cannot span independent databases.
  A pilot must define the control-plane authority, monotonic generation issue,
  partition behavior, stale-writer rejection at each data shard, and recovery
  when catalog and shard disagree.
- **Credentials and topology:** each physical shard needs bounded connection
  pools, least-privilege credentials, rotation, health policy, allowlisted
  routing metadata, readiness, secret distribution, and safe addition/removal.
  DSNs and physical topology remain absent from customer responses, logs, and
  metrics.
- **Migration and recovery:** copy, quiescence, validation, cutover, rollback,
  source retention, backup/restore, disaster recovery, and cleanup must remain
  resumable when no transaction can atomically update both databases and the
  catalog. RTO, RPO, abort conditions, and operator ownership require measured
  evidence rather than inherited assumptions.

Do not assume two-phase commit or another generic distributed transaction
coordinator. Any proposed coordination protocol must enumerate its prepared
transaction lifecycle, coordinator-loss behavior, blocking and recovery,
operational ownership, security, and measured failure results. Prefer a design
that preserves one train-run writer and makes each durable transition explicit
over hiding partial failure behind a nominally transparent interface.

The next eligible step is a bounded **Physical PostgreSQL Shard Pilot and
Online Rebalancing** design, not an automatic production rollout. Its initial
scope should use a fixed small topology, synthetic data, one region, selected
train runs, an explicit maintenance/quiescence window, and no source/target dual
writes. It must retain the current segment-mask predicate and ensure that one
physical data shard rejects every stale generation locally even when the
router, catalog client, or another API replica is stale.

The pilot requires evidence for:

- populated logical-to-physical migration and independently verified row,
  mask, lifecycle, idempotency, quota, locator, ticket, and outbox invariants;
- stale replicas and network partitions between API, catalog, source, target,
  and event relay;
- crashes before and after each durable step, including coordinator restart,
  retry, resume, direct-rollback prohibition, and reverse migration;
- shard-local fencing when the catalog is slow, unavailable, or restored from
  an older observation;
- credential rotation, least privilege, connection exhaustion, shard addition,
  shard removal, and safe topology/config rollout;
- backup, point-in-time restore, source retention, reconciliation, orphan
  detection, and measured RTO/RPO;
- bounded locator/idempotency/quota/outbox protocols under concurrent retries;
- worker fairness, relay discovery, admin partial results, health/readiness,
  metrics cardinality, and sanitized failure reporting; and
- load, latency, disk amplification, copy/validation throughput, cutover
  interruption, and recovery measurements without extrapolating national-scale
  or production capacity.

Only after those protocols and tests pass may a later ADR choose a physical
topology and rollout policy. Logical-shard results remain useful input, but are
not substituted for database/process/network failure evidence.

## Consequences

- Milestone 4 remains a truthful, reversible logical-sharding readiness PoC.
- The current modular monolith, PostgreSQL authority, and one-writer-per-train-
  run rule remain intact.
- Physical extraction has an explicit blocker register instead of relying on
  cross-database foreign keys or an accidental global transaction.
- A future pilot can reuse the train-run key, locator interface, monotonic
  generation vocabulary, migration states, reconciliation checks, and bounded
  operator workflow, but must replace same-database implementations where
  required.
- Physical shard availability, failover, online rebalancing, and capacity
  remain unproven until the separate-database evidence exists.

## Rejected alternatives

- Move `booking_shard_0` and `booking_shard_1` to separate databases in
  Milestone 4: rejected because foreign-key, locator, quota, idempotency,
  outbox, cutover, and recovery protocols are unresolved.
- Preserve the current cross-schema transaction interface and call remote
  writes underneath it: rejected because the interface would hide partial
  commits and falsely imply atomicity.
- Add two-phase commit by default: rejected because coordinator loss, prepared
  transaction recovery, blocking, operational ownership, and measured need are
  not established.
- Dual write source and target during migration: rejected because one-sided
  commits can diverge inventory, lifecycle, idempotency, locators, quotas, and
  event intent.
- Active-active writers with later seat-conflict repair: rejected because two
  successful overlapping allocations cannot be repaired safely after sale.
- Use Redis locks, queues, or Cluster slot ownership as the physical write
  fence: rejected because Redis cannot commit with authoritative PostgreSQL
  state and may partition or expire independently.
- Route only by hashing `train_run_id`: rejected because hashing does not
  provide migration state, fenced generations, health-aware placement, source
  retention, or reversible ownership transfer.
- Present schema-level outage tests as physical-shard evidence: rejected because
  the schemas share one database engine and physical failure domain.
