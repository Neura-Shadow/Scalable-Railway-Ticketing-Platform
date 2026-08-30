# ADR 062: Fixed Active-Passive Regional Topology

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

The existing three-PostgreSQL topology is one region. A train-run generation
fence rejects stale routes inside one database but cannot stop a disconnected
old regional primary. Milestone 7 needs bounded recovery without active-active
writes or a general discovery system.

## Decision

Use exactly two configured regions: one active and one passive. Each has fixed
control, shard 0, and shard 1 database identities. The active region serves
customer/worker writes. The passive region contains read-only streaming
standbys and recovery-mode processes that reject normal writes.

Every control and shard write transaction validates configured region, role,
durable active region, monotonic epoch, and writes-enabled state. Shard writes
also validate the existing train-run generation fence. Redis remains regional
and non-authoritative. No command falls back to another region or shard.

External ingress, process, credential, database-network, and archive-writer
fencing of the old region is a mandatory promotion precondition. The database
authority row is defense in depth, not consensus.

## Invariants

- At most one region is permitted to accept control or booking writes.
- Passive/recovery readiness never advertises customer-write readiness.
- DR activation requires every configured required database to be promoted,
  current schema, and on the same region/epoch before writes.
- Steady-state loss of one assigned shard degrades only its routes; healthy
  shards continue and no fallback is invented.

## Consequences

- A one-host Compose topology can prove protocol ordering but not geographic
  isolation or production availability.
- Cross-region waiting-room continuity is not guaranteed; Redis rebuilds and
  hot-train admission remains fail closed until ready.
- Regional RPO is bounded by the worst observed database gap.

## Rejected alternatives

- Active-active or multi-primary: rejected because seat and financial conflicts
  cannot be repaired safely after both sides confirm.
- Health-based automatic promotion: rejected because reachability is not
  fencing or authority.
- Unbounded region/shard discovery: rejected to keep pools, fanout, secrets, and
  evidence finite.

