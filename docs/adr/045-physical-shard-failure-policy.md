# ADR 045: Physical Shard Failures Degrade Without Changing Authority

- Status: Accepted for the Milestone 5 bounded pilot
- Date: 2026-07-29

## Context

Milestone 5 introduces three independent PostgreSQL failure domains: control,
booking shard 0, and booking shard 1. Failure isolation is useful only if an
unavailable database cannot cause a stale router, retrying command, worker, or
operator to write another database as an accidental substitute. Cross-database
command finalization, global quota, the reservation directory, shard-local
outboxes, and global read models can also lag independently.

The pilot is single-region and has no automatic physical-shard failover. Health
signals must report availability without becoming ownership authority.

## Decision

The control assignment plus a matching database-local generation fence is
required for every normal booking write. A route cache, Redis value, health
probe, previous success, hash, or operator guess cannot replace either check.
Failure never changes assignment, enables a fence, promotes a migration target,
or selects another shard automatically.

### Request and command behavior

If control PostgreSQL is unavailable, new booking commands fail closed because
they cannot reserve global idempotency/quota coordination or prove current
assignment. Already committed shard results remain durable. The command
reconciler finalizes them when control returns using the immutable command ID,
fingerprint, shard receipt, and reservation result; retry cannot allocate a
second seat.

If the assigned physical shard is unavailable, requests for that shard return
a bounded topology-safe retryable error. A control reservation or quota lease
remains conservative while shard outcome is uncertain. It is released only
after a verified non-commit or bounded lease expiry and reconciliation; Redis
loss cannot authorize release or bypass quota. No active reservation-directory
entry is created for a resource not proven durable.

Healthy-shard requests continue through their own pools and fences. Customer
errors expose no shard ID, connection reference, DSN, host, generation,
migration ID, SQL, or credential. A stale router may refresh once after a local
fence rejection; it does not fan out writes or fall back to legacy storage.

### Workers, relay, and read models

Global workers enumerate only the configured shard allowlist with a total
concurrency cap, per-shard batch and timeout, stable fair rotation, and
independent error accounting. One failed shard cannot consume every worker or
connection slot. Shard-local expiration, reconciliation, command-receipt, and
outbox work uses only that shard's DSN and stops safely on its failure. The
event relay continues draining healthy shards and reports bounded per-shard
lag and partial completion.

The global read model and caches remain non-authoritative. They consume
globally unique event IDs idempotently and may serve explicitly bounded stale
or partial read behavior according to the existing read-model policy. They do
not fabricate availability, hide an unavailable booking owner as healthy, or
authorize a command. When a delayed shard recovers, receipts and event replay
converge the projection without duplicating booking effects.

Admission remains shard-neutral until token use enters the booking command
path. Admission or Redis health cannot select a physical shard, change quota,
or convert an unavailable-shard result into a successful booking.

### Health, readiness, and resource isolation

Liveness reports whether a process loop is responsive. Readiness is role
specific:

- an API requires valid bounded topology, control connectivity and schema, and
  at least one configured booking shard that passes the documented serving
  policy; it reports degraded state when another shard is unavailable;
- a shard-local worker requires only control state needed for ownership plus
  its assigned shard and schema;
- the relay can remain ready but degraded when at least one shard is healthy,
  while surfacing partial status and lag; and
- migration commands require control, source, and target readiness and fail
  closed if any required version, fence, or checkpoint disagrees.

Each database uses a separately bounded pool. Startup validates per-shard open
and idle limits, lifetimes, timeouts, maximum configured shard count, and the
total process pool budget. A connection storm or slow shard therefore cannot
silently exhaust healthy-shard capacity. DSNs come only from application
secrets/configuration through allowlisted connection references and never from
catalog or request input.

### Migration and partition policy

- Target failure before cutover leaves source authoritative and target
  unroutable; copy and replay resume from durable checkpoints.
- Source failure before final validation forbids promotion of the target.
- Failure after source disable preserves a recoverable zero-writer state.
- Failure after target enable but before control switch leaves no valid normal
  target route; recovery follows ADR 043 rather than enabling source blindly.
- Failure after control switch leaves target authoritative; delayed ledger,
  directory, outbox, and cache finalization is repaired idempotently.
- A control/source/target network partition never permits health state or stale
  cache state to override local fencing.

No health-only automatic failover, shard autoscaling, split/merge, automatic
rebalance, or source deletion is part of this decision. PostgreSQL replica
failover, backup restore, and regional disaster recovery require separate
operator evidence.

## Consequences

- One failed booking shard can reject only its assigned work while healthy
  shards, control reads, relay work, and bounded stale read models continue.
- Control loss intentionally stops new booking commands to preserve global
  coordination and single-writer ownership.
- Conservative quota leases may temporarily reduce availability after an
  uncertain shard outcome.
- Worker fairness, per-shard pools, circuit state, retries, and partial results
  become explicit test surfaces.
- Recovery converges durable saga and event state without making Redis, caches,
  or health checks authoritative.
- The topology is a fixed, single-region bounded pilot. Independent-container
  fault injection does not establish production failover, sustained capacity,
  multi-region availability, national-scale throughput, or certified RPO/RTO.

## Rejected alternatives

- Fall back to another shard or legacy storage after an error: rejected because
  it can create a second writer for the same train run.
- Use cached routing while control is unavailable: rejected because freshness
  cannot prove current assignment.
- Release quota immediately on timeout: rejected because the shard may have
  committed before the response was lost.
- Mark all APIs dead when one optional shard fails: rejected because healthy
  assignments can continue safely with explicit degradation.
- Let one worker retry a failed shard indefinitely: rejected because it can
  starve healthy shards and exhaust connection pools.
- Promote a partially copied target after source loss: rejected because health
  does not prove copy completeness or booking authority.
- Claim zero downtime or production capacity from the pilot: rejected because
  final cutover pauses writes and bounded tests do not certify production load.
