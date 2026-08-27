# ADR 064: Regional Write Fencing and Promotion

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

PostgreSQL does not detect failures or fence an old primary. A locally stored
epoch cannot stop a disconnected old database from accepting stale writes.
Promotion and write activation therefore need independent fencing evidence and
transaction-local authority checks.

## Decision

Introduce one regional-authority write module used by every control and shard
mutator. It begins the local transaction, locks and validates region, role,
epoch, active region, and writes-enabled, and for shard writes then locks and
validates the train-run generation fence in fixed order. External I/O is not
permitted inside the write callback.

Failover uses one typed resumable operation for fixed control/shard0/shard1
databases. External attestation binds operation ID, source region/epoch,
incident, operator, timestamp/expiry, nonce, and hashes of ingress, process,
credential, and database-network fencing observations. It is single-use.

The operation records positions/RPO, promotes all required databases, verifies
roles/timelines, installs one newer epoch, resets pools, enters recovery mode,
reconciles, enables workers/webhook ingress, switches ingress, and enables
customer writes last. Each phase is idempotent, re-observes its preconditions,
and advances by compare-and-set.

## Invariants

- No caller supplies an arbitrary host, command, phase, region, or epoch.
- Application roles cannot write fence attestations or DR operation state.
- Multi-host DSN and `target_session_attrs=read-write` are connection selection,
  not authority.
- Pools reset and fresh connections verify identity, primary role, timeline,
  schema, region, and epoch before write readiness.

## Consequences

- Every existing direct write transaction must migrate through the authority
  module; a repository guard rejects new bypasses.
- A failed promotion resumes from durable observations rather than repeating
  arbitrary operator commands.
- Detailed topology and positions remain operator-only.

## Rejected alternatives

- Accept `fenced=true` from a caller: rejected because it is not an observation
  or independently protected attestation.
- Reuse physical-migration engine as a generic workflow: rejected because train-
  run movement and regional database authority have different invariants.
- Automatic DNS or health promotion: rejected because it can create split
  brain.

