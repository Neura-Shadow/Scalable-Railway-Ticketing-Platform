# Release Checklist

This checklist is a release gate, not evidence of zero downtime or measured
capacity. Store approvals and sanitized command output in the release record;
never copy DSNs, credentials, tokens, real emails, row payloads, or certificate
key paths into it.

## Source and artifact boundary

- [ ] The release commit is reviewed, signed off, and identified by immutable
  commit SHA; the container is identified by immutable digest.
- [ ] The working tree and release artifact contain no `.env` files, local test
  databases, logs, coverage, benchmark output, or runtime volumes.
- [ ] Required unit, integration, concurrency, race, static, vulnerability,
  secret, migration, container, and deployment checks pass.
- [ ] Deferred waiting-room, payment, multi-region, and cache work is not
  represented as part of this release.

## Configuration and recovery

- [ ] Runtime and migration identities are separate and least privileged.
- [ ] API and each worker receive only their documented process-specific
  secrets; production publisher configuration is explicitly validated.
- [ ] A backup/recovery point exists and a recent restore exercise is verified.
- [ ] Monitoring, alert routing, incident owner, rollback owner, and abort
  thresholds are recorded before rollout.
- [ ] Migration and runtime commands obtain secrets from the approved secret
  environment; no DSN is passed on the command line or emitted in logs.

## Migration 5 gate

- [ ] The operator followed
  [migration-5-production-rollout.md](migrations/migration-5-production-rollout.md).
- [ ] The starting database is `version=4 dirty=false`.
- [ ] `migration-5-preflight.sql` completed with zero incompatible rows; row
  estimates, table sizes, long transactions, and locks were reviewed.
- [ ] A sanitized production-like rehearsal established the timeout values,
  maintenance-window length, storage/WAL headroom, and replica-lag threshold.
- [ ] Booking and affected administrative writers can be drained for the
  migration. If not, the release is held for a separately reviewed staged path.
- [ ] `lock_timeout`, `statement_timeout`, and abort criteria are approved and
  configured before opening the migration connection.
- [ ] `up` completed once, a second `up` returned no change, and `version`
  returned `version=5 dirty=false`.
- [ ] `migration-5-post-validation.sql` found all expected columns, constraints,
  indexes, and triggers present/valid/enabled and zero incompatible rows.
- [ ] The version-4 writer compatibility trigger remains enabled.
- [ ] The canary used only predesignated synthetic data; booking reconciliation
  and outbox monitoring remained clean.
- [ ] The rollback checks, one-step schema-down boundary, and dirty migration
  recovery decision tree are understood by the on-call operators.

## Application rollout

- [ ] One canary reports liveness and version-aware readiness before traffic.
- [ ] Synthetic smoke checks cover authentication, station/search/availability,
  one reservation lifecycle, outbox progress, and reconciliation without using
  real customer data.
- [ ] Mixed-version write compatibility is observed before rolling the remaining
  process groups.
- [ ] Traffic is restored gradually while database locks, errors, latency,
  connections, WAL/storage, replica lag, worker health, outbox backlog, and
  reconciliation are monitored.

## Closeout

- [ ] All processes are on the intended immutable image, readiness is stable,
  and migration state remains clean through the observation window.
- [ ] No Critical or High review/security finding remains open.
- [ ] Rollback remains available; migration 5's compatibility trigger is not
  removed in this release.
- [ ] The release record contains decisions and bounded evidence, not secrets or
  unsupported availability/throughput claims.
