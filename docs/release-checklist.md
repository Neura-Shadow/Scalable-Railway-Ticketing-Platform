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
- [ ] Payment and multi-region active-active writes are not represented as part
  of this release. The Milestone 2 waiting room/admission controls and the
  Milestone 3 PostgreSQL read model/Redis read caches are included only with
  the evidence gates below; Redis never becomes booking authority.

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

## Migration 6 and hot-admission gate

- [ ] The operator followed
  [migration-6-production-rollout.md](migrations/migration-6-production-rollout.md)
  and recorded its preflight, lock/statement timeout, backup, capacity, and
  abort decisions.
- [ ] The starting database is `version=5 dirty=false`, the populated
  Milestone 1.1 rehearsal passes, and incompatible or duplicate policy rows are
  absent.
- [ ] `up` completed once, a second `up` returned no change, and `version`
  returned `version=6 dirty=false`.
- [ ] Version-6 schema and populated-data assertions verify the policy
  uniqueness/bounds, quota indexes, outbox event types, and preservation of
  existing reservation, inventory, idempotency, ticket, and outbox data.
- [ ] The API and admission-worker image is compatible with both the planned
  rollout order and the enabled/disabled policy state. No enabled hot policy is
  exposed before its Redis generation and continuity marker are initialized.
- [ ] A one-step version-6 down rehearsal, if performed in disposable
  infrastructure, proves the documented version-5 shape and destructive
  cleanup behavior before reapplying version 6.
- [ ] Version-6 down is treated as destructive: it removes durable hot-policy
  rows and policy audit/outbox events that version 5 cannot represent. It is
  never an automatic production rollback action; preserve evidence and require
  an explicit incident decision before considering it.

## Application rollout

### Migration 7 and read-model/cache gate

- [ ] The operator followed
  [migration-7-production-rollout.md](migrations/migration-7-production-rollout.md),
  starting from `version=6 dirty=false`, and recorded preflight, capacity,
  timeout, lock, backup, and abort decisions.
- [ ] Fresh up, repeated up, populated version-6 to version-7, and disposable
  down/up rehearsals pass. Version 7 constraints reject unknown and mismatched
  aggregate/event pairs while preserving every version-6 booking outbox event.
- [ ] `rebuild-all` dry-run/apply pages use the exact returned cursor, expose
  source fallback until the final readiness checkpoint, and complete without a
  partial visible projection. `reconcile --limit 100` is clean before enablement.
- [ ] The read-model worker first starts disabled; PostgreSQL, Redis, migration,
  private health/metrics, and process-owned configuration pass before one
  replica is enabled and then scaled to two distinct consumer identities.
- [ ] Bounded tests prove duplicate/out-of-order convergence, pending-entry
  takeover, Redis stream/PEL loss recovery through `replay-outbox`, poison-event
  recovery through `resume-event`, and clean reconciliation after recovery.
- [ ] Station/search/availability cache loss produces safe source fallback;
  restoration creates fresh generations without key enumeration. Availability
  remains a hint and a stale/poisoned value cannot authorize a booking.
- [ ] Stream length, PEL size, pending age, projection lag, Redis memory, cache
  failures/fallbacks, invalidations, and reconciliation mismatches have alert
  ownership. Any source-stream retention is group-floor-aware and never blind
  `MAXLEN` trimming.

- [ ] One canary reports liveness and version-aware readiness before traffic.
- [ ] Synthetic smoke checks cover authentication, station/search/availability,
  one reservation lifecycle, outbox progress, and reconciliation without using
  real customer data.
- [ ] Mixed-version write compatibility is observed before rolling the remaining
  process groups.
- [ ] Admission workers are first deployed with
  `ADMISSION_WORKER_ENABLED=false`; PostgreSQL, Redis, migration, keyring, and
  worker readiness pass before one reviewed regional worker is enabled.
- [ ] The API accept-key set contains every worker issue key before that worker
  can issue tokens. Old accept keys remain through the maximum token and
  processing-lease window plus the approved safety margin.
- [ ] Bounded multi-replica evidence proves shared duplicate joins, global
  admission-rate/inflight limits, API and worker termination recovery, and one
  durable result per admitted identity.
- [ ] A real Redis-outage smoke proves enabled-hot join/admission fails closed
  with bounded retry guidance, a non-hot reservation still executes its
  PostgreSQL-authoritative path, Redis restoration returns API/worker readiness,
  and the expected hot generation retains continuity.
- [ ] Traffic is restored gradually while database locks, errors, latency,
  connections, WAL/storage, replica lag, worker health, outbox backlog, and
  reconciliation are monitored.
- [ ] Detect-only `seat-inventory` reconciliation passes for every synthetic
  hot and non-hot canary train run; `reservation-quotas` and `admission-state`
  also pass after dependency restoration. No repair command is run implicitly.

## Closeout

- [ ] All processes are on the intended immutable image, readiness is stable,
  and migration state remains clean through the observation window.
- [ ] No Critical or High review/security finding remains open.
- [ ] Application rollback remains available; migration 5's compatibility
  trigger is not removed, Migration 6 down remains a separately approved
  destructive incident action, and the default Milestone 3 rollback leaves
  additive migration 7 in place with workers disabled and source reads enabled.
- [ ] The release record contains decisions and bounded evidence, not secrets or
  unsupported availability/throughput claims.
