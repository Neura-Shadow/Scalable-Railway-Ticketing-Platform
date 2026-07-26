# Migration 8 Production Rollout

## Purpose and status

Migration 8 introduces the fixed Milestone 4 logical-shard topology, catalog,
assignments, fences, migration control state, global locators/claims, central
outbox provenance, and explicit schema-local booking tables. It bootstraps
existing train runs to explicit generation-1 `legacy` assignments. It does not
move any train run to `shard-0` or `shard-1` automatically.

This runbook describes a production-like evidence gate for a single-region,
same-cluster schema-sharding proof of concept. It is not approval for physical
database sharding, zero-downtime migration, or an unattended production
rollout. `cmd/shard-admin` and focused tests are present in the work-in-progress
branch; final controlled runtime, CI, independent review, and release
acceptance remain pending.

## Preconditions

- Record the exact commit and immutable image/migration artifact.
- Verify Migration 7 is current and clean; stop on a dirty database.
- Use a migration principal separate from runtime roles. Runtime roles must not
  own the database or create arbitrary schemas.
- Take and verify a restorable backup/PITR point. Record recovery ownership and
  deadline outside the repository.
- Measure free disk, WAL/replication headroom, connection use, lock waits,
  vacuum state, and relevant table/index sizes on a production-like restore.
- Verify no incompatible old writer will remain when `schema_poc` is enabled.
  Every serving writer must implement generation fencing and meet the catalog
  minimum protocol version.
- Keep `BOOKING_SHARD_MODE=legacy` during schema expansion and bootstrap.
- Confirm monitoring surfaces and operator CLI access are private, bounded,
  audited, and free of customer traffic.

## Apply and bootstrap

Run the versioned migration artifact, then repeat `up` and inspect status:

```powershell
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations version
```

Migration 8 takes a transaction-scoped PostgreSQL advisory lock to serialize
creation of the fixed process-wide schemas. Run one reviewed migrator, bound
the database operation externally, and investigate lock contention rather than
starting competing migration processes.

Do not run AutoMigrate. Do not enable schema mode merely because the migration
command exited successfully.

Validate, with read-only queries or tested administration commands:

- schemas `booking_shard_0` and `booking_shard_1` and every required local
  table/index/trigger exist;
- catalog contains only `legacy`, `shard-0`, and `shard-1` with expected kinds;
- every existing train run has one positive generation-1 `legacy` assignment
  and one matching enabled legacy fence;
- schema shard fences are disabled until an explicit plan;
- populated legacy reservations, orders, tickets, quota claims, idempotency
  references, and locators have complete relationships;
- legacy reservations/tickets have bootstrapped global locators and quota
  claims, legacy idempotency has the required global key-claim relationship,
  and central outbox rows have bounded provenance;
- only an unresolved legacy `in_progress` idempotency record may temporarily
  use the documented nullable train-run compatibility case; completing it must
  populate `train_run_id` atomically with its durable result;
- retained-public DML guards and monotonic generation/state constraints are
  installed; and
- migration version is clean and API legacy-path readiness passes.

Run the populated version-7 fixture rehearsal and one-step down/up only in an
isolated disposable database. A production down migration is not the default
rollback strategy.

## Legacy compatibility rollout

1. Deploy generation-aware binaries in `legacy` mode.
2. Prove create/get/confirm/cancel/expire, tickets, idempotency, quotas, outbox,
   admission, projection/cache, and reconciliation remain correct.
3. Prove incompatible writer versions are drained and cannot pass schema-mode
   readiness.
4. Observe lock waits, database errors, outbox backlog, and guard rejections.
5. Only after all gates pass, explicitly configure the fixed topology and opt
   into `schema_poc`. Production requires
   `BOOKING_SHARD_SCHEMA_POC_PRODUCTION_ENABLED=true`.

Production examples must remain legacy by default. Do not log the full config
or any database URL.

## Per-train-run migration

The commands below are implemented in the work-in-progress branch but remain
pending final controlled runtime, CI, independent review, and release
acceptance. They require operator-controlled execution, bounded output,
cancellation, non-zero failure exits, dry-run where meaningful, and `--confirm`
for destructive operations.

### Inspect and plan

```text
shard-admin list-shards
shard-admin list-assignments
shard-admin inspect-health
shard-admin inspect-train-run --train-run-id <train-run-id>
shard-admin plan-migration --train-run-id <train-run-id> --target-shard shard-1 --dry-run
shard-admin plan-migration --train-run-id <train-run-id> --target-shard shard-1
```

Verify source assignment/fence, target health, strictly newer reserved
generation, no active migration, bounded locator count, disk amplification,
rollback window, and clean preflight reconciliation.

### Drain, quiesce, copy, and resume

```text
shard-admin start-migration --migration-id <migration-id> --confirm
shard-admin resume-migration --migration-id <migration-id>
```

Drain rejects new creates retryably. Existing lifecycle work may finish only
within the documented bound. Quiescence disables the source under assignment/
fence locks and may reject all train-run writes. It uses database lock/
statement timeouts, not sleeps.

Copy uses configured deterministic batches and a durable cursor. A failed
batch leaves source authority unchanged and target partial state unroutable.
Resume is idempotent; it does not restart from scratch or enable target.

Retained-public guards default deny after reassignment. The reviewed migration
adapter authorizes its narrowly scoped copy transaction with the internal
transaction-local `railway.booking_migration_id` setting. Operators must not
set that value manually or use it as a general guard bypass; the database still
revalidates the matching active migration.

### Validate and cut over

```text
shard-admin validate-migration --migration-id <migration-id> --dry-run
shard-admin validate-migration --migration-id <migration-id>
shard-admin cutover --migration-id <migration-id> --dry-run
shard-admin cutover --migration-id <migration-id> --confirm
```

Validation must be complete and untruncated across rows, identities, masks,
lifecycle, quotas, local idempotency/expiry, tickets, central outbox intent,
locators, and reconciliation. Cutover must enforce locator indexes, row cap,
and statement timeout, and atomically switch assignment, fences, locators,
availability generation, target-write evidence, and migration state.

Record the measured zero-writer/retryable interval honestly. There is no zero-
downtime claim.

### Post-cutover gate

```text
shard-admin inspect-train-run --train-run-id <train-run-id>
shard-admin reconcile --train-run-id <train-run-id>
shard-admin inspect-health
```

Require exactly one writable target, rejected stale/source mutations, correct
target lifecycle reads/writes, rotated availability namespace without Redis
scan, central outbox/read-model progress, and complete reconciliation. Retain
the source read-only until the rollback deadline.

## Rollback

Before cutover, rollback keeps the source assignment and re-enables its fence;
partial target data remains unroutable. After cutover, direct rollback is
allowed only when target-generation successful-write evidence is still zero
under the same assignment/fence/evidence locks.

```text
shard-admin rollback --migration-id <migration-id> --dry-run
shard-admin rollback --migration-id <migration-id> --confirm
```

Direct rollback uses a newer generation and atomically changes fences,
assignment, and bounded locators. If any target mutation succeeded, this
command must fail. Plan a reverse migration from the current target; never flip
the mapping or discard target writes.

## Source retention and cleanup

Source retention increases table/index/WAL/backup size. Monitor it throughout
the rollback window. Completion does not delete source data.

```text
shard-admin cleanup-source --migration-id <migration-id> --dry-run
shard-admin cleanup-source --migration-id <migration-id> --confirm
```

Cleanup requires a completed migration, expired rollback window, current target
assignment/fence, no conflicting migration, clean reconciliation, approved
backup/retention decision, and explicit confirmation. Interrupted cleanup must
not remove current authority. Never schedule automatic cleanup at cutover.
The reviewed cleanup adapter uses the internal transaction-local
`railway.booking_cleanup_migration_id` setting, which is valid only after the
database rechecks those conditions. It is not an operator escape hatch.

## Abort conditions

Abort or fail safely on:

- dirty/unexpected migration version or incomplete bootstrap;
- an incompatible serving writer or failed minimum-protocol gate;
- insufficient backup, restore evidence, disk, WAL, or connection headroom;
- catalog/fence/locator constraint or reconciliation mismatch;
- inability to quiesce within the approved bound;
- source unavailability before complete validation;
- target unavailability, partial copy, validation truncation/mismatch, missing
  locator index, row-cap breach, or cutover timeout;
- any observation of two write-enabled fences;
- topology/credential/PII leakage; or
- target-write evidence when direct rollback was intended.

An abort does not promote target, delete source, clear Redis as a repair, or
down-migrate automatically.

## Down-migration restriction

Migration 8 down is destructive to logical-shard/control objects and must be
rehearsed only on a restorable copy. It refuses to run unless every assignment
is stable on `legacy`, every migration is terminal, and both logical schemas
contain no booking or local idempotency data. When safe, it preserves public
version-7 booking, idempotency, and outbox rows while removing the Milestone 8
extension columns and topology. Application rollback should normally keep the
expanded schema and return to a compatible legacy-mode image.

## Required evidence record

Record sanitized commit/image/migration identity, database version, fixture or
production-like dataset bounds, backup/restore proof, all pre/post checks,
copy/validation counts and durations, lock/retryable interval, disk
amplification, reconciliation, and operator outcome outside Git. Never record
credentials, DSNs, raw booking rows, passenger PII, idempotency/admission
material, migration dumps, or machine-local paths.

See [shard migration](../shard-migration.md),
[cutover and rollback](../shard-cutover-and-rollback.md), and
[Milestone 4 limitations](../milestone-4-limitations.md).
