# Migration 5 Production Rollout

This runbook covers `000005_inventory_and_route_integrity`. It is deliberately
conservative: the checked-in migration performs its work in one transaction and
has not been proven zero downtime on a production-sized database. For large
tables, the default is an approved maintenance window with booking writers
drained. If that interruption is unacceptable, stop and build a separately
reviewed staged migration path; do not improvise against production.

## Change summary and affected objects

The expected starting point is schema version 4 with `dirty = false`. Migration
5 changes these tables and objects:

| Table | Migration 5 behavior | Operational consequence |
|---|---|---|
| `seat_inventory` | Scans existing rows against `train_runs`, `seats`, and `coaches`; replaces `seat_inventory_validate_class` so future writes also enforce run/train ownership | Full integrity scan plus trigger DDL |
| `reservations` | Adds `reservations_id_train_run_segment_count_key` on `(id, train_run_id, segment_count)` | Builds a unique index/constraint while the migration transaction is open |
| `reservation_seats` | Adds nullable `train_run_id`, installs the version-4 compatibility trigger, backfills all rows, forces deferred checks, sets the column `NOT NULL`, replaces the legacy reservation foreign key, and adds an inventory foreign key | Full-table write, WAL/dead-tuple growth, and blocking DDL |
| `route_stops` | Scans for zero-based contiguous indexes and non-decreasing offsets; updates the sequence validator; adds the deferred minimum-stop trigger | Full integrity scans plus trigger DDL |
| `routes` | Scans for routes with fewer than two stops; adds the deferred minimum-stop trigger | Full scan plus trigger DDL |

The affected constraints are:

- new unique constraint `reservations_id_train_run_segment_count_key`;
- removed foreign key `reservation_seats_reservation_id_segment_count_fkey`;
- new foreign key `reservation_seats_reservation_run_segment_fkey`;
- new foreign key `reservation_seats_inventory_fkey`; and
- deferred constraint triggers `routes_validate_minimum_stops` and
  `route_stops_validate_minimum`.

The existing deferred `reservation_seats_validate` checks queued by the
backfill are forced immediate before the table constraints change. The existing
deferred `route_stops_validate_sequence` trigger remains in place, but its
function is strengthened to validate both the old and new route when a stop
moves between routes.

The `reservation_seats_populate_train_run_id` trigger is a rolling-deployment
compatibility boundary, not cleanup debt. A version-4 writer can omit the new
column and the trigger derives it from the authoritative reservation. Keep that
trigger until every version-4 process has drained and rollback to version 4 is
no longer allowed.

## Preflight and backup gate

Complete every item before starting:

1. Record the release commit and immutable image digest. Confirm the target is
   clean schema version 4, not merely that the application reports healthy.
2. Take a production backup or storage snapshot and verify a recent restore
   exercise. Record the recovery point and the person authorized to restore.
3. Rehearse version 4 to 5 on a recent sanitized restore using production-like
   PostgreSQL settings and hardware. Record wall-clock duration, peak WAL,
   temporary/disk growth, replica lag, and lock-wait behavior.
4. Refresh planner statistics before the window through the normal database
   maintenance process. Do not run an unplanned `VACUUM FULL` or other blocking
   maintenance as part of this rollout.
5. Run `sql/migration-5-preflight.sql`. Review its estimated row counts and table
   sizes for `reservations`, `reservation_seats`, `seat_inventory`, `routes`, and
   `route_stops`. These estimates can be stale; use the rehearsal as the sizing
   authority.
6. Require zero incompatible rows for the projected `train_run_id` backfill,
   reservation/inventory relationship, inventory train/class relationship,
   route sequence, route offset, and minimum-stop checks. A NULL projection or
   missing referenced inventory row blocks migration.
7. Review active transactions and locks from the preflight output. Drain long
   transactions and conflicting DDL. Do not terminate sessions without the
   approved incident/change procedure.
8. Confirm free database, WAL/archive, replica, and backup storage against the
   measured rehearsal peak plus the platform's safety margin.
9. Predeclare abort thresholds for lock wait, statement duration, write-error
   rate, replica lag, WAL/disk consumption, and reconciliation failures. The
   operator must be able to stop traffic and the migration independently.

The preflight script is read-only. A timeout or permission failure is a blocker,
not permission to skip the corresponding check.

## Lock and transaction behavior

Migration 5 includes `BEGIN` and `COMMIT`; locks acquired by its DDL are retained
until the final commit. PostgreSQL normally takes `ACCESS EXCLUSIVE` for
`ALTER TABLE` forms unless a weaker mode is documented for the individual
subcommand. In this file:

- adding the reservation uniqueness constraint builds its backing unique index
  and can block reservation writes;
- adding a column takes a brief table DDL lock when acquired, but the later full
  `UPDATE reservation_seats` takes row locks, emits WAL, creates dead tuples, and
  holds a `ROW EXCLUSIVE` table lock until commit;
- `ALTER COLUMN ... SET NOT NULL` can scan the table while holding the stronger
  `reservation_seats` DDL lock because this migration does not first validate a
  proving check constraint;
- dropping/adding foreign keys and creating triggers acquire DDL locks and may
  wait behind concurrent writers or DDL on both referencing and referenced
  tables; and
- inventory and route validation blocks perform full reads and may add I/O
  pressure even before blocking DDL is reached.

Do not assume that reads or writes remain continuously available. PostgreSQL
minor versions and the exact subcommand can affect lock details, so confirm the
target version's lock behavior during rehearsal and observe `pg_locks` during
the change.

Configure both timeouts before the migration connection is opened. Keep
`lock_timeout` short enough to fail rather than queue behind an unexpected
blocker, while `statement_timeout` must exceed the rehearsed migration duration
and remain inside the approved window. One example, to be replaced by values
approved from rehearsal, is:

```powershell
$env:PGOPTIONS = '-c lock_timeout=5s -c statement_timeout=30min -c idle_in_transaction_session_timeout=35min'
go run ./cmd/migrate -path migrations up
```

`DATABASE_URL` is supplied through the secret-managed environment, never as a
command argument. A timeout leaves golang-migrate metadata potentially dirty;
follow the dirty recovery procedure rather than blindly retrying.

## Maintenance-window decision

Use the checked-in migration unchanged only when all of the following are true:

- the sanitized-restore rehearsal completes inside the approved window;
- all preflight incompatibility counts are zero;
- booking writers can be drained for the measured lock/backfill interval;
- backup restore readiness and storage headroom are confirmed; and
- rollback and dirty-state operators are present.

For a large `reservation_seats` table, drain API booking writes, hold-expirer,
and administrative route/inventory writes before applying migration 5. The
outbox worker does not write these affected tables and therefore is not a lock
prerequisite, although the release procedure may drain it separately. Keep only
explicitly approved read traffic. This is a maintenance-window procedure and is
not a zero-downtime claim.

If rehearsal does not meet these gates, do not run migration 5. A separately
reviewed forward migration design can stage the same end state:

1. Add the nullable column and compatibility trigger in a short expand step.
2. Backfill bounded primary-key batches (`reservation_seats.id`), committing each
   batch, recording progress outside the table, and throttling on lock time,
   replica lag, WAL, and application latency. Each batch must join the
   authoritative `reservations.train_run_id`; it must be idempotent and leave
   already populated rows unchanged.
3. Add a proving `CHECK (train_run_id IS NOT NULL) NOT VALID`, then run
   `VALIDATE CONSTRAINT` separately. After validation, use the target PostgreSQL
   version's supported fast path for `SET NOT NULL`, and later remove the
   temporary check in a reviewed migration.
4. Build the reservation unique index with `CREATE UNIQUE INDEX CONCURRENTLY`,
   then attach it with `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE USING INDEX`.
5. Add foreign keys as `NOT VALID`, validate them separately, and add any
   referencing-side indexes justified by measured parent update/delete plans.
6. Install the final triggers and perform route validation in bounded, reviewed
   steps before enabling the corresponding constraint triggers.

`NOT VALID` applies to check and foreign-key constraints, not to a unique
constraint or `NOT NULL`. `CREATE INDEX CONCURRENTLY` cannot run inside a
transaction block; with golang-migrate it must live in a dedicated
non-transactional migration file with explicit failure/invalid-index recovery.
Never mix it into the current transaction or run the staged outline as ad hoc
production SQL.

## Execution and monitoring

1. Announce the change window and freeze unrelated schema changes.
2. Verify the backup/recovery point, clean version 4 state, preflight result, and
   predeclared thresholds again.
3. Drain affected writers and verify no old transaction remains on the affected
   tables.
4. Apply `up` once. Do not start a second migration process.
5. Monitor database lock waits, deadlocks, active transaction age, migration
   statement duration, CPU/I/O, free storage, WAL/archive growth, replica lag,
   connection saturation, write errors/timeouts, and application readiness.
6. After success, run `up` again and require `no change`; run `version` and
   require `version=5 dirty=false`.
7. Run `sql/migration-5-post-validation.sql`; every incompatible count must be
   zero and every expected catalog object must be present, enabled, and valid.

Abort before commit when any preflight fact changes materially, a lock or
statement timeout occurs, a deadlock appears, a predeclared WAL/disk/replica-lag
threshold is crossed, customer write failures exceed the approved threshold,
or reconciliation reports an invariant failure. Preserve logs and database
evidence without credentials or row payloads.

## Canary and mixed-version compatibility

Keep normal traffic drained while one version-5 canary starts and reports clean
version-5 readiness. Exercise a predesignated synthetic account/train run only:
station/search/availability reads, one held reservation, and its approved
terminal action. Confirm reservation-seat `train_run_id`, inventory masks,
outbox progress, and reconciliation through bounded tooling; do not use a real
email or passenger record.

The schema is designed to accept both writer shapes during rollout: version 4
omits `reservation_seats.train_run_id`, while version 5 supplies it. Confirm the
compatibility trigger remains enabled before rolling any application instance.
Roll out one process group at a time, watch write errors and constraint failures,
then restore traffic gradually. Do not remove the trigger in this release.

## Rollback

Prefer application rollback while retaining schema version 5. The compatibility
trigger allows a version-4 writer to operate against version 5, and keeping the
schema avoids removing integrity protections during an incident.

Only consider schema down when application rollback is insufficient and all of
these are true:

1. every version-5 writer is stopped and cannot restart;
2. a fresh backup/recovery point is verified;
3. `sql/migration-5-rollback-checks.sql` passes and version 5 is clean;
4. the incident commander accepts removal of `train_run_id`, the new foreign
   keys, and the strengthened route/minimum-stop enforcement; and
5. the one-step down and restore procedures were rehearsed.

Run exactly one migration step down through the repository command, then require
`version=4 dirty=false`. Do not loop downward and do not manually drop objects.
Validate the version-4 application with read-only health checks before restoring
writes. If any check fails, keep traffic drained and restore or roll forward
under the incident procedure.

## Dirty migration recovery

Never clear `dirty` merely to make tooling proceed.

1. Stop migration processes and keep affected writers drained.
2. Capture the bounded migration error category, schema version/dirty state,
   PostgreSQL server logs, and catalog/object state. Do not capture DSNs or row
   payloads.
3. Confirm whether migration 5's explicit transaction rolled back completely.
   Compare the actual catalog with both the preflight and post-validation object
   lists.
4. If no version-5 effects committed, correct the data/lock/capacity cause, then
   use a pinned, operator-approved golang-migrate recovery operation to set the
   last known-good version 4 before rerunning `up`.
5. If every version-5 effect committed but only migration metadata failed,
   require database-owner review and full post-validation before marking version
   5 clean with approved tooling.
6. If the state is partial or uncertain, do not force either version. Restore the
   verified backup or apply a reviewed repair migration.

Do not update `schema_migrations` by hand. Record the recovery decision and its
evidence in the release record.

## Completion criteria

The migration portion of the release is complete only when version 5 is clean,
post-validation passes, the canary and mixed-version checks pass, reconciliation
is clean, monitoring is stable through the agreed observation window, and the
rollback/backup evidence remains available. This runbook does not establish a
zero-downtime or capacity claim.
