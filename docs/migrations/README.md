# Database Migration Operations

SQL migrations are owned by `migrations/` and are applied with the repository's
`cmd/migrate` command. Application processes must never mutate the schema at
startup. Production migrations use a dedicated migration identity, a verified
backup, an approved change window, and an operator who can stop the rollout.

## Migration 5 operator material

| File | Intended schema state | Purpose |
|---|---|---|
| [migration-5-production-rollout.md](migration-5-production-rollout.md) | Version 4 before rollout; version 5 after rollout | Decision, lock, staged-backfill, canary, recovery, and rollback runbook |
| [sql/migration-5-preflight.sql](sql/migration-5-preflight.sql) | Clean version 4 | Read-only sizing, activity, lock, NULL-projection, and incompatible-data checks |
| [sql/migration-5-post-validation.sql](sql/migration-5-post-validation.sql) | Clean version 5 | Read-only catalog, constraint, trigger, and data-integrity validation |
| [sql/migration-5-rollback-checks.sql](sql/migration-5-rollback-checks.sql) | Clean version 5 | Read-only checks before an application rollback or exceptional schema down |

The three checked-in operator scripts contain no data-changing statements. They
set bounded session timeouts and run in an explicit read-only transaction. Run
them with `ON_ERROR_STOP` so a timeout or SQL error cannot be mistaken for a
passing check.

Use a libpq service definition or secret-managed environment variables. Do not
place a DSN, password, certificate-key path, or access token on the command line
or in captured output. For example:

```powershell
$env:PGSERVICE = 'railway-migrations'
psql --no-psqlrc --set=ON_ERROR_STOP=1 --file docs/migrations/sql/migration-5-preflight.sql
```

Run the scripts against a recent production-like restore before the release and
against the target primary during the approved window. Estimated row counts are
planning inputs, not exact counts. Every `incompatible_rows` result must be zero,
the migration version must match the script's expectation, and `dirty` must be
false. A timeout is an inconclusive result and blocks the rollout; it is not a
pass.

Never edit a migration that has been applied to any shared environment. If a
large-table rehearsal shows that migration 5 cannot fit the approved window,
hold the release and create a separately reviewed migration path as described in
the runbook. Do not execute ad hoc write SQL from these documentation files.
