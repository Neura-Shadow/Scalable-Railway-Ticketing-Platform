# Migration 9 Control-Plane Rollout

## Scope

Migration 9 extends the control database for the fixed physical-shard pilot. It
adds allowlisted physical catalog metadata, booking commands, conservative
quota leases, the reservation directory, physical migration/checkpoint/write-
evidence/reconciliation state, and bounded control outbox event types. It also
renames existing storage kinds to `legacy_schema` and `logical_schema` and
backfills the directory from Milestone 4 reservation locators.

It inserts disabled catalog rows for `physical-shard-0` and
`physical-shard-1`; it does not assign or move a train run, enable a writer, or
store a database URL. Physical mode remains disabled until explicit rollout.

## Preconditions

- Record exact commit and migration checksum; verify Migration 8 is clean.
- Take and restore-test a control backup in a disposable environment.
- Drain incompatible writers; all serving versions must understand renamed
  storage kinds before enabling the pilot.
- Bound statement/lock time and inspect locators, table sizes, locks, disk/WAL
  headroom and connection budget.
- Use the migration principal; do not use application runtime credentials.

## Disposable rehearsal

Set `DATABASE_URL` through the secret mechanism without printing it, then run:

```powershell
go run ./cmd/migrate -path migrations version
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations version
```

Validate only the five fixed catalog rows exist with correct storage kinds;
physical connection references are bounded and unique; physical rows are
disabled; every old locator has a lossless active legacy-imported directory
row; command/quota/migration tables are empty; constraints, indexes and update
triggers exist; and migration history is clean.

Exercise populated Migration 8 to 9, one-step `down`, then `up` in a disposable
database. The down migration intentionally refuses while physical assignments,
commands, divergent directory state, physical migration/reconciliation
evidence, or new control outbox intent remains. A refused down is a safety
result, not a reason to delete evidence or force migration state.

## Rollout and rollback

Deploy compatible binaries in legacy mode first and prove Milestones 1-4
regressions. Apply Migration 9 once, verify invariants, then explicitly install
booking-shard schema version 1 before enabling any physical assignment.

Schema rollback is not the normal operational rollback. If no version-9-only
durable work exists and the checked down preflight passes, restore the prior
binary and execute a reviewed one-step down. Otherwise leave schema expanded,
disable physical opt-in, keep physical fences off, and repair forward. Never
drop the reservation directory or migration ledger merely to make down pass.

This runbook does not authorize a production rollout or claim runtime evidence.
See [booking-shard version 1](booking-shard-v1-rollout.md).
