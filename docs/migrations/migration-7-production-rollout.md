# Migration 7 Production Rollout

Migration 7 adds the Milestone 3 journey projection, durable read-model event
receipts, supporting indexes/constraints, and offering event types. This is an
operator runbook, not authorization to modify production.

## Change summary

The migration creates `train_run_journey_read_model` and
`read_model_event_receipts`. It extends the existing outbox aggregate/event
constraints for station, route, train, coach, seat, fare, and train-run change
notifications. It does not modify seat masks, reservation allocation,
idempotency, ticket authority, or hot-train policy semantics.

Projection tables start empty. Initial population is an explicit bounded admin
backfill, not hidden migration work.

## Preconditions

1. Record release commit, migration checksum, backup, and restore rehearsal.
2. Verify clean schema version 6 and all Migration 6 reconciliation gates.
3. Rehearse version 6 to 7 on a production-like restore; record DDL lock time,
   table/index size, and outbox constraint validation time.
4. Estimate projection rows as ordered stop pairs multiplied by active fare
   classes; confirm disk/WAL/headroom.
5. Verify the version-7 application can serve source fallback with an empty
   projection.
6. Keep read-model workers disabled until DDL, readiness, and bounded backfill
   checks pass.
7. Confirm outbox publishes Redis Stream events and that worker credentials do
   not include JWT or admission-token secrets.

Unexpected lock duration, a dirty schema, failed restore rehearsal, invalid
legacy outbox row, insufficient disk/WAL headroom, or unbounded backfill cost
blocks rollout.

## Apply and validate

Use a dedicated migration identity and secret-managed libpq configuration:

```powershell
$env:PGSERVICE = 'railway-migrations'
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations version
```

Expected version is `7`, dirty false. Validate table/constraint/index presence,
accepted and rejected outbox event pairs, empty initial projection, and
unchanged authoritative row counts. Run seat, quota, and admission
reconciliation before backfill.

Dry-run and then apply bounded backfill:

```powershell
read-model-admin rebuild-all --batch-size 100
read-model-admin rebuild-all --batch-size 100 --apply
read-model-admin reconcile --limit 100
read-model-admin inspect-lag
```

Record batch duration, rows, locks, WAL/disk growth, failures, and resumable
cursor. Do not claim zero downtime: each run is replaced transactionally, but
the total backfill consumes real database resources. Enable one worker only
after reconciliation is clean, then validate pending recovery, DLQ, metrics,
and cache rotation before scaling to two.

## Application rollback

Prefer rolling the application back while leaving additive schema version 7 in
place. Stop read-model workers, keep outbox evidence, and use the source search
path. Verify the older binary against a version-7 restore before traffic. Cache
keys can expire naturally; never enumerate the keyspace for cleanup.

## Exceptional schema down

The one-step down deletes only M3-specific outbox events before restoring the
version-6 constraint, then drops disposable receipts/projection tables. It does
not delete or repair authoritative station, route, train, fare, seat,
reservation, ticket, or hot-policy rows.

Because down destroys derived receipts/projection evidence, run it only after a
separately reviewed decision, all new binaries are stopped, evidence is
exported safely, and the step is rehearsed on a restore. After down, verify
clean version 6, authoritative row counts, outbox backlog/constraints, and all
Milestone 1/1.1/2 reconciliations. A later up starts with an empty projection
and requires a new bounded backfill.
