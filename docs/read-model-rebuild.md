# Read-model Rebuild and Reconciliation

The projection is disposable. Operators can reproduce it entirely from
authoritative PostgreSQL source tables without reading Redis or event payload
history.

## Commands

All examples require a secret-managed `DATABASE_URL`; never place credentials
in logs or committed files.

```powershell
read-model-admin rebuild-train-run --train-run-id <uuid>
read-model-admin rebuild-train-run --train-run-id <uuid> --apply
read-model-admin rebuild-all --batch-size 100
read-model-admin rebuild-all --batch-size 100 --apply
read-model-admin reconcile --limit 100
read-model-admin inspect-lag
read-model-admin resume-event --event-id <uuid>
read-model-admin resume-event --event-id <uuid> --apply
read-model-admin replay-outbox --batch-size 100
read-model-admin replay-outbox --batch-size 100 --apply
```

Rebuild commands default to dry-run. `rebuild-all` selects stable
`(service_date,id)` batches of at most 100 and reports its cursor so an operator
can resume. Context cancellation rolls back the active train-run transaction;
already committed earlier batches remain complete.

An apply run also checkpoints `read_model_projection_state`. Starting from an
empty cursor marks journey search unavailable; each next page must present the
exact durable cursor returned by the previous page. Projection search remains
fail-closed to the authoritative source until the final page commits and marks
the state ready. A skipped or stale cursor is rejected, so an interrupted
backfill cannot silently expose a non-empty partial projection.

`resume-event` is the controlled exact-ID recovery path for an event that
reached the read-model DLQ after creating durable fan-out progress or the
PostgreSQL outbox dead letter before publication. Dry-run verifies that durable
progress or the unreceipted dead-lettered outbox event still exists. Apply
re-enqueues only the bounded event envelope after the failed dependency is
repaired. It never deletes progress or bypasses projection fallback; successful
worker completion does that atomically with the receipt.

`replay-outbox` is the bounded recovery path after complete Redis stream/PEL
loss. It scans only published PostgreSQL outbox events without the durable
read-model receipt, returns a stable `(published_at,event_id)` cursor, and
re-enqueues only validated safe envelope fields. Repeating a page is harmless
because normal receipt and current-source rebuild rules remain authoritative.
Projection search fails closed to the source while a published projection event
has neither progress nor a receipt.

`reconcile --limit 100` scans at most 100 train runs in UUID order and reports
only aggregate mismatch counts plus a bounded `next_cursor`. Pass that cursor
back with `--after`; `--train-run-id` remains available for one-run diagnosis.
`inspect-lag` reports both projection/source drift and the age of the oldest
projection-affecting outbox event without a durable receipt, including events
that have not yet created a progress row.

## Initial backfill

1. Apply migration 7 and keep the read-model worker disabled.
2. Record source run counts, free disk, PostgreSQL connection/lock telemetry,
   and the release commit.
3. Run a dry-run with the intended batch size.
4. Apply bounded batches during a measured window, passing each returned cursor
   as the next `--after` value. Do not claim zero downtime;
   each train-run rebuild reads source rows and writes one transaction.
5. Run detect-only read-model reconciliation and inspect lag.
6. Enable one worker, confirm stream pending/DLQ behavior and cache rotations,
   then scale only after multi-replica evidence.

Projection disk growth is proportional to ordered stop pairs multiplied by
active fare classes. Estimate it on a production-like restore using actual
route lengths and indexes before rollout. A route with `n` stops can produce
`n*(n-1)/2` journey pairs per applicable class.

## Reconciliation

`reconcile read-model` detects missing/extra rows, duplicates, invalid ordered
pairs, station/fare/schedule/status mismatches, stale source timestamps, and a
processed receipt without expected state. `reconcile cache-versions` reads only
known exact generation keys and detects missing or malformed tokens. Both are
read-only by default.

Seat-inventory reconciliation remains independent and authoritative. A clean
read-model result is not evidence that seats reconcile; a read-model mismatch
is not permission to mutate seat inventory.

## Rollback

Prefer application rollback while retaining additive version-7 tables. Stop
read-model workers and fall back to source search. The exceptional schema down
drops only projection/receipt state and M3-only outbox events required by the
older constraint; it must be separately reviewed because it destroys derived
history. It does not alter station, route, train, fare, seat, reservation, or
ticket authority.
