# ADR 025: Bounded Rebuild and Detect-Only Reconciliation

- Status: Accepted
- Date: 2026-07-22

## Context

The journey projection can lag, be corrupted, be partially unavailable, or be
lost. An event log is not a sufficient rebuild authority: Redis Streams are
bounded, delivery is asynchronous, and payloads are minimal. Operators need a
safe way to restore derived state and detect drift without risking source data.

## Decision

Rebuild the projection from authoritative PostgreSQL tables, never by replaying
cache values or trusting event payloads. `RebuildTrainRun` uses one bounded
transaction and complete replacement. `RebuildAll` visits train runs in stable
order with a bounded batch size and resumable cursor. Cancellation and context
timeouts stop after a completed item/batch; no partial train-run set commits.

`cmd/read-model-admin` exposes bounded `rebuild-train-run`, `rebuild-all`,
`reconcile`, and `inspect-lag` commands. Dry-run is supported where it can
report intended work without mutation. Output is bounded and contains no DSN,
Redis credential, full event payload, passenger data, or other PII.

The read-model reconciler compares deterministic expected rows with stored
rows and detects missing/extra/duplicate rows, journey pairs, fares, station
fields, times, cancelled status, source timestamps, and receipt/projection
mismatches. Cache-version reconciliation checks only exact version keys and
token format. It does not scan all data keys.

Reconciliation is read-only by default and never repairs production source
tables. An explicitly invoked rebuild may repair only disposable projection
state. Existing seat, quota, and admission reconciliation remain independent
acceptance gates.

## Consequences

- Projection recovery does not depend on retained stream history.
- Operators can distinguish source correctness from derived-state health.
- Full backfill cost is bounded per batch but can still be significant and must
  be measured and scheduled.
- No zero-downtime backfill or rollback claim is made without evidence.

## Rejected alternatives

- Rebuild solely from Redis Stream history: rejected because retention is
  bounded and payloads are not authoritative snapshots.
- Automatic production repair on mismatch: rejected because detection should
  not silently mutate state during an incident.
- One transaction for all train runs: rejected because duration, locks, and
  rollback work would be unbounded.
