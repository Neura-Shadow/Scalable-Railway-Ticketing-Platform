# Milestone 6 Load and Failure Testing

## Current status

**Passed for bounded local correctness, recovery and pressure evidence.** The
canonical run is `railway-m6-full-20260809z`; its 391-file index has SHA-256
`f53baccc96bf6f0b930e4be08b0ba4ed8033359fdb51ad7737113194b2829464`.
See [`benchmark-report-milestone-6.md`](benchmark-report-milestone-6.md) for
measured values. CI runs the same ten-scenario driver and uploads the sanitized
summary, k6 results, pool/payment metrics, invariant counts, secret scan,
artifact index and teardown result for 14 days.

The scoped execution source is SHA-256
`5ae444064eabc60a69e2e5d0109fb8f062bd48d5b7bf3bc905bf8e34ce360ced`;
the rendered Compose configuration is SHA-256
`80c89805897e29e4393c4e47cd0734e7f52b3d4176494d94d83bedd1cc074be9`.
Only the two post-run publication files
`docs/benchmark-report-milestone-6.md` and
`docs/milestone-6-load-testing.md` are excluded from that source digest.

## Evidence contract

The runner creates a fresh project-scoped topology: three API replicas, two
payment workers, one detect-only reconciler, deterministic sandbox, control
PostgreSQL, two independent booking-shard PostgreSQL instances, Redis and the
existing bounded workers. It refuses an existing project/evidence directory,
generates and masks a fresh JWT key, records a dirty/clean source manifest,
uses independent customer/reservation fixtures, and always restores the caller
environment and tears down only project-labelled resources.

Artifacts record source/config hashes, image inventory, fixture seed, host and
Docker capacity, PostgreSQL settings, pool caps, tool versions, timestamps,
failure-hook sequence, k6 summaries, allowlisted metrics, database invariants,
container samples, reconciliation, secret scan and teardown. No raw provider
body, signature, token, password, DSN, passenger data or payment reference may
survive the evidence scan.

These results do not prove production capacity, PCI compliance, live-provider
behavior, exactly-once delivery, national-scale throughput, multi-region
availability or an RPO/RTO.

## Executed scenario matrix

| Script | Pressure/fault | Required correctness assertion | Status |
|---|---|---|---|
| `payment-intent-create.js` | Intent through three-API topology | One active intent/saga; authoritative immutable money | `passed` |
| `payment-idempotency.js` | Same and conflicting request identities | Exact replay; changed fingerprint conflicts; no raw key | `passed` |
| `payment-webhook-burst.js` | Duplicate/out-of-order signed event burst | Immutable inbox identity; harmless duplicates; store-only HTTP | `passed` |
| `payment-capture-recovery.js` | Capture response loss/unknown result | Query-before-retry; one capture and convergent durable result | `passed` |
| `ticket-issuance.js` | Multi-worker issuance | Captured proof first; one receipt/ticket per seat and global code claim | `passed` |
| `payment-refund.js` | Cancellation, refund retry and compensation | Full refund; durable compensation before ticket/seat cancellation | `passed` |
| `payment-provider-outage.js` | Unavailable/malformed/oversized provider | Bounded retry/manual semantics; no blind duplicate financial write | `passed` |
| `payment-shard-outage.js` | Assigned shard stopped | No fallback/cross-shard write; healthy shard remains available | `passed` |
| `payment-during-migration.js` | Payment concurrent with v2 cutover | Receipt/state preservation, route fence and zero journal gap | `passed` |
| `multi-replica-payment.js` | Three APIs and two workers | Stable operation/ticket effects and convergent saga | `passed` |

## Deterministic failure coverage

The runtime bundle uses named sandbox hooks for response loss, timeout,
malformed/oversized responses and provider outage; it externally stops/restarts
one shard and drives a validating-online through rollback-window physical
migration. Focused sandbox/HTTP/worker/shard tests additionally cover
before/after-commit authorize/capture/void/refund outcomes, status-query
timeouts, stale leases, duplicate/out-of-order/delayed webhooks, invalid
signatures/key IDs/timestamps, worker claim/finalize loss, issuance replay,
refund uncertainty and receipt conflicts. No race is coordinated solely by an
unbounded sleep; every runtime wait and hook has a finite deadline.

## Measurements and interpretation

Every scenario publishes request count, iteration count, check result, HTTP
p50/p95/p99 and, where meaningful, convergence p50/p95/p99. API and worker
Prometheus snapshots retain bounded provider-operation, webhook, ticket,
reconciliation and pgx histogram/counter data. Pool records include total,
acquired, idle, max, acquire count/duration, empty/cancelled acquire and peak,
using only bounded role/shard labels.

The canonical run observed 5,734 pgx acquires, 0.095535 seconds cumulative
acquire duration, 18 empty acquires, zero cancelled acquires and maximum
per-process/per-pool peak 1. These are honest local observations, not a sizing
recommendation. Exact scenario and histogram measurements are in the benchmark
report and raw bundle.

## Required invariants and publication gate

A run passes only if all k6 checks pass, final control/shard violation counts
are zero, detect-only `payment-all` reconciliation examines non-empty control
and shard state without mismatch or truncation,
the secret scan is clean, and project-labelled containers/volumes/networks are
zero after teardown. The canonical run satisfied every gate and ended physical
migration in `rollback_window`.

Publication proof:
`rows_examined=13; shard_rows_found=13; issued_orders=6; mismatch_count=0; manual_reviews=0; truncated=false`.

Future edits must not replace these bounded values with estimates or reuse the
bundle for a different commit. Re-run
`scripts/run-milestone-6-payment-evidence.ps1` in a new empty directory and
publish the new indexed bundle. A production claim requires separate evidence
with a real provider and production network, database, settlement and regional
failure controls.
