# Milestone 6 Load and Failure Testing

## Current status

**Passed for bounded local correctness, recovery, and pressure evidence.** The
canonical run is `railway-m6-full-20260809ii`; its 427-file index has SHA-256
`f2842fb37c6cea65dd77bc2b8b9b945805650127f22430b5ba56983b2dcdba01`.
The scoped execution source has SHA-256
`10341116a4dcbba9efa4c096404f2cb8ae05716b6856e77f15b9feec4e9bed05`;
the rendered Compose configuration has SHA-256
`762e67710ee7a63cd11271e5d35bd224d157a2f702aa973ca9b5bbbca586776e`.
Only this file and `docs/benchmark-report-milestone-6.md` are excluded from the
source digest.

See [`benchmark-report-milestone-6.md`](benchmark-report-milestone-6.md) for
measured values and the prebuilt-image/source-build limitation. CI runs the
same bounded topology driver and uploads its sanitized summary, k6 results,
metrics, invariants, secret scan, artifact index, and teardown result.

## Evidence contract

The runner creates a new project-scoped topology with three APIs, two payment
workers, one detect-only reconciler, deterministic sandbox, control PostgreSQL,
two independent booking-shard PostgreSQL instances, Redis, and the existing
bounded workers. It refuses an existing project or evidence directory,
generates and masks a fresh JWT key, records exact source/config/image
identities, creates independent customer/reservation fixtures, and always
restores caller environment before project-labelled teardown.

Artifacts record source/config hashes, image inventory, fixture seed, host and
Docker capacity, PostgreSQL settings, pool caps, tool versions, failure-hook
sequence, k6 summaries, allowlisted metrics, database invariants, active
provider-restart recovery, reconciliation, secret scan, and teardown. No raw
provider body, signature, token, password, DSN, passenger data, or payment
reference may survive the evidence scan.

These results do not prove production capacity, PCI compliance, live-provider
behavior, exactly-once delivery, national-scale throughput, multi-region
availability, or an RPO/RTO.

## Executed scenario matrix

| Script | Pressure/fault | Required correctness assertion | Status |
|---|---|---|---|
| `payment-intent-create.js` | Intent through three APIs | One active intent/saga; authoritative immutable money | `passed` |
| `payment-idempotency.js` | Same and conflicting identities | Exact replay; changed fingerprint conflicts; no raw key | `passed` |
| `payment-webhook-burst.js` | Duplicate/out-of-order signed events | Immutable inbox identity; harmless duplicates; store-only HTTP | `passed` |
| `payment-capture-recovery.js` | Capture response loss | Query-before-retry; one capture and convergent result | `passed` |
| `ticket-issuance.js` | Multi-worker issuance | Captured proof first; one receipt/ticket per seat and global code claim | `passed` |
| `payment-refund.js` | Refund retry and compensation | Full refund; compensation before ticket/seat cancellation | `passed` |
| `payment-provider-outage.js` | Unavailable/malformed/oversized provider | Bounded retry/manual semantics; no blind duplicate write | `passed` |
| `payment-shard-outage.js` | Assigned shard stopped | No fallback/cross-shard write; healthy shard remains available | `passed` |
| `payment-during-migration.js` | Payment during v2 cutover | Receipt/state preservation, route fence, zero journal gap | `passed` |
| `multi-replica-payment.js` | Three APIs and two workers | Stable operation/ticket effects and convergent saga | `passed` |

## Provider-restart acceptance

In addition to the ten scripts, the runner creates a dedicated active saga,
injects capture `response_loss`, confirms the persisted operation is
`uncertain`, restarts the sandbox before capture-webhook delivery, and resumes
the normal workers. The accepted result is:

- recovery mode `status_query_before_retry`;
- one persisted provider capture result with stable operation identity;
- one control capture operation and one success;
- final intent/saga `completed`/`completed`;
- one issued order and one active ticket;
- provider state `captured` before and after restart.

This proves the bounded disposable restart contract. It does not turn the
sandbox snapshot or pull-style webhook queue into a production provider
datastore or delivery service.

## Measurements and interpretation

Each scenario publishes request/iteration counts, checks, HTTP p50/p95/p99,
and meaningful convergence p50/p95/p99. API and worker snapshots retain
bounded provider-operation, webhook, ticket, reconciliation, and pgx metrics.

The canonical run observed 3,243 pgx acquires, 0.085739 seconds cumulative
acquire duration, 19 empty acquires, zero cancelled acquires, and maximum
per-process/per-pool peak 1 across 1,755 pool samples. These are local
observations, not sizing guidance.

## Required invariants and publication gate

A run passes only if all k6 checks pass, provider restart converges without a
duplicate capture, final control/shard violation counts are zero, detect-only
`payment-all` reconciliation examines non-empty control and shard state without
mismatch or truncation, the secret scan is clean, and project-labelled
containers/volumes/networks are zero after teardown. This bundle satisfied all
gates and ended physical migration in `rollback_window`.

Publication proof:
`rows_examined=14; shard_rows_found=14; issued_orders=7; mismatch_count=0; manual_reviews=0; truncated=false`.

Future source changes must not reuse this bundle. Re-run
`scripts/run-milestone-6-payment-evidence.ps1` in a new empty directory and
publish a new indexed bundle. Production claims require separate evidence with
a real provider, source-built release images, production network/database
controls, settlement, and regional failure exercises.
