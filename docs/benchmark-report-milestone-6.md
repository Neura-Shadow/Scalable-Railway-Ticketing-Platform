# Milestone 6 Benchmark Report

## Status and evidence boundary

**Status: `passed` for bounded disposable correctness/recovery evidence.** The
canonical local bundle is `railway-m6-full-20260809u`, with 386 indexed files
and bundle SHA-256
`c16993a8c9a2f8d6ac7402605911b394115dffaac7f806a6dbd6e84afa262b8b`.
All ten scenarios, final reconciliation, invariant checks, secret scan and
project-scoped teardown passed. This does not establish production capacity,
a live-provider SLO, PCI scope, multi-region behavior or an RPO/RTO.

The execution-source inventory was frozen for the runtime run. Its only
source-digest exclusions are the two post-run publication files
`docs/benchmark-report-milestone-6.md` and
`docs/milestone-6-load-testing.md`; every executable, workflow, test,
migration, configuration and runner file remains covered. These reports are
then verified against the completed external bundle by the generic publication
guardrail.

## Run manifest

| Field | Recorded value |
|---|---|
| Source commit / worktree state | `c12d7fc6e050abd597ea27e2e1e375c1d9464a43`; dirty 732-file scoped source inventory SHA-256 `e1ba2fbbcaf2f3cf7ba9e13d6100ead4ac4c4416847f9af58523f70bb0a27d7d` |
| Start / end | 2026-08-09 11:09:18Z / 11:12:59Z; 221.777 seconds |
| Bundle | `railway-m6-full-20260809u`; 386 indexed files; SHA-256 above |
| Compose hashes | wrapper `15aa2d15c83a928989606e49626fee0ef867439a5ff87a2fe6940682995592d3`; rendered config `38adfa5821f418fb18bad5004111a9af8ec547273283baa2fc81c613f39ff044` |
| Fixture seed | `81065939f4`; one bounded iteration unless the fault scenario requires 2 or 3 |
| Topology | 3 APIs, 2 payment workers, 1 reconciler, sandbox, control PostgreSQL, 2 booking-shard PostgreSQL instances |
| Host | Windows 10.0.22631; 24 logical processors; repository drive 2,048,390,066,176 bytes total / 1,275,199,926,272 free |
| Docker engine | Docker Desktop x86_64; 24 CPUs; 16,619,581,440-byte memory limit |
| Tools | Go 1.25.12; Docker 29.6.2; Compose 5.3.1; k6 0.55.0; PostgreSQL 16.14 |
| PostgreSQL | `max_connections=100`, `shared_buffers=128MB`, `work_mem=4MB`, `wal_level=replica` |
| Pool caps | control 4/process; each shard 3/process; two-shard process budget 6; shard idle cap 2 |
| Sensitive-data scan | Passed; zero exact-secret/credential/DSN violations |
| Teardown | Passed; zero project-labelled containers, volumes or networks remained |

Image names/digests, raw scenario summaries, allowlisted Prometheus samples,
container samples, database snapshots and failure-hook logs are retained in the
indexed bundle. CI reproduces and uploads the same bounded artifact shape.

## Scenario results

Latency values are k6 request p95/p99. Convergence is the script's end-to-end
Trend p50/p95/p99; blank means the scenario asserts an immediate boundary
rather than a convergence duration.

| Scenario | Requests / iterations | Checks | HTTP p95 / p99 ms | Convergence p50 / p95 / p99 ms | Status |
|---|---:|---:|---:|---:|---|
| Payment intent create | 1 / 1 | 1/1 | 41.085 / 41.085 | - | `passed` |
| Capture recovery | 17 / 1 | 3/3 | 13.303 / 16.510 | 1009.5 / 1689.45 / 1749.89 | `passed` |
| Ticket issuance | 13 / 1 | 3/3 | 14.228 / 16.465 | 382 / 495.4 / 505.48 | `passed` |
| Full refund | 23 / 1 | 4/4 | 11.808 / 16.549 | 506 / 1874 / 1995.6 | `passed` |
| Multi-replica payment | 15 / 1 | 2/2 | 13.956 / 16.586 | 379 / 492.4 / 502.48 | `passed` |
| Payment idempotency | 3 / 1 | 2/2 | 15.408 / 16.700 | - | `passed` |
| Webhook burst | 19 / 2 | 6/6 | 16.319 / 24.554 | 254 / 254 / 254 | `passed` |
| Provider outage | 9 / 3 | 6/6 | 16.279 / 16.507 | 1 / 1 / 1 | `passed` |
| Shard outage | 2 / 1 | 2/2 | 1727.464 / 1799.206 | - | `passed` |
| Payment during migration | 16 / 1 | 2/2 | 17.082 / 28.026 | 379 / 492.4 / 502.48 | `passed` |

The shard-outage latency includes the bounded failed-shard timeout. The paired
healthy-shard request still returned a durable intent, and no fallback writer
or cross-shard mutation occurred.

## Operational and pool measurements

Final worker histograms aggregate both payment-worker replicas. Percentiles are
Prometheus bucket upper bounds, not interpolated exact percentiles.

| Measurement | Samples | Average ms | p50 / p95 / p99 upper-bound ms |
|---|---:|---:|---:|
| Provider create-checkout success | 13 | 9.320 | 10 / 25 / 25 |
| Provider capture success | 7 | 11.078 | 25 / 25 / 25 |
| Provider refund success | 1 | 7.778 | 10 / 10 / 10 |
| Ticket issuance success | 7 | 31.359 | 50 / 100 / 100 |
| Webhook processing success | 27 | 7.107 | 10 / 25 / 25 |

Authorize, query-status and void did not produce a success histogram sample in
this hosted-checkout bundle, so no percentile is invented for them. Their
timeout/before-commit/after-commit/idempotency behavior is covered by focused
sandbox, HTTP-adapter and worker integration tests.

Pool evidence contains 1,620 bounded observations and 2,443 allowlisted payment
metric samples across three API and two worker processes. Final pgx totals were
5,640 acquires, 0.103958 seconds cumulative acquire duration, 17 empty acquires,
0 cancelled acquires and a maximum per-process/per-pool observed peak of 1.
Every record includes total/acquired/idle/max plus only `database_role` and
allowlisted `shard_id`; no DSN, host, port or entity identifier is a label.

## Invariant and reconciliation results

| Invariant | Result |
|---|---|
| Duplicate provider financial operation/effect | Passed; zero duplicate authorize/capture/void/refund identities |
| Intent/operation amount and currency | Passed; zero mismatch |
| Unknown capture handling | Passed; query-before-retry convergence, one durable capture |
| Ticket issuance | Passed; no issue before captured proof, one receipt/ticket per seat, globally claimed code |
| Refund/compensation | Passed; full refund only, durable compensation before seat release |
| Webhook identity/order | Passed; duplicate/out-of-order handling and changed-hash quarantine assertions |
| Shard failure isolation | Passed; no fallback writer and healthy shard accepted work |
| Physical migration | Passed; v2 payment state/receipts survived cutover; final state `rollback_window` |
| Leases/journals/routes | Passed; zero expired claimed lease, journal gap or stale-route effect |
| Reconciliation | `payment-all`, detect-only; 13 intents and 13 shard rows examined, 6 issued orders observed, 0 mismatches, 0 manual reviews, not truncated |
| Final database violations | control 0; shard 0 |
| Secrets and teardown | scan passed; zero retained project resources |

Publication proof:
`rows_examined=13; shard_rows_found=13; issued_orders=6; mismatch_count=0; manual_reviews=0; truncated=false`.

## Interpretation

This run is deliberately small and correctness-oriented. Request counts and
latencies are reported to make the result reproducible, not as a capacity
claim. Production sizing requires a separate clean-environment campaign with a
real provider, production ingress/egress, sustained concurrency, resource and
WAL/I/O sampling, settlement reconciliation, and region-failure objectives.
