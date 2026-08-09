# Milestone 6 Benchmark Report

## Status and evidence boundary

**Status: `passed` for bounded disposable correctness/recovery evidence.** The
canonical local bundle is `railway-m6-full-20260809x`, with 390 indexed files
and bundle SHA-256
`7892880d56a832615c73a62c4a290c332630912e3c02856247994597d56cecde`.
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
| Source commit / worktree state | `3938eadca4415ca6d6e61b9760b3efda02058937`; dirty 732-file scoped source inventory SHA-256 `11862b4cd5d0f0de3a0a416239eaa13c7040890c35372f53e556783e927720f5` |
| Start / end | 2026-08-09 11:38:19Z / 11:41:57Z; 217.170 seconds |
| Bundle | `railway-m6-full-20260809x`; 390 indexed files; SHA-256 above |
| Compose hashes | wrapper `15aa2d15c83a928989606e49626fee0ef867439a5ff87a2fe6940682995592d3`; rendered config `408de6814c42a57b40c4a1124ee2a5e296152d8db3293daadb29ee9cf0ff5ff0` |
| Fixture seed | `7e991aa779`; one bounded iteration unless the fault scenario requires 2 or 3 |
| Topology | 3 APIs, 2 payment workers, 1 reconciler, sandbox, control PostgreSQL, 2 booking-shard PostgreSQL instances |
| Host | Windows 10.0.22631; 24 logical processors; repository drive 2,048,390,066,176 bytes total / 1,271,558,811,648 free |
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
| Payment intent create | 1 / 1 | 1/1 | 42.244 / 42.244 | - | `passed` |
| Capture recovery | 16 / 1 | 3/3 | 13.357 / 15.129 | 883 / 1449.1 / 1499.42 | `passed` |
| Ticket issuance | 13 / 1 | 3/3 | 20.600 / 22.561 | 385 / 498.4 / 508.48 | `passed` |
| Full refund | 21 / 1 | 4/4 | 8.538 / 15.471 | 252 / 1614.6 / 1735.72 | `passed` |
| Multi-replica payment | 14 / 1 | 2/2 | 10.808 / 14.187 | 253 / 253 / 253 | `passed` |
| Payment idempotency | 3 / 1 | 2/2 | 12.846 / 13.927 | - | `passed` |
| Webhook burst | 20 / 2 | 6/6 | 12.273 / 13.187 | 253 / 253.9 / 253.98 | `passed` |
| Provider outage | 9 / 3 | 6/6 | 14.489 / 14.770 | 1 / 1.9 / 1.98 | `passed` |
| Shard outage | 2 / 1 | 2/2 | 1723.061 / 1794.656 | - | `passed` |
| Payment during migration | 15 / 1 | 2/2 | 19.198 / 46.603 | 381.5 / 496.25 / 506.45 | `passed` |

The shard-outage latency includes the bounded failed-shard timeout. The paired
healthy-shard request still returned a durable intent, and no fallback writer
or cross-shard mutation occurred.

## Operational and pool measurements

Final worker histograms aggregate both payment-worker replicas. Percentiles are
Prometheus bucket upper bounds, not interpolated exact percentiles.

| Measurement | Samples | Average ms | p50 / p95 / p99 upper-bound ms |
|---|---:|---:|---:|
| Provider create-checkout success | 13 | 8.194 | 10 / 25 / 25 |
| Provider capture success | 7 | 12.480 | 25 / 25 / 25 |
| Provider refund success | 1 | 11.730 | 25 / 25 / 25 |
| Ticket issuance success | 7 | 33.338 | 50 / 100 / 100 |
| Webhook processing success | 27 | 5.888 | 10 / 25 / 25 |

Authorize, query-status and void did not produce a success histogram sample in
this hosted-checkout bundle, so no percentile is invented for them. Their
timeout/before-commit/after-commit/idempotency behavior is covered by focused
sandbox, HTTP-adapter and worker integration tests.

Pool evidence contains 1,620 bounded observations and 3,120 allowlisted payment
metric samples across three API and two worker processes. Final pgx totals were
5,507 acquires, 0.092466 seconds cumulative acquire duration, 19 empty acquires,
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
