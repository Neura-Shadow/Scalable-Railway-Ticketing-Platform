# Milestone 6 Benchmark Report

## Status and evidence boundary

**Status: `passed` for bounded disposable correctness and recovery evidence.**
The canonical local bundle is `railway-m6-full-20260809ii`, with 427 indexed
files and bundle SHA-256
`f2842fb37c6cea65dd77bc2b8b9b945805650127f22430b5ba56983b2dcdba01`.
All ten workload scenarios, the active provider-restart recovery, final
reconciliation, invariant checks, secret scan, and project-scoped teardown
passed. This does not establish production capacity, a live-provider SLO, PCI
scope, multi-region behavior, or an RPO/RTO.

The execution-source inventory is SHA-256
`10341116a4dcbba9efa4c096404f2cb8ae05716b6856e77f15b9feec4e9bed05`
over 734 files. Its only exclusions are this report and
`docs/milestone-6-load-testing.md`; every executable, workflow, test,
migration, configuration, and runner file is included. The rendered Compose
configuration SHA-256 is
`762e67710ee7a63cd11271e5d35bd224d157a2f702aa973ca9b5bbbca586776e`.

## Run manifest

| Field | Recorded value |
|---|---|
| Source commit / worktree state | `0af72c1d290b413989eb6ab08182f68c8e4b0d44`; dirty 734-file scoped source inventory with the SHA-256 above |
| Start / end | 2026-08-09 13:59:24Z / 14:03:24Z; 240.508 seconds scenario runtime |
| Bundle | `railway-m6-full-20260809ii`; 427 indexed files; SHA-256 above |
| Build mode | `prebuilt-image-digests`; exact image names and digests are indexed |
| Compose hashes | wrapper `15aa2d15c83a928989606e49626fee0ef867439a5ff87a2fe6940682995592d3`; rendered config SHA-256 above |
| Fixture seed | `642f8d3d18`; one bounded iteration unless a fault scenario requires 2 or 3 |
| Topology | 3 APIs, 2 payment workers, 1 reconciler, sandbox, control PostgreSQL, and 2 booking-shard PostgreSQL instances |
| Host | Windows 10.0.22631; 24 logical processors |
| Docker engine | Docker Desktop x86_64; 24 CPUs; 16,619,581,440-byte memory limit |
| Tools | Go 1.25.12; Docker 29.6.2; Compose 5.3.1; k6 0.55.0; PostgreSQL 16.14 |
| PostgreSQL | `max_connections=100`, `shared_buffers=128MB`, `work_mem=4MB`, `wal_level=replica` |
| Pool caps | control 4/process; each shard 3/process; two-shard process budget 6; shard idle cap 2 |
| Sensitive-data scan | Passed; zero exact-secret, credential, or DSN violations |
| Teardown | Passed; zero project-labelled containers, volumes, or networks remained |

The local Docker source-build attempt was blocked before startup because the
network returned a certificate for `safebrowsing.hinet.net` while resolving
`proxy.golang.org`. TLS verification was not disabled. The accepted local run
therefore records `prebuilt-image-digests`; remote CI must independently prove
the clean source-build path.

## Scenario results

Latency values are k6 request p95/p99. Convergence values are exact Trend
p50/p95/p99; a dash means the scenario asserts an immediate boundary.

| Scenario | Requests / iterations | Checks | HTTP p95 / p99 ms | Convergence p50 / p95 / p99 ms | Status |
|---|---:|---:|---:|---:|---|
| Payment intent create | 1 / 1 | 1/1 | 40.257 / 40.257 | - | `passed` |
| Capture recovery | 16 / 1 | 3/3 | 12.213 / 17.462 | 888 / 1456.8 / 1507.36 | `passed` |
| Ticket issuance | 13 / 1 | 3/3 | 18.469 / 21.196 | 700 / 871.9 / 887.18 | `passed` |
| Full refund | 22 / 1 | 4/4 | 16.668 / 19.550 | 506 / 1644.5 / 1745.7 | `passed` |
| Multi-replica payment | 15 / 1 | 2/2 | 17.838 / 19.474 | 382 / 495.4 / 505.48 | `passed` |
| Payment idempotency | 3 / 1 | 2/2 | 13.471 / 14.606 | - | `passed` |
| Webhook burst | 20 / 2 | 6/6 | 19.519 / 28.137 | 254 / 254.9 / 254.98 | `passed` |
| Provider outage | 9 / 3 | 6/6 | 15.400 / 15.890 | 1 / 1 / 1 | `passed` |
| Shard outage | 2 / 1 | 2/2 | 1735.089 / 1807.261 | - | `passed` |
| Payment during migration | 15 / 1 | 2/2 | 18.313 / 31.653 | 379.5 / 492.45 / 502.49 | `passed` |

The shard-outage latency includes the bounded failed-shard timeout. The paired
healthy shard still accepted durable work and no fallback writer or
cross-shard mutation occurred.

## Active provider-restart recovery

The runner stopped the regular workers, created a fresh payment intent, drove
hosted authorization, produced one pending capture, injected capture response
loss, and observed the durable control operation in `uncertain`. It then
restarted the sandbox before delivering the capture webhook and resumed the
normal workers.

| Assertion | Result |
|---|---|
| Provider state before / after restart | `captured` / `captured` |
| Recovery mode | `status_query_before_retry` |
| Provider capture results | 1; provider operation ID stable across restart |
| Control capture operations / succeeded | 1 / 1 |
| Final intent / saga | `completed` / `completed` |
| Issued orders / active tickets | 1 / 1 |
| Capture webhook before recovery | Not delivered; provider status query was authoritative |

Focused contract tests additionally prove capture and refund response-loss
replay, restart before hosted-authorization delivery, active-key re-signing,
delayed-webhook logical-clock persistence, corrupt/oversized state rejection,
and fail-closed readiness after a definite save failure.

## Operational and pool measurements

The bundle contains 1,755 bounded pgx observations and 2,554 allowlisted
payment metric samples across the API and worker processes. It recorded 3,243
acquires, 0.085739 seconds cumulative acquire duration, 19 empty acquires, zero
cancelled acquires, and a maximum observed per-process/per-pool peak of 1.
Labels contain only bounded database role and allowlisted shard identity; no
DSN, host, port, customer, reservation, payment, or ticket identity is used.

## Invariant and reconciliation results

| Invariant | Result |
|---|---|
| Duplicate provider financial operation/effect | Zero duplicate authorize/capture/void/refund identities |
| Intent/operation amount and currency | Zero mismatch |
| Unknown capture | Query-before-retry converged to one durable capture |
| Ticket issuance | Captured proof first; one receipt/ticket per seat and globally claimed code |
| Refund/compensation | Full refund only; durable compensation before release |
| Webhook identity/order | Duplicate/out-of-order handling and changed-hash quarantine passed |
| Shard failure isolation | No fallback writer; healthy shard continued |
| Physical migration | v2 payment state/receipts survived; final state `rollback_window` |
| Leases/journals/routes | Zero expired claimed lease, journal gap, or stale-route effect |
| Reconciliation | `payment-all`, detect-only; 14 intents and 14 shard rows, 7 issued orders, 0 mismatches, 0 manual reviews, not truncated |
| Final database violations | control 0; shards 0 |
| Secrets and teardown | scan passed; zero retained project resources |

Publication proof:
`rows_examined=14; shard_rows_found=14; issued_orders=7; mismatch_count=0; manual_reviews=0; truncated=false`.

## Interpretation

This run is deliberately small and correctness-oriented. Request counts and
latencies make the result reproducible; they are not capacity claims. A
production campaign still needs a real provider, source-built release images,
production ingress/egress, sustained concurrency, WAL/I/O sampling, settlement
reconciliation, and explicit regional failure objectives.
