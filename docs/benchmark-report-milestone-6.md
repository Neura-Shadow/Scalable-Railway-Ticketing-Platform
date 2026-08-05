# Milestone 6 Benchmark Report

## Status and evidence boundary

**Status: `not_run`.** No canonical Milestone 6 load/failure bundle has been
produced, and no payment latency, throughput, recovery, pool-pressure,
container, capacity or production-readiness result is claimed. This file is an
honest publication scaffold for the evidence contract in
[`milestone-6-load-testing.md`](milestone-6-load-testing.md).

Do not infer results from unit/integration tests, Milestone 5 benchmarks,
Compose startup, synthetic provider semantics, or a passing HTTP status. A
future result must identify the exact source commit and configuration, retain
raw artifacts, and prove database/provider invariants after every fault.

## Run manifest

| Field | Recorded value |
|---|---|
| Source commit / worktree state | Not recorded |
| Start / end time and timezone | Not recorded |
| Canonical bundle path and SHA-256 | Not recorded |
| Compose file/config hash | Not recorded |
| Image names and immutable digests | Not recorded |
| Fixture seed and scale | Not recorded |
| Host CPU, memory, disk and OS | Not recorded |
| Go, Docker, Compose, k6 and PostgreSQL versions | Not recorded |
| Database settings and pool caps | Not recorded |
| Provider sandbox profile/version | Not recorded |
| Failure-hook sequence | Not recorded |
| Evidence secret/sensitive-data scan | Not recorded |
| Teardown result | Not recorded |

## Scenario results

| Scenario | Requests / iterations | Checks | Latency / rate | Required observation | Status |
|---|---:|---:|---|---|---|
| Payment intent create | - | - | Not measured | One active intent/saga; immutable server-derived amount/currency | `not_run` |
| Payment idempotency | - | - | Not measured | Exact replay converges; changed fingerprint conflicts | `not_run` |
| Webhook burst | - | - | Not measured | Duplicate/order/conflict/signature semantics and store-only HTTP | `not_run` |
| Capture recovery | - | - | Not measured | Query-before-retry after unknown; no duplicate capture | `not_run` |
| Ticket issuance | - | - | Not measured | One receipt and one ticket per reservation seat after capture | `not_run` |
| Full refund | - | - | Not measured | Full refund accounting and post-refund seat release | `not_run` |
| Provider outage | - | - | Not measured | Bounded backoff/manual review and no resource leak | `not_run` |
| Shard outage | - | - | Not measured | No fallback writer; healthy work isolated | `not_run` |
| Payment during migration | - | - | Not measured | Version-2 state preserved through cutover/reverse | `not_run` |
| Multi-replica payment | - | - | Not measured | Stable provider/ticket/refund effects under retries | `not_run` |

## Requested measurements

| Measurement | Result | Evidence |
|---|---|---|
| Intent create request/accept rate | Not measured | None |
| Webhook receive/authenticate/duplicate/conflict/process rate | Not measured | None |
| Provider authorize/capture/query/void/refund p50/p95/p99 | Not measured | None |
| Ticket issuance and control-finalization p50/p95/p99 | Not measured | None |
| End-to-end payment-to-active-ticket p50/p95/p99 | Not measured | None |
| Refund completion p50/p95/p99 | Not measured | None |
| Unknown/manual/retry/exhausted counts and repair latency | Not measured | None |
| Queue depth, oldest age and claim throughput | Not measured | None |
| pgx total/acquired/idle/max/peak and acquire pressure | Not measured | None |
| Per-role and allowlisted-shard connection counts | Not measured | None |
| Host and PostgreSQL resource limits | Not measured | None |

## Invariant and reconciliation results

| Invariant | Result | Evidence |
|---|---|---|
| No duplicate authorize/capture/void/refund provider effect | Not evaluated | None |
| Intent/receipt/provider amount and currency agree | Not evaluated | None |
| Unknown provider outcome is queried before retry | Not evaluated | None |
| No issue before durable captured proof | Not evaluated | None |
| One stable ticket per reservation seat and unique ticket code | Not evaluated | None |
| Full refund only; refunded amount within captured amount | Not evaluated | None |
| No seat release before durable successful refund | Not evaluated | None |
| Duplicate webhook harmless; changed-hash event rejected | Not evaluated | None |
| No stale/fallback writer or payment migration mismatch | Not evaluated | None |
| No leaked lease, goroutine, HTTP body or database connection | Not evaluated | None |
| No secret, raw card data or unbounded telemetry label | Not evaluated | None |

## Publication gate

Replace `not_run` and `Not measured` only with values directly parsed from a
sanitized canonical result and linked raw artifacts. Record sample sizes,
units, time window and percentile method. Expected fault responses must be
separated from unexpected failures. Missing host or database metrics remain
explicitly unavailable; they must not be estimated.

A future passing report is bounded disposable evidence only. It must state its
limits and cannot claim live-provider behavior, PCI compliance, production
capacity, exactly-once delivery, zero downtime, multi-region resilience,
national-scale throughput, RPO/RTO or cost without separate direct evidence.
