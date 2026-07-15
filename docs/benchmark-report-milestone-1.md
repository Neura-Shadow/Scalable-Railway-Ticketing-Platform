# Milestone 1 Benchmark Report

## Status

No sustained load benchmark has been executed or accepted as of 2026-07-15. The nine k6 scenarios are present, but this document intentionally contains no fabricated throughput, latency, capacity, database, Redis, or outbox values.

No national-scale, multi-region, or multi-replica capacity conclusion can be drawn from the current evidence.

## Evidence register

| Evidence | Status | Notes |
|---|---|---|
| Script source review | Validated | Nine required scripts and shared metric/config helpers exist |
| k6 syntax/inspect | Passed | All nine scripts inspected with `grafana/k6:2.0.0` on 2026-07-15; this did not send traffic |
| Local smoke run | Not measured | Requires a running seeded API and short-lived customer token |
| Sustained single-region run | Not measured | Requires controlled infrastructure and monitoring |
| Hot-train correctness gate | Not measured | Requires fresh known seat inventory plus post-run reconciliation |
| Final inventory reconciliation | Not measured for k6 | Repository integration tests are separate evidence, not a k6 benchmark |
| Multi-replica run | Out of Milestone 1 acceptance evidence | Recommended for Milestone 2 |

## Reproduction metadata

Complete every field before publishing results:

| Field | Measured value |
|---|---|
| Commit SHA | Not recorded |
| Container image digest | Not recorded |
| k6 version | Not recorded |
| Scenario and command | Not recorded |
| UTC start/end | Not recorded |
| Region/topology | Not recorded |
| API/worker replicas | Not recorded |
| CPU/memory limits | Not recorded |
| PostgreSQL version/size | Not recorded |
| Redis version/size | Not recorded |
| Dataset and physical seats | Not recorded |
| VUs and duration | Not recorded |
| Hold TTL/worker interval | Not recorded |

## Scenario results

Enter measured values only. Use `Not measured` rather than estimating.

| Scenario | RPS | Reservation TPS | p50 | p95 | p99 | 4xx | Unexpected 5xx | Conflicts | Domain successes | Reconciliation |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Station browse | Not measured | N/A | Not measured | Not measured | Not measured | Not measured | Not measured | N/A | N/A | N/A |
| Train search | Not measured | N/A | Not measured | Not measured | Not measured | Not measured | Not measured | N/A | N/A | N/A |
| Availability read | Not measured | N/A | Not measured | Not measured | Not measured | Not measured | Not measured | N/A | N/A | N/A |
| Reservation normal | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Reservation hot train | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Reservation idempotency | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Reservation confirm | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Reservation expiration storm | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Rate limit | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |

## Dependency and worker observations

| Signal | Measured value |
|---|---|
| PostgreSQL active/idle/waiting connections | Not measured |
| PostgreSQL CPU/I/O/lock waits | Not measured |
| Redis latency/errors | Not measured |
| Outbox pending/processing/dead-letter | Not measured |
| Hold-expirer successes/failures | Not measured |
| API/worker CPU and memory | Not measured |

## Acceptance decision

- Zero unexpected 5xx: **Not evaluated**
- No overlapping active allocations: **Not evaluated for k6**
- Same idempotency request creates one reservation: **Not evaluated for k6**
- Hot-train successes do not exceed available seats: **Not evaluated**
- No expiry/cancellation mask leaks: **Not evaluated for k6**
- Final inventory equals active reservations: **Not evaluated for k6**
- p95 and p99 measured rather than guessed: **Not measured**

Current decision: **benchmark evidence incomplete**. This is not a capacity failure or success result; the required controlled run has not occurred.

## How to complete this report

Follow [load-testing.md](load-testing.md), preserve the raw output outside Git, copy sanitized aggregates here, attach the reconciliation result, and state every environment limitation. A local smoke run may validate the harness but must not be presented as sustained production capacity.
