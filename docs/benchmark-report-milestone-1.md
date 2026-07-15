# Milestone 1 Benchmark Report

## Status

No sustained load benchmark has been executed or accepted. The nine k6 scenarios are present. Two five-second, one-VU Docker Desktop smoke runs were executed on 2026-07-16 to validate the station-browse and availability-read harnesses; their measurements are reported below as local smoke evidence only.

No national-scale, multi-region, or multi-replica capacity conclusion can be drawn from the current evidence.

## Evidence register

| Evidence | Status | Notes |
|---|---|---|
| Script source review | Validated | Nine required scripts and shared metric/config helpers exist |
| k6 syntax/inspect | Passed | All nine scripts inspected with `grafana/k6:2.0.0` on 2026-07-15; this did not send traffic |
| Local smoke run | Passed for two read scenarios | One VU for five seconds against a local container; not capacity evidence |
| Sustained single-region run | Not measured | Requires controlled infrastructure and monitoring |
| Hot-train correctness gate | Not measured | Requires fresh known seat inventory plus post-run reconciliation |
| Final inventory reconciliation | Not measured for k6 | Repository integration tests are separate evidence, not a k6 benchmark |
| Multi-replica run | Out of Milestone 1 acceptance evidence | Recommended for Milestone 2 |

## Reproduction metadata

Complete every field before publishing results:

| Field | Measured value |
|---|---|
| Commit SHA | `444b03f5ed466532977cd842e85a3ddd809b9bb7` |
| Container image digest | `sha256:be711d45c3d6c135d048039c89a7b1adfb1f2a257f095e0baa9bf7483eb136b5` |
| k6 version | `grafana/k6:2.0.0` |
| Scenario and command | `station-browse.js` and `availability-read.js`, each through the documented container runner |
| UTC start/end | Short interactive runs on 2026-07-16; exact timestamps were not retained, so these results are not a formal benchmark |
| Region/topology | One Docker Desktop host; one API, one PostgreSQL, one Redis |
| API/worker replicas | API: 1; background workers: 0 |
| CPU/memory limits | No explicit container limits; unsuitable for capacity conclusions |
| PostgreSQL version/size | PostgreSQL 16.14; local disposable database |
| Redis version/size | Redis 7.4.9; local disposable instance |
| Dataset and physical seats | Three stations, one train-run, two physical seats |
| VUs and duration | 1 VU, 5 seconds per measured scenario |
| Hold TTL/worker interval | Not recorded |

## Scenario results

Enter measured values only. Use `Not measured` rather than estimating.

| Scenario | RPS | Reservation TPS | p50 | p95 | p99 | 4xx | Unexpected 5xx | Conflicts | Domain successes | Reconciliation |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Station browse | 174.08 local requests/s | N/A | 4.98 ms | 8.68 ms | 13.97 ms | 0 | 0 | N/A | 1,742/1,742 checks passed | N/A |
| Train search | Not measured | N/A | Not measured | Not measured | Not measured | Not measured | Not measured | N/A | N/A | N/A |
| Availability read | 140.24 local requests/s | N/A | 6.36 ms | 9.96 ms | 14.94 ms | 0 | 0 | N/A | 1,404/1,404 checks passed | N/A |
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

- Zero unexpected 5xx: **Passed for the two local read smoke runs only**
- No overlapping active allocations: **Not evaluated for k6**
- Same idempotency request creates one reservation: **Not evaluated for k6**
- Hot-train successes do not exceed available seats: **Not evaluated**
- No expiry/cancellation mask leaks: **Not evaluated for k6**
- Final inventory equals active reservations: **Not evaluated for k6**
- p95 and p99 measured rather than guessed: **Not measured**

Current decision: **sustained benchmark evidence incomplete**. The two smoke measurements validate the harness and local request path only. They are neither a production capacity success nor a national-scale throughput claim; the required controlled run has not occurred.

## How to complete this report

Follow [load-testing.md](load-testing.md), preserve the raw output outside Git, copy sanitized aggregates here, attach the reconciliation result, and state every environment limitation. A local smoke run may validate the harness but must not be presented as sustained production capacity.
