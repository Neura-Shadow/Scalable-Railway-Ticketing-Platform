# Milestone 3 Benchmark Report

## Evidence status

The nine Milestone 3 k6 modules passed syntax inspection. A bounded local
Compose run also exercised three APIs, two admission workers, two read-model
workers, PostgreSQL, Redis, and the reverse proxy. It proved a cross-replica
warm hit, worker-driven generation rotation, one-worker failover/restart,
one-API restart, Redis-outage PostgreSQL read fallback, fresh namespace
recovery, and clean read-model reconciliation. This is functional evidence,
not a sustained benchmark. Throughput, latency, ratios, utilization, and
capacity fields remain **not measured**.

A syntax check, unit test, short functional run, container build, or CI pass is
not production-capacity evidence. No national-scale, 12306-equivalent,
multi-region, or zero-downtime claim is made.

## Run identity

| Field | Observed value |
|---|---|
| Commit SHA | Working-tree validation before final delivery commit; CI rerun required |
| UTC start/end | Bounded local run completed 2026-07-22; duration not retained as benchmark evidence |
| Environment/region | Local Docker Desktop, single host; not a production region |
| API/admission/read-model replicas | 3 / 2 / 2 in isolated Compose |
| PostgreSQL/Redis versions and topology | PostgreSQL 16 Alpine and Redis 7 Alpine, one instance each |
| CPU, memory, disk, network | Not run |
| Source/projection/cache cardinality | Not run |
| TTL/jitter/worker bounds | Not run |
| k6 version and script commit | Syntax inspected with k6 0.54.0; no load run |

## Functional and recovery evidence

| Scenario | Status | Accepted evidence |
|---|---|---|
| Station/search/availability cache | Not run | Pending controlled smoke |
| Cold versus warm search | Not run | Pending independent-generation samples |
| Shared multi-replica fill/rotation | Passed bounded local functional run | API 1 fill, API 2 hit, worker event rotation, LB smoke |
| Redis restart and refill | Passed bounded local functional run | read fallback during stop; fresh valid generation after restart |
| Worker pause/restart | Passed bounded local functional run | worker 2 processed event while worker 1 stopped; worker 1 returned ready |
| Invalidation storm | Not run | Pending bounded synthetic event run |
| Mixed search/booking | Not run | Pending conflicts plus reconciliation |
| Projection live rebuild | Not run | Pending old/new-complete observation |

## Sustained measurements

| Measurement | Result |
|---|---|
| Station/search/availability RPS | Not measured |
| Cold search p50/p95/p99 | Not measured |
| Warm search p50/p95/p99 | Not measured |
| Cache hit/miss/failure ratio | Not measured |
| Fallback/source-query count | Not measured |
| Singleflight shared count | Not measured |
| Projection rebuild duration/lag | Not measured |
| Invalidation rate/failures | Not measured |
| Redis latency/errors | Not measured |
| PostgreSQL connections/locks | Not measured |
| Booking conflicts/unexpected 5xx | Not measured |

## Correctness verdict

| Gate | Result |
|---|---|
| PostgreSQL source and seat authority preserved | Covered by automated tests; load run not evaluated |
| No partial projection set | Covered by transaction tests; load run not evaluated |
| Duplicate/out-of-order convergence | Covered by integration tests; load run not evaluated |
| Redis loss creates fresh namespaces | Covered by real Redis tests; load run not evaluated |
| Stale hint cannot oversell | Covered by booking boundary tests; load run not evaluated |
| Seat/read-model reconciliation clean | Read-model clean in bounded local run; sustained mixed-booking seat gate not evaluated |

When a controlled run occurs, replace `Not run`/`Not measured` only with
observed values and cite sanitized raw artifacts stored outside git. Record
warm-up, repetitions, failures, variance, and rejected runs. The current honest
verdict is that the repository has bounded local functional evidence and
executable sustained-test procedures, but no accepted sustained Milestone 3
performance result.
