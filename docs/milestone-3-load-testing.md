# Milestone 3 Load Testing

The nine Milestone 3 k6 scenarios exercise public read caches, mixed
search/booking safety, externally controlled failure windows, and shared
multi-replica cache behavior. All fixtures must be synthetic and disposable.
No script contains credentials, privileged mutation endpoints, cache tokens,
or customer data.

## Evidence levels

- `k6 inspect` proves only module parsing and option construction.
- A functional smoke proves responses and selected invariants in one bounded
  environment.
- A failure/recovery run additionally proves observed behavior during a
  controlled dependency or process transition.
- A sustained benchmark requires recorded hardware, dataset, replica counts,
  warm-up, duration, repetitions, server telemetry, and reconciliation.

None of these scripts by itself establishes production sizing, national-scale
capacity, zero downtime, multi-region behavior, or a 12306-equivalent result.

## Scenarios

| Script | Purpose | Special precondition |
|---|---|---|
| `station-cache.js` | station browse RPS and latency | active synthetic stations |
| `train-search-cold-cache.js` | one honest cold-search observation | rotate exact search generation immediately before run |
| `train-search-warm-cache.js` | repeated warm search | setup successfully warms one query |
| `availability-cache.js` | availability hint RPS/latency | known run/journey/class |
| `mixed-search-booking.js` | read load plus periodic authoritative hold/cancel | synthetic customer/passenger; bounded `BOOKING_EVERY` |
| `cache-invalidation-storm.js` | reads during bounded event-driven rotations | external controller emits synthetic offering changes |
| `redis-cache-outage.js` | search/availability source fallback | external controller restarts disposable Redis |
| `read-model-worker-pause.js` | reads while projection lag grows | external controller pauses/resumes worker |
| `multi-replica-search-cache.js` | shared cache behind round-robin proxy | three-API Compose load-balancer URL |

Failure scripts intentionally do not receive infrastructure or operator
credentials. The controller must record transition times and restore state in
a `finally`/trap path. Never inject failure into production with these scripts.

## Common environment

```powershell
$env:BASE_URL = 'http://127.0.0.1:<ephemeral-port>'
$env:ORIGIN_CODE = 'M2A'
$env:DESTINATION_CODE = 'M2B'
$env:SERVICE_DATE = '<synthetic-service-date>'
$env:SEAT_CLASS = 'standard'
$env:TRAIN_RUN_ID = '<synthetic-train-run-uuid>'
$env:VUS = '5'
$env:DURATION = '15s'
k6 run loadtest/k6/train-search-warm-cache.js
```

`mixed-search-booking.js` additionally requires an injected synthetic
`CUSTOMER_TOKEN` and `PASSENGER_IDS`. Do not write expanded values to reports,
shell history on shared hosts, or repository files. The script cancels each
successfully created hold, but post-run seat and reservation reconciliation is
still mandatory.

## Cold/warm method

Cold and warm measurements must not be blended. For the cold script, rotate
`cache:train-search:version` through the worker/admin test harness, then run its
single iteration. Repeat independent generation rotations if a distribution is
required and report all samples. Do not label one first request plus thousands
of warm requests as a cold p95.

For the warm script, `setup()` performs a successful warm request, after which
the measured scenario uses the same normalized query. Compare only on the same
host, data, replica topology, load, and PostgreSQL/Redis state.

## Required telemetry

Capture client and server evidence together:

- station/search/availability RPS and p50/p95/p99;
- cold and warm search samples, cache hit/miss/failure/fill counts;
- fallback and source-query counts;
- singleflight shared count and fill duration;
- projection rebuild duration, rows, lag, backlog, retry, and DLQ;
- invalidation rate/failures and Redis latency/errors;
- PostgreSQL pool use, query latency, locks, and rollback count;
- booking conflicts, unexpected healthy-state 5xx, and rate limits; and
- final seat, reservation-quota, admission-state, read-model, and cache-version
  reconciliation.

k6 HTTP metrics cannot infer a cache hit. Use the bounded Prometheus counters
from the API/worker plus dependency telemetry. Do not manufacture a hit ratio
from response latency.

## Failure run order

1. Record commit, images, host, topology, fixture cardinality, and all bounds.
2. Apply migration 7 twice and verify clean version/readiness.
3. Backfill the synthetic projection and run all reconciliations.
4. Record a no-load baseline and warm-up separately.
5. Start one bounded scenario and record exact injection time.
6. Inject one failure only: Redis restart, worker pause, API termination,
   projection lock, delayed outbox, or bounded invalidation storm.
7. Restore within a declared deadline and wait for dependency readiness.
8. Verify no partial projection, infinite retry, leaked goroutine symptom,
   unexpected source mutation, or booking invariant failure.
9. Preserve sanitized summaries outside git and remove disposable volumes.

## Acceptance

Healthy steady state requires zero unexpected 5xx. Redis outage must produce
safe PostgreSQL read fallback where defined; enabled hot-run admission retains
its Milestone 2 fail-closed policy. A stale availability hint must not create a
successful overlapping allocation. Projection readers must observe old-complete
or new-complete rows. Invalidation work and retries must stay bounded. Final
seat and read-model reconciliation must be clean.

Only measurements actually observed in an identified controlled run belong in
the benchmark report.
