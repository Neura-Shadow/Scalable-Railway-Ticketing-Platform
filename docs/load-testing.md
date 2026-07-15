# Milestone 1 Load Testing

The load suite is a reproducible correctness and measurement harness for a controlled, single-region environment. It is not evidence of national-scale capacity. Run it only against disposable test data: the reservation scenarios create, confirm, cancel, or intentionally leave holds for the expiration worker.

## Scenarios

| Script | Purpose | Cleanup behavior |
|---|---|---|
| `station-browse.js` | Public station-page reads | Read-only |
| `train-search.js` | Public route/date/class search | Read-only |
| `availability-read.js` | Point-in-time availability hints | Read-only |
| `reservation-normal.js` | Create, read, and cancel ordinary holds | Cancels successful holds |
| `reservation-hot-train.js` | One synchronized attempt per VU against one scarce train run | Leaves successful holds active for reconciliation |
| `reservation-idempotency.js` | Concurrent and repeated identical idempotency requests | Leaves the one resulting hold active |
| `reservation-confirm.js` | Hold, confirm, and cancellation cleanup | Cancels confirmed reservations |
| `reservation-expiration-storm.js` | Burst holds followed by worker-driven expiration verification | Relies on hold-expirer |
| `rate-limit.js` | Reservation-create pressure and 429 observation | Cancels successful holds |

## Required environment

Never commit real tokens. Inject short-lived test tokens from the shell or a secret manager.

| Variable | Used by |
|---|---|
| `BASE_URL` | All scenarios |
| `CUSTOMER_TOKEN` | Reservation scenarios |
| `ADMIN_TOKEN` | Reserved for authenticated administrative setup; no current scenario sends it |
| `OPERATOR_TOKEN` | Reserved for train-run setup/inspection; no current scenario sends it |
| `TRAIN_RUN_ID` | Availability and reservation scenarios |
| `ORIGIN_CODE`, `DESTINATION_CODE` | Search, availability, reservation scenarios |
| `SEAT_CLASS` | Search, availability, reservation scenarios |
| `PASSENGER_IDS` | Comma-separated passenger IDs owned by the customer token |
| `EXPECTED_SEAT_COUNT` | Hot-train upper-bound assertion |
| `VUS` | Virtual users; defaults to `1` |
| `DURATION` | Duration or one-attempt scenario deadline; defaults to `30s` |
| `SERVICE_DATE` | Train search, formatted `YYYY-MM-DD` |
| `IDEMPOTENCY_KEY` | Required shared non-production key for the idempotency scenario; use a new value per dataset/run |
| `EXPIRATION_WAIT_SECONDS` | Expiration observation delay; defaults to `20` |

Example PowerShell setup, using placeholders held only in the current process:

```powershell
$env:BASE_URL = 'http://localhost:8080'
$env:CUSTOMER_TOKEN = '<short-lived-test-token>'
$env:ADMIN_TOKEN = '<short-lived-test-token>'
$env:OPERATOR_TOKEN = '<short-lived-test-token>'
$env:TRAIN_RUN_ID = '<test-train-run-uuid>'
$env:ORIGIN_CODE = 'TPE'
$env:DESTINATION_CODE = 'KHH'
$env:SEAT_CLASS = 'standard'
$env:PASSENGER_IDS = '<passenger-uuid-1>,<passenger-uuid-2>'
$env:EXPECTED_SEAT_COUNT = '10'
$env:SERVICE_DATE = '2026-08-01'
$env:VUS = '10'
$env:DURATION = '30s'
```

## Running the suite

Run scenarios independently so each result has one workload shape:

```powershell
k6 run loadtest/k6/station-browse.js
k6 run loadtest/k6/train-search.js
k6 run loadtest/k6/availability-read.js
k6 run loadtest/k6/reservation-normal.js
k6 run loadtest/k6/reservation-hot-train.js
k6 run loadtest/k6/reservation-idempotency.js
k6 run loadtest/k6/reservation-confirm.js
k6 run loadtest/k6/reservation-expiration-storm.js
k6 run loadtest/k6/rate-limit.js
```

For expiration testing, use a disposable environment with a short `RESERVATION_HOLD_TTL_SECONDS`, enable the `hold-expirer`, and set `EXPIRATION_WAIT_SECONDS` longer than the hold TTL plus worker interval. Do not weaken production expiry settings merely to run this scenario.

The hot-train script performs exactly one overlapping attempt per VU and asserts `successful_holds <= EXPECTED_SEAT_COUNT`. Run it against a fresh train run or reconcile and clear test holds before repeating it. The idempotency script deliberately shares one test key and payload and asserts that both responses reference the same reservation.

## Measurements

k6 reports request rate and `http_req_duration` with median, p90, p95, and p99. Custom counters provide:

- `http_4xx`, `http_5xx`, and `unexpected_5xx`;
- `allocation_conflicts` and `rate_limited`;
- `successful_holds`, `confirmed_reservations`, `expired_holds`, and `cancelled_holds`;
- `idempotency_mismatches`; and
- `reservation_duration`.

Record these external observations alongside the k6 summary:

- PostgreSQL pool active/idle/waiting connections and database saturation;
- Redis command latency and errors, if Redis is used;
- outbox pending, processing, stale, and dead-letter counts;
- API and worker CPU/memory;
- deployment replica count and resource limits; and
- the exact commit, image digest, dataset, database/Redis versions, region, and network topology.

## Correctness gates

HTTP results alone cannot prove booking correctness. After every write scenario:

1. run the repository reconciliation invariant against the tested train run;
2. verify no active reservation-seat masks overlap;
3. verify stored inventory equals the union of held/confirmed masks;
4. verify expired/cancelled masks are absent;
5. verify one resource for the shared idempotency request;
6. compare successful hot-train holds with the seeded physical seat count; and
7. inspect outbox rows and worker state for bounded failures.

Acceptance requires zero unexpected 5xx, no duplicate idempotent resource, no over-allocation, no leaked masks, and a passing final reconciliation. Allocation conflicts and 429 responses are bounded domain/control outcomes, not server errors. p95 and p99 must come from the executed run and must never be guessed.

## Result capture

Copy only aggregated, sanitized results into [benchmark-report-milestone-1.md](benchmark-report-milestone-1.md). Do not commit raw tokens, request bodies, passenger identifiers, database dumps, Prometheus label values containing IDs, or temporary k6 result files.
