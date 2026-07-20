# Milestone 2 Benchmark Report

## Evidence status

No controlled Milestone 2 load or sustained multi-replica benchmark has been
executed for this report. Capacity, latency, throughput, queue-depth, database,
Redis, outbox, DLQ, and reconciliation result fields are therefore **not
measured**. This template must not be interpreted as production capacity
evidence.

Syntax checks, unit tests, container configuration, or a functional smoke are
not substitutes for sustained evidence. No national-scale,
12306-equivalent, global-fairness, or multi-region claim is made.

## Run identity

| Field | Observed value |
|---|---|
| Commit SHA | Not run |
| UTC start/end | Not run |
| Operator | Not run |
| Environment and region | Not run |
| API/worker replicas | Not run |
| PostgreSQL/Redis versions and topology | Not run |
| Host CPU, memory, disk, network | Not run |
| Dataset and seat counts | Not run |
| Policy limits | Not run |
| Quota/backpressure settings | Not run |
| k6 version and script commit | Not run |

## Functional smoke

| Scenario | Status | Evidence reference |
|---|---|---|
| Waiting-room join/status/cancel | Not run | Pending |
| Duplicate and mismatch join | Not run | Pending |
| Admission issuance/delivery | Not run | Pending |
| Hot reservation and idempotent replay | Not run | Pending |
| Durable quota | Not run | Pending |
| Redis outage fail-closed | Not run | Pending |
| API/worker termination | Not run | Pending |
| Final reconciliation | Not run | Pending |

## Sustained measurements

| Measurement | Result |
|---|---|
| Join RPS and p50/p95/p99 | Not measured |
| Status RPS and p50/p95/p99 | Not measured |
| Queue depth maximum | Not measured |
| Admission issuance rate | Not measured |
| Inflight admission maximum | Not measured |
| Join-to-admission wait p50/p95/p99 | Not measured |
| Reservation RPS and p50/p95/p99 | Not measured |
| Token conflicts | Not measured |
| Quota rejects | Not measured |
| Backpressure rejects | Not measured |
| Seat conflicts | Not measured |
| Unexpected healthy-state 5xx | Not measured |
| PostgreSQL connections/lock waits | Not measured |
| Redis latency/errors/AOF state | Not measured |
| Outbox backlog and DLQ growth | Not measured |

## Correctness verdict

| Gate | Result |
|---|---|
| No duplicate active entry | Not evaluated |
| No wrong owner/request token accepted | Not evaluated |
| Global admission rate respected | Not evaluated |
| Inflight bound respected | Not evaluated |
| Durable quota respected | Not evaluated |
| Queue capacity bounded | Not evaluated |
| No overlapping seat allocation | Not evaluated |
| Seat/quota/admission reconciliation clean | Not evaluated |

## Required completion notes

When a controlled run occurs, replace `Not run` and `Not measured` only with
observed values and link sanitized raw summaries stored outside git. Record
warm-up, steady-state duration, repetitions, failure injection, caveats, and
variance. Distinguish functional smoke from sustained results and retain failed
runs rather than selecting only a favorable sample.

The current honest verdict is: the repository provides an evidence procedure,
but it records no accepted Milestone 2 performance result.
