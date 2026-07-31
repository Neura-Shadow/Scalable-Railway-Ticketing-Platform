# Milestone 5 Benchmark Report

## Status and evidence boundary

**Passed in a bounded disposable local environment.** The recorded run used
commit `f5d243ec91cf2444f5380eed3f774d6ee8047e05` and Compose configuration
SHA-256 `15a8df26fb0c149c5eee8f97100fbbf49bd35235b02f45d93e81808b78b5dbe6`.
Its canonical result is `canonical/milestone-5-summary.json`; raw k6 summaries
and command transcripts are under `raw/` in the same uncommitted, temporary
evidence bundle. The bundle was scanned before publication and contained no
DSN-, authorization-, or JWT-shaped values.

The topology contained one control PostgreSQL, two independent booking
PostgreSQL instances, Redis, and three API replicas. Docker was 29.6.2,
Docker Compose was 5.3.1, k6 was `grafana/k6:0.54.0`, PostgreSQL was
`postgres:16-alpine`, and Redis was `redis:7-alpine`. All ten scenarios used
synthetic fixtures and bounded iterations. The evidence runner destroyed the
containers and volumes with `docker compose down -v --remove-orphans` after
the run.

This is correctness and bounded performance evidence for a laptop-scale
pilot. It is not production-capacity, zero-downtime, national-scale,
multi-region, or exactly-once distributed-transaction evidence.

## Result table

| Scenario | Requests / iterations | Checks | HTTP p50 / p95 / p99 (ms) | Required observation |
| --- | ---: | ---: | ---: | --- |
| Physical routing | 24 / 12 | 24 / 0 failed | 39.881 / 183.477 / 204.398 | 12 physical-route successes; no conflict or unexpected 5xx |
| Cross-shard quota | 4 / 2 | 6 / 0 failed | 29.339 / 56.949 / 60.088 | 2 holds and 2 conservative rejections; no unexpected 5xx |
| Command recovery | 3 / 1 | 3 / 0 failed | 12.552 / 22.379 / 23.252 | one repaired command; no duplicate observation |
| Shard outage | 8 / 8 | 8 / 0 failed | 527.448 / 991.684 / 992.254 | 4 expected 503s and 2 healthy-shard commits; no fallback writer |
| Online base copy | 12 / 4 | 16 / 0 failed | 9.612 / 99.754 / 120.056 | 2 distinct source writes plus idempotent replays while copy ran; no duplicate observation |
| Journal catch-up | 6 / 3 | 9 / 0 failed | 30.798 / 91.784 / 98.213 | 3 source mutations; no duplicate apply effect |
| Physical cutover | 16 / 16 | 16 / 0 failed | 14.039 / 40.722 / 55.331 | 2 pause observations, 2 post-cutover successes, no split brain |
| Stale router | 6 / 3 | 6 / 0 failed | 46.990 / 142.427 / 149.228 | 3 refresh successes; no split brain |
| Reverse migration | 2 / 1 | 3 / 0 failed | 5.623 / 5.889 / 5.913 | target-era reservation preserved after reverse |
| Legacy comparison | 24 / 8 | 32 / 0 failed | 6.140 / 35.970 / 50.916 | 2 independent successes per path; no duplicate observation |

Each row is backed by `raw/<scenario>-summary.json`. Expected 409, 429, and
503 responses are workload outcomes rather than unexpected 5xx responses.

## Requested measurements

- Booking latency: the physical-routing workload measured p50 39.881 ms,
  p95 183.477 ms, and p99 204.398 ms across 24 requests.
- Physical-routing comparison: the isolated legacy/physical workload measured
  physical p50/p95/p99 of 5.338/43.504/52.917 ms and legacy
  6.258/31.013/35.274 ms. The corresponding physical-minus-legacy deltas were
  -0.921/+12.491/+17.643 ms. Twelve requests per path are too few for a
  production overhead claim.
- Global quota: the complete four-request quota workload measured
  p50/p95/p99 of 29.339/56.949/60.088 ms and never exceeded its configured
  quota.
- Command and control finalization: command-recovery HTTP p50/p95/p99 was
  12.552/22.379/23.252 ms; its bounded client-observed repair interval was
  289 ms. This is end-to-end repair latency, not a separately isolated SQL
  control-command overhead.
- Shard receipt/journal replay: three client replay observations measured
  p50/p95/p99 of 8.536/10.097/10.236 ms. These are client observations; the
  database replay rate is published by newer canonical evidence runs.
- Online capture: source writes during copy measured p50/p95/p99 of
  9.612/99.754/120.056 ms. Because fixture identity and execution phase differ
  from the legacy comparison, this report does not subtract the numbers or
  mislabel the difference as isolated journal-capture overhead.
- Final write pause: the durable driver measured 8503.620 ms against a
  30000 ms maximum. k6 clients observed two bounded pause intervals with
  median 6036 ms; the durable driver value is authoritative for the final
  quiesce measurement.
- Cutover and stale routing: there were two expected pause observations, zero
  split-brain observations, three stale-route refresh successes, and stale
  refresh p50/p95/p99 of 46.990/142.427/149.228 ms.
- Outage isolation: failed-shard requests returned four expected 503s while
  the healthy shard completed two independent writes. There were no fallback
  writer observations and no unexpected 5xx.
- Reverse migration: one target-era reservation was observed before reverse
  and preserved at generation 4 after reverse from generation 3. The k6
  recovery probe measured p50/p95/p99 of 5.623/5.889/5.913 ms; this is not the
  full operator migration duration.
- Reconciliation: dual-writer, assignment-ledger, directory, quota, journal
  gap, apply-receipt, command-receipt, and unreconciled-command counts were all
  exactly zero. Online copy observed 2 source mutation rows and 16 journal
  rows.

The recorded run did not put base-copy rows/second, server-side journal replay
rows/second and final lag, full reverse-migration duration, or PostgreSQL
connection counts in its canonical summary. The committed evidence contract
now queries those values directly from migration checkpoints and
`pg_stat_activity`, rejects missing/non-positive rates, requires final journal
lag to equal zero, and verifies every observed connection count is within the
server limit. They must be taken from a later passing
`canonical/migration-measurements.json` and
`canonical/database-invariants.json`, not inferred from this earlier snapshot.

## Invariants and limitations

The canonical database proof recorded one enabled Train C writer fence and
zero dual-writer violations. Target generation 3 accepted a successful write;
reverse generation 4 retained it. The topology was cleanly torn down and the
source was retained through the rollback window before controlled completion.

The run did not collect host CPU, host memory, disk throughput, image digests,
or long-duration saturation data. PostgreSQL image defaults and synthetic
fixture scale differ from a managed production deployment. The small sample
sizes are designed to prove state transitions and failure semantics, not to
establish capacity, tail-latency SLOs, or cost. Temporary evidence is not a
durable release archive; an operator must retain a sanitized bundle separately
if long-term provenance is required.
