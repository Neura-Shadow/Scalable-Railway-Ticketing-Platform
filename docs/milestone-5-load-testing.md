# Milestone 5 Load and Failure Testing

## Evidence contract

Run Milestone 5 scenarios only against the disposable three-PostgreSQL
topology with a recorded commit, image digests, configuration profile, fixture,
host CPU/memory/disk, PostgreSQL versions/settings, pool caps, k6 version, start
and end timestamps, and raw artifacts. Redact credentials, tokens, passenger
data and infrastructure endpoints.

The scenarios are bounded correctness and degradation tests. They do not prove
production capacity, national-scale throughput, zero downtime, multi-region
availability, or an RPO/RTO. A passing HTTP threshold is insufficient unless
database invariants and post-run reconciliation also pass.

## Required scenario matrix

| Script | Primary assertion |
| --- | --- |
| `physical-shard-routing.js` | current assignment plus local fence selects one physical writer; 100 same-idempotency requests across three API replicas converge to one physical reservation |
| `cross-shard-global-quota.js` | parallel shard commands cannot exceed control quota |
| `booking-command-recovery.js` | lost finalization/retry converges to one reservation |
| `physical-shard-outage.js` | failed assigned shard is isolated; no fallback writer |
| `online-base-copy.js` | source serves while bounded resumable copy advances |
| `journal-catchup.js` | concurrent mutations reach target through unique apply receipts |
| `physical-cutover.js` | source-disable precedes target-enable and pause is measured |
| `stale-router-physical.js` | stale generation rejects before mutation and refreshes once |
| `reverse-migration.js` | target-era write survives a newer-generation reverse migration |
| `legacy-vs-physical.js` | legacy compatibility and physical path are compared without capacity extrapolation |

## Deterministic execution

Use seeded fixtures and explicit barriers/failure hooks for command commit,
control finalization, capture start, snapshot established, copy checkpoint,
journal high watermark, source disable, target enable, and control switch. Do
not coordinate concurrency with arbitrary sleeps. Bound VUs, duration, request
rate, retry count, response size, batch size, and artifact size.

The disposable acceptance driver deliberately stops Redis only while the
100-request same-idempotency probe runs. This exercises the reservation
limiter's documented fail-open path so all 100 requests reach the real control
and booking-shard PostgreSQL instances instead of being absorbed by the public
per-owner rate limit. The driver restores Redis and waits for `PONG` before it
proves waiting-room admission token issuance, physical reservation creation,
and shard-local hold expiration. This bounded evidence technique is not a
production rate-limiter or availability recommendation.

Before and after each run, capture assignments, both local fences, command and
receipt counts, quota/directory state, inventory-mask uniqueness, outbox/event
IDs, journal/apply watermarks, target-write evidence, and a complete detect-only
reconciliation report. Abort on truncated evidence.

## Reporting

The canonical machine-readable result must distinguish `passed`, `failed`,
`blocked`, and `not_run`; raw console transcripts are `.log`, not JSON. Report
latency percentiles, error categories, throughput, pool pressure, copy/replay
rates, lag, write-pause duration and host limits only when actually observed.
The current summary is [the Milestone 5 benchmark report](benchmark-report-milestone-5.md).
