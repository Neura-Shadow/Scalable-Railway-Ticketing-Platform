# Milestone 4 Benchmark Report

## Evidence status

**Pending. No Milestone 4 runtime or benchmark result is accepted in this
report.** The tables below are an evidence template and must not be populated
from estimates, unit tests, `k6 inspect`, earlier milestones, or an unrecorded
smoke run.

Milestone 4 is a same-cluster logical schema-sharding proof of concept. Even a
completed local run would not establish physical-shard isolation, production
sizing, zero-downtime migration, multi-region behavior, or national-scale
capacity.

## Commands pending execution

The scripts, Migration 8, `shard-admin`, multi-replica topology, and
reconciliation scopes are present in the work-in-progress branch. Execute only
after focused validation and from a clean committed tree:

```powershell
$scripts = @(
  'shard-routing.js',
  'shard-route-cache.js',
  'shard-route-prewarm.js',
  'two-hot-train-shards.js',
  'shard-cutover.js',
  'stale-router-refresh.js',
  'shard-post-cutover-lifecycle.js',
  'shard-outage-isolation.js',
  'cross-shard-admin.js',
  'legacy-vs-schema-shard.js'
)
foreach ($script in $scripts) {
  k6 inspect "loadtest/k6/$script"
}
```

After injecting the documented synthetic environment and satisfying each
scenario's precondition, run one scenario at a time:

```powershell
k6 run loadtest/k6/shard-routing.js
k6 run loadtest/k6/shard-route-cache.js
k6 run loadtest/k6/shard-route-prewarm.js
k6 run loadtest/k6/two-hot-train-shards.js
k6 run loadtest/k6/shard-cutover.js
k6 run loadtest/k6/stale-router-refresh.js
k6 run loadtest/k6/shard-post-cutover-lifecycle.js
k6 run loadtest/k6/shard-outage-isolation.js
k6 run loadtest/k6/cross-shard-admin.js
k6 run loadtest/k6/legacy-vs-schema-shard.js
```

The coordinated bounded runner supplies fixtures, external transitions,
reconciliation, and sanitized evidence:

```powershell
pwsh -File scripts/run-milestone-4-multi-replica-evidence.ps1
```

A runner `status=passed` is a bounded functional/failure-recovery and latency
smoke. It is not, by itself, an accepted sustained benchmark, CI verdict,
release verdict, or production-capacity result.

The canonical `milestone-4-summary.json` is valid only when it was published
after required teardown and artifact sanitization. A teardown, sanitization, or
summary-finalization failure must leave no canonical passed summary.

Transition/failure scripts observe an external controller; they do not receive
infrastructure or operator credentials. Exact variables must follow
[Milestone 4 load testing](milestone-4-load-testing.md) and each implemented
script's contract. Do not commit expanded secrets, identifiers, or raw output.

## Run identity

| Field | Accepted value |
|---|---|
| Source commit / image digest | Pending |
| Run date and timezone | Pending |
| Host/OS/container runtime | Pending |
| CPU and memory limits | Pending |
| PostgreSQL/Redis/k6 versions | Pending |
| API/admission/read-model replicas | Pending |
| Logical storage topology | Pending |
| Fixture train runs/inventory/reservations | Pending |
| Route-cache and fanout bounds | Pending |
| Migration batch/quiesce/rollback bounds | Pending |
| Warm-up, duration, VUs/rate, repetitions | Pending |

## Performance measurements

Client latency and achieved rate must be accompanied by server/dependency
telemetry. `Pending` means no claim.

| Scenario | Requests | Achieved rate | p50 ms | p95 ms | p99 ms | Unexpected 5xx | Status |
|---|---:|---:|---:|---:|---:|---:|---|
| Shard routing | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Route cache | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Two hot train shards | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Cutover window | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Stale-router refresh | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Catalog-disabled logical route | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Customer cross-route batch read | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Legacy versus schema | Pending | Pending | Pending | Pending | Pending | Pending | Pending |

Setup and correctness probes are not performance rows:

| Probe | Accepted observation | Status |
|---|---|---|
| Per-replica source-route prewarm | Each selected API starts with the intended source route; all earlier train-run events are durably receipted before the version baseline | Pending |
| Cutover namespace attribution | The unique generation-bound `shard_cutover` event is durably receipted before accepting a Redis namespace change | Pending |
| Post-cutover lifecycle | Create/read/replay/confirm/cancel and ticket reads succeed on target | Pending |
| Post-cutover expiration | Runner arms a target hold, shard-aware expiry runs, and locator GET confirms expired | Pending |

Do not report p95/p99 from a one-iteration setup or lifecycle probe as a
performance distribution. Every accepted percentile requires request count,
duration, and repetitions.

## Migration and routing measurements

| Measurement | Accepted observation |
|---|---|
| Legacy-path latency distribution | Pending |
| Schema-path latency distribution | Pending |
| Routing overhead on like-for-like fixture | Pending |
| Route-cache hit/miss/eviction counts | Pending |
| Stale-assignment rejects | Pending |
| Refresh latency and successful bounded retries | Pending |
| Copy rows/second by table group | Pending |
| Validation rows/duration | Pending |
| Source-fence-to-target-ready interruption | Pending |
| First/last retryable cutover rejection | Pending |
| Locator rows and cutover transaction duration | Pending |
| Admin fanout complete/partial/recovered and duration | Pending |
| Admin fanout unavailable result | Pending; requires separate injection |

## Correctness and recovery gates

| Gate | Acceptance | Evidence |
|---|---|---|
| Single writer | Never more than one write-enabled fence | Pending |
| Stale replicas | No successful source write after cutover | Pending |
| Retained source | Six table counts and fingerprints remain unchanged while copied holds transition only on their targets | Pending |
| Inventory | No overlapping allocation; exact mask reconciliation | Pending |
| Idempotency | No duplicate resource; conflict/replay stable across move | Pending |
| Locator | Complete current route; no shard scan or partial switch | Pending |
| Quota/admission | No leaked claim, token, or inflight lease on rejection | Pending |
| Migration resume | Partial copy unroutable; retry idempotent | Pending |
| Rollback restriction | Target write blocks direct rollback | Pending |
| Catalog-disabled route | Disabled route bounded; enabled peer functional while PostgreSQL remains healthy | Pending |
| Admin fanout | Serial bounded traversal reports complete/partial/recovered explicitly | Pending |
| Post-cutover expiration | Target-side expiry transition and reconciliation succeed | Pending |
| Final reconciliation | Every required scope complete and clean | Pending |
| Leakage | No credential, topology, identifier, or PII leak | Pending |

## Dependency telemetry

Record PostgreSQL pool use, transaction/query latency, lock waits, deadlocks,
rollbacks, CPU, I/O, disk amplification, and connection exhaustion signals.
Record Redis latency/errors only where admission or cache behavior participates.
All values are pending.

The bounded runner currently supplies custom k6 latency/correctness metrics,
strict check and request/sample/iteration counts, achieved rates, one-run
duration context, aggregate migration timing, both cutovers' generation
rotation, retained-source fingerprints, reconciliation summaries, PostgreSQL
connection samples, and Redis PING latency. Those are only a subset of this
section.

Accepted sustained benchmark evidence still requires repeated achieved-rate
measurements, per-copy-group throughput, independent warm-up, host/runtime
versions and limits, full PostgreSQL telemetry, Redis errors, and dependency
saturation.

## Verdict

No Milestone 4 benchmark verdict is available. Acceptance requires a controlled,
identified, repeatable run with complete reconciliation and preserved sanitized
evidence. Until then, no routing-overhead, cutover-duration, throughput,
capacity, or failure-isolation claim is supported.
