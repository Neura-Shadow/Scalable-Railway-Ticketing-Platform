# Milestone 4 Load Testing

## Evidence status

The Milestone 4 scripts, CLI, migration, and multi-replica harness are present
in the work-in-progress branch. Final controlled runtime, CI, independent
review, release acceptance, throughput, and latency results remain pending.

All fixtures must be synthetic and disposable. Scripts receive customer
credentials/tokens through the environment and must not print or persist them.
Infrastructure failure injection and operator commands stay in an external
controller, not in k6 scripts.

## Evidence levels

- `k6 inspect` proves parsing and option construction only.
- A bounded smoke proves selected HTTP outcomes and post-run invariants in one
  identified environment.
- A failure/recovery run additionally records a controlled transition and
  recovery deadline.
- A sustained benchmark requires recorded hardware, dataset, database/Redis
  versions, replicas, warm-up, duration, repetitions, server telemetry, and
  clean reconciliation.

No level by itself proves physical-shard isolation, zero-downtime migration,
production sizing, multi-region behavior, or national-scale capacity.

## Automated bounded runner

The repository runner requires a clean committed tree and writes sanitized
evidence outside Git:

```powershell
pwsh -File scripts/run-milestone-4-multi-replica-evidence.ps1
```

A runner `status=passed` is a bounded functional/failure-recovery and latency
smoke for its exact commit, synthetic fixture, topology, and duration. It is
not a sustained benchmark or release verdict.

## Required scenarios

| Script | Role | Purpose | Required precondition |
|---|---|---|---|
| `shard-routing.js` | Measurement | Exercise one-storage routing and booking latency | Synthetic runs assigned across fixed storages |
| `shard-route-cache.js` | Measurement | Compare bounded cache hit/miss/refresh observations | Cache telemetry and identical controlled routes |
| `shard-route-prewarm.js` | Setup probe | Warm one API replica on the source route | Direct private access to one API replica |
| `two-hot-train-shards.js` | Measurement | Exercise two hot train runs on different logical shards | Admission fixtures and sufficient synthetic inventory |
| `shard-cutover.js` | Transition smoke | Observe retryable interruption during external cutover | Fully copied `cutover_ready` migration |
| `stale-router-refresh.js` | Correctness probe | Observe three stale replicas reject, refresh once, and reach target | Three APIs prewarmed on the source route |
| `shard-post-cutover-lifecycle.js` | Correctness probe | Exercise bounded target lifecycle and ticket reads | Completed cutover and two synthetic customers |
| `shard-outage-isolation.js` | Failure smoke | Compare a catalog-disabled logical route with an enabled peer | Reversible catalog-state injection |
| `cross-shard-admin.js` | Customer-path smoke | Batch-read two customer reservations on different routes | Customer token and two owned reservation IDs |
| `legacy-vs-schema-shard.js` | Measurement | Compare like-for-like legacy/schema overhead | Equivalent fixtures and identical request mix |

Despite its historical filename, `cross-shard-admin.js` is an authenticated
customer cross-route batch read. It is not an admin endpoint or admin-fanout
test.

Admin fanout evidence comes from the private `reconcile shard-assignments` CLI.
It traverses the fixed allowlist serially, so effective concurrency is `1`.
The bounded runner records complete, partial, and recovered results.

The runner does not inject an all-shards-unavailable admin result. That status
requires separate bounded evidence before it is reported as observed.

## Lifecycle coverage

| Phase | Dynamically covered | Not dynamically established by that probe |
|---|---|---|
| Before migration | Held, confirmed, cancelled, and expired fixtures; ticket, idempotency, and outbox state copied and reconciled | Production data distribution and long-route cardinality |
| After cutover | Create/read/replay/confirm/cancel/ticket reads; copied pre-cutover holds confirmed/cancelled through the target; retained legacy-source six-table fingerprints unchanged; runner-created target hold armed in target schema; shard-aware expiry confirmed by locator GET; final reconciliation | Dedicated read-model lifecycle workload |

Final read-model reconciliation is an indirect post-run invariant. It is not a
dedicated post-cutover lifecycle projection workload.

## Concurrency evidence boundary

The deterministic PostgreSQL suite has two explicit gates. The routing gate
drives 100 concurrent routed-transaction/fencing attempts around cutover. The
full-booking gate drives 100 distinct-user `CreateHold` commands through the
same controlled stale-route boundary across three caches, requires 100 bounded
refreshes, exactly one target reservation for the one-seat fixture, zero legacy
reservation mutations, no duplicate reservation ID, no overlapping allocation,
and a clean target reconciliation. This is bounded correctness evidence, not a
throughput or production-capacity result.

The bounded runtime runner separately exercises three prewarmed stale replicas
and a short cutover workload. Before recording each availability-version
baseline, it requires durable `railway-read-model` receipts for all earlier
events of that train run. After cutover it identifies the unique
`trainrun.updated` event whose reason is `shard_cutover`, waits for that exact
event's receipt, and only then accepts a changed Redis namespace as cutover
evidence. Do not merge these observations or the deterministic 100-request test
into a throughput claim.

For the public `CreateHold` path, the shard-local idempotency replay lookup is
the first routed database transaction. A cached legacy generation is therefore
rejected on that preflight read, the replica performs one authoritative refresh,
and the booking write starts only on the target. The runtime gate requires this
sequence on all three APIs, plus a zero legacy-write delta and a successful
target-write delta. It does not relabel the preflight rejection as a stale
write. Direct stale-write fencing remains covered by the deterministic
100-attempt routed-transaction gate described above.

Parse all scripts before a runtime attempt:

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

## Common environment

Use only synthetic IDs and short bounded defaults. Exact scenario variables
are authoritative in each implemented script.

```powershell
$env:BASE_URL = 'http://127.0.0.1:<ephemeral-port>'
$env:TRAIN_RUN_ID = '<synthetic-train-run-id>'
$env:SECOND_TRAIN_RUN_ID = '<synthetic-train-run-id>'
$env:VUS = '5'
$env:DURATION = '15s'
k6 run loadtest/k6/shard-routing.js
```

Do not commit expanded tokens, passenger IDs, migration IDs, database URLs,
operator credentials, raw k6 output, or machine-specific paths.

## Run sequence

1. Record source commit, image digests, host, topology, versions, resource
   limits, fixture cardinality, configuration bounds, and clock source.
2. Apply Migration 8 twice, verify clean version, and prove explicit legacy
   assignments/locators for the populated fixture.
3. Start the no-sticky-session topology: three APIs, two admission workers, two
   read-model workers, hold-expirer, central outbox worker, one PostgreSQL with
   three logical storages, one Redis, and the reverse proxy.
4. Prove health and run all pre-test reconciliation scopes.
5. Record no-load baseline and warm-up separately from measurements.
6. Run one scenario at a time with synchronized client/server telemetry.
7. For transition tests, record the exact drain, source-fence-disable, cutover,
   fault, restore, and readiness timestamps from the external controller.
8. Stop load before reconciliation. Prove assignment/fence, locator, inventory,
   quota, idempotency, ticket, outbox, admission, read-model, and cache
   invariants.
9. Preserve sanitized evidence outside Git and remove disposable fixtures and
   volumes through the approved harness.

## Required measurements

- booking p50/p95/p99 and achieved request rate;
- like-for-like legacy and schema-path p50/p95/p99;
- route-cache hit/miss/eviction and route-refresh counts;
- stale-assignment rejects and refresh latency;
- retryable cutover rejection interval, including first/last rejection;
- migration rows per second by copy group and validation duration;
- catalog-disabled-route affected/unaffected success and unexpected 5xx
  counts;
- fanout duration and complete/partial/unavailable counts;
- PostgreSQL pool use, transaction/query latency, locks, deadlocks, CPU, I/O,
  disk, and rollback counts;
- Redis latency/errors where admission or cache behavior participates; and
- final reconciliation status.

Do not infer a cache hit, current owner, or successful fence from client latency
alone. Use bounded server metrics and database evidence.

## Runner and benchmark measurement boundary

The bounded runner records custom k6 latency/correctness metrics, routing and
cache deltas, strict check pass/fail counts, request/sample/iteration counts,
achieved rates, derived measurement duration, stale refresh observations,
aggregate migration timing, both train runs' assignment/availability namespace
rotation, retained-source fingerprints, admin completeness, reconciliation,
PostgreSQL connection samples, and Redis PING.

It does not by itself provide per-copy-group throughput, independent warm-up or
repeated runs, an all-unavailable fanout run, or complete host and dependency
telemetry. A single runner invocation records `repetitions=1`; one-iteration
setup and lifecycle probes are explicitly marked `not_distribution`.

CI uploads the complete evidence directory only after the runner's final
post-teardown sanitization marker passes. The runner builds its summary in
memory, scans a non-canonical candidate only after required teardown succeeds,
and publishes canonical `status=passed` evidence only after both gates. A
teardown, sanitization, or summary-finalization failure leaves no canonical
passed summary. Failure diagnostics suppress raw artifact contents unless the
sanitization marker is present and successful.

A sustained benchmark must additionally record hardware and runtime versions,
resource limits, PostgreSQL pool/query/transaction/lock/deadlock/CPU/I/O/disk
signals, Redis errors, and repeated request-rate measurements.

Do not treat p95/p99 from one-iteration setup or lifecycle probes as
performance distributions. Report sample count, duration, and repetitions with
every accepted percentile.

## Acceptance gates

- no source and target are write-enabled together;
- no successful source mutation occurs after cutover;
- no overlapping seat allocation or duplicate reservation occurs;
- stale replicas either refresh once to target or fail boundedly;
- failed/migrating attempts do not strand idempotency, quota, or admission
  state;
- unaffected logical-route requests continue while a peer route is disabled in
  the catalog and the shared PostgreSQL engine remains healthy;
- admin partial results are explicitly partial and stay within all bounds;
- no unexpected 5xx occurs outside declared failure windows;
- every final reconciliation is complete and clean; and
- the report states routing overhead and cutover interruption honestly.

Only observed, reproducible, accepted results belong in
[the benchmark report](benchmark-report-milestone-4.md).
