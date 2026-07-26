# Milestone 4 Benchmark Report

## Evidence status

**Passed as a bounded local functional, failure-recovery, and latency smoke.**
The accepted run used clean committed source
`85011c851e6b42063f1a2551833a6a92de224891` and completed with canonical
`status=passed`. Every required k6 check passed; all 11 final reconciliation
artifacts ended healthy or complete, and the six bounded shard reports were
non-truncated with zero violations. Artifact sanitization passed after teardown,
and the Compose project left no containers, volumes, or networks behind.

This is not a production-capacity benchmark. Milestone 4 deliberately uses
three logical booking schemas in one PostgreSQL process. The run does not prove
physical database-host isolation, sustained capacity, zero-downtime migration,
multi-region behavior, or national-scale throughput.

## Reproduction command and provenance

The accepted parameters can be reproduced without embedding a workstation
username as follows:

```powershell
$evidence = Join-Path ([System.IO.Path]::GetTempPath()) `
  'railway-m4-evidence-85011c8-final'
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File scripts/run-milestone-4-multi-replica-evidence.ps1 `
  -CustomerCount 20 `
  -LoadDuration 15s `
  -ProjectName railway-m4-85011c8-final `
  -EvidenceDirectory $evidence
```

The canonical artifact is `milestone-4-summary.json`, generated only after the
runner completed teardown and scanned the evidence directory. Its SHA-256 is
`1D360D69343447E1CA72EAF34E842C564C344E5EED151F9B7D397EEA53E5B4AF`.
The companion `integrity-evidence.json` SHA-256 is
`BB606A2FEA21A70150409D3966F37B5064CFB03E649D7B04658C3FA860C29C68`.
Raw artifacts remain local and are not committed because they contain
synthetic identifiers and expanded runtime detail.

The command-level transcripts carrying a `*.json` suffix intentionally
preserve Docker Compose progress lines before their final structured envelope,
so 35 of 61 files with that suffix are not strict whole-file JSON. The runner
parses the final JSON envelope and publishes the normalized, strict canonical
summary used for this report. Consumers must use the canonical summary or
extract the final envelope; this is a machine-readability limitation of the raw
diagnostic captures.

## Run identity

| Field | Accepted value |
|---|---|
| Source commit | `85011c851e6b42063f1a2551833a6a92de224891`; clean committed tree captured before build |
| Representative image digests | API-1 `sha256:eb74301a9cbcfdf322baa3d4d0fc0de01bc385453549074c6a80343df5c0ad57`; reconcile `sha256:31dd90e95368cbfbc723c6842d032d773d9262123f24aada7598e253a504482c`; shard-admin `sha256:b80ad175b99e2bf834aee8b3a5eadc0de330c6ff97fde5bab23065bb25dbf25d` |
| Run completion | 2026-07-26 13:50:09 UTC / 2026-07-26 21:50:09 Asia/Taipei |
| Host | Windows 11 build 22631; Intel Core i7-13700HX; 24 logical processors; 31.73 GiB host memory |
| Container runtime | Docker Engine 29.6.2, Linux/amd64, overlay2; 24 CPUs and 15.48 GiB visible to Docker |
| Service versions | PostgreSQL `16-alpine`; Redis `7-alpine`; k6 `0.54.0`; Go build toolchain `go1.25.12` |
| Resource limits | No per-service CPU or memory limits were configured; host saturation was not measured |
| Runtime replicas | 3 API, 2 admission workers, 2 read-model workers, 1 hold expirer, 1 outbox worker |
| Storage topology | `public`, `booking_shard_0`, and `booking_shard_1` in one PostgreSQL process |
| Fixture | 2 train runs; pre-copy lifecycle included 2 each of held, confirmed, cancelled, and expired reservations, plus 2 orders and 2 tickets |
| Route/cache bounds | 30-second route-cache TTL, 1,000-entry process-local bound; 20 synthetic customers |
| Migration bounds | Batch size 100, command timeout 30 seconds, rollback window 5 minutes; reconciliation page size 100 and max 100,000 rows |
| Load shape | One bounded run per scenario; primary hot-shard duration 15 seconds; scenario-specific VUs and durations are preserved in k6 summaries |

Host and Docker resource details were queried immediately after the run on the
same machine. They are environment context, not utilization telemetry.

## Performance measurements

All values below come from the canonical summary. Expected rebalancing and
outage `503` responses are reported separately and are not counted as
unexpected `5xx` failures.

| Scenario | Requests | Achieved req/s | p50 ms | p95 ms | p99 ms | Expected 503 | Unexpected 5xx | Checks | Status |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Shard routing | 96 | 51.613 | 2.671 | 29.673 | 47.780 | 0 | 0 | 144/144 | Passed |
| Route cache | 222 | 12.725 | 2.604 | 8.850 | 22.856 | 0 | 0 | 444/444 | Passed |
| Two hot train shards | 2,730 | 180.938 | 3.758 | 9.041 | 19.736 | 0 | 0 | 2,730/2,730 | Passed |
| Cutover window | 50 | 4.838 | 16.514 | 133.907 | 166.291 | 10 rebalancing | 0 | 50/50 | Passed |
| Legacy versus schema | 1,152 | 66.544 | 3.054 | 5.280 | 24.466 | 0 | 0 | 1,152/1,152 | Passed |
| Customer cross-route, healthy | 364 | 36.129 | 7.064 | 15.080 | 19.123 | 0 | 0 | 182/182 | Passed |
| Catalog-disabled logical route | 854 | 49.103 | 4.933 | 7.605 | 9.191 | 429 outage responses | 0 | 854/854 | Passed |
| Customer cross-route, partial | 376 | 37.572 | 4.961 | 7.121 | 7.880 | 188 outage responses | 0 | 188/188 | Passed |

Each distribution above has an explicit request count and duration in its k6
summary. The two-hot-train result is the main 15-second throughput smoke; it is
not a sustained capacity number.

The configured common threshold contract requires zero unexpected `5xx`, zero
duplicate-identity mismatches, request p95 below 2,000 ms, request p99 below
5,000 ms, and a check rate above 0.95. Correctness-only scenarios strengthen
the check rate to exactly 1 and add exact or positive-count success predicates.
Routing, cache, and cutover also require a positive routing success count and
apply the same p95/p99 bounds to successful bookings. Legacy/schema and the two
hot-train workload apply the p95/p99 limits independently to both branches.
All configured thresholds passed.

The bounded accepted response mix was: shard routing 12 successes, 66
rate-limited responses, and 18 allocation conflicts; route cache 30 successes
and 192 rate-limited responses; cutover 5 successes, 10 expected rebalancing
HTTP 503 responses, and 35 allocation conflicts. k6 may count accepted
non-2xx responses in its raw `http_req_failed` metric, so this report claims
100% scenario checks and zero unexpected `5xx`, not a zero raw HTTP failure
rate. Achieved rate, maximum latency, copy throughput, command duration, and
the rejection window had no configured acceptance threshold.

## Functional probes

Setup and lifecycle probes are correctness observations, not meaningful
latency distributions.

| Probe | Observation | Status |
|---|---|---|
| Per-replica source-route prewarm | All three API processes restarted and each completed an exact source-route prewarm before cutover | Passed |
| Stale-router refresh | 3 requests; 3 stale preflight rejects; 3 successful refreshes; 0 source writes; 3 target writes; mean refresh 0.554 ms | Passed |
| Availability namespace rotation | Train A receipted 17 prior events and train B 12; each then receipted the exact generation-bound cutover event and rotated generation 1 to 2 | Passed |
| Post-cutover lifecycle | 13 requests and 34/34 checks covered create, replay, GET, confirm, cancel, copied-hold transitions, and ticket-order read | Passed |
| Post-cutover expiration | Target hold was armed, shard-aware expiry ran, and locator GET observed `expired` | Passed |

## Migration and routing measurements

| Measurement | Accepted observation |
|---|---|
| Legacy-path latency | p50 3.040 ms, p95 5.160 ms, p99 20.731 ms |
| Schema-path latency | p50 3.084 ms, p95 5.697 ms, p99 33.464 ms; one 1,421.456 ms maximum outlier |
| Like-for-like interpretation | Median and p95 were close in this bounded fixture, but the single run and schema maximum do not support a general routing-overhead claim |
| Route-cache result | 33 lookups, 33 hits, 0 misses during the measured delta; hit ratio 1.0; total hit counter 92 |
| Stale assignments | Exactly one stale preflight rejection and one successful refresh on each of 3 API replicas; no successful source write |
| Train A copy | 123 rows in 5,426.674 ms, 22.666 rows/s; validation examined 246 rows in 901.355 ms |
| Train B copy | 108 rows in 5,571.341 ms, 19.385 rows/s; validation examined 216 rows in 1,037.536 ms |
| Cutover command | Train A 926.401 ms; train B 915.183 ms |
| Observed retryable interruption | 10 explicit rebalancing rejections across a 1,045.560 ms window |
| Admin fanout | Complete before: 803.527 ms, 3/3 healthy; partial while shard-0 catalog-disabled: 903.663 ms, 2 healthy/1 unavailable; complete after restore: 848.011 ms, 3/3 healthy |

## Correctness and recovery gates

| Gate | Accepted evidence | Status |
|---|---|---|
| Single writer | 2 assignments, 2 enabled authoritative fences, 0 mismatched fences, 0 enabled legacy fences | Passed |
| Stale replicas | Each API rejected its stale generation, refreshed once, wrote zero times to source, then wrote once to target | Passed |
| Retained source | Six-table counts and fingerprints stayed unchanged for both train runs while copied reservations transitioned only on targets | Passed |
| Inventory | 0 overlap violations and zero seat-mask reconciliation violations | Passed |
| Idempotency | 0 duplicate authoritative reservation IDs; claim routes and local records reconciled cleanly | Passed |
| Locator | 0 missing or stale reservation, ticket-order, or ticket locators; shard-locator scope complete with 0 violations | Passed |
| Quota/admission | Migration quota structural/cardinality checks were clean; admission reconciliation reported healthy with 0 violations | Passed |
| Migration resume | Each migration advanced through six bounded copy invocations to complete validation; copied/audited rows matched | Passed |
| Rollback restriction evidence | Two target-generation evidence rows recorded 16 successful target writes. Runtime ended in rollback windows and did not attempt a direct rollback; enforcement is covered by automated tests | Qualified |
| Catalog-disabled route | Healthy logical peer remained functional; affected route returned 429 expected HTTP 503 responses; operator health degraded then recovered | Passed |
| Admin fanout | Complete, explicit partial, and recovered states were all observed without truncation | Passed |
| Post-cutover expiration | Target-side expiry and final locator/reconciliation checks succeeded | Passed |
| Final reconciliation | All 11 required artifacts ended healthy/complete; the six bounded shard reports had 0 violations and `truncated=false`, while the five specialized artifacts passed their consistency and zero-anomaly checks; train A examined 374 rows and train B 300 | Passed |
| Leakage and cleanup | Post-teardown artifact scan passed; no project containers, volumes, or networks remained | Passed |

The repaired train A reconciliation specifically reported
`quota_claim_integrity` with 32 inspected local rows and zero violations,
`quota_claim_cardinality` with 19 authoritative reservations and 19 claims,
and one exact target-generation write-evidence row. Train B reported 21 local
quota rows with zero violations, 13 authoritative reservations and claims, and
one exact target-generation evidence row.

## Dependency telemetry

PostgreSQL connection samples were 10 at readiness, 11 after train A cutover,
and 16 after train B cutover; 16 was the maximum observed. Redis recorded 1,000
PING requests across 10 clients with p50 0.063 ms for inline PING and 0.055 ms
for multibulk PING.

The runner did not capture PostgreSQL CPU, I/O, lock-wait distribution,
deadlocks, rollback counts, disk amplification, or saturation, nor Redis error
rate under sustained load. Those omissions prevent any production sizing or
dependency headroom claim.

## Deferred reconciliation classifications

The complete final reports explicitly retain these non-blocking deferred
classifications rather than presenting them as checked invariants:

- retained source-row cleanup state without a cleanup ledger;
- outbox transition completeness without an event ledger;
- unresolved legacy idempotency train-run attribution; and
- retained-source duplicate cleanup classification in cross-shard locator
  traversal.

They are limitations of the logical-schema pilot's evidence model, not hidden
runtime failures.

## Verdict

Milestone 4's same-cluster logical-sharding implementation passes its bounded
multi-replica functional, migration, reconciliation, outage-isolation, and
latency-smoke gates at commit
`85011c851e6b42063f1a2551833a6a92de224891`. The result supports proceeding to
code review and CI for this milestone. It does not support a production
capacity, zero-downtime, physical-shard-isolation, or multi-region claim.
