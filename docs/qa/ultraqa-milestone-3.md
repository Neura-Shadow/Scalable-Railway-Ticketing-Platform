# UltraQA Milestone 3 Report

## Scope and verdict

UltraQA exercised the Milestone 3 projection, cache, recovery, migration,
metrics, and evidence harness against adversarial and misleading-success
conditions. The accepted state has no open Critical or High correctness
finding. This report separates product defects, harness defects, expected
safety behavior, and unmeasured production claims.

The optional OMX state-tracking command was unavailable in the local
environment. Scenario execution, evidence collection, failure classification,
and regression verification continued without it.

## Cycle matrix

| Cycle | Adversarial condition | Initial result | Classification | Corrective evidence |
|---|---|---|---|---|
| 1 | Prometheus text with CRLF line endings | Counters parsed as zero | Harness defect | Parser now permits trailing whitespace and reports captured values |
| 1 | Rebuild selected train runs only | Search silently used source fallback | Harness and readiness defect | Harness paginates `rebuild-all`, requires `ready=true`, and rejected earlier search numbers |
| 1 | Independent cold-sample helper | PowerShell parser errors | Temporary harness defect | Fresh 30-sample run completed across three upstreams |
| 1 | Invalidation while event progress exists | Fallback counter increased | Expected safety behavior | Progress, pending, and DLQ converged to zero; follow-up warm run had no new fallback |
| 2 | Duplicate and out-of-order events | No projection corruption | Pass | Current authoritative state wins; durable receipts prevent repeat application |
| 2 | Redis stream/Pending Entry List failover | Unclaimed work risk tested | Pass | Real `XAUTOCLAIM` recovery, durable receipt, zero pending residue |
| 2 | Published outbox event missing Redis delivery | Potential permanent projection lag | Product defect fixed | Bounded durable outbox replay, stable cursor, replay/recovery integration tests |
| 2 | Redis loss after prior namespaces | Stale namespace reuse attempted | Pass | Lazy race-aware generation install creates a fresh namespace |
| 2 | Oversized or semantically wrong cache payload | Poisoned cache attempted | Pass | 1 MiB read bound plus envelope, scope, schema, and semantic validation |
| 2 | Concurrent misses and cancelled waiters | Stampede/leak risk tested | Pass | Stable batch singleflight and cancellation-aware shared work |
| 2 | Missing real Redis test dependency | Tests could skip and look green | Test defect fixed | Configured dependency now fails explicitly; isolated Redis integration tests pass |
| 3 | Both read-model workers paused | Lag and safe fallback observed | Pass | Lag 0 to 16.562979 seconds to 0; 26,733 requests, zero check failures, durable receipt |
| 3 | Redis unavailable for 15 seconds | Slow dependency/fallback path | Pass with limitation | Reads stayed valid with zero unexpected 5xx; high latency documented |
| 3 | Thirty invalidations at 4/second | Partial projection/read failure risk | Pass | 30,096 checks passed; projection ready; zero progress, pending, and DLQ residue |
| 3 | Mixed search and authoritative booking | Stale hint/seat-authority risk | Pass | 5 holds and 5 cancellations; zero cancellation failure; reconciliation passed |
| 3 | Default Compose key plus network-wide API publication | Customer-to-admin/operator escalation | Independent High security finding fixed | Compose now requires a unique key and binds loopback; JWT parsing rejects claim/database role mismatch |
| 3 | Dirty user worktree and bounded timeouts | Destructive cleanup/hang risk | Pass | Existing changes preserved; controllers use bounded restore paths |

## Misleading-success defenses

- Tests no longer skip when `TEST_REDIS_ADDR` is configured but unreachable.
- The load harness validates nonzero metric values rather than only endpoint
  status and includes actual captured values in failures.
- Search performance is accepted only after global projection readiness, not
  merely after rebuilding individual fixture rows.
- A cache hit is not inferred from latency; API and worker counters are used.
- Expected booking conflicts or 4xx responses are separated from unexpected
  5xx responses.
- Failure controllers restore dependencies in bounded cleanup paths and verify
  convergence after restoration.

## Residual QA limits

The test matrix is strong for bounded functional, integration, race, and local
failure/recovery behavior. It does not prove long-duration soak stability,
production network partitions, managed Redis failover semantics, large-data
query plans, production capacity, or multi-region behavior. Those remain
explicitly outside Milestone 3 acceptance.
