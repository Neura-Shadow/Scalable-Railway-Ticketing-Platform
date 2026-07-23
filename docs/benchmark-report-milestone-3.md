# Milestone 3 Benchmark Report

## Evidence status

Milestone 3 has accepted **bounded local evidence**, not a production-capacity
benchmark. The accepted run used one Docker Desktop host with three API
replicas, two admission workers, two read-model workers, one PostgreSQL 16.14
instance, one Redis 7.4.9 instance, and the evidence reverse proxy. k6 0.54.0
generated the client load. The host exposed 24 logical CPUs and 16.62 GiB of
memory to Docker Desktop.

Every accepted search measurement below was taken only after a paginated
`rebuild-all` completed and `read_model_projection_state.ready` was true.
Earlier search runs made before global readiness was proved were rejected and
are not used for the cold/warm comparison. Raw summaries and server telemetry
were kept outside Git; no credentials, customer data, or machine-local paths
are committed.

This report does not establish sustained throughput, production sizing,
national-scale capacity, multi-region behavior, or zero downtime.

## Run identity and bounds

| Field | Observed value |
|---|---|
| Source state | Milestone 3 delivery working tree; final PR head is the delivery trace |
| Run date | 2026-07-22, Asia/Taipei |
| Environment | Local Docker Desktop, one host, one region |
| API / admission / read-model replicas | 3 / 2 / 2 |
| PostgreSQL / Redis | PostgreSQL 16.14 / Redis 7.4.9, one instance each |
| Client | k6 0.54.0 |
| Search fixture | 2 stations, 2 train runs, 160 authoritative seat rows, 2 journey rows |
| Steady-state scenario bound | 5 VUs for 15 seconds unless stated otherwise |
| Cold-search method | 30 sequential requests, each after an exact version reset, through all 3 upstreams |

## Accepted measurements

Latency is client-observed HTTP duration. RPS is only the achieved rate in this
short local run. It is not a service-level objective or capacity claim.

| Scenario | Requests | RPS | p50 ms | p95 ms | p99 ms | Checks failed | Unexpected 5xx |
|---|---:|---:|---:|---:|---:|---:|---:|
| Station cache | 32,800 | 2,186.37 | 1.854 | 3.188 | 4.407 | 0 | 0 |
| Projection-ready warm search | 20,498 | 1,365.69 | 2.999 | 5.057 | 7.792 | 0 | 0 |
| Availability hint cache | 14,174 | 849.80 | 2.867 | 12.086 | 15.302 | 0 | 0 |
| Projection-ready mixed search/booking | 14,776 | 984.51 | 4.097 | 6.890 | 10.784 | 0 | 0 |
| Projection-ready multi-replica search | 15,775 | 946.29 | 3.866 | 6.811 | 10.785 | 0 | 0 |
| Invalidation storm, 30 events | 15,048 | 900.62 | 4.011 | 7.335 | 10.720 | 0 | 0 |
| Worker-pause window | 26,733 | 1,781.83 | 2.324 | 3.757 | 5.371 | 0 | 0 |
| Redis-outage window | 41 | 2.29 | 1,072.782 | 3,830.257 | 4,023.320 | 0 | 0 |

The Redis-outage latency is intentionally reported rather than hidden: reads
remained schema-valid and returned no unexpected 5xx, but bounded synchronous
Redis attempts made fallback very slow in this local outage. This is recovery
evidence and an operational limitation, not healthy-state performance.

## Independent cold versus warm search

Thirty cold samples each used a fresh exact version namespace and reached all
three API upstreams. Their latency was p50 23.305 ms, p95 48.450 ms, and p99
54.170 ms (minimum 13.205 ms, maximum 54.521 ms). The projection-ready warm
run measured p50 2.999 ms, p95 5.057 ms, and p99 7.792 ms.

On this bounded fixture, the cold-to-warm latency ratios were 7.77x at p50,
9.58x at p95, and 6.95x at p99. These ratios describe this run only.

## Correctness and recovery observations

| Gate | Accepted observation |
|---|---|
| Global projection readiness | Bounded paginated rebuild completed; `ready=true` before accepted search runs |
| Multi-replica cache | Three upstreams observed; cross-replica warm hit and version rotation passed |
| Mixed booking safety | 5 holds succeeded and all 5 were cancelled; cancellation failures 0 |
| Worker pause and recovery | Aggregate lag grew from 0 to 16.562979 seconds, then returned to 0; durable receipt present |
| Redis outage and recovery | Read fallback increased 11 to 87 and cache failures 11 to 87; recovery used a fresh namespace |
| Invalidation storm | 30 events at 4 events/second; 30,096 checks passed; no pending or DLQ residue |
| Safety fallback during invalidation | Projection fallback increased by 56 while event progress existed, then stopped increasing after convergence |
| Final reconciliation | Read model 2/2 consistent; seat inventory, quotas, admission state, and cache versions passed |

The bounded fallback during invalidation is expected safety behavior: search
does not read a projection while a projection-affecting event is in progress.
After convergence, projection readiness was true, progress rows and pending
entries were zero, and a follow-up warm run left the fallback counter unchanged.

## Dependency telemetry snapshot

At final reconciliation Redis reported about 1.48 MiB used memory, 40,630
processed commands, 5,037 keyspace hits, 4,512 misses, zero evictions, and zero
rejected connections. PostgreSQL reported 14 connections, zero deadlocks, zero
temporary files, and no ungranted locks. These are point-in-time diagnostics,
not utilization envelopes.

## Verdict

The run provides bounded evidence for cache acceleration, multi-replica shared
namespace behavior, safe projection fallback, worker recovery, Redis fallback,
and authoritative booking reconciliation. It does not provide a sustained
benchmark: dataset size, duration, failure-window latency, and single-host
topology are deliberately too small for production sizing.
