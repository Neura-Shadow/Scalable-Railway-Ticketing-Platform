# Milestone 6 Load and Failure Testing

## Current status

**Not run.** This document is the evidence contract and planned scenario matrix
for Milestone 6. It contains no throughput, latency, capacity, correctness or
production-readiness result. The benchmark summary remains `not_run` until a
sanitized canonical bundle, raw artifacts and post-run database invariants are
available.

## Evidence contract

Run only against the disposable single-region payment topology: three API
replicas, two payment workers, one reconciler, the deterministic payment
sandbox, control PostgreSQL, two independent booking-shard PostgreSQL
instances, Redis, existing workers and the proxy. Record exact commit, clean or
explicitly described worktree, image digests, Compose configuration hash,
fixture seed, host CPU/memory/disk, PostgreSQL/settings, pool caps, Go/Docker/k6
versions, start/end timestamps and failure-hook sequence.

Keep credentials, signatures, payment references, bearer tokens, request
bodies, passenger data, DSNs, hosts and ports out of committed evidence. Every
machine-readable result must use one of `passed`, `failed`, `blocked` or
`not_run`; a timeout, missing dependency, truncated artifact or missing
invariant is `blocked`/`failed`, never `passed`.

These are bounded correctness, recovery and pressure tests. They do not prove
production capacity, a PCI compliance level, a live-provider SLO, exactly-once
delivery, national-scale throughput, multi-region availability, or an RPO/RTO.

## Planned scenario matrix

| Script | Planned pressure/fault | Required correctness assertion | Status |
|---|---|---|---|
| `payment-intent-create.js` | Bounded intent creation rate across three APIs | One active intent/saga per reservation; server-derived immutable amount/currency | `not_run` |
| `payment-idempotency.js` | Same key/body and same key/different body retries | Exact replay returns one intent; changed fingerprint conflicts; no raw key persists | `not_run` |
| `payment-webhook-burst.js` | Duplicate and out-of-order signed event burst | One immutable inbox identity/hash; equal duplicates harmless; changed hash conflicts; HTTP performs no financial/shard effect | `not_run` |
| `payment-capture-recovery.js` | Timeout, disconnect and crash around capture/finalization | Unknown outcome is queried before retry; one provider capture and one durable final result | `not_run` |
| `ticket-issuance.js` | Multi-replica issue retries and injected shard/control crashes | One stable issuance receipt, one ticket per reservation seat and no pre-capture issue | `not_run` |
| `payment-refund.js` | Cancellation/permanent issuance failure and refund retry | Full refund only; `0 <= refunded <= captured`; seat releases only after durable successful refund | `not_run` |
| `payment-provider-outage.js` | Provider unavailable/slow/oversized/malformed response | Bounded retry/backoff; unknown/manual review retained; no blind financial retry or worker leak | `not_run` |
| `payment-shard-outage.js` | Assigned shard unavailable during issue/refund | No fallback writer or cross-shard scan; healthy shard/payment work remains bounded and available | `not_run` |
| `payment-during-migration.js` | Payment, issue and refund across capture/copy/cutover/reverse | Both shards at v2; zero journal lag; all receipts/tickets/refund state preserved; stale route rejected before effect | `not_run` |
| `multi-replica-payment.js` | Three APIs, two workers and duplicate leases/retries | One operation effect per stable provider key, one ticket set, one refund and convergent saga | `not_run` |

## Deterministic failure matrix

Use injected clocks, barriers and named fault hooks; do not coordinate races by
arbitrary sleep. The sandbox/driver must cover at least:

1. authorize response timeout before and after provider persistence;
2. capture response timeout before and after provider persistence;
3. void response timeout before and after provider persistence;
4. refund response timeout before and after provider persistence;
5. provider status query timeout;
6. duplicate signed webhook delivery;
7. valid out-of-order webhook delivery;
8. same provider/event ID with a different body hash;
9. stale timestamp, unknown key ID and invalid signature;
10. unknown but correctly signed event type;
11. oversized/malformed webhook body;
12. worker crash after claim and before provider I/O;
13. worker crash after provider I/O and before result persistence;
14. shard commit followed by control-finalization failure;
15. ticket issuance transient/permanent failure and refund uncertainty;
16. shard outage or cutover between route resolution and local transaction.

Every hook has an explicit release barrier and bounded deadline. Record the
hook order and final state; a test that never reaches its barrier is blocked.

## Required measurements

Publish observed values only, with count/sample window and units:

- payment-intent created requests/second and accepted intents/second;
- webhook received, authenticated, duplicate, conflict and processed rates;
- provider authorize/capture/query/void/refund latency p50/p95/p99;
- ticket issuance and control-finalization latency p50/p95/p99;
- end-to-end payment-to-active-ticket latency p50/p95/p99;
- refund request-to-durable-compensation latency p50/p95/p99;
- uncertain, manual-review, retry, exhausted and reconciliation finding counts;
- bounded repair latency and oldest queue/lease age;
- queue depth and claim throughput for each finite work type;
- pgx total, acquired, idle, max, acquire count/duration, empty acquire,
  cancelled acquire and peak acquired values;
- control and per-allowlisted-shard connection counts, database lock waits and
  failed-shard isolation;
- CPU, memory, disk, PostgreSQL WAL/I/O and network limits when available.

Metrics may use only finite state/operation/result/reason/provider aliases plus
bounded `database_role` and allowlisted `shard_id`. Never label intent,
reservation, ticket, event, operation, user or migration IDs; DSN, host, port,
`connection_ref`, token, signature, key or payment reference.

## Required pre/post invariants

Before and after each scenario capture sanitized counts/checksums for intent and
saga state, provider operations/observations, inbox IDs/body hashes, current
route/fence/generation, shard receipts, reservations/seats, ticket orders,
tickets, refund totals, local outbox, journal/apply watermarks, target-write
evidence and reconciliation findings.

A scenario cannot pass unless all applicable assertions hold:

- no duplicate authorize/capture/void/refund effect for one stable operation;
- no intent or receipt amount/currency mismatch;
- no issue before durable captured proof;
- exactly one stable ticket per reservation seat and no duplicate ticket code;
- no seat release while capture/refund is unknown or refund is incomplete;
- refunded amount is never negative, partial or greater than captured;
- no changed-hash event accepted as a duplicate and no unknown signed event
  mutates payment/shard state;
- no stale/wrong-shard commit, fallback writer, journal gap, unapplied payment
  mutation or post-cutover reconciliation mismatch;
- no leaked worker lease, goroutine, HTTP body or database connection;
- no secret, raw card data, unbounded identifier label or infrastructure
  endpoint in logs and evidence.

## Execution and reporting gate

Validate `docker compose -f docker-compose.payment.yml config` before starting.
Run one scenario at a time against reset seeded volumes unless the scenario
explicitly validates recovery across restart. Preserve canonical JSON, raw k6
summary, bounded service logs, database invariant snapshot, reconciliation
report and resource measurements before teardown. Scan the bundle, then tear
down containers and volumes explicitly.

Do not replace the status in this document or
[`benchmark-report-milestone-6.md`](benchmark-report-milestone-6.md) until the
evidence is reproducible and reviewable.
