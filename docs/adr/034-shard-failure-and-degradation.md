# ADR 034: Fail Closed for Ownership and Degrade by Logical Shard

- Status: Accepted
- Date: 2026-07-23

## Context

Milestone 4 routes each train run through a public catalog, a monotonic
assignment generation, and one local PostgreSQL write fence. A cached route can
reduce lookup work, but it cannot prove that an observed owner is still allowed
to commit. Catalog loss, a disabled logical shard, a migration failure, Redis
loss, or a worker timeout must therefore have explicit behavior that preserves
single-writer ownership without turning one bounded failure into corruption or
an unbounded retry storm.

The `legacy`, `shard-0`, and `shard-1` booking storages are schemas in one
PostgreSQL database. Schema-specific permission, injected query, or operational
failures can exercise bounded routing and degradation behavior, but all three
still share the database engine, connections, disk, primary, and physical
failure domain. This milestone cannot infer independent shard availability from
logical isolation.

## Decision

Treat the public catalog and assignment state as mandatory write dependencies.
If the catalog cannot be read and locked, every new train-run-scoped mutation
fails closed. A route-cache hit, Redis value, process-local observation, or
previous successful request cannot authorize a write. There is no random
fallback, hash fallback, blind retry against another storage, or attempt to
write every shard.

When the catalog is healthy but one logical booking storage is degraded or
unavailable:

- requests whose current assignment targets that storage fail with a bounded
  retryable response;
- requests assigned to healthy storages continue through their normal fenced
  transaction;
- customer responses expose neither shard ID, schema, generation, migration
  ID, DSN, nor database host;
- route refresh occurs at most once after a stale-assignment result and a
  repeated failure stops safely; and
- no failed request completes idempotency, opens quota, changes a mask, creates
  a locator, appends outbox intent, or records successful-write evidence.

Use bounded internal categories such as `shard_assignment_stale`,
`shard_write_fenced`, `shard_unavailable`, and `train_run_migrating`. The HTTP
module maps them to a safe response such as
`service_temporarily_rebalancing` or a bounded `503` with an allowlisted
`Retry-After`. Logs and metrics use bounded result/reason and fixed shard-ID
labels; they never include raw identifiers, topology, SQL, credentials, or
passenger data.

Health distinguishes process life from serving capability:

- liveness reports whether the process loop is responsive and does not fail
  merely because an optional logical storage is degraded;
- API readiness requires reachable catalog/control PostgreSQL, valid fixed
  topology and migration schema version, and at least one storage allowed by
  the documented serving policy;
- catalog loss makes the API unready because no write can prove ownership;
- one optional storage failure may leave the API ready but degraded, while
  requests for that storage fail safely;
- internal health reports bounded per-dependency categories without exposing
  schema names or connection details; and
- each worker validates only the configuration and dependencies required by
  its own role and exposes `/livez`, `/readyz`, and `/metrics`.

Shard-aware workers enumerate only the fixed enabled workset with a per-shard
batch limit, timeout, and fair order. The hold expirer and reconciliation
modules continue healthy work after one shard error and return a bounded
aggregate of successes and normalized failures. Reconciliation remains
detect-only. The central outbox worker does not enumerate booking schemas and
continues the existing bounded claim/publish/finalize protocol in `public`.
Admission work remains shard-neutral until token use enters the booking path.

Customer hot paths never return an incomplete fanout as complete data.
Operator/admin fanout alone may aggregate the fixed allowlisted shard set, with
bounded concurrency, per-shard and global deadlines, stable ordering, bounded
memory/output, and explicit `complete`, `partial`, or `unavailable` status. One
failed shard cannot starve healthy work. Cancellation stops dispatching new
work and allows only already bounded operations to finish.

Migration failure policy preserves the current authority:

- if the target is unavailable before cutover, the source remains assigned and
  authoritative; migration is resumable or enters a bounded failed state;
- if the source is unavailable before validation, cutover is forbidden and the
  target cannot be promoted from an unvalidated copy;
- if quiescence, copy, validation, locator-cap preflight, or cutover times out,
  the atomic step rolls back and no partial assignment/fence switch is visible;
- after a successful cutover, target remains authoritative if the retained
  source becomes unavailable, although direct-rollback evidence may be lost;
  and
- a source/target failure never enables both fences or bypasses the reverse
  migration requirement after a target write.

Redis remains non-authoritative. Cache loss falls back according to Milestone
3, and availability remains a hint. Hot-train admission follows Milestone 2's
fail-closed continuity policy. Redis failure cannot select a shard, advance a
generation, enable a fence, or change a committed PostgreSQL result.

## Consequences

- Loss of the catalog intentionally reduces booking availability rather than
  risking an unfenced write.
- A logical storage failure can be contained at the routing and worker
  interfaces while healthy logical work continues when the shared database is
  otherwise usable.
- Readiness can remain true with a documented degraded state, so one optional
  logical shard does not automatically remove every API replica from service.
- Operators and callers receive bounded, topology-safe errors instead of
  silent fallback or unlimited retries.
- Worker and admin partial results are explicit and independently testable.
- Source/target failures have deterministic abort, resume, or post-cutover
  behavior without dual writes.
- Because all logical shards share one PostgreSQL cluster, these results are
  not evidence of independent physical fault isolation, physical-shard RTO/RPO,
  or production capacity.

## Rejected alternatives

- Use a cached route when the catalog is unavailable: rejected because cache
  freshness cannot authorize the transaction or exclude a newer owner.
- Fall back to `legacy` or hash a train run after a shard error: rejected
  because it can create a second writer against retained or incomplete state.
- Probe every shard for a customer command: rejected because it is unbounded,
  leaks topology, and can turn one stale request into several write attempts.
- Mark the entire API dead for one optional logical shard: rejected because
  healthy assignments can remain safe and useful while degradation is explicit.
- Return partial operator data as complete: rejected because it hides the
  failed scope and can drive unsafe operational decisions.
- Let Redis health or waiting-room ownership determine the database route:
  rejected because Redis cannot fence or commit with PostgreSQL booking state.
- Promote an unvalidated migration target after source loss: rejected because
  copied rows are not authoritative merely because the source is unavailable.
- Claim physical failure isolation from schema fault injection: rejected
  because every schema shares the same PostgreSQL engine and failure domain.
