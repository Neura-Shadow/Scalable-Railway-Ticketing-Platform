# Production Deployment

This guide describes a conservative single-region deployment. It does not turn
Milestone 4's same-cluster logical schemas into independent physical shards, a
multi-region system, or a nationally sized platform.

Deploying these artifacts adds explicit train-run routing, monotonic database
fencing, bounded quiesced migration controls, disposable read projections/
caches, and a hot-train waiting-room control plane. It does not add payment, a
complete anti-bot platform, physical shard isolation, zero-downtime
rebalancing, multi-region active-active writes, national-scale capacity
evidence, or real passenger identity verification. Cached availability is a
hint and admission permits an attempt, not a seat.

## Authority and topology

- One regional PostgreSQL primary is authoritative for train-run status, seat occupancy, reservations, tickets, durable idempotency, and outbox rows.
- The fixed `legacy`, `shard-0`, and `shard-1` booking storages are schemas in
  that same database. Exactly one storage owns a train run's writes in stable
  state; assignment generation and a local fence are checked in the mutation
  transaction. The schemas share one physical failure domain.
- Redis provides rate-limit state, event transport, ephemeral waiting-room/token control state, and versioned station/search/availability read caches. Cache loss degrades read performance but cannot move booking authority out of PostgreSQL.
- The PostgreSQL journey projection is disposable and rebuildable. API,
  admission-worker, read-model-worker, hold-expirer, and outbox-worker processes
  use the same database and the documented global/routed schema boundaries and
  lock ordering.
- Run migrations as an explicit release step before deploying compatible application processes. Do not run automatic schema mutation on application startup.

The manifests under `deploy/kubernetes/base` are a security-conscious baseline, not a complete cloud platform. A production overlay must supply an immutable image digest, secret references, ingress/TLS, database and Redis endpoints, topology-specific network policy, resource sizing, autoscaling policy, and observability integration.

## Required secrets

Create the referenced `railway-runtime-secrets` object out of band. Never commit its values or render them into CI logs.

Required keys, scoped to the process that consumes them:

- `database-url`: TLS-enabled PostgreSQL URL required by the API, admission-worker, read-model-worker, hold-expirer, and outbox-worker; a production overlay should replace the baseline shared reference with process-specific least-privilege roles where their grants differ;
- `redis-address`: regional Redis `host:port` endpoint used by the API, admission-worker, read-model-worker, and Redis Streams outbox-worker;
- `redis-password`: Redis credential, if authentication is enabled, used only by those Redis clients; and
- `jwt-secret`: at least 32 random bytes, managed and rotated through the deployment secret system and mounted only into the API; and
- `admission-token-keyring`: one to eight `key-id=base64url` entries whose decoded material is exactly 32 bytes, mounted only into APIs and admission workers with separately configured issue and accept key IDs.

Admission derivation keys are not JWT keys and must not be reused for database,
Redis, TLS, or publisher credentials. Rotate API accept sets before a worker
starts issuing with a new key, retain the old accept key through the maximum
token TTL plus a safety margin, and keep admission/idempotency headers out of
ingress, APM, trace, and error-report logs.

The database migration principal should be separate from the runtime principal. The runtime principal must not own the database or create arbitrary schemas in production. Integration tests use separate ephemeral infrastructure because they require isolated schema creation.

`shard-admin` should use a distinct, private, audited operator role with only
the reviewed catalog, fence, migration, copy, validation, and cleanup grants it
needs. It requires no JWT or admission-token keyring. Do not place database
credentials on its command line or include them in its output.

## Configuration

Set `APP_ENV=production` and validate process-owned settings:

- API: HTTP, PostgreSQL, Redis, JWT, admission accept keyring, hold TTL, passenger limit, durable quota, local execution bound, proxy/CORS, dependency, and lifecycle settings;
- admission-worker: PostgreSQL, Redis, admission issue/accept keyring, bounded batch/interval, worker health, pass timeout, and lifecycle settings, with no JWT secret;
- read-model-worker: PostgreSQL, Redis Stream, bounded batch/retry/pending/interval, worker health, pass timeout, and lifecycle settings, with no JWT or admission-token secret;
- hold-expirer: PostgreSQL, expiration batch/interval, worker health, pass timeout, and lifecycle settings, with no JWT or Redis secret;
- log outbox-worker: PostgreSQL, outbox loop, worker health, pass timeout, and lifecycle settings, with no JWT or Redis secret;
- Redis Streams outbox-worker: the log-worker settings plus only its Redis publisher address/credential;
- trusted proxy CIDRs and explicit CORS origins; and
- request, dependency, and shutdown timeouts.

Milestone 4 additionally validates the fixed logical shard IDs, booking mode,
bounded route cache, and routed query timeout. Worker traversal is a serial,
fail-isolated pass over at most the configured subset of the three fixed
storages. Migration operation duration, copy size, and retained-source duration
are bounded per `shard-admin` invocation by `--timeout`, `--batch-size`, and
`plan-migration --rollback-window`; they are not application runtime settings.
`BOOKING_SHARD_MODE` defaults to `legacy`. Production `schema_poc` mode also requires
`BOOKING_SHARD_SCHEMA_POC_PRODUCTION_ENABLED=true`; that acknowledgement does
not replace Migration 8, writer-version drain, reconciliation, or operator
approval. Never configure schema names where logical shard IDs are expected.

Production configuration validation rejects the committed local Compose database password, the development JWT default, and universal trusted-proxy CIDRs. Terminate TLS at a trusted ingress/load balancer, use TLS to managed dependencies where supported, replace the baseline loopback-only proxy trust with only the exact ingress addresses or narrow topology-specific CIDRs, and keep CORS disabled unless explicit origins are required.

An enabled production outbox worker must use `OUTBOX_PUBLISHER=redis_stream` by default. An enabled `log` publisher is rejected unless `ALLOW_LOG_PUBLISHER_IN_PRODUCTION=true` is explicitly set for an emergency; that override emits a bounded warning and the log adapter never logs event payloads. `OUTBOX_PUBLISHER_ENABLED=false` disables publication without requiring Redis.

## Release sequence

1. Record the commit SHA and build the multi-stage image.
2. Scan source and the resulting image; resolve blocking findings.
3. Push the image and record its immutable digest.
4. Back up PostgreSQL and verify recovery readiness.
5. Run `migrate up` from the release artifact using the migration principal.
6. Run `migrate up` again and verify the current version/clean state.
7. Deploy API instances in the single target region, pinned by digest.
8. Wait for `/livez` and `/readyz`; do not route traffic before readiness succeeds.
9. Run a dry-run and bounded initial projection backfill; reconcile the read model before enabling its worker.
10. Deploy one read-model worker disabled, prove PostgreSQL/Redis/migration/config readiness, then enable it and validate pending/DLQ/cache rotation before scaling. Set a stable group and a unique consumer name per replica; require claim-min-idle to exceed the pass timeout.
11. Deploy admission workers disabled, prove their PostgreSQL/Redis/migration/config readiness, then enable one and verify policy generations before scaling.
12. Deploy one hold-expirer and one outbox-worker initially. Scale only after concurrency and database impact are measured.
13. Run sanitized read, hot/non-hot booking, cache-loss, and source-fallback smokes plus seat, quota, admission, read-model, and cache-version reconciliation.
14. Observe cache/fallback/projection lag, queue/inflight bounds, lock waits, connections, outbox backlog, DLQ, and worker failures through rollback.

For Milestone 4, keep the general release in `legacy` mode while applying and
validating Migration 8. Drain every incompatible pre-fencing writer, deploy
generation-aware binaries, and prove the legacy path before explicitly opting
into `schema_poc`. Move selected synthetic or approved train runs only through
the private bounded workflow in
[Migration 8 production rollout](migrations/migration-8-production-rollout.md).
Each move has its own maintenance window, backup/reconciliation evidence,
bounded zero-writer interval, source-retention window, and rollback decision.

The release artifact contains the detect-only `reconcile` binary. Run the
following checks with a read-only operational PostgreSQL role where the query
contract permits it and with exact network access to the authoritative Redis
instance:

```text
reconcile seat-inventory --train-run-id <canonical-uuid>
reconcile reservation-quotas
reconcile admission-state
reconcile read-model
reconcile cache-versions
reconcile shard-assignments
reconcile shard-locators
reconcile shard-migration --migration-id <canonical-uuid>
```

Each command emits a bounded JSON summary and exits non-zero on a detected
violation, dependency failure, invalid configuration, timeout, or page bound.
Do not put connection credentials on the command line. These commands never
auto-repair state; a failure is an incident gate requiring preserved evidence
and reviewed operator action.

`admission-state` uses bounded `SCAN` only for strict continuity-key discovery
inside this operator CLI. It inspects current, previous-version, and disabled
policy generations so a policy update cannot hide stranded leases or tokens.
After the complete bounded scan it also subtracts every observed generation
from the set of enabled PostgreSQL policies whose current version was already
initialized. Any missing current generation is a reconciliation violation,
including the reachable-but-empty Redis data-loss case. For every observed
initialized current generation it separately performs an exact read-only check
of both the shared policy-version marker and generation continuity sentinel;
either missing or mismatched marker is also a violation. Historical generations
remain continuity-only inspectable after a policy version advances.
The API and admission worker continue to use exact keys only. A PostgreSQL
idempotency replay repairs a lost post-commit token finalize through the exact,
bounded token-hash locator for the original generation; it never scans Redis
and never treats Redis as durable booking authority.

Migrations must remain backward compatible with the currently running application during rolling deployment. Destructive column/table removal requires a later release after all readers and writers have stopped using the old shape.

Migration 5 is an expand migration: a compatibility trigger derives `reservation_seats.train_run_id` for a version-4 writer that omits the new column, while version-5 writers provide it directly. Do not remove that trigger until every version-4 process has drained and rollback to version 4 is no longer permitted.

Before applying Migration 5 to a populated database, follow [the Migration 5 production rollout runbook](migrations/migration-5-production-rollout.md) and record every applicable gate in [the release checklist](release-checklist.md). The committed preflight, post-validation, and rollback-check SQL is read-only.

Migration 6 creates `hot_train_policies`, supporting quota-count indexes, and
new policy outbox types. Follow
[the Migration 6 production rollout runbook](migrations/migration-6-production-rollout.md)
and record every applicable Migration 6, worker-readiness, Redis-outage, and
reconciliation gate in [the release checklist](release-checklist.md).
Do not down-migrate automatically: version 6 down deletes durable policy outbox
events because the older constraints cannot represent them.

Migration 8 expands the database with two fixed schema-isolated booking
storages, explicit legacy assignments/fences, catalog and migration control,
global resource locators/claims, and retained-public guards. It does not move
bookings automatically. Its down migration is blocked unless every run is
stable on `legacy`, all migrations are terminal, and both logical schemas are
empty. Follow the dedicated runbook; do not use a down migration as the first
incident response.

Migration 9 adds physical-shard control metadata and command/recovery state but
does not enable any physical shard. Apply it to the control database using the
dedicated rollout runbook. Apply `migrations/booking-shard` independently to
each allowlisted physical database and require clean shard schema version 1
before enabling writes. Keep DSNs in process-specific secrets only; the control
catalog stores fixed connection references, never endpoints or credentials.

Physical cutover is an operator-controlled, single-region pilot. Disable and
durably record the source fence, complete final journal catch-up and validation,
enable the target at a newer generation, and only then switch the control
assignment. The final phase includes a bounded write pause. Retain the source
read-only. If the target has accepted a successful write, direct rollback is
forbidden; perform a reviewed reverse migration. Do not present this workflow
as zero downtime, automatic failover, or production certification.

Physical fare, seat-booking-state and booking-policy changes are optimistic,
durable operator commands. Read the current value through the corresponding
operator GET, retain its `source_version`, and send that exact value with a new
`Idempotency-Key` on PATCH. Do not retry with a changed body under the same key.
The API reserves the control ledger before the shard write; the shard commits
the snapshot and receipt together; the finalizer commits the control projection
and ledger state together. Run the booking-command reconciler with access only
to the fixed allowlisted shard connections so an interrupted finalization can
converge without repeating a receipted write. Alert on reserved/executing/
committed/repair command age and reconciliation failures. Route-level fare
fanout is not supported by this endpoint.

## Health and rollout behavior

- The API `/livez` is process-only and must not depend on PostgreSQL or Redis.
- The API `/readyz` uses short checks for PostgreSQL, Redis, migrations, and required configuration without exposing credentials.
- In schema mode, API readiness also requires catalog/control access, Migration
  8, a valid fixed topology, and a compatible fencing-protocol version.
  Catalog loss makes the API unready. One optional logical storage may leave
  the API ready but explicitly degraded while requests assigned there fail
  boundedly; this is logical degradation, not physical isolation.
- In physical mode, readiness requires clean control Migration 9, a validated
  fixed registry and compatible shard schema version 1. One unavailable shard
  may be reported as bounded degradation while healthy shards continue; a
  request assigned to the failed shard returns a bounded unavailable result.
  Readiness output exposes only fixed shard IDs, never DSNs or endpoints.
- Physical query routes receive one non-resetting request deadline inherited by
  control routing, pool acquisition, refresh, and shard work. Each physical
  transaction additionally sets local statement and lock timeouts. Control-only
  routes and complete booking sagas keep their separately configured deadlines.
- Hold expiration and outbox publication run control and physical lanes
  concurrently with distinct budgets: `WORKER_PASS_TIMEOUT` bounds the control
  lane and outer pass, while `PHYSICAL_WORKER_SHARD_TIMEOUT` bounds one physical
  shard pass. A slow control database therefore cannot starve healthy physical
  shards, and the per-statement timeout is not misused as a whole-batch limit.
- Each worker exposes a private `WORKER_HTTP_ADDRESS` (default `:9090`) with process-only `/livez`, dependency/config `/readyz`, and its own `/metrics`. Admission-worker readiness checks PostgreSQL, Redis, migrations, and its process-owned keyring/config; queue backlog does not fail readiness. Hold-expirer `/readyz` checks PostgreSQL; a Redis Streams outbox-worker checks both PostgreSQL and Redis; a log outbox-worker checks PostgreSQL only. Each pass is bounded by `WORKER_PASS_TIMEOUT`.
- Outbox backlog, pending consumer work, or a dead-letter item is alertable but is not by itself a readiness failure.
- The source stream is intentionally not blind-`MAXLEN` trimmed. Alert on
  stream length, PEL size, pending age, and Redis memory. Do not trim below any
  consumer group's delivered/pending floor. For a repaired DLQ event with
  durable read-model progress, preview and apply `read-model-admin resume-event`
  rather than deleting the progress gate.
- `/metrics` must remain internal or protected by the platform network boundary.
- Use graceful termination with a pre-stop/drain window long enough for the configured HTTP shutdown timeout.

The baseline Kubernetes deployment runs as a non-root numeric user with a read-only root filesystem, drops Linux capabilities, disables privilege escalation, and applies the runtime-default seccomp profile. No secret values are included in the manifests.

## Database and Redis operations

PostgreSQL needs automated backups, point-in-time recovery, tested restore procedures, connection limits, statement/lock timeouts, slow-query and deadlock visibility, disk/replication alerts, and scheduled vacuum/analyze management. A replica may serve suitable read-only queries only when the resulting staleness is explicit; booking commands remain on the primary.

Redis needs authentication, network isolation, bounded memory/eviction policy,
command latency/error alerts, AOF or equivalent managed persistence, backup and
restore exercises, and sufficient durability for the accepted continuity risk.
Authentication and passenger-profile creation fail closed when their limiter is
unavailable. Enabled hot-run join, status, admission, and token operations also
fail closed; the API must not downgrade an enabled PostgreSQL policy. Non-hot
reservation rate limiting preserves its existing documented fail-open behavior,
while PostgreSQL still enforces durable quotas and seat correctness. Redis loss
must not alter committed seat authority.

Never use Redis `KEYS` in production. Read-cache request paths use only exact
generation/data keys; bounded operator reconciliation reads only known version
keys. Never treat a cache hit as proof that a seat can be sold.

Never treat route-cache or Redis state as a shard assignment. Every booking
mutation must lock and validate the public assignment and selected storage
fence in PostgreSQL. The routed transaction uses only fixed transaction-local
paths: `pg_catalog, public, pg_temp` for legacy,
`pg_catalog, booking_shard_0, public, pg_temp` for `shard-0`, and
`pg_catalog, booking_shard_1, public, pg_temp` for `shard-1`. Keeping
`pg_catalog` first and `pg_temp` explicit and last prevents temporary-object
shadowing.

## Monitoring and alerts

At minimum monitor:

- HTTP requests, latency, bounded status/error categories, panic recovery, and in-flight work;
- reservation attempt/success/conflict/confirm/cancel/expire counters;
- PostgreSQL connections, lock waits, deadlocks, transaction latency, CPU, I/O, disk, and replication/backup health;
- Redis latency, errors, memory, authentication/passenger rate-limit
  fail-closed events, enabled-hot-admission fail-closed events, and non-hot
  reservation-limiter fail-open events;
- waiting-room join/duplicate/full/expiry, admission issuance/failure/wait, token lifecycle/conflict, quota rejection, local backpressure, and hot-reservation conflict/duration with bounded labels;
- outbox pending age/count, processing leases, publish failures, retries, and dead letters;
- read-model events/duplicates/rebuild duration/rows/lag/reconciliation and cache hit/miss/fill/invalidation/fallback metrics;
- worker loop success/failure and last successful pass; and
- reconciliation failures as correctness incidents.
- shard route/cache/refresh results, stale/fence rejections, logical-storage
  unavailability, bounded fanout/partial results, migration phase/duration/copy
  counts, validation failures, cutover/rollback results, and shard
  reconciliation mismatches.

Do not use user, passenger, reservation, ticket, train-run, seat, event, idempotency key, station input, or raw path values as metric labels.

## Rollback and incidents

Application rollback is safe only while the migrated schema remains compatible with the previous image. Do not automatically migrate down during an incident: a down migration may destroy data needed by the current or previous binary.

If reconciliation fails, stop new reservation writes for the affected train run, preserve database evidence, identify the transaction/invariant failure, and repair only through a reviewed operator procedure. Redis cache deletion is not an inventory repair.

For a train run in migration, preserve source authority until atomic cutover
commits. Before cutover, a failed copy or validation remains resumable and the
partial target is unroutable. After cutover, direct rollback is allowed only
when target-generation write evidence is still zero under the same locks; any
successful target mutation requires a full reverse migration with a newer
generation. Source cleanup is never automatic and cannot run before the
rollback window expires and current authority is revalidated.

If Redis is unavailable, keep browsing fallbacks bounded, fail authentication
and passenger-profile creation closed, and fail enabled hot-run admission
closed with bounded retry guidance. Do not silently recreate a continuity
marker for an already initialized generation. Non-hot booking may preserve its
existing limiter fallback and remains subject to the authoritative PostgreSQL
transaction. If PostgreSQL is unavailable, policy management, quota checks, and
reservation writes must fail; do not queue speculative bookings elsewhere.

## Capacity statement

No accepted Milestone 4 runtime benchmark is recorded in the repository.
Replica counts, fixed schemas, local Compose, and baseline resource requests
are evidence fixtures, not capacity or physical-shard claims. Production
sizing requires [Milestone 4 load testing](milestone-4-load-testing.md) and a
completed [benchmark report](benchmark-report-milestone-4.md).
