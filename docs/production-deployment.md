# Production Deployment

This guide describes a conservative single-region deployment. It does not turn Milestone 2 into a multi-region or nationally sized platform.

Deploying these artifacts adds a hot-train waiting-room control plane but does not add real payment, a complete anti-bot platform, multi-region active-active writes, national-scale capacity evidence, or real passenger identity verification. Admission permits an attempt and does not guarantee a seat.

## Authority and topology

- One regional PostgreSQL primary is authoritative for train-run status, seat occupancy, reservations, tickets, durable idempotency, and outbox rows.
- Redis provides rate-limit state, optional event transport, and ephemeral waiting-room/token control state. Station, search, and availability caches are deferred; current reads use PostgreSQL, and any future cached availability remains a hint that reservation writes must recheck.
- API, admission-worker, hold-expirer, and outbox-worker processes use the same schema and documented lock ordering.
- Run migrations as an explicit release step before deploying compatible application processes. Do not run automatic schema mutation on application startup.

The manifests under `deploy/kubernetes/base` are a security-conscious baseline, not a complete cloud platform. A production overlay must supply an immutable image digest, secret references, ingress/TLS, database and Redis endpoints, topology-specific network policy, resource sizing, autoscaling policy, and observability integration.

## Required secrets

Create the referenced `railway-runtime-secrets` object out of band. Never commit its values or render them into CI logs.

Required keys, scoped to the process that consumes them:

- `database-url`: TLS-enabled PostgreSQL URL required by the API, admission-worker, hold-expirer, and outbox-worker; a production overlay should replace the baseline shared reference with process-specific least-privilege roles where their grants differ;
- `redis-address`: regional Redis `host:port` endpoint used by the API, admission-worker, and a Redis Streams outbox-worker;
- `redis-password`: Redis credential, if authentication is enabled, used only by those Redis clients; and
- `jwt-secret`: at least 32 random bytes, managed and rotated through the deployment secret system and mounted only into the API; and
- `admission-token-keyring`: one to eight `key-id=base64url` entries whose decoded material is exactly 32 bytes, mounted only into APIs and admission workers with separately configured issue and accept key IDs.

Admission derivation keys are not JWT keys and must not be reused for database,
Redis, TLS, or publisher credentials. Rotate API accept sets before a worker
starts issuing with a new key, retain the old accept key through the maximum
token TTL plus a safety margin, and keep admission/idempotency headers out of
ingress, APM, trace, and error-report logs.

The database migration principal should be separate from the runtime principal. The runtime principal must not own the database or create arbitrary schemas in production. Integration tests use separate ephemeral infrastructure because they require isolated schema creation.

## Configuration

Set `APP_ENV=production` and validate process-owned settings:

- API: HTTP, PostgreSQL, Redis, JWT, admission accept keyring, hold TTL, passenger limit, durable quota, local execution bound, proxy/CORS, dependency, and lifecycle settings;
- admission-worker: PostgreSQL, Redis, admission issue/accept keyring, bounded batch/interval, worker health, pass timeout, and lifecycle settings, with no JWT secret;
- hold-expirer: PostgreSQL, expiration batch/interval, worker health, pass timeout, and lifecycle settings, with no JWT or Redis secret;
- log outbox-worker: PostgreSQL, outbox loop, worker health, pass timeout, and lifecycle settings, with no JWT or Redis secret;
- Redis Streams outbox-worker: the log-worker settings plus only its Redis publisher address/credential;
- trusted proxy CIDRs and explicit CORS origins; and
- request, dependency, and shutdown timeouts.

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
9. Deploy admission workers disabled, prove PostgreSQL/Redis/migration/config readiness, then enable one and verify policy generations before scaling to the tested count.
10. Deploy one hold-expirer and one outbox-worker initially. Scale only after concurrency and database impact are measured.
11. Run a sanitized hot/non-hot smoke flow and read-only seat, quota, and admission reconciliation against disposable production-like data.
12. Observe queue depth, issuance/inflight bounds, token failures, quota/backpressure rejection, lock waits, connection saturation, outbox backlog, and worker failures through the rollback window.

The release artifact contains the detect-only `reconcile` binary. Run the
following checks with a read-only operational PostgreSQL role where the query
contract permits it and with exact network access to the authoritative Redis
instance:

```text
reconcile seat-inventory --train-run-id <canonical-uuid>
reconcile reservation-quotas
reconcile admission-state
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

## Health and rollout behavior

- The API `/livez` is process-only and must not depend on PostgreSQL or Redis.
- The API `/readyz` uses short checks for PostgreSQL, Redis, migrations, and required configuration without exposing credentials.
- Each worker exposes a private `WORKER_HTTP_ADDRESS` (default `:9090`) with process-only `/livez`, dependency/config `/readyz`, and its own `/metrics`. Admission-worker readiness checks PostgreSQL, Redis, migrations, and its process-owned keyring/config; queue backlog does not fail readiness. Hold-expirer `/readyz` checks PostgreSQL; a Redis Streams outbox-worker checks both PostgreSQL and Redis; a log outbox-worker checks PostgreSQL only. Each pass is bounded by `WORKER_PASS_TIMEOUT`.
- Outbox backlog, pending consumer work, or a dead-letter item is alertable but is not by itself a readiness failure.
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

Never use Redis `KEYS` in production. Never treat a cache hit as proof that a seat can be sold.

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
- worker loop success/failure and last successful pass; and
- reconciliation failures as correctness incidents.

Do not use user, passenger, reservation, ticket, train-run, seat, event, idempotency key, station input, or raw path values as metric labels.

## Rollback and incidents

Application rollback is safe only while the migrated schema remains compatible with the previous image. Do not automatically migrate down during an incident: a down migration may destroy data needed by the current or previous binary.

If reconciliation fails, stop new reservation writes for the affected train run, preserve database evidence, identify the transaction/invariant failure, and repair only through a reviewed operator procedure. Redis cache deletion is not an inventory repair.

If Redis is unavailable, keep browsing fallbacks bounded, fail authentication
and passenger-profile creation closed, and fail enabled hot-run admission
closed with bounded retry guidance. Do not silently recreate a continuity
marker for an already initialized generation. Non-hot booking may preserve its
existing limiter fallback and remains subject to the authoritative PostgreSQL
transaction. If PostgreSQL is unavailable, policy management, quota checks, and
reservation writes must fail; do not queue speculative bookings elsewhere.

## Capacity statement

No sustained Milestone 2 benchmark is recorded in the repository. Replica counts in local Compose and resource requests in the baseline are evidence fixtures or starting configuration, not a capacity claim. Production sizing requires the controlled process in [milestone-2-load-testing.md](milestone-2-load-testing.md) and a completed [benchmark report](benchmark-report-milestone-2.md).
