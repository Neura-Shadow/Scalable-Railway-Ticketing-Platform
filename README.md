# Scalable Railway Ticketing Platform

A production-minded, single-region Go backend for railway booking with Redis-backed hot-train waiting-room admission, PostgreSQL-authoritative route-segment seat allocation, temporary holds, durable quotas, idempotency, and transactional outbox events under concurrency.

Milestone 2 is intentionally bounded. Admission permits a booking attempt; it does not guarantee a seat. This is not a national-scale capacity claim and does not implement real payment, a complete anti-bot platform, or multi-region active-active writes. See [Milestone 2 limitations](docs/milestone-2-limitations.md).

## Architecture

The repository is a modular monolith with separate API and worker processes built from one codebase:

```text
HTTP transport
    -> application commands/queries
        -> domain model and consumer-owned ports
            -> PostgreSQL, Redis, Prometheus, and publisher adapters
```

PostgreSQL is authoritative for policies, train-run status, seat inventory, reservation lifecycle, durable quotas, tickets, idempotency, outbox state, and all current station/search/availability reads. Redis provides rate controls, optional event transport, and ephemeral waiting-room/token control state. Redis never allocates a seat. Station, search, and availability Redis caches remain deferred to Milestone 3; any future cached availability remains only a hint and booking always rechecks PostgreSQL.

### Module boundaries

| Module | Responsibility |
|---|---|
| Accounts | Password hashing, JWT lifecycle, roles, users, and owner-scoped passengers |
| Railway Offering | Stations, ordered routes/stops, trains/coaches/seats, fares, and dated train runs |
| Admission | Durable hot policy resolution, bounded waiting-room control, token lifecycle, and global admission limits |
| Booking | Segment masks, atomic allocation, holds, lifecycle transitions, tickets, idempotency, and reconciliation |
| Query | Direct PostgreSQL browse, search, and availability projections/hints; Redis read caches deferred |
| Event Relay | Outbox claim, publish, retry, stale-lease recovery, and finalize |
| Platform | Configuration, pools, metrics, clock, middleware, and process lifecycle |

Booking owns each reservation transaction. Event Relay delivers already committed events and never decides booking state. Domain packages do not depend on Gin, pgx, Redis, Prometheus, Docker, or HTTP status codes.

For an enabled hot policy, the API requires one short-lived, owner/request-bound admission token before attempting the existing booking transaction. Redis Lua scripts atomically enforce duplicate joins, monotonic policy-local FIFO ordering, queue capacity, worker-global issue rate, inflight capacity, token leases, and single-use transitions. Hot-run Redis failure fails closed rather than bypassing admission. Complete Redis loss may lose queue continuity, but it cannot corrupt PostgreSQL inventory. See [waiting-room-design.md](docs/waiting-room-design.md) and [hot-train-protection.md](docs/hot-train-protection.md).

The detailed model is in [domain-model.md](docs/domain-model.md); design decisions are under [docs/adr](docs/adr).

## Route-segment inventory

For a route `A -> B -> C -> D`, three bits represent the three travel segments:

```text
A -> C = 110
B -> D = 011
C -> D = 001
```

`A -> C` overlaps `B -> D`, so those bookings cannot share a physical seat. `A -> C` and `C -> D` do not overlap and may reuse it. The Go domain uses a variable-length mask, and PostgreSQL stores `BIT VARYING`; routes longer than 64 segments are supported.

Allocation uses equal-length `CASE`-guarded bit operations and deterministic row locks. A multi-passenger request updates exactly the requested number of seats or none. Reconciliation verifies:

```text
seat_inventory.occupied_segments
==
union(reservation_seats.segment_mask where reservation is held or confirmed)
```

See [segment-inventory.md](docs/segment-inventory.md) and [high-concurrency-design.md](docs/high-concurrency-design.md).

## Reservation lifecycle

```text
held -> confirmed
held -> expired
held -> cancelled
confirmed -> cancelled
```

- A hold snapshots the route interval, passengers, physical seats, fare minor units, currency, expiry, and exact masks.
- Confirmation keeps inventory occupied and creates one ticket order plus tickets. It is domain confirmation only; no payment authorization occurs.
- Cancellation releases the immutable masks once and cancels ticket artifacts when present.
- Expiration workers claim due holds with `FOR UPDATE SKIP LOCKED`; every reservation is processed in its own transaction so one failed item does not roll back the rest of a batch.
- Confirm/expire and cancel/confirm races serialize on the reservation row and finish in a valid state.

See [reservation-state-machine.md](docs/reservation-state-machine.md) and [consistency-model.md](docs/consistency-model.md).

## Durable idempotency and outbox

Reservation create, confirm, and cancel commands bind a SHA-256 key hash and canonical request fingerprint to one owner/operation in PostgreSQL. The raw client key is never persisted or logged. The idempotency record, domain mutation, and outbox event commit in the same transaction; a retry with the same fingerprint returns the resource, while a changed fingerprint conflicts.

Outbox publication is at least once. Workers claim and finalize in short transactions, publish outside a transaction, retry with bounded backoff, reclaim stale leases, and move exhausted events to a dead letter state. Consumers must deduplicate by event ID. See [outbox-events.md](docs/outbox-events.md).

## HTTP surface

The API is versioned under `/api/v1`. Customer reservation routes derive ownership only from the validated bearer token, never from request-body identity.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/livez` | Process-only liveness |
| `GET` | `/readyz` | Bounded dependency/configuration readiness |
| `GET` | `/metrics` | Prometheus exposition; keep internal in production |
| `POST` | `/api/v1/auth/register` | Create a customer account; always returns the same `202 Accepted` message for new/existing valid email |
| `POST` | `/api/v1/auth/login` | Authenticate an existing customer and issue access/refresh credentials |
| `GET` | `/api/v1/stations` | Station browse |
| `GET` | `/api/v1/train-runs/search` | Train-run search |
| `GET` | `/api/v1/train-runs/:id/availability` | Point-in-time availability hint |
| `POST` | `/api/v1/waiting-room/entries` | Join a selected hot-run policy queue |
| `GET` | `/api/v1/waiting-room/entries/:id` | Read owned approximate position/state; may deliver one token header |
| `DELETE` | `/api/v1/waiting-room/entries/:id` | Cancel an owned active queue entry |
| `POST` | `/api/v1/reservations` | Create a temporary hold; requires `Idempotency-Key` and, for a hot policy, `X-Admission-Token` |
| `GET` | `/api/v1/reservations/:id` | Read an owned reservation |
| `POST` | `/api/v1/reservations/:id/confirm` | Confirm an owned hold |
| `POST` | `/api/v1/reservations/:id/cancel` | Cancel an owned held/confirmed reservation |
| `POST` | `/api/v1/admin/routes` | Create a named, timezone-bound route with ordered station codes and arrival/departure offsets |
| `GET/POST` | `/api/v1/operator/hot-train-policies` | List/create bounded policies as operator or admin |
| `GET/PUT/DELETE` | `/api/v1/operator/hot-train-policies/:id` | Read, version-update, or soft-disable a policy |

Errors use bounded public codes and do not expose SQL, DSNs, credentials, tokens, raw idempotency keys, or passenger data.

Registration and login are intentionally separate. Registration never returns access/refresh tokens, and its direct generic accepted response does not distinguish a new email from an existing one. Because Milestone 1 creates immediately active accounts without an email-verification workflow, a caller can still attempt a later login with an attacker-chosen registration password and infer whether that new credential took effect. Database timing can also differ and is not claimed to be constant-time. These residuals are rate-limited and documented rather than hidden; stronger activation proof and anti-automation controls remain outside this milestone.

The focused registration/login response contract is recorded in [docs/openapi.yaml](docs/openapi.yaml). It is intentionally scoped and is not a complete platform API specification.

## Health and metrics

The API `/livez` does not call PostgreSQL or Redis. API `/readyz` checks PostgreSQL, Redis, migration version, and required production configuration with short timeouts and structured, sanitized component states. Each worker has a private `:9090` `/livez`, `/readyz`, and `/metrics` surface. Admission-worker readiness checks PostgreSQL, Redis, schema version, and its process-owned keyring/configuration; queue backlog alone does not fail readiness. Hold-expirer readiness checks only PostgreSQL; Redis Streams outbox readiness checks PostgreSQL and Redis, while log publishing checks only PostgreSQL. Worker pass duration is capped. Outbox backlog and dead-letter conditions are metrics/alert signals rather than direct readiness failures.

Prometheus labels use bounded operations, normalized route templates, result/status classes, and bounded reasons. User, passenger, reservation, train-run, seat, ticket, event, and arbitrary input values are excluded from labels.

## Migrations

Explicit SQL migrations live in `migrations/`; AutoMigrate is not used.

```powershell
$env:DATABASE_URL = 'postgres://railway:railway-local@localhost:5432/railway?sslmode=disable'
make migrate-up
make migrate-status
make migrate-create name=add_example
make migrate-down
```

`migrate-down` is a local/development command, not an automatic production rollback strategy. Production releases use a separate migration principal and backward-compatible schema changes.

Migration 5 requires the production procedure in [migration-5-production-rollout.md](docs/migrations/migration-5-production-rollout.md); the SQL checks under `docs/migrations/sql` are read-only operator aids, not automatic schema changes.

Migration 6 adds the durable hot-train policy, safe bound/uniqueness constraints, policy outbox event types, and indexes for derived held-reservation quota checks. Follow [migration-6-production-rollout.md](docs/migrations/migration-6-production-rollout.md); its down migration removes policy audit events and is not an automatic production rollback.

## Read-only reconciliation

`cmd/reconcile` provides detect-only correctness checks. It never repairs
production state, returns non-zero for violations or unavailable dependencies,
and emits bounded JSON summaries without customer or token identifiers.

```powershell
go run ./cmd/reconcile seat-inventory --train-run-id <canonical-uuid>
go run ./cmd/reconcile reservation-quotas
go run ./cmd/reconcile admission-state
```

All commands require `DATABASE_URL`; `admission-state` also requires
`REDIS_ADDRESS` or `REDIS_ADDR`. Quota checks use the same bounded
`RESERVATION_MAX_ACTIVE_*` settings as the API unless explicit CLI limits are
supplied. The container build exposes an optional `reconcile` target while the
default image remains the API.

## Local setup

Requirements:

- Go 1.25.12+
- Docker Engine with Compose v2
- GNU Make for the documented shortcuts (or run the underlying Go/Docker commands)

Copy `.env.example` only as a reference and inject local values through the environment. Do not commit `.env` files or real tokens.

```powershell
$env:JWT_SECRET = 'local-only-random-secret-at-least-32-bytes'
docker compose up --build
```

Start the optional workers with:

```powershell
docker compose --profile workers up --build
```

The workers profile includes admission-worker, hold-expirer, and outbox-worker. The committed admission derivation key is synthetic local-test material only. Production must inject an independent 32-byte keyring. A local three-API, two-admission-worker topology with Redis AOF and a non-sticky load balancer is available with:

```powershell
docker compose -f docker-compose.multi-replica.yml up --build
```

Then verify:

```powershell
$loadBalancerPort = (
    docker compose -f docker-compose.multi-replica.yml port load-balancer 8080
) -replace '^.*:', ''
Invoke-RestMethod "http://127.0.0.1:$loadBalancerPort/livez"
Invoke-RestMethod "http://127.0.0.1:$loadBalancerPort/readyz"
```

On Linux, the multi-replica harness reaches its evidence-only endpoints through
the isolated Compose bridge. On Windows/Docker Desktop, it resolves
Compose-assigned ephemeral loopback ports. Both paths avoid fixed host-port
collisions. The bounded automated procedure is documented in
[Milestone 2 load testing](docs/milestone-2-load-testing.md). Compose
credentials are explicit local-development defaults only. There is no
committed production secret, default production database, or production-ready
customer bootstrap token.

## Tests and validation

Unit tests do not require dependencies. PostgreSQL integration tests run when `DATABASE_URL` is present and otherwise skip.

```powershell
go mod tidy
go vet ./...
go test ./... -count=1 -timeout 300s
go test -race ./...
staticcheck ./...
govulncheck ./...
```

Migration and container checks:

```powershell
make migrate-up
make migrate-up
make migrate-status
docker compose config
docker compose -f docker-compose.multi-replica.yml config
docker build -t scalable-railway-ticketing-platform:milestone-2 .
```

CI also verifies tidy/gofmt, action syntax, secret scanning,
filesystem/image vulnerabilities, populated Migration 5-to-6 and destructive
down/reapply rehearsals, integration tests, the Docker build, and a bounded
three-API/two-worker smoke with API, worker, and real Redis failure recovery.

## Load tests

The original nine k6 scenarios remain, and eight Milestone 2 scenarios cover waiting-room join/status, admission, hot reservation, admission idempotency, durable quota, Redis outage, and multi-replica shared state. They accept configuration only through environment variables and contain no credentials or tokens.

See [Milestone 2 load testing](docs/milestone-2-load-testing.md) for setup and correctness gates. [benchmark-report-milestone-2.md](docs/benchmark-report-milestone-2.md) intentionally records no capacity numbers until a controlled run is executed. A smoke run is not evidence of production or national-scale throughput.

## Deployment

[production-deployment.md](docs/production-deployment.md) defines the single-region release, migration, secret, health, monitoring, backup, and rollback contract. `deploy/kubernetes/base` supplies a hardened baseline that requires a production overlay and externally managed secrets.

## Current limitations

- Single-region PostgreSQL primary for all authoritative writes.
- Redis AOF reduces loss risk but does not guarantee waiting-room continuity; hot-run Redis loss fails closed.
- Admission does not guarantee a seat and token delivery is at-most-once.
- Station, search, and availability Redis read caches are not implemented; current reads use PostgreSQL and future cached values remain hints.
- No real payment authorization/capture/refund integration.
- No complete anti-bot/fraud system or real identity proof; account quotas do not prevent Sybil identities.
- No multi-region active-active booking writes.
- No accepted sustained benchmark or national-scale capacity claim.
- No government-ID or real passenger identity verification.

The complete list is in [milestone-2-limitations.md](docs/milestone-2-limitations.md). Future multi-region ideas are design direction only in [future-multi-region-design.md](docs/future-multi-region-design.md); none are implemented in Milestone 2.

## License

[MIT](LICENSE)
