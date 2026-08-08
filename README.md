# Scalable Railway Ticketing Platform

A production-minded, single-region Go backend for railway booking with explicit
train-run routing, monotonic PostgreSQL writer fencing, a bounded physical-
PostgreSQL shard pilot, a disposable journey read model, versioned Redis read
caches, Redis-backed hot-train admission, and authoritative route-segment
allocation under concurrency. Milestone 6 adds a provider-neutral payment saga,
signed durable webhook inbox, deterministic sandbox provider, idempotent
capture/full-refund operations, and physical-shard-local ticket issuance.

Milestone 4 is intentionally bounded. `legacy`, `shard-0`, and `shard-1` are
logical schemas in one PostgreSQL database, not independent physical shards.
Migration uses a bounded quiesced cutover, may reject writes for the selected
train run, never dual writes, and retains the source for a rollback window.
This is not a zero-downtime, multi-region, production-capacity, or national-
scale claim. See
[Milestone 4 limitations](docs/milestone-4-limitations.md).

Milestone 5 adds an opt-in three-database pilot: one control PostgreSQL and two
independent booking PostgreSQL instances. A durable control command saga owns
global idempotency, quota leases, and the reservation directory; exactly one
physical shard owns each routed booking transaction and its local receipt,
VARBIT inventory mutation, fence, and outbox. No transaction spans databases.
Online rebalancing means base copy and journal catch-up while the source serves,
followed by a measured bounded write pause. It never dual writes, retains the
source, and requires reverse migration after target-era writes. The pilot is
single-region, not zero-downtime or production-capacity certification. See
[the physical topology](docs/physical-shard-topology.md) and
[Milestone 5 limitations](docs/milestone-5-limitations.md).

Milestone 6 coordinates payment across control PostgreSQL, one configured
provider, and exactly one current physical booking shard without a distributed
transaction. The local sandbox provides hosted synthetic checkout and
deterministic faults only; it is not a live gateway. Browser redirects are not
authority, raw card data is never accepted, ambiguous financial outcomes are
queried before retry, and inventory is retained until a void/full refund and
local compensation are proven. See the
[payment PRD](docs/prd/milestone-6-payment-ticket-issuance.md) and
[Milestone 6 limitations](docs/milestone-6-limitations.md).

## Architecture

The repository is a modular monolith with separate API and worker processes built from one codebase:

```text
HTTP transport
    -> application commands/queries
        -> domain model and consumer-owned ports
            -> PostgreSQL, Redis, Prometheus, and publisher adapters
```

PostgreSQL public/control tables and the currently assigned routed booking
storage are authoritative for policies, train-run status, seat inventory,
reservation lifecycle, durable quotas, tickets, idempotency, and outbox state.
A PostgreSQL journey projection and versioned Redis station/search/availability
caches accelerate public reads but remain disposable. Redis also provides rate
controls, event transport, and ephemeral waiting-room/token state. Redis never
allocates a seat or selects a write owner; booking always rechecks PostgreSQL.

### Module boundaries

| Module | Responsibility |
|---|---|
| Accounts | Password hashing, JWT lifecycle, roles, users, and owner-scoped passengers |
| Railway Offering | Stations, ordered routes/stops, trains/coaches/seats, fares, and dated train runs |
| Admission | Durable hot policy resolution, bounded waiting-room control, token lifecycle, and global admission limits |
| Booking | Segment masks, atomic allocation, holds, lifecycle transitions, tickets, idempotency, and reconciliation |
| Sharding | Fixed catalog, locator routing, fenced transactions, bounded migration, cutover, and rollback controls |
| Query | Journey projection, source fallback, versioned station/search caches, and short-lived availability hints |
| Event Relay | Outbox claim, publish, retry, stale-lease recovery, and finalize |
| Payment | Intent/saga coordination, provider operations, verified webhook inbox, current-shard issuance/compensation, and detect-first reconciliation |
| Platform | Configuration, pools, metrics, clock, middleware, and process lifecycle |

Booking owns each reservation transaction. Event Relay delivers already committed events and never decides booking state. Domain packages do not depend on Gin, pgx, Redis, Prometheus, Docker, or HTTP status codes.

For an enabled hot policy, the API requires one short-lived, owner/request-bound admission token before attempting the existing booking transaction. Redis Lua scripts atomically enforce duplicate joins, monotonic policy-local FIFO ordering, queue capacity, worker-global issue rate, inflight capacity, token leases, and single-use transitions. Hot-run Redis failure fails closed rather than bypassing admission. Complete Redis loss may lose queue continuity, but it cannot corrupt PostgreSQL inventory. See [waiting-room-design.md](docs/waiting-room-design.md) and [hot-train-protection.md](docs/hot-train-protection.md).

Milestone 4 routes each train run to exactly one of the fixed logical storages
`legacy`, `shard-0`, or `shard-1`. Every booking mutation locks and validates
the public assignment generation and the selected storage's write fence in the
same PostgreSQL transaction as inventory, lifecycle, idempotency, quota,
locator, and central outbox changes. Route caches are bounded hints; Redis is
never ownership authority. See
[train-run-sharding-design.md](docs/train-run-sharding-design.md),
[shard-routing.md](docs/shard-routing.md), and
[single-writer-fencing.md](docs/single-writer-fencing.md).

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
held -> payment_pending -> confirmed
held -> expired
held -> cancelled
confirmed -> refund_pending -> cancelled
```

- A hold snapshots the route interval, passengers, physical seats, fare minor units, currency, expiry, and exact masks.
- With payment enabled, direct confirmation is disabled. A durable captured
  operation authorizes one fenced shard-local issuance transaction that
  confirms the reservation and creates one ticket per reserved seat.
- Cancellation releases the immutable masks once and cancels ticket artifacts when present.
- Expiration workers claim due holds with `FOR UPDATE SKIP LOCKED`; every reservation is processed in its own transaction so one failed item does not roll back the rest of a batch.
- Confirm/expire and cancel/confirm races serialize on the reservation row and finish in a valid state.

See [reservation-state-machine.md](docs/reservation-state-machine.md) and [consistency-model.md](docs/consistency-model.md).

## Durable idempotency and outbox

Reservation create, confirm, and cancel commands bind a SHA-256 key hash and canonical request fingerprint to one owner/operation in PostgreSQL. The raw client key is never persisted or logged. A minimal global key claim preserves uniqueness across logical storages, while the authoritative completion stays with routed booking state. Claim, completion, domain mutation, quota transition, locator, and central outbox event commit in the same transaction; a retry with the same fingerprint returns the resource, while a changed fingerprint conflicts.

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
| `POST` | `/api/v1/reservations/:id/payment-intents` | Create/replay an owner-scoped payment intent from server-derived money |
| `GET` | `/api/v1/payment-intents/:id` | Read bounded owned payment/session state |
| `POST` | `/api/v1/payment-intents/:id/cancel` | Request/replay void or full-refund compensation |
| `POST` | `/webhooks/payments/:provider` | Authenticate and durably deduplicate a provider webhook; no financial/shard effect in HTTP |
| `GET` | `/api/v1/tickets/:id` | Read one owned ticket through its control locator and current physical shard |
| `POST` | `/api/v1/admin/routes` | Create a named, timezone-bound route with ordered station codes and arrival/departure offsets |
| `GET/PATCH` | `/api/v1/operator/train-runs/:id/fares/:resource_id` | Read authoritative physical fare version or submit an idempotent train-run fare update |
| `GET/PATCH` | `/api/v1/operator/train-runs/:id/seats/:resource_id/booking-state` | Read or idempotently change shard-local seat booking eligibility |
| `GET/PATCH` | `/api/v1/operator/train-runs/:id/booking-policy-version` | Read or idempotently advance the shard-local booking-policy version |
| `GET/POST` | `/api/v1/operator/hot-train-policies` | List/create bounded policies as operator or admin |
| `GET/PUT/DELETE` | `/api/v1/operator/hot-train-policies/:id` | Read, version-update, or soft-disable a policy |

Errors use bounded public codes and do not expose SQL, DSNs, credentials, tokens, raw idempotency keys, or passenger data.

Physical operator mutations require an `Idempotency-Key` and the exact
authoritative `source_version` returned by the corresponding GET. They first
reserve a durable control command, then commit one shard-local receipt and
snapshot change, and finally atomically update the control projection and
command state. A reconciler completes an interrupted finalization; it never
repeats an already receipted shard mutation. The fare endpoint accepts only a
fare already scoped directly to that train run. Route-level shared fares are
rejected before shard execution.

Registration and login are intentionally separate. Registration never returns access/refresh tokens, and its direct generic accepted response does not distinguish a new email from an existing one. Because Milestone 1 creates immediately active accounts without an email-verification workflow, a caller can still attempt a later login with an attacker-chosen registration password and infer whether that new credential took effect. Database timing can also differ and is not claimed to be constant-time. These residuals are rate-limited and documented rather than hidden; stronger activation proof and anti-automation controls remain outside this milestone.

The focused registration/login response contract is recorded in [docs/openapi.yaml](docs/openapi.yaml). It is intentionally scoped and is not a complete platform API specification.

## Health and metrics

The API `/livez` does not call PostgreSQL or Redis. API `/readyz` checks PostgreSQL, Redis, migration version, and required production configuration with short timeouts and structured, sanitized component states. Each worker has a private `:9090` `/livez`, `/readyz`, and `/metrics` surface. Admission-worker readiness checks PostgreSQL, Redis, schema version, and its process-owned keyring/configuration; queue backlog alone does not fail readiness. Hold-expirer readiness checks only PostgreSQL; Redis Streams outbox readiness checks PostgreSQL and Redis, while log publishing checks only PostgreSQL. Payment-worker readiness checks control v10, every configured physical schema v2 shard, and provider readiness; payment-reconciler readiness checks its bounded control/current-shard/provider dependencies. Worker pass duration is capped. Backlog, uncertainty and manual-review conditions are metrics/alert signals rather than direct readiness failures.

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

Migration 8 adds the fixed logical-shard schemas, catalog, explicit legacy
assignments, monotonic fences, migration control state, global locators/claims,
and retained-public guards. It does not move train runs automatically. Keep
`BOOKING_SHARD_MODE=legacy` through expansion and follow
[migration-8-production-rollout.md](docs/migrations/migration-8-production-rollout.md)
before any explicit `schema_poc` opt-in. The down migration is blocked while a
run is non-legacy, a migration is active, or a logical shard retains booking
data.

Migration 9 expands the control database with bounded physical connection
references, the booking-command saga ledger, conservative quota leases, the
reservation directory, physical migration checkpoints, and reconciliation
evidence. Physical booking databases use the independent migration history at
`migrations/booking-shard`, starting at version 1. Catalog rows never contain a
DSN. Follow [the control rollout](docs/migrations/migration-9-control-plane-rollout.md)
and [the booking-shard rollout](docs/migrations/booking-shard-v1-rollout.md).

Migration 10 adds payment intents, sagas, provider operations, webhook/conflict
evidence, reconciliation checkpoints, manual-review cases, payment-aware
logical compatibility layouts, and physical catalog schema version 2. Physical
booking databases independently apply booking-shard Migration 2 for payment
receipts, refund/compensation state, tickets, outbox and migration-journal
coverage. Follow the [control rollout](docs/migrations/migration-10-payment-control-rollout.md)
and [booking-shard v2 rollout](docs/migrations/booking-shard-v2-payment-rollout.md).

## Read-only reconciliation

`cmd/reconcile` provides detect-only correctness checks. It never repairs
production state, returns non-zero for violations or unavailable dependencies,
and emits bounded JSON summaries without customer or token identifiers.

```powershell
go run ./cmd/reconcile seat-inventory --train-run-id <canonical-uuid>
go run ./cmd/reconcile reservation-quotas
go run ./cmd/reconcile admission-state
go run ./cmd/reconcile read-model --train-run-id <canonical-uuid>
go run ./cmd/reconcile cache-versions --train-run-id <canonical-uuid>
go run ./cmd/reconcile shard-assignments
go run ./cmd/reconcile shard-locators
go run ./cmd/reconcile shard-migration --migration-id <canonical-uuid>
```

The standalone seat-inventory and reservation-quota commands predate shard
routing and are not sufficient by themselves as Milestone 4 shard-wide proof.
`shard-admin reconcile` is a bounded locator/resource scope, not every scope.

See [shard-reconciliation.md](docs/shard-reconciliation.md) for the scope and
shard-awareness matrix. Final acceptance combines all applicable scopes and
the bounded integrity evidence; no single command is a complete verdict.

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

The single-replica Compose file refuses to start without `JWT_SECRET` and
publishes the API on loopback only. Use a distinct random value for every
environment; never reuse the example above or expose a development deployment
to an untrusted network.

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

Logical schema routing remains disabled by default. `schema_poc` requires the
fixed allowlisted topology, Migration 8, compatible fenced writers, an explicit
production acknowledgement, and the operator gates in the rollout runbook.

The physical pilot is also disabled by default. A local isolated topology is
available after its required command/worker binaries are built:

```powershell
docker compose -f docker-compose.physical-shards.yml up --build
```

It uses fixed `physical-shard-0` and `physical-shard-1` configuration keys and
separate secret DSNs. Do not reuse its synthetic local credentials.

## Tests and validation

Unit tests do not require dependencies. PostgreSQL integration tests run when `DATABASE_URL` is present and otherwise skip.

```powershell
go mod tidy
go vet ./...
go test ./... -count=1 -timeout 480s
go test -race ./... -count=1 -timeout 600s
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
docker build -t scalable-railway-ticketing-platform:milestone-4 .
```

CI also verifies tidy/gofmt, action syntax, secret scanning,
filesystem/image vulnerabilities, populated migration rehearsals, routing and
fencing safety, migration/cutover/rollback restrictions, read-model/cache
regressions, the Docker build, and the bounded multi-replica topology.

## Load tests

The original Milestone 4 measurement/failure scenarios remain. The bounded
runner also uses a per-replica prewarm helper, drains all earlier train-run
events to durable read-model receipts before taking its cache-version baseline,
and requires the exact `shard_cutover` event receipt before attributing a
namespace rotation to cutover. A separate probe covers post-cutover lifecycle
correctness.

Despite its filename, `cross-shard-admin.js` is a customer cross-route batch
read. Private admin fanout uses `reconcile shard-assignments` and currently
traverses the three fixed storages serially, with effective concurrency `1`.

The outage smoke disables one logical catalog route; it is not a schema,
PostgreSQL-process, host, disk, or network failure. All scripts use environment
configuration and contain no embedded credentials or tokens.

See [Milestone 4 load testing](docs/milestone-4-load-testing.md) for setup and
correctness gates. [benchmark-report-milestone-4.md](docs/benchmark-report-milestone-4.md)
records every Milestone 4 result as pending until a controlled run is accepted.
A smoke run is not physical-shard, production, or national-scale evidence.

Milestone 6 adds ten bounded payment correctness/recovery scripts. Their
committed benchmark report remains `not_run` until a sanitized canonical bundle
and post-run provider/control/shard invariants exist; scripts alone cannot prove
no duplicate charge, refund, ticket, or production capacity. See
[Milestone 6 load testing](docs/milestone-6-load-testing.md).

## Deployment

[production-deployment.md](docs/production-deployment.md) defines the single-region release, migration, secret, health, monitoring, backup, and rollback contract. `deploy/kubernetes/base` supplies a hardened baseline that requires a production overlay and externally managed secrets.

## Current limitations

- Single-region control and current-shard PostgreSQL primaries; the optional
  logical-schema mode still shares one physical failure domain.
- Quiesced train-run migration may reject writes and does not claim zero
  downtime. Source and target are never dual writable.
- Source retention increases disk/backup scope. After any target write, direct
  rollback is forbidden and a reverse migration is required.
- Fixed global locators, quota/idempotency claims, central outbox, and cross-
  schema foreign keys are not a physical-database protocol.
- Redis AOF reduces loss risk but does not guarantee waiting-room continuity; hot-run Redis loss fails closed.
- Admission does not guarantee a seat and token delivery is at-most-once.
- Projection lag and cache staleness are possible; availability remains hint-only and cache loss increases PostgreSQL read load.
- Only the deterministic sandbox payment provider is implemented; there is no
  live gateway, settlement, dispute, partial-refund, PCI-certification, or
  production-capacity evidence.
- No complete anti-bot/fraud system or real identity proof; account quotas do not prevent Sybil identities.
- No multi-region active-active booking writes.
- No accepted sustained benchmark or national-scale capacity claim.
- No government-ID or real passenger identity verification.

The complete list is in
[milestone-6-limitations.md](docs/milestone-6-limitations.md). Multi-region
ideas are design direction only in
[future-multi-region-design.md](docs/future-multi-region-design.md); none are
implemented in Milestone 6.

## License

[MIT](LICENSE)
