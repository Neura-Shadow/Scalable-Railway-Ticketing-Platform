# Scalable Railway Ticketing Platform

A production-minded, single-region Go backend that proves the correctness-critical core of railway booking: route-segment seat allocation, temporary holds, confirmation/cancellation/expiration, durable idempotency, and transactional outbox events under concurrency.

Milestone 1 is intentionally bounded. It is not a national-scale capacity claim, does not implement a real payment gateway or waiting room, does not support multi-region active-active writes, and does not perform real passenger identity verification. See [Milestone 1 limitations](docs/milestone-1-limitations.md).

## Architecture

The repository is a modular monolith with separate API and worker processes built from one codebase:

```text
HTTP transport
    -> application commands/queries
        -> domain model and consumer-owned ports
            -> PostgreSQL, Redis, Prometheus, and publisher adapters
```

PostgreSQL is authoritative for train-run status, seat inventory, reservation lifecycle, tickets, durable idempotency, and outbox state. Redis is limited to bounded caches, rate controls, and optional event transport; cached availability is only a point-in-time hint. Booking always rechecks PostgreSQL.

### Module boundaries

| Module | Responsibility |
|---|---|
| Accounts | Password hashing, JWT lifecycle, roles, users, and owner-scoped passengers |
| Railway Offering | Stations, ordered routes/stops, trains/coaches/seats, fares, and dated train runs |
| Booking | Segment masks, atomic allocation, holds, lifecycle transitions, tickets, idempotency, and reconciliation |
| Query | Read-only browse, search, and availability projections/hints |
| Event Relay | Outbox claim, publish, retry, stale-lease recovery, and finalize |
| Platform | Configuration, pools, metrics, clock, middleware, and process lifecycle |

Booking owns each reservation transaction. Event Relay delivers already committed events and never decides booking state. Domain packages do not depend on Gin, pgx, Redis, Prometheus, Docker, or HTTP status codes.

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
| `GET` | `/api/v1/stations` | Station browse |
| `GET` | `/api/v1/train-runs/search` | Train-run search |
| `GET` | `/api/v1/train-runs/:id/availability` | Point-in-time availability hint |
| `POST` | `/api/v1/reservations` | Create a temporary hold; requires `Idempotency-Key` |
| `GET` | `/api/v1/reservations/:id` | Read an owned reservation |
| `POST` | `/api/v1/reservations/:id/confirm` | Confirm an owned hold |
| `POST` | `/api/v1/reservations/:id/cancel` | Cancel an owned held/confirmed reservation |

Errors use bounded public codes and do not expose SQL, DSNs, credentials, tokens, raw idempotency keys, or passenger data.

## Health and metrics

`/livez` does not call PostgreSQL or Redis. `/readyz` checks PostgreSQL, Redis, migration version, and required production configuration with short timeouts and structured, sanitized component states. Outbox backlog and dead-letter conditions are metrics/alert signals rather than direct readiness failures.

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

## Local setup

Requirements:

- Go 1.25.2+
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

Then verify:

```powershell
Invoke-RestMethod http://localhost:8080/livez
Invoke-RestMethod http://localhost:8080/readyz
```

Compose credentials are explicit local-development defaults only. There is no committed production secret, default production database, or production-ready customer bootstrap token.

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
docker build -t scalable-railway-ticketing-platform:milestone-1 .
```

CI also verifies tidy/gofmt, action syntax, secret scanning, filesystem/image vulnerabilities, a clean migration-up path, integration tests, and the Docker build.

## Load tests

Nine k6 scenarios cover station browse, train search, availability, ordinary reservations, a hot train, idempotency, confirmation, expiration storms, and rate limiting. They accept configuration only through environment variables and contain no real tokens.

See [load-testing.md](docs/load-testing.md) for commands and correctness gates. [benchmark-report-milestone-1.md](docs/benchmark-report-milestone-1.md) intentionally records no capacity numbers until a controlled run is executed. A smoke run is not evidence of production or national-scale throughput.

## Deployment

[production-deployment.md](docs/production-deployment.md) defines the single-region release, migration, secret, health, monitoring, backup, and rollback contract. `deploy/kubernetes/base` supplies a hardened baseline that requires a production overlay and externally managed secrets.

## Current limitations

- Single-region PostgreSQL primary for all authoritative writes.
- Redis availability and search results are hints, not booking guarantees.
- No real payment authorization/capture/refund integration.
- No waiting room, durable reservation quota, or complete anti-bot/fraud system.
- No multi-region active-active booking writes.
- No accepted sustained benchmark or national-scale capacity claim.
- No government-ID or real passenger identity verification.

The complete list is in [milestone-1-limitations.md](docs/milestone-1-limitations.md). Future multi-region ideas are design direction only in [future-multi-region-design.md](docs/future-multi-region-design.md); none are implemented in Milestone 1.

## License

[MIT](LICENSE)
