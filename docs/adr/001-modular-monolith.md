# ADR 001: Start as a Modular Monolith

- Status: Accepted
- Date: 2026-07-15

## Context

Milestone 1 must prove one correctness-critical workflow: route-aware inventory allocation, reservation lifecycle transitions, durable idempotency, and an outbox event must agree under concurrency. Splitting that workflow across deployable services would add network failure modes and distributed transactions before any independent scaling or ownership evidence exists.

The project is an API-only Go backend. PostgreSQL is the system of record, Redis is a non-authoritative accelerator, and no real-time client protocol is required. The API uses REST/JSON through Gin. Authentication uses short-lived typed JWT access tokens plus refresh tokens, bcrypt password hashes, and token-version revocation. Errors are typed in domain/application modules and mapped once at the HTTP edge.

## Decision

Implement Milestone 1 as one deployable modular monolith plus separately runnable processes that reuse the same internal modules:

- `cmd/api`: HTTP API.
- `cmd/hold-expirer`: reservation expiration worker.
- `cmd/outbox-worker`: outbox publisher.
- `cmd/admin`: optional administrative command surface.

Organize source feature-first under `internal/<domain>`, with platform adapters under `internal/platform`. The business bounded contexts are deliberately broad:

1. Accounts: users, credentials, JWTs, RBAC, passenger profiles, and passenger ownership.
2. Railway Offering: stations, routes/stops, trains/coaches/seats, fares, train runs, operational status, and inventory commissioning.
3. Booking: segment occupancy, reservations, reservation seats, ticket orders, tickets, idempotent commands, and reconciliation.

Supporting modules are not separate business contexts:

- Query: station browse, train-run search, availability hints, and Redis decorators.
- Event Relay: outbox claim/finalize processing and publisher adapters. Transactional event insertion remains owned by the command transaction.
- Platform: configuration, PostgreSQL/Redis connections, lifecycle, clock, HTTP middleware/response mapping, and metrics.

The compile-time direction is:

```text
HTTP transport -> application use case -> domain model and consumer-owned ports
                                             ^
                                             |
                              PostgreSQL/Redis/outbox adapters
```

Domain modules do not import Gin, pgx, Redis, Prometheus, Docker, or HTTP status codes. Adapters implement consumer-owned interfaces. A seam is introduced only when it has a real second adapter (for example production clock and deterministic test clock) or isolates external technology from domain code.

The Booking application module owns every reservation transaction. SeatInventory, Reservation, TicketOrder/Ticket, booking idempotency, and transactional event append remain inside this context. The PostgreSQL adapter may read Railway Offering and Accounts tables through transaction-scoped, consumer-owned ports; it never makes remote calls inside the transaction.

Railway Offering owns `CommissionTrainRun`, one transaction that creates the non-visible run and all Booking inventory rows before exposing it as scheduled/bookable. This is a documented modular-monolith exception across owned tables. A future extraction would require a non-bookable preparation handshake, which is not added now.

Deep modules concentrate correctness behind small interfaces:

- `SegmentMask`: all variable-length mask validation and algebra.
- `Reservation`: the complete state transition graph and idempotent repeat policy.
- `Money`: checked minor-unit arithmetic and currency compatibility.
- `BookingStore`: atomic create/confirm/cancel operations, including inventory, tickets, idempotency, and outbox writes.
- `ExpirationProcessor.RunOnce`: bounded safe expiration.
- `OutboxPublisher.RunOnce`: claim, publish, and finalize behavior.
- `Reconciler`: stored-mask versus active-reservation invariant.

## Full-stack baseline decisions

- Project structure: feature-first modular monolith, because domain locality matters more than technical-layer grouping.
- API client: none in Milestone 1; expose stable REST/JSON contracts suitable for later OpenAPI generation.
- Authentication: typed JWT access/refresh flow with signing-method/type/version validation and explicit RBAC.
- Real-time method: none; downstream events are asynchronous internal integration, not a client WebSocket/SSE feature.
- Error handling: typed domain/application errors with one safe global HTTP mapper.

## Consequences

- Booking correctness remains inside one PostgreSQL transaction and one deployment failure domain.
- Workers can scale separately as processes without duplicating domain logic; hold expiration is an adapter into Booking, not another domain owner.
- Package dependencies and tests make bounded-context violations visible.
- Some modules share a database. That is intentional for Milestone 1 and must not be presented as independent services.
- Future extraction requires evidence of an independent scaling profile, transaction/consistency boundary, deployment lifecycle, failure domain, and ownership boundary.

## Rejected alternatives

- Microservices per context: rejected because it fragments the authoritative transaction without evidence.
- Layer-first global controller/service/repository packages: rejected because railway rules lose locality.
- A shared internal framework: rejected because it would create speculative, shallow abstractions.
- Event-first seat allocation: rejected because asynchronous consumers cannot authoritatively prevent overlap.
