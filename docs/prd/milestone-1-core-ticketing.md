# Milestone 1: Core Railway Ticketing

Status: Accepted for implementation

Target: Milestone 1
Last updated: 2026-07-15

## Problem Statement

Railway booking differs from quantity-based inventory because one physical seat can be sold more than once on non-overlapping portions of the same route, while overlapping journeys must remain mutually exclusive. A correct backend must coordinate search, temporary holds, confirmation, cancellation, expiration, passenger ownership, and event publication without overselling under concurrency.

This milestone must prove those correctness properties for a production-minded, single-region backend. It must not imply national-scale capacity, global consistency, or completed payment and identity-verification systems.

The accurate project description is:

> A production-minded, single-region railway ticketing backend that proves segment-based seat allocation, temporary reservation correctness, idempotency, transactional consistency, and high-concurrency safety.

## Solution

Build a new Go modular monolith whose authoritative booking transaction runs in PostgreSQL. A variable-length segment mask represents the occupied route portions for each seat and train run. Atomic SQL and deterministic row locking allocate all seats for a reservation or none. Durable PostgreSQL idempotency records, transactionally written outbox events, explicit state transitions, and a reconciliation invariant close the main correctness gaps.

Redis provides bounded-TTL read hints, rate limits, and optional event transport, but never owns seat occupancy, reservation state, confirmed tickets, or durable idempotency. Gin exposes versioned HTTP APIs. Explicit SQL migrations, Prometheus metrics, workers with coordinated lifecycle, concurrency tests, k6 scenarios, security gates, and hardened containers establish a reproducible operational baseline.

## Actors

- **Customer**: browses stations and services, manages their own passengers, creates and manages reservations, and reads their own tickets.
- **Admin**: manages stations, routes, stops, trains, coaches, seats, and fares.
- **Railway operator**: manages train runs, initializes inventory, changes operational state, and performs controlled reconciliation or expiration operations.
- **Hold-expiration worker**: expires elapsed holds and releases only their exact occupied segments.
- **Outbox publisher**: claims committed events, publishes outside database transactions, and finalizes or dead-letters outcomes.
- **Event consumer**: processes published events idempotently by event ID without becoming authoritative for booking correctness.

## User Journeys

1. A customer registers or signs in, creates synthetic passenger profiles, browses stations, and searches a service date from an origin that precedes the destination on the route.
2. The customer inspects an availability hint and submits an authenticated hold request with a durable idempotency key.
3. The system validates the train run, route direction, passenger ownership, seat class, fare, and configured passenger limit in one booking transaction.
4. The system atomically allocates one seat per passenger for the exact requested segments, snapshots fares, creates a held reservation, completes idempotency, and writes an outbox event.
5. Repeating the same command with the same key and canonical fingerprint returns the existing resource; changing the fingerprint returns conflict.
6. Before expiry, the customer confirms the hold. The system keeps seat masks occupied and creates a ticket order and one ticket per reservation seat.
7. The customer may cancel a held or confirmed reservation. Exact masks are released, ticket artifacts are cancelled when present, and the command is stable when repeated.
8. If a hold reaches its deadline, one expiration worker locks it, releases exact masks, marks it expired, and commits the corresponding event. Confirmation cannot subsequently revive it.
9. An admin configures route topology and rolling stock. An operator creates a train run, initializes one inventory row per active seat, and changes the run only through authorized routes.
10. An operator or test harness reconciles stored occupancy against the union of active reservation-seat masks and treats any mismatch as a correctness failure.
11. An outbox publisher claims a bounded batch, publishes without holding a database transaction, and finalizes in a short transaction; stale processing claims are recoverable.

## User Stories

1. As a customer, I want to list active stations so that I can select a valid origin and destination.
2. As a customer, I want to search train runs by service date and ordered route stops so that reverse or invalid journeys are rejected.
3. As a customer, I want to inspect seat-class availability so that I can decide whether to attempt a hold.
4. As a customer, I want final allocation to recheck PostgreSQL so that a stale availability hint cannot oversell a seat.
5. As a customer, I want to book multiple owned passengers together so that they either all receive seats or none do.
6. As a customer, I want a temporary hold with an explicit expiry so that I have a bounded confirmation window.
7. As a customer, I want the same idempotent request to return the same reservation so that retries are safe.
8. As a customer, I want a changed request under the same idempotency key to fail clearly so that accidental key reuse is visible.
9. As a customer, I want to read only my reservations and ticket orders so that other customers cannot access my data.
10. As a customer, I want to confirm an unexpired hold so that tickets are issued without releasing its seats.
11. As a customer, I want repeated confirmation to return a stable confirmed result so that network retries are safe.
12. As a customer, I want to cancel my held or confirmed reservation so that exact route segments become available again.
13. As a customer, I want repeated cancellation to be stable so that retries cannot double-release inventory.
14. As an admin, I want to manage stations, routes, stops, trains, coaches, seats, and fares behind explicit role checks.
15. As an operator, I want to create train runs and initialize inventory so that every active seat has the correct zero mask length.
16. As an operator, I want to make a train run non-bookable so that no new hold can commit after that status is authoritative.
17. As a hold-expiration worker, I want to claim expired holds with `SKIP LOCKED` so that multiple workers do not expire the same hold twice.
18. As an outbox publisher, I want domain events committed with domain state so that committed changes are never silently lost.
19. As an event consumer, I want stable event IDs so that duplicate delivery can be handled idempotently.
20. As an operator, I want bounded operational metrics so that failures and backlogs are visible without leaking identifiers.
21. As an operator, I want separate liveness and readiness checks so that process health is not confused with dependency readiness.
22. As a security reviewer, I want raw secrets, tokens, idempotency keys, and passenger identifiers absent from logs and metrics.
23. As a developer, I want explicit reversible migrations so that a clean PostgreSQL database is reproducible.
24. As a developer, I want deterministic concurrency tests so that overselling and lifecycle races are reproducible without sleeps.
25. As a reviewer, I want a reconciliation invariant so that leaked, double-assigned, or incorrectly released masks fail acceptance.
26. As an operator, I want Docker and CI security gates so that the runtime and delivery workflow have a hardened baseline.

## Functional Requirements

### Identity and access

- Support `customer`, `admin`, and `operator` roles.
- Hash passwords with bcrypt.
- Issue typed access and refresh JWTs with signing-method enforcement and token-version support.
- Derive the acting user and role from the validated token, never request bodies.
- Enforce ownership on passengers, reservations, ticket orders, and tickets.
- Apply atomic Redis-backed rate limits to registration, login, and reservation creation. Redis failure must fail safely according to the documented endpoint policy without corrupting booking state.

### Railway topology and schedule

- Model stations, routes, contiguous zero-based route stops, trains, coaches, seats, fares, and dated train runs.
- Disallow duplicate stations within one Milestone 1 route.
- Require non-decreasing arrival/departure offsets; allow offsets above 1,440 minutes for overnight travel.
- Treat `service_date` as the operating date in the configured route/operator timezone while storing database timestamps in UTC.
- Permit new holds only for explicitly bookable train-run statuses.

### Segment inventory

- Model a route with N stops as N-1 ordered segments.
- Support more than 64 segments through a variable-length domain value object and PostgreSQL `VARBIT` persistence.
- Define a request from `from_stop_index` to `to_stop_index` as bits `[from, to)`.
- Reject non-positive segment counts, same-stop journeys, reverse direction, out-of-range indices, and incompatible mask lengths.
- Provide overlap, union, subtract, zero, length, equality, and stable string operations.
- Keep one `seat_inventory` row per active seat and train run, with stored mask length validated against `train_runs.segment_count`.

### Reservations and tickets

- Create one reservation for one or more unique owned passengers and allocate exactly one deterministic, class-matching seat per passenger.
- Allocate all requested seats or roll back the entire transaction.
- Store held, confirmed, expired, and cancelled reservations.
- Permit `held -> confirmed`, `held -> expired`, `held -> cancelled`, and `confirmed -> cancelled`; reject all other state changes except specified idempotent repeats.
- Create a ticket order and one active ticket per reservation seat when confirming.
- Release exact masks on cancellation or expiration; never release confirmed inventory through expiration.
- Resolve confirm, cancel, expiration, and train-run status races through consistent row-lock ordering and authoritative predicates.

### Durable idempotency

- Make PostgreSQL authoritative for `reservation.create`, `reservation.confirm`, and `reservation.cancel` idempotency.
- Scope uniqueness by user, operation, and a SHA-256 key hash; never persist a raw key.
- Compare a stable fingerprint over canonical command fields.
- Complete the idempotency record in the same transaction as the resource and outbox event.
- Return the same resource for the same key/fingerprint and `409 Conflict` for a different fingerprint.
- Allow Redis only as an optional completed-result cache.

### Outbox and workers

- Write reservation and ticket events in the same PostgreSQL transaction as state changes.
- Implement `pending`, `processing`, `published`, and `dead_letter` states with attempts, retry time, lock owner/time, and publication time.
- Claim in a short transaction, publish outside it, and finalize in a second short transaction.
- Recover stale processing claims and use bounded retry/dead-letter behavior.
- Provide `log` and optional, disabled-by-default `redis_stream` publishers.
- Provide a deterministic `RunOnce` hold-expiration seam and clean cancellable worker loops.

### APIs

- Provide station listing, train-run search, availability, reservation create/read/confirm/cancel, and ticket-order list/read endpoints.
- Provide authenticated admin/operator write endpoints for the resources in their permissions.
- Normalize pagination, bound page size, and use an allowlist for sort order.
- Return a stable JSON envelope with machine-readable error code, safe message, request ID, and field details where appropriate.
- Do not expose raw database or internal errors.

### Cache and rate limits

- Cache station metadata, normalized search results, and short-lived availability hints with versioned keys, bounded TTLs, and jitter.
- Prefer exact-key invalidation and use `SCAN`, never production `KEYS`.
- Treat all availability cache values as hints; booking must execute the authoritative overlap predicate.
- Use atomic Lua counters for rate limiting.

### Health, metrics, and lifecycle

- Provide process-only `/livez`, dependency/config/migration-aware `/readyz`, and `/metrics`.
- Coordinate the HTTP server, optional workers, and publisher from one signal-derived root context.
- Bound metric label values and normalize Gin route patterns.
- Never label by user, reservation, ticket, train-run, seat, event, idempotency, passenger, or arbitrary station/request data.
- Treat outbox backlog and dead-letter counts as metrics, not direct readiness failures.

## Non-Functional Requirements

### Correctness and consistency

- PostgreSQL is the sole authority for seat occupancy, active reservation state, confirmed tickets, final ticket-order status, and durable idempotency.
- Use explicit SQL and transactions for booking, release, expiration, idempotency, and outbox hot paths.
- Use deterministic lock ordering and bounded retries only for recognized transient SQLSTATEs such as `40001` and `40P01`.
- Prefer the lowest isolation level proven correct by atomic predicates and row locks; do not select `SERIALIZABLE` without evidence.
- Keep domain packages independent from Gin, pgx, Redis, Prometheus, Docker, and HTTP status codes.

### Performance

- Establish measured p50, p95, p99, RPS, reservation TPS, error, allocation conflict, database, Redis, and outbox observations through k6 and Prometheus.
- Do not fabricate values or make national-scale claims.
- Make hot-path SQL bounded, indexed, deterministic, and explainable.

### Reliability

- Shut down gracefully on `SIGINT` and `SIGTERM` without worker or goroutine leaks.
- Make repeated worker execution safe and recover abandoned outbox claims.
- Keep Redis outages outside authoritative database correctness.
- Ensure migration-up is repeatable when no new migrations remain.

### Security and privacy

- Minimize passenger data and use synthetic identifiers in tests.
- Do not require government identity data in Milestone 1.
- Never log passwords, JWTs, DSNs, Redis credentials, idempotency keys, passenger identifiers, or full event payloads.
- Enforce body-size limits, trusted-proxy configuration, safe errors, JWT type/method/version validation, role checks, and ownership checks.
- Validate production secrets and configuration at startup.
- Run static analysis, vulnerability, secret, filesystem, image, and workflow checks in CI when tooling is available.

### Operability

- Use Go 1.25.2, the verified stable local toolchain, and document that it differs from the preferred 1.26.x baseline.
- Target PostgreSQL 16+, Redis 7+, Gin, pgx/v5/pgxpool, golang-migrate, Prometheus `client_golang`, k6, and Docker Compose.
- Build a multi-stage non-root container with a minimal runtime image and read-only-root-filesystem support.
- Keep production secrets environment-driven and local-development defaults explicitly scoped.

## Domain Terminology

- **Station**: a named railway location with a stable code and timezone.
- **Route**: an ordered, reusable sequence of route stops.
- **Route stop**: a station's zero-based position and schedule offsets within a route.
- **Segment**: the directed interval between two adjacent route stops.
- **Segment mask**: a variable-length bit set whose set bits are occupied route segments.
- **Train**: rolling stock composed of coaches and seats.
- **Train run**: a train operating on one route for one `service_date` with an authoritative status and segment count.
- **Fare**: an active price in integer minor units for a route interval and seat class.
- **Seat inventory**: the authoritative occupied segment mask for one seat on one train run.
- **Passenger**: a minimal customer-owned traveler profile; it is not a verified national identity.
- **Reservation**: one atomic group of passenger-seat allocations over a route interval.
- **Reservation seat**: the immutable passenger, seat, segment mask, and fare snapshot within a reservation.
- **Hold**: a reservation in `held` state that blocks inventory until confirmation, cancellation, or expiration.
- **Ticket order**: the confirmed booking artifact; it does not represent a real payment workflow.
- **Ticket**: the travel entitlement created per reservation seat on confirmation.
- **Durable idempotency record**: the PostgreSQL authority that binds a key hash and fingerprint to one command result.
- **Outbox event**: an event persisted atomically with domain state and published asynchronously.
- **Availability hint**: a possibly stale cache/read response that never guarantees booking success.
- **Reconciliation invariant**: equality between stored occupancy and the union of `held` and `confirmed` reservation-seat masks.

## Core Domain Invariants

1. Origin and destination differ and satisfy `from_stop_index < to_stop_index`.
2. Both indices belong to the selected train run's ordered route.
3. A segment-mask length equals the train run's segment count.
4. Every reservation passenger is unique, owned by the customer, and receives exactly one unique class-matching seat.
5. All seat mutations for a reservation commit or roll back together.
6. Held and confirmed reservations occupy their exact segments; expired and cancelled reservations do not.
7. Expiration never releases a confirmed reservation, and confirmation never revives an elapsed or terminal reservation.
8. Overlapping active allocations cannot share a seat; non-overlapping intervals may.
9. Fares and totals use checked integer minor-unit arithmetic.
10. The same durable key and fingerprint returns one resource; a changed fingerprint conflicts, even if Redis is unavailable.
11. Every committed domain transition has its outbox event in the same transaction.
12. For each train run and seat, stored occupancy equals the union of active reservation-seat masks and contains no duplicate active assignment.

## Consistency Requirements

The correctness hierarchy is fixed:

1. PostgreSQL authoritative seat inventory.
2. Reservation transaction.
3. Durable outbox.
4. Event publisher.
5. Downstream consumers.
6. Regional caches and read models.

Redis and consumers never bypass or reverse this hierarchy. Search and availability can be eventually consistent, but hold, confirm, cancel, expire, idempotency, and reconciliation decisions use authoritative PostgreSQL state.

## Privacy Requirements

- Store only a passenger display name and ownership link unless a later milestone approves additional fields.
- If document identifiers are introduced later, encrypt them at rest, use keyed equality hashes, redact all output, and document key management.
- Use synthetic passenger data in all fixtures and load tests.
- Prevent passenger identifiers and raw event payloads from reaching logs, metrics, or error responses.
- Bound access through customer ownership and explicit admin/operator permissions.

## Failure Behavior

- Invalid routes, transitions, ownership, roles, masks, seat classes, passengers, money, or request shapes return bounded domain/API errors.
- Allocation shortfall rolls back every seat update, reservation row, reservation-seat row, idempotency completion, and outbox event.
- Recognized allocation conflicts return a business conflict rather than an internal error.
- Same-key/different-fingerprint requests return `409 Conflict`.
- Redis cache failure falls back to PostgreSQL where safe and cannot create duplicate reservations.
- Confirm-versus-expire and cancel-versus-confirm races serialize on the reservation lock and end in one valid state.
- Train-run cancellation prevents a new hold from committing after non-bookable status becomes authoritative.
- Worker failure isolates one item and continues the bounded batch safely.
- Outbox publication failure schedules bounded retry and eventually dead-letters; it does not roll back already committed domain state.
- Readiness reports dependency/config/migration component status with short timeouts and no secrets; liveness remains process-only.

## Implementation Decisions

- Keep Milestone 1 as a modular monolith with bounded internal packages and a shared PostgreSQL transaction boundary where booking correctness requires it.
- Use consumer-defined interfaces and constructor injection. Do not build a speculative internal framework.
- Use PostgreSQL `VARBIT` per train run and seat; isolate encoding/decoding in the pgx adapter.
- Use atomic overlap predicates and deterministic row locking for all-or-nothing multi-seat allocation.
- Use explicit SQL migrations and never GORM or AutoMigrate.
- Use integer minor-unit money with checked addition and multiplication.
- Use PostgreSQL durable idempotency and an optional Redis completed-result cache.
- Use claim, publish, finalize outbox processing with stale-lock recovery.
- Use a single root lifecycle context for servers and optional workers.
- Document ambiguous correctness decisions in ADRs before implementing the booking transaction.

## Testing Decisions

- Test externally observable domain behavior and database invariants rather than private implementation details.
- Exhaustively test small segment masks and add deterministic/property-style coverage, including masks longer than 64 segments.
- Test the reservation transition graph, checked money arithmetic, route validation, ownership, roles, errors, and metric label safety.
- Run PostgreSQL repository and integration tests against controlled fixtures and real migrations.
- Test one-seat, ten-seat, overlapping/non-overlapping, all-or-nothing multi-seat, concurrent idempotency, multiple expirer, confirm/expire, cancel/confirm, and train-run-cancel/new-hold races without arbitrary sleeps.
- Run reconciliation after concurrency suites and fail on leaks, double allocation, confirmed-seat release, or partial rollback.
- Run `go test ./... -count=1 -timeout 300s`, `go test -race ./...`, and repeated critical concurrency cases.
- Provide complete k6 scenarios and report only measured results; use an honest template when sustained testing cannot run.

## Milestone 1 Acceptance Criteria

- [ ] The repository has independent Git history and work occurs on `feat/milestone-1-core-ticketing`.
- [ ] The PRD, ten required ADRs, architecture, domain, security, consistency, load, deployment, future, and limitation documents exist.
- [ ] Explicit migrations create a clean PostgreSQL database and a second `up` is safe; AutoMigrate is absent.
- [ ] Station, route, route-stop, train, coach, seat, fare, train-run, passenger, reservation, ticket, idempotency, and outbox models exist.
- [ ] Route-stop order, segment masks, route direction, mask length, and routes above 64 segments are validated.
- [ ] One-seat allocation is atomic; multi-seat allocation is deterministic and all-or-nothing.
- [ ] Overlapping allocations cannot both succeed and non-overlapping intervals may reuse a seat.
- [ ] Holds block inventory; expiration and cancellation release exact masks; confirmed holds are not expired.
- [ ] Confirm/expire, cancel/confirm, and train-run-cancel/new-hold races remain consistent.
- [ ] Durable idempotency survives Redis loss and rejects changed fingerprints.
- [ ] Domain state and outbox events commit atomically; outbox processing recovers stale claims.
- [ ] Auth, JWT type/method/version validation, RBAC, ownership, bounded rate limits, and safe errors are enforced.
- [ ] Liveness, readiness, and bounded-label Prometheus metrics work without identifier leakage.
- [ ] Domain, repository, integration, race, repeated concurrency, and reconciliation gates pass.
- [ ] k6 scenarios exist and any reported p50/p95/p99 or throughput values are measured.
- [ ] The Docker image builds and Compose validates with non-root/hardened runtime configuration.
- [ ] CI contains formatting, vet, tests, race, staticcheck, govulncheck, actionlint, Gitleaks, Trivy, Docker, and migration gates.
- [ ] Security scans pass or findings are documented.
- [ ] Independent review has zero Critical and zero High findings before the non-draft pull request opens.
- [ ] Limitations state that the system is single-region and has no real payment, waiting room, identity verification, active-active writes, or national-scale capacity claim.
- [ ] No `v1.0.0` tag is created; `v0.1.0` remains subject to explicit approval.

## Out of Scope

- Multi-region active-active seat writes or global consensus.
- Database sharding, Kafka, service meshes, Kubernetes Operators, or cross-region transaction coordination.
- A real payment gateway, refund accounting, or payment saga implementation.
- Real email/SMS, anti-bot platform, waiting room, candidate queue, or waitlist.
- Ticket rescheduling, dynamic pricing, adjacency optimization, or cross-operator settlement.
- Frontend, mobile applications, real national ID validation, or real passenger identity verification.
- National-scale capacity claims or a 12306-equivalent positioning.

## Further Notes

- Future extraction requires evidence of an independent scaling profile, transaction/consistency boundary, deployment lifecycle, failure domain, and ownership boundary; a package name alone is not evidence.
- Future multi-region work may add active-active search, regional caches, event-driven invalidation, failover, and a single authoritative writer per train-run shard. It must not change Milestone 1's PostgreSQL booking authority.
- The Milestone 2 recommendation is limited to admission control, hot-run protection, reservation quotas, shard ownership, read-model/cache optimization, multi-replica benchmarks, chaos/failover testing, an optional payment-saga design, and operational reconciliation tooling.
