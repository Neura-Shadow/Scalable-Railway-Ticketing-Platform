# Milestone 2: Hot Train Protection and Waiting Room Admission

Status: Accepted for implementation

Target: Milestone 2

Last updated: 2026-07-18

## Problem Statement

Milestone 1 and 1.1 protect PostgreSQL seat inventory from overlapping allocation and overselling, but a hot train run can still receive an uncontrolled burst of reservation attempts. Per-request rate limits alone do not provide a shared fair queue, global multi-replica admission bounds, single-use booking admission, or durable account-level hold quotas. A burst can therefore overload application and database resources or let customers hoard temporary holds even though the inventory transaction remains correct.

Milestone 2 must add a bounded Redis-backed waiting room and admission-control layer without weakening the existing PostgreSQL booking authority. Redis controls when a customer may attempt a hot-run reservation; it never owns seats, reservations, tickets, or durable booking idempotency.

The accurate project description is:

> A single-region railway ticketing backend with Redis-backed hot-train waiting-room admission and PostgreSQL-authoritative segment seat allocation.

Admission allows a booking attempt. Admission does not guarantee a seat.

## Solution

Add a durable operator-managed hot-train policy for each train run and seat class. For enabled policies, authenticated customers join a bounded Redis waiting room. An atomic Lua operation assigns a server-side monotonic sequence, deduplicates an active entry by user and policy, rejects changed requests, and enforces queue capacity.

A deterministic admission worker reads enabled PostgreSQL policies and invokes atomic Redis scripts that reclaim expired leases, enforce one global rate and inflight limit across worker replicas, admit the earliest eligible entries, and store only SHA-256 hashes of short-lived random tokens. The raw token is returned once to its owner.

Hot-run reservation creation validates and acquires the token atomically before entering a bounded per-instance execution slot. The existing PostgreSQL transaction remains responsible for durable idempotency, account quotas, segment allocation, reservation rows, seat rows, and outbox events. After commit, Redis token finalization is retryable; durable idempotency prevents duplicate reservations if Redis finalization fails.

Non-hot train runs preserve the Milestone 1 booking path and do not require admission. Redis failure for hot runs fails closed with bounded retry guidance, while PostgreSQL continues to prevent overselling in every path.

## Actors

- **Customer**: joins a hot-run waiting room, reads or cancels their own entry, receives one admission token, and attempts an idempotent reservation.
- **Admin**: may manage hot-train policy configuration under existing privileged authorization.
- **Railway operator**: enables, changes, disables, and observes hot-train policies and reconciliation results.
- **Admission worker**: performs bounded, globally coordinated admission for enabled policies.
- **API replica**: enforces ownership, admission, local backpressure, durable idempotency, quotas, and booking orchestration.
- **Hold-expiration worker**: continues to expire holds and thereby release durable active-hold quota.
- **Outbox worker**: publishes committed policy and reservation events at least once.
- **Operator/test harness**: runs read-only seat, quota, and admission-state reconciliation.

## Customer Journey

1. A customer searches existing PostgreSQL read paths and chooses a train run, ordered journey, seat class, and passenger count.
2. If no enabled policy applies, the existing reservation flow remains available without an admission token.
3. If an enabled policy applies, a direct reservation attempt returns `428 Precondition Required`.
4. The customer joins the waiting room. The API derives the user from JWT claims and resolves authoritative route stop indices before creating the Redis request fingerprint.
5. An identical active join returns the same entry and does not consume another queue position. A changed join under the same user and policy conflicts.
6. Queue capacity exhaustion returns `429 Too Many Requests` with bounded `Retry-After`.
7. The customer polls their own entry. Queue position is explicitly approximate.
8. The admission worker eventually admits the earliest eligible entry within the configured global rate and inflight limit.
9. The customer receives the raw admission token once and sends it in `X-Admission-Token` with the matching reservation and idempotency key.
10. A successful token acquisition permits one PostgreSQL booking attempt. It does not promise inventory.
11. On commit, the token becomes consumed and the same idempotency key replays the durable reservation.
12. If allocation conflicts, the attempt follows the documented one-attempt policy and the customer may need to rejoin.

## Operator Journey

1. An authorized admin or operator creates a bounded policy for one train run and seat class.
2. The system validates positive safe limits and commits a corresponding outbox event.
3. The operator observes queue, admission, token, quota, backpressure, Redis, PostgreSQL, and worker metrics with bounded labels.
4. The operator may update or disable a policy without modifying seat inventory.
5. The operator runs read-only reconciliation for duplicate entries, inflight/token drift, expired leases, token ownership mismatch, and durable quota state.
6. Redis continuity loss is handled operationally through persistence and recovery procedures; it is never reported as an inventory loss.

## User Stories

1. As a customer, I want to join a hot-run waiting room so that reservation bursts are served through a bounded shared control plane.
2. As a customer, I want my identical join retry to return the same active entry so that network retries do not create duplicate queue members.
3. As a customer, I want a changed active join to conflict so that one queue identity cannot silently change its booking intent.
4. As a customer, I want ordering to use a server-side monotonic sequence so that client clock manipulation cannot improve queue priority.
5. As a customer, I want queue position labeled approximate so that I am not promised an unstable exact rank.
6. As a customer, I want queue-full and overload responses to include bounded retry guidance.
7. As a customer, I want to read and cancel only my own queue entry.
8. As a customer, I want admission tokens to be short-lived and bound to my exact booking request.
9. As a customer, I want same-key retries after a successful booking to return the same reservation.
10. As a customer, I want stolen or mismatched admission tokens rejected before inventory mutation.
11. As a customer, I want Redis loss to leave PostgreSQL seat inventory correct even if queue continuity is lost.
12. As a customer, I want non-hot bookings to preserve the existing booking flow.
13. As a customer, I want admission to be described as permission to attempt booking rather than a seat guarantee.
14. As an operator, I want policies scoped by train run and seat class so that only selected demand is protected.
15. As an operator, I want positive bounded policy limits so that an unsafe configuration cannot create an unbounded queue or inflight population.
16. As an operator, I want policy changes committed with outbox events.
17. As an operator, I want admission rate and inflight limits shared across API and worker replicas.
18. As an operator, I want expired processing leases reclaimed so that a failed worker does not permanently consume capacity.
19. As an operator, I want hot-run Redis failure to fail closed so that a queue outage cannot flood PostgreSQL.
20. As an operator, I want per-instance reservation concurrency bounded so that API handlers never form a hidden in-memory queue.
21. As an operator, I want durable PostgreSQL quotas so that Redis counters cannot be bypassed to hoard holds.
22. As an operator, I want cancellation and expiration to release active-hold quota while confirmation behavior remains explicit.
23. As an operator, I want read-only reconciliation to detect admission and quota drift without automatic production repair.
24. As an operator, I want multi-replica tests to prove no double admission and no configured global-limit breach.
25. As a security reviewer, I want raw admission tokens, hashes, idempotency keys, payloads, passenger IDs, and identifiers absent from logs and metric labels.
26. As a security reviewer, I want at least 256 bits of secure token randomness and only SHA-256 hashes stored.
27. As a developer, I want Lua operations isolated behind small typed interfaces so that atomic behavior is testable with real Redis.
28. As a developer, I want deterministic clocks, barriers, and `RunOnce` seams so that concurrency tests avoid arbitrary sleeps.
29. As a reviewer, I want all Milestone 1 and 1.1 inventory, lifecycle, idempotency, outbox, and security tests to remain green.
30. As a reviewer, I want load scenarios and an honest benchmark report that never fabricate measurements or claim national scale.

## Queue Semantics and Fairness Model

- One policy queue exists per normalized train-run and seat-class key space.
- Fairness is FIFO by an atomic Redis sequence assigned at accepted join time.
- Client timestamps never affect ordering.
- Fairness is scoped to one policy in one Redis deployment; no global or cross-region fairness is claimed.
- An active user-to-entry mapping prevents more than one active entry per user and policy.
- Same user plus the same canonical request fingerprint returns the existing entry.
- Same user plus a different fingerprint returns conflict.
- Queue membership and metadata have bounded TTLs.
- Cancelled or expired entries are not eligible for admission.
- Queue position is an approximate observation, not an ordering authority or promise.

## Admission Semantics

- A disabled or absent policy preserves the existing non-hot booking flow.
- An enabled policy requires a matching admission token for reservation creation.
- Admission permits one matching booking attempt and does not reserve or guarantee inventory.
- Global rate and inflight limits are enforced atomically in Redis across worker replicas.
- Admission selects the earliest eligible queued entries in bounded batches.
- One entry cannot be admitted twice.
- Redis waiting-room state never modifies PostgreSQL seat masks.

## Admission Token Lifecycle

- Supported states are `issued`, `processing`, `consumed`, `expired`, and `cancelled`.
- Valid transitions are `issued -> processing -> consumed`, `issued -> expired`, `processing -> issued` after safe lease recovery, `processing -> expired`, and `issued|processing -> cancelled`.
- Tokens contain at least 256 bits of cryptographically secure randomness.
- Only the raw token is returned to the authenticated owner, once.
- The worker builds a self-authenticating raw token from a fresh 32-byte nonce and a process-owned HMAC key; Redis stores only the SHA-256 token hash, nonce, immutable claims, and bounded bindings, never the raw token or issuance MAC.
- The API reconstructs and hash-verifies the raw token during a read-only preflight before atomically claiming delivery, then verifies the claimed fields again; delivery is at-most-once, so a lost response requires bounded cancellation or expiry and rejoin.
- The admission fingerprint covers train run, ordered interval, seat class, and passenger count. It remains distinct from the durable booking fingerprint, which also binds passenger IDs.
- First acquisition atomically binds user, admission fingerprint, durable booking fingerprint, and durable idempotency-key hash; the binding cannot be replaced.
- Same-request processing retries may query completed durable idempotency, but cannot enter another PostgreSQL create while the lease is active; changed data conflicts.
- Same-request consumed retries are allowed only so PostgreSQL durable idempotency can return the original reservation.
- A post-commit Redis-finalization failure is recoverable without creating a duplicate reservation.
- Completed durable idempotency replay is checked before the current policy gate so a policy update cannot hide an already committed reservation.

## Queue Capacity and Backpressure

- `max_queue_size` is enforced atomically at join.
- `admission_rate_per_second` and `max_inflight_admissions` are global per policy.
- `reservation_max_inflight_per_instance` bounds each API replica locally.
- Full local capacity rejects immediately with `503 Service Unavailable` and bounded `Retry-After`.
- The API does not block indefinitely or enqueue work behind the semaphore.
- No hidden unbounded in-process queue is introduced.

## Redis Failure Policy

- Hot-run join, status, token acquisition, and admission require Redis and fail closed.
- Hot-run Redis failure returns a bounded `503` response with retry guidance and never bypasses admission.
- Non-hot booking preserves the existing documented path and PostgreSQL correctness.
- Redis restart or total data loss may lose queue entries and admission tokens; continuity is not guaranteed.
- A missing continuity sentinel for an already initialized policy fails closed and requires an operator-opened new generation; workers do not silently reset global limits.
- Redis AOF or equivalent managed persistence is required operationally.
- Redis loss cannot alter PostgreSQL inventory, reservations, tickets, or durable idempotency.

## PostgreSQL Failure Policy

- PostgreSQL readiness failure makes policy management, policy resolution, quota enforcement, and booking unavailable.
- No admission token is reported consumed unless a durable booking exists.
- A transaction either commits idempotency, quotas, allocation, reservation data, and outbox intent together or rolls them all back.
- Redis token state is released or recovered through a bounded lease when the database fails before commit.

## Durable Reservation Quotas

- Quotas are enforced inside the reservation-creation transaction.
- The design must serialize concurrent quota decisions for one user without changing the VARBIT seat-allocation algorithm.
- Required bounds cover active holds per user, active holds per user and train run, and active passengers per user.
- Quota rejection creates no reservation, seat mutation, completed idempotency result, or outbox event.
- Cancellation and expiration release active-hold quota because only current held reservations count.
- Confirmation removes a reservation from the active-hold quota while retaining authoritative seat occupancy.
- PostgreSQL is the quota authority; Redis counters may be observational only.

## Observability

- Metrics use bounded `operation`, `result`, `reason`, and `seat_class` labels only.
- Waiting-room metrics cover join, duplicate, queue-full, cancellation, expiry, admission attempts, issuance, failures, and wait duration.
- Token metrics cover acquire, consume, release, expiry, and conflict.
- Booking metrics cover quota rejection, local backpressure rejection, hot-run attempts, allocation conflict, and duration.
- No metric label contains a user, entry, token, token hash, train run, reservation, seat, idempotency key, passenger, Redis key, or arbitrary route input.
- Logs and safe errors omit raw tokens, hashes, keys, request payloads, passenger IDs, and dependency secrets.
- Backlog and queue depth are operational signals, not automatic readiness failures.

## Load-Test Requirements

- Provide separate k6 scenarios for join, status, admission, reservation, idempotency, quota, Redis outage, and multi-replica behavior.
- Measure join/status throughput, queue depth, issuance rate, inflight count, wait and reservation latency percentiles, conflicts, rejections, unexpected 5xx, PostgreSQL connections, Redis latency when available, reconciliation, outbox backlog, and dead-letter growth.
- Healthy steady state accepts zero unexpected 5xx.
- Tests must prove queue, rate, inflight, quota, ownership, token binding, and seat-allocation bounds.
- Results must distinguish functional smoke evidence from sustained capacity evidence.
- The benchmark report must not fabricate measurements or claim 12306-equivalent or national-scale capacity.

## Implementation Decisions

- Preserve the modular monolith and existing bounded contexts.
- Add an Admission bounded context with pure domain types, an application service, a Redis adapter, and an admission-worker process.
- Keep hot-train policy persistence and operator commands in the modular monolith, with explicit PostgreSQL transactions and outbox events.
- Encapsulate waiting-room join, admission issuance, token acquisition, token finalization, release, and lease recovery as deep Redis modules backed by bounded Lua scripts.
- Use Redis Cluster hash tags so every key touched by one script occupies one hash slot.
- Encode or validate key components; never place arbitrary request input directly into Redis keys.
- Keep PostgreSQL as the only seat-allocation and durable quota authority.
- Add quota enforcement to the existing booking transaction without redesigning core seat-inventory SQL.
- Preserve PostgreSQL durable booking idempotency as the recovery authority across Redis-finalization failure.
- Add a non-blocking per-instance execution gate around reservation work.
- Extend existing worker health, readiness, metrics, root-context, and shutdown patterns.
- Extend reconciliation with read-only admission-state and quota checks; do not auto-repair production.
- Keep station, search, and availability Redis caches deferred to Milestone 3.
- Keep microservice extraction, sharding, and multi-region active-active writes deferred.

## Testing Decisions

- Tests assert externally observable state transitions, HTTP contracts, Redis atomicity, PostgreSQL invariants, and reconciliation results rather than private implementation details.
- Pure domain tests cover policy bounds, key encoding, fingerprints, token state transitions, secure token shape, and bounded result categories.
- Redis integration tests execute the real Lua scripts and use deterministic setup, barriers, and bounded polling.
- PostgreSQL integration tests apply real migrations and exercise policy uniqueness, outbox atomicity, quotas, idempotency, allocation, rollback, cancellation, confirmation, and expiration.
- Multi-replica tests use multiple service/worker instances against one PostgreSQL and Redis pair.
- Required concurrency cases include identical and conflicting joins, capacity, three-worker admission, repeated token use, token theft, request mismatch, Redis outage, quota races, inventory shortfall, worker crash, post-commit finalize failure, and API termination.
- Critical concurrency tests run repeatedly and under the race detector without arbitrary sleeps.
- Every Milestone 1 and 1.1 regression test remains part of acceptance.
- Metrics tests inspect gathered families and reject secret or identifier cardinality.
- Load tests report only measurements actually observed in the current environment.

## Acceptance Criteria

- [ ] Work is isolated on `feat/milestone-2-hot-train-admission`.
- [ ] The PRD and ADRs 011 through 018 exist and agree with implementation.
- [ ] A durable, unique, bounded hot-train policy exists per train run and seat class.
- [ ] Authorized operator/admin APIs manage policies and write outbox events.
- [ ] One atomic join script provides stable deduplication, conflict detection, monotonic ordering, capacity enforcement, and bounded TTLs.
- [ ] Waiting-room ownership is enforced for join, status, cancellation, and token delivery.
- [ ] Multiple admission workers cannot double-admit or exceed the global rate or inflight limit.
- [ ] Raw admission tokens are returned once, never stored or logged, and have at least 256 bits of secure randomness.
- [ ] Token acquisition validates every user/request/idempotency binding and follows the documented lifecycle.
- [ ] DB-commit/Redis-finalize failure replays one durable reservation without duplication.
- [ ] Enabled hot-run reservations cannot bypass admission; non-hot bookings remain functional.
- [ ] Hot-run Redis failure fails closed and cannot flood PostgreSQL.
- [ ] Durable quotas cannot be exceeded under concurrency and reject without partial mutations.
- [ ] Per-instance reservation execution is bounded and rejects without queuing.
- [ ] PostgreSQL remains the authority for segment overlap, allocation, reservations, tickets, idempotency, quotas, and outbox events.
- [ ] Seat reconciliation remains green and new quota/admission reconciliation is read-only.
- [ ] Worker lifecycle, health, readiness, and disabled behavior follow existing bounded patterns.
- [ ] Multi-replica Compose and concurrency tests prove shared queue/token state and global limits.
- [ ] All Milestone 1 and 1.1 tests, race tests, migrations, CI, and security scans pass.
- [ ] Independent reviews report zero Critical and zero High findings.
- [ ] Documentation and load evidence state limitations honestly.
- [ ] A non-draft pull request is open against `main`, is not merged, and no tag is created.

## Out of Scope

- Milestone 3 station, search, and availability Redis read caches.
- Real payment, refunds, payment sagas, or ticket rescheduling.
- Real identity verification, email, SMS, CAPTCHA, or a full anti-bot platform.
- Frontend UI or mobile applications.
- Kafka, service mesh, Kubernetes Operators, or microservice extraction.
- PostgreSQL sharding, train-run shard migration, regional Redis replication, global consensus, or multi-region active-active writes.
- Redesigning PostgreSQL VARBIT seat inventory or moving authoritative allocation into Redis.
- Global fairness, guaranteed seat availability after admission, national-scale capacity, or 12306-equivalent throughput claims.

## Further Notes

- Redis is control-plane state for hot-run admission. PostgreSQL remains the business and inventory authority.
- Total Redis loss is a continuity incident, not an inventory-integrity incident.
- Admission and durable booking idempotency are separate layers: the former permits one attempt; the latter identifies the committed result.
- Future train-run shard ownership is documented only as an extraction boundary. This milestone remains a single-region modular monolith.
- A later Milestone 3 may add non-authoritative read caches and further measured scaling work, but it must preserve the authority hierarchy established here.
