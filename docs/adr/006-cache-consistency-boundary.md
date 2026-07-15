# ADR 006: Redis Is a Bounded, Non-Authoritative Accelerator

- Status: Accepted
- Date: 2026-07-15

## Context

Station lists, train searches, and availability are read-heavy and can benefit from caching. Booking correctness cannot depend on cache freshness or Redis availability.

## Decision

Redis may store:

- versioned station metadata;
- normalized train-search results;
- short-lived availability hints;
- login, registration, passenger-create, and reservation-create rate counters;
- optional completed-idempotency lookup hints keyed only by hashes;
- optional Redis Stream events, processed-event IDs, and bounded consumer failure counts.

Redis never owns occupied segments, reservation state, confirmed tickets, ticket-order status, train-run bookability, or durable idempotency.

Cache keys use versioned namespaces and stable, server-generated hashes:

```text
cache:stations:v{version}
cache:train-search:v{version}:{normalized_hash}
cache:availability:v1:{train_run_id}:{from_index}:{to_index}:{seat_class}
```

Values always have bounded TTLs with jitter. Availability TTL is deliberately short. Write paths prefer exact-key invalidation; broad maintenance uses cursor-based `SCAN`, never `KEYS`. Arbitrary station codes or payload values do not become metric labels.

Availability endpoints may return cached counts, but the response contract identifies them as point-in-time hints. Create-hold always runs the PostgreSQL overlap and train-run-status predicates.

Atomic Lua scripts implement rate-limit increments and expiry. Each route has an explicit Redis failure policy:

- authentication and passenger-profile creation limits fail closed in production after a small bounded backend-error response;
- reservation-create admission degrades open on a limiter error because PostgreSQL still enforces every Milestone 1 booking invariant; durable account/train-run quotas and waiting-room admission remain Milestone 2 controls;
- read-only public browsing may fail open with metrics when configured, because it does not mutate authoritative state;
- test/local profiles may use a documented in-memory fallback that is never enabled as a production authority.

Redis connection errors are bounded by short timeouts and surfaced in readiness. Redis unavailability does not alter committed PostgreSQL state.

## Consequences

- Cache staleness can cause a hold conflict but never overselling.
- Authentication and passenger-profile creation can become temporarily unavailable when their anti-abuse dependency is unavailable; reservation creation remains available but continues through the authoritative PostgreSQL transaction.
- Cache invalidation is observable but does not participate in database transactions.
- Future regional caches can reuse the same hint-only contract.

## Rejected alternatives

- Cache-aside seat ownership: rejected because eviction/failover is not a booking transaction.
- Unbounded or no-TTL keys: rejected because stale results would accumulate indefinitely.
- Production in-memory rate limits per replica: rejected because replicas would not share limits.
