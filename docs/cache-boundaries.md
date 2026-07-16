# Redis and Cache Boundaries

## Current implementation status

Milestone 1.1 reads station metadata, train search results, and availability directly from PostgreSQL. Their Redis cache keys and consistency rules are design boundaries only; implementing those caches is deferred to a later read-model/cache milestone. Availability remains a point-in-time hint even when read directly, and reservation correctness never depends on a cached availability value.

## Allowed uses

| Use | Authority | TTL/failure behavior |
|---|---|---|
| Station metadata (deferred cache) | PostgreSQL | future versioned bounded TTL; database fallback |
| Train search (deferred cache) | PostgreSQL | future normalized hash and bounded TTL; database fallback |
| Availability count (deferred cache) | PostgreSQL seat inventory | future very short hint TTL; booking always rechecks |
| Completed idempotency lookup (deferred cache) | PostgreSQL record | future optional hashed-key hint; database fallback |
| Registration/login/passenger-create rate limit | Redis atomic Lua | production writes fail closed when limit state is unavailable |
| Create-hold admission rate limit | Redis atomic Lua | degrades open on limiter errors; PostgreSQL invariants remain authoritative |
| Redis Streams | PostgreSQL outbox | optional disabled publisher; at-least-once only |
| Processed event IDs/failure counters | Consumer contract | bounded TTL/retention; never booking authority |

## Key rules

- Version namespaces and canonical server-side hashing.
- Exact-key invalidation where possible.
- TTL on every cache/ephemeral key, with bounded jitter.
- `SCAN` for maintenance; no production `KEYS`.
- No raw idempotency key, JWT, passenger identifier, password, DSN, or arbitrary payload in a key.
- No arbitrary key or station code in metric labels.

## Stale data

A search or availability response is a point-in-time observation. Staleness may produce a later `seat_unavailable` conflict. It must never be described as a guarantee or used to skip the PostgreSQL overlap predicate.

## Redis outage

Current reads continue against PostgreSQL because the read caches are not implemented. Future read caches must be bypassable during Redis failure. Read-only public browsing can use a documented fail-open policy for its rate counter. Authentication and passenger-profile creation fail closed in production. Reservation-create admission degrades open because it is only an availability hint; the PostgreSQL transaction still enforces ownership, one-active-reservation-per-passenger/run, allocation, lifecycle, and idempotency invariants. Durable account/train-run reservation quotas remain a Milestone 2 control. Already committed reservations and worker database transactions remain correct.

Readiness reports Redis failure without secrets. Liveness remains process-only.
