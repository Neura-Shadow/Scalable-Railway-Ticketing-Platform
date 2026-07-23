# Redis and Cache Boundaries

## Current implementation status

Milestone 3 decorates PostgreSQL station, projected train-search, and
authoritative availability reads with optional bounded Redis caches.
Availability remains a point-in-time hint, and reservation correctness never
depends on a cached availability value.

## Allowed uses

| Use | Authority | TTL/failure behavior |
|---|---|---|
| Station metadata cache | PostgreSQL | versioned bounded TTL and payload; database fallback |
| Train search cache | PostgreSQL source/projection | normalized hash, versioned bounded TTL; source fallback |
| Availability hint cache | PostgreSQL seat inventory | very short max-stale/TTL; booking always rechecks |
| Completed idempotency lookup (deferred cache) | PostgreSQL record | future optional hashed-key hint; database fallback |
| Registration/login/passenger-create rate limit | Redis atomic Lua | production writes fail closed when limit state is unavailable |
| Non-hot create-hold rate limit | Redis atomic Lua | limiter errors degrade open; PostgreSQL quotas, idempotency, and inventory remain authoritative |
| Enabled hot-train waiting room and admission | PostgreSQL policy plus Redis atomic Lua | Redis/state failure fails closed with bounded retry guidance; admission is never bypassed |
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

Read caches are bypassed during Redis failure and reads continue against
PostgreSQL.
Read-only public browsing can use a documented fail-open policy for its rate
counter. Authentication and passenger-profile creation fail closed in
production.

For a train run and seat class with no enabled hot policy, only the existing
reservation-create rate limiter degrades open on a Redis error. The request
still executes the PostgreSQL-authoritative quota, ownership, idempotency,
inventory, lifecycle, and outbox transaction. For an enabled hot policy,
waiting-room join/status, admission-token acquisition, and hot reservation
admission all require valid Redis control state and fail closed with bounded
retry guidance. A Redis incident must never downgrade an enabled PostgreSQL
policy into the non-hot path. Already committed reservations and worker
database transactions remain correct.

Readiness reports Redis failure without secrets. Liveness remains process-only.
