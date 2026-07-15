# Redis and Cache Boundaries

## Allowed uses

| Use | Authority | TTL/failure behavior |
|---|---|---|
| Station metadata | PostgreSQL | versioned bounded TTL; database fallback |
| Train search | PostgreSQL | normalized hash and bounded TTL; database fallback |
| Availability count | PostgreSQL seat inventory | very short hint TTL; booking always rechecks |
| Completed idempotency lookup | PostgreSQL record | optional hashed-key hint; database fallback |
| Registration/login/create-hold rate limit | Redis atomic Lua | production writes fail closed when limit state is unavailable |
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

Read caches are bypassed. Read-only public browsing can use a documented fail-open policy for its rate counter. Authentication and reservation-create rate limiting fail closed in production because an outage must not remove anti-hoarding/credential-stuffing controls. Already committed reservations and worker database transactions remain correct.

Readiness reports Redis failure without secrets. Liveness remains process-only.
