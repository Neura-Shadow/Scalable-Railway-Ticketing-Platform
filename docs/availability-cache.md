# Availability Hint Cache

Runtime controls are `AVAILABILITY_CACHE_ENABLED`,
`AVAILABILITY_CACHE_TTL_SECONDS`, `AVAILABILITY_CACHE_JITTER_SECONDS`, and
`AVAILABILITY_CACHE_MAX_STALE_SECONDS`. The observed-at age is checked even
while the Redis key still exists; a value beyond max-stale is refreshed from
PostgreSQL and is never served as fresh.

Availability is a short-lived observation of PostgreSQL seat inventory. It is
not an inventory ledger, lock, hold, allocation decision, or proof that a seat
can be sold.

Exact keys:

```text
cache:availability:version:{trainRunID}
cache:availability:{versionToken}:{trainRunID}:{from}:{to}:{class}
```

Station codes and seat class are normalized and validated before key creation.
The default TTL is ten seconds plus up to two seconds of jitter. Values contain
the public availability response, UTC observation time, and `postgres` source
marker. Negative counts, malformed values, wrong train-run identity, or absent
observation metadata are rejected as misses.

Reservation, ticket, coach, seat, and train-run state events rotate only the
affected run's generation. A confirmation can rotate even when occupancy is
unchanged; this conservative duplicate work is bounded and harmless.

When Redis is unavailable, the API queries PostgreSQL. When PostgreSQL is
unavailable, the API returns a service error; stale-if-error is not enabled.
Batch availability uses one bounded PostgreSQL query for misses rather than a
per-search-result N+1 loop.

A stale positive value can lead only to a normal booking conflict. The booking
transaction never accepts this cached value and always rechecks the
authoritative `occupied_segments` overlap predicate. Redis failure therefore
never justifies bypassing status, admission, quota, or seat checks.
