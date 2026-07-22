# Train-search Cache

Runtime controls are `TRAIN_SEARCH_CACHE_ENABLED`,
`TRAIN_SEARCH_CACHE_TTL_SECONDS`, `TRAIN_SEARCH_CACHE_JITTER_SECONDS`, and
`TRAIN_SEARCH_FALLBACK_ENABLED`. The safe default keeps fallback enabled so a
projection error or incomplete result uses the normalized authoritative source
query rather than fabricating a response.

Train-search caching sits in front of the disposable journey projection and
the normalized source fallback. It does not alter booking or availability
authority.

Exact keys:

```text
cache:train-search:version
cache:train-search:{versionToken}:{sha256QueryHash}
```

The hash input is a schema-versioned canonical structure containing normalized
origin/destination codes, service date, seat class, page, bounded page size,
sort field, and sort direction. Case/whitespace equivalents converge. Unsafe
sort values are rejected before SQL or key construction. Raw query strings and
station values never appear in Redis keys or metric labels.

The default TTL is sixty seconds plus up to ten seconds of bounded jitter. A
station, route, train, fare, train-run, or cancellation change rotates the
global generation. Rotation is O(1); old pages expire without `KEYS` or a
keyspace scan.

On miss, one local singleflight leader reads the complete projection. If the
projection query fails or yields no usable row set, it executes the existing
parameterized source query. Redis/version/fill failures are observable but do
not suppress a successful PostgreSQL result.

Search results may include observed availability, but they are not a promise
that a later reservation will succeed. Reservation creation independently
resolves the journey and executes authoritative PostgreSQL segment-overlap,
status, admission, quota, and idempotency checks.
