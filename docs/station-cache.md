# Station Cache

Runtime controls are `STATION_CACHE_ENABLED`,
`STATION_CACHE_TTL_SECONDS`, and `STATION_CACHE_JITTER_SECONDS`. Disabling the
cache sends station browsing directly to PostgreSQL and does not change booking
behavior.

Serialized cache values are capped at 1 MiB. An oversized station catalog is
still returned from PostgreSQL, but that response is not written to Redis.

The station cache stores only public active-station fields in deterministic
code order. PostgreSQL stations remain authoritative.

Exact keys:

```text
cache:stations:version
cache:stations:{versionToken}
```

The current implementation uses a five-minute default TTL with up to thirty
seconds of bounded jitter. A station create, update, or disable event rotates
the version token. Old data expires naturally; request paths do not enumerate
or delete old namespaces.

On a valid hit, cached JSON is decoded and domain-validated before use. A miss,
decode error, absent/malformed version, or Redis error loads PostgreSQL. A
successful source result is returned even if the best-effort cache fill fails.
Identical cold fills on one API process use exact-key singleflight.

Cached payloads contain no user, reservation, passenger, credential, or
availability data. Cache keys/tokens and full values are excluded from logs and
metrics.
