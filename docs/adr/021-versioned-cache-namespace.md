# ADR 021: Collision-Resistant Versioned Cache Namespaces

- Status: Accepted
- Date: 2026-07-22

## Context

Station and normalized search results need broad invalidation when source
metadata changes. Availability hints need per-train-run invalidation. Deleting
all matching data keys requires keyspace enumeration and can block or overload
Redis. A small numeric version can accidentally reuse an old namespace when
only the version key is evicted or lost.

## Decision

Use exact version keys and collision-resistant version tokens:

```text
cache:stations:version
cache:stations:{versionToken}

cache:train-search:version
cache:train-search:{versionToken}:{queryHash}

cache:availability:version:{trainRunID}
cache:availability:{versionToken}:{trainRunID}:{from}:{to}:{class}
```

Tokens are generated from a cryptographically secure random source, encoded as
a fixed 24-character unpadded Base64URL value, and validated before use. `GetOrCreate`
atomically keeps an existing valid token or installs one fresh candidate.
`Rotate` atomically replaces the version with a newly generated token. `Build`
accepts only validated, server-normalized components and creates an exact
cluster-compatible key.

If a version key is missing, `GetOrCreate` installs a fresh random token. It
never guesses, increments, or restores a prior token, so surviving old data
cannot become current after partial cache loss. Candidate collisions are
rejected/retried in deterministic tests and are practically prevented by the
token entropy.

Every data key has a bounded TTL plus bounded jitter. Old namespaces expire
naturally. Request paths use exact version/data keys and never Redis `KEYS` or
broad `SCAN`. Maintenance may use cursor-based `SCAN` outside hot paths.

Search `queryHash` is SHA-256 of an explicitly versioned stable serialization
containing normalized origin, destination, service date, seat class, page,
limit, safe sort field, and safe sort direction. Raw query strings never enter
the key. Redis Cluster operations touch exact keys; no multi-key cross-slot
transaction is required.

## Consequences

- Broad logical invalidation is constant bounded work.
- Old entries occupy memory until their TTL expires, so TTL and cache sizing
  are operational limits.
- Losing all Redis data causes cache refill, not authority loss.
- Multiple API replicas observe the same current namespace without sticky
  sessions.

## Rejected alternatives

- Incrementing integers: rejected because losing only the version key can
  recreate a previously used number and resurrect stale entries.
- Delete by `KEYS`: rejected because it blocks Redis and scales with key count.
- Broad request-path `SCAN`: rejected because cursor work is still unbounded
  relative to one request.
- Raw canonical query text in keys: rejected because it leaks input and creates
  long or unsafe key material.
