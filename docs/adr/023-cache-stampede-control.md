# ADR 023: Bound Cache Stampedes with In-Process Singleflight

- Status: Accepted
- Date: 2026-07-22

## Context

A missing or rotated station/search/availability entry can cause many
simultaneous API requests to execute the same PostgreSQL fill. The system runs
multiple API replicas, but no measurement currently proves that a distributed
lease is required. A global lock would serialize unrelated reads, and spawning
one goroutine per waiter would make overload unbounded.

## Decision

Each API process coalesces concurrent identical cache misses by the exact
logical data key. One caller executes the bounded source fill; local waiters
share its result or error through a context-aware in-process singleflight
mechanism. Unrelated keys use independent flights.

The fill runs in the initiating request context with existing database/Redis
timeouts. It does not spawn unbounded background work. Cancelled waiters may
leave without cancelling a fill still needed by other callers. A failed fill
releases the flight so a later request can retry. Only complete successful
values are cached.

Metrics count shared fills through bounded cache-type/result labels, never the
key or query hash. Tests use barriers and bounded polling to prove one local
fill, unrelated-key independence, failure/retry, and absence of goroutine
leaks.

A distributed Redis lease is not enabled by default. It may be introduced only
after multi-replica measurements demonstrate unacceptable cross-replica source
amplification, and it must retain bounded wait, lease expiry, owner-safe
release, and PostgreSQL fallback.

## Consequences

- One process bounds identical cold-miss source work.
- Several replicas may still fill the same new key concurrently; this is a
  known bounded multiplication by replica count.
- No new Redis dependency is added to the fallback path beyond the cache itself.
- Failure remains retryable and does not poison future fills.

## Rejected alternatives

- One global mutex: rejected because unrelated cache keys would block.
- Unbounded goroutines or channels: rejected because waiters would become a
  hidden queue.
- Mandatory distributed lock: rejected because current evidence does not
  justify its failure modes and Redis outage must still fall back safely.
