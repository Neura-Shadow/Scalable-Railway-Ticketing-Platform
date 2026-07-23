# Milestone 3: Read Model and Availability Cache

Status: Implemented; final CI and independent review pending

Target: Milestone 3

Last updated: 2026-07-22

## Problem Statement

The current station, train-search, and availability endpoints read normalized
PostgreSQL source tables directly. Train search joins train runs, trains,
routes, route stops, stations, and fares, then performs an additional
authoritative availability query for every returned run. The path is correct,
but repeated browse traffic performs the same joins and segment-mask counts and
cannot scale reads independently from booking transactions.

Milestone 3 must introduce a disposable PostgreSQL journey projection and
bounded Redis caches without moving any booking decision out of PostgreSQL.
Projection and cache state are derived observations. They may lag, disappear,
or be rebuilt. Seat allocation, reservation lifecycle, durable idempotency,
hot-train admission, quotas, and reconciliation remain governed by the
existing authoritative transactions.

The accurate project description is:

> A single-region railway ticketing backend with event-maintained PostgreSQL
> query projections and bounded Redis station, search, and availability-hint
> caches, while PostgreSQL seat inventory remains authoritative.

The system does not promise zero projection lag, strongly consistent cached
availability, unlimited read scalability, globally consistent regional
caches, national-scale throughput, or cache-backed booking authority.

## Solution

Add `train_run_journey_read_model`, one denormalized row for each valid ordered
journey and active seat-class fare. A rebuild service derives the complete row
set from current PostgreSQL source tables and atomically replaces a train run's
projection. A read-model worker consumes the existing at-least-once event
transport, records durable event receipts in the same transaction as a
projection update, and always reloads current source state rather than applying
event payload fields as current truth.

Add collision-resistant version namespaces for station, normalized search,
and per-train-run availability caches. Old namespaces expire naturally, so
request paths never need `KEYS` or broad `SCAN`. Cache misses are bounded by
in-process singleflight, filled from PostgreSQL, and protected by TTL plus
jitter. Redis failures bypass the read cache. Projection failures fall back to
the existing normalized source query and schedule bounded repair. Availability
values are short-lived hints computed from authoritative segment masks; every
booking still executes the existing PostgreSQL overlap predicate.

Provide bounded rebuild, lag inspection, and detect-only reconciliation tools,
bounded-label metrics, worker health endpoints, multi-replica verification,
failure tests, and honest load-test artifacts.

## Actors and Journeys

- **Customer** browses stations, searches dated journeys, reads an availability
  hint, and attempts a reservation whose success is decided only by PostgreSQL.
- **API replica** normalizes requests, reads versioned caches, coalesces local
  identical misses, and follows documented PostgreSQL fallback paths.
- **Read-model worker** consumes events at least once, reloads authoritative
  state, updates projections and durable receipts atomically, and rotates
  appropriate cache generations.
- **Railway operator** runs bounded rebuild, reconciliation, and lag-inspection
  commands and observes metrics without exposing customer data or secrets.
- **Outbox worker** continues publishing minimized committed events through the
  existing transport; publication is not part of source transactions.
- **Booking and expiration workers** retain PostgreSQL authority and publish
  availability-affecting events without depending on cache invalidation.
- **Reviewer/test harness** proves projection determinism, fallback, cache
  coherence, concurrency bounds, and preservation of prior milestones.

### Customer journey

1. The customer lists active stations. A valid cache hit is returned; a miss or
   Redis failure loads PostgreSQL and may fill a new bounded entry.
2. The customer searches by normalized origin, destination, service date, seat
   class, page, limit, and allowlisted sort order.
3. A valid search-cache hit is returned. Otherwise, the API reads the journey
   projection; if that projection is unavailable or inconsistent, it runs the
   existing source query and records/schedules bounded repair.
4. Search results may include short-lived availability hints. A later booking
   attempt can still conflict because the response is a point-in-time view.
5. The reservation path resolves the journey and executes the authoritative
   PostgreSQL run-status, admission, quota, idempotency, and seat-mask checks.
   Cached availability is never consulted to accept or bypass those checks.

### Operator journey

1. The operator inspects projection lag, event backlog, cache behavior, and
   reconciliation metrics.
2. The operator can rebuild one train run or run a bounded, resumable backfill.
3. Reconciliation is detect-only by default and reports a bounded set of
   mismatches without mutating source tables.
4. An explicit rebuild command may repair disposable projection state. Cache
   versions may be rotated, but no production keyspace scan is required.
5. A Redis or worker incident is handled as degraded read performance and
   bounded staleness, never as authority loss or permission to bypass the
   Milestone 2 hot-run fail-closed policy.

## User Stories

1. As a customer, I want repeated station browsing served from a bounded cache
   so that identical metadata reads do not repeatedly query PostgreSQL.
2. As a customer, I want equivalent search parameters normalized identically
   so that harmless case or whitespace differences share one cache entry.
3. As a customer, I want unsafe sort input rejected before a key is created or
   SQL ordering is selected.
4. As a customer, I want a Redis outage to fall back to PostgreSQL for station,
   search, and availability reads.
5. As a customer, I want projection failure to use a correct normalized source
   query rather than return incomplete or known-inconsistent results.
6. As a customer, I want availability described as an observed hint rather
   than a guaranteed seat count.
7. As a customer, I want a stale positive hint to result at worst in a normal
   booking conflict, never overselling.
8. As a customer, I want overnight and non-zero first-stop schedules to use the
   same established journey-time calculation in every read path.
9. As an API operator, I want one replica's cache fill and version rotation to
   be visible to the other replicas without sticky sessions.
10. As an API operator, I want 100 identical local cold misses coalesced into a
    bounded source fill while unrelated keys continue independently.
11. As an API operator, I want failed fills to release waiters and permit later
    retries without leaked goroutines.
12. As a railway operator, I want the journey projection indexed for origin,
    destination, service date, seat class, status, and departure ordering.
13. As a railway operator, I want each train run rebuilt atomically so readers
    see either the old complete set or the new complete set.
14. As a railway operator, I want repeated rebuilds to produce identical rows
    except for documented observation timestamps.
15. As a railway operator, I want rebuild-all bounded, cancellable, resumable,
    and explicit about batch progress and failures.
16. As a railway operator, I want a deleted train run's derived rows removed
    without mutating railway source tables.
17. As a railway operator, I want a cancelled run represented consistently and
    excluded from customer search while remaining diagnosable.
18. As an event consumer, I want duplicate delivery recorded once and treated
    as an idempotent success.
19. As an event consumer, I want out-of-order events to rebuild from current
    source state instead of applying stale payload fields.
20. As an event consumer, I want receipts and projection changes committed in
    one transaction so neither can claim success without the other.
21. As an event consumer, I want bounded retry, pending-entry recovery, poison
    handling, and dead-letter behavior so one event cannot halt the stream.
22. As a railway operator, I want station and search invalidation implemented
    by collision-resistant version rotation rather than key deletion scans.
23. As a railway operator, I want loss of a version key to create a fresh
    namespace that cannot accidentally resurrect an old entry.
24. As a railway operator, I want reservation and inventory events to rotate
    only the affected train run's availability namespace.
25. As a railway operator, I want invalidation failures retried independently
    while short TTLs continue bounding staleness.
26. As a railway operator, I want projection lag, rebuilds, cache hits, misses,
    fills, invalidations, failures, and fallbacks visible through metrics.
27. As a security reviewer, I want metric labels limited to allowlisted cache,
    operation, result, reason, and event-type values.
28. As a security reviewer, I want cache keys, query hashes, version tokens,
    arbitrary stations, events, reservations, users, and passengers absent from
    metric labels and logs.
29. As a security reviewer, I want cache payloads limited to public read fields
    and want full event/cache payload logging prohibited.
30. As an operator, I want worker liveness independent from dependencies and
    readiness based only on its PostgreSQL, Redis-stream, migration, and owned
    configuration requirements.
31. As an operator, I want a disabled worker to perform no initial pass and
    shut down cleanly.
32. As an operator, I want reconciliation to detect missing, extra, duplicate,
    stale, mismatched, or invalid projection/cache-version state without
    automatically repairing production.
33. As a developer, I want explicit reversible migration 7 coverage for fresh,
    repeat, populated-v6 upgrade, constraints, indexes, and safe down/up.
34. As a developer, I want deterministic `RunOnce` seams and real PostgreSQL
    and Redis tests without arbitrary sleeps.
35. As a reviewer, I want all Milestone 1, 1.1, and 2 correctness, security,
    race, reconciliation, and deployment gates to remain green.
36. As a reviewer, I want load artifacts and benchmark reports to distinguish
    functional evidence from sustained capacity and never fabricate values.

## Projection Consistency Model

- PostgreSQL source tables remain authoritative. The journey projection is a
  disposable, eventually consistent query model.
- `train_run_journey_read_model` contains schedule, route, station, train,
  journey, seat-class, and fare observations; it never contains authoritative
  seat masks or an authoritative availability count.
- Its unique identity is train run, origin stop index, destination stop index,
  and seat class. Only ordered origin/destination pairs with an active fare are
  materialized.
- `RebuildTrainRun` begins one transaction, locks or consistently observes the
  relevant source state, generates deterministic rows using the established
  `ResolveJourney` time anchoring, replaces the complete train-run set in a
  batch, records timestamps, and commits atomically.
- Event consumers insert `(consumer_name, event_id)` into
  `read_model_event_receipts` in the same transaction as the resulting
  projection change. Duplicate receipts do no further work.
- Event payloads select affected aggregates but do not supply complete current
  state. Every handler reloads authoritative tables, making stale event order
  safe for the final projection.
- Cancelled rows retain their status for diagnosis/reconciliation and are
  excluded by the public search predicate. Permanently missing runs have their
  projection removed.
- Projection lag is measured as a bounded observation derived from source and
  projected timestamps or event backlog. Lag alone does not fail API readiness.

## Cache Consistency Model

- Redis is an optional read accelerator. A cache hit is accepted only from the
  currently resolved version namespace and before its bounded TTL expires.
- Station namespace: `cache:stations:version` and
  `cache:stations:{versionToken}`.
- Search namespace: `cache:train-search:version` and
  `cache:train-search:{versionToken}:{queryHash}`.
- Availability namespace: `cache:availability:version:{trainRunID}` and
  `cache:availability:{versionToken}:{trainRunID}:{from}:{to}:{class}` with
  encoded bounded components.
- Version tokens use a cryptographically secure, collision-resistant bounded
  encoding. `GetOrCreate` is atomic; `Rotate` always writes a fresh token.
  Losing only a version key creates a new namespace and cannot reuse stale data.
- Every data key has a bounded TTL plus bounded jitter. Old namespaces expire
  naturally. Production request paths use exact keys only and never `KEYS` or
  broad `SCAN`.
- Search keys use SHA-256 over stable canonical serialization of normalized
  origin, destination, date, seat class, page, limit, sort field, sort
  direction, and an explicit schema version. Raw query strings never appear in
  keys.
- Availability values contain a non-negative count, observation time, and
  source marker. They are short-lived and hint-only; stale-if-error is disabled
  by default.
- In-process singleflight is keyed by the exact logical cache entry. It bounds
  identical local fills, does not serialize unrelated keys, propagates failure,
  permits retry, and creates no unbounded goroutine population. A distributed
  lease is optional only if later measurements justify it.

## Cache Behavior and Invalidation

### Station cache

- Return a stable, deterministic serialization of public active-station fields.
- On miss, query PostgreSQL and fill the current namespace when Redis is
  available. Redis errors are metrics-bearing PostgreSQL fallbacks.
- Rotate the station version on station create, update, or disable events.

### Train-search cache

- Read the journey projection first and use the normalized source join when the
  projection is missing, unavailable, or known inconsistent.
- Fill only complete successful results. Cache no raw database errors or
  partial pages.
- Rotate the global search version on station, route, train, fare, train-run,
  and cancellation changes. A mutation that affects many runs is rebuilt in
  bounded batches before or alongside safe generation rotation as documented.

### Availability hint cache

- Compute the count from authoritative `seat_inventory.occupied_segments` and
  the requested `[from,to)` segment mask.
- Rotate only the affected train-run generation for reservation held,
  confirmed, cancelled, expired, seat, coach, inventory-affecting train-run,
  or cancellation events.
- Confirmation may preserve occupancy, but duplicate rotation is harmless and
  favors a simple conservative invalidation map.
- Redis failure reads PostgreSQL. PostgreSQL failure returns a service error;
  no stale availability value is served by default.

Invalidation is asynchronous and is never part of the booking transaction.
Failed invalidation is retried with bounded attempts/dead-letter visibility;
TTL remains the final staleness bound.

## Failure and Degradation Policy

| Failure | Read behavior | Correctness behavior |
|---|---|---|
| Redis unavailable | Station and availability read source PostgreSQL; search reads projection then source fallback | Booking remains PostgreSQL-authoritative; enabled Milestone 2 waiting rooms still fail closed |
| Read-model worker stopped | Existing complete projection remains readable; lag and backlog increase; source fallback remains available | Source data is unchanged; no readiness failure solely for lag |
| Projection missing/inconsistent | Use normalized source query, count fallback, and schedule bounded rebuild | Never return a known partial projection |
| Projection table unavailable/locked | Use normalized source query when safe and expose fallback/failure metrics | No source-table mutation or booking bypass |
| Cache fill/invalidation failure | Return the successful source result where available; release singleflight and retry later | TTL bounds stale state; no distributed transaction |
| Redis flush/version loss | Atomically create new random namespaces and refill on demand | Old cache data cannot become current authority |
| Duplicate/out-of-order event | Durable duplicate success or current-state rebuild | No stale payload overwrite |
| Poison event | Bounded retry then dead letter with safe metadata | Other events continue; no full payload logging |
| PostgreSQL source failure | Return a safe service error when no complete projection policy applies; availability does not serve stale by default | Booking and authoritative commands fail normally |

## Rebuild, Reconciliation, and Operations

- `cmd/read-model-admin` supports `rebuild-train-run`, `rebuild-all`,
  `reconcile`, and `inspect-lag` with context cancellation, bounded output,
  non-zero failure exits, and no DSN, Redis secret, PII, or payload output.
- Rebuild-all uses stable ordering, bounded batch sizes, and a resumable cursor.
  Dry-run is supported where it can report intended work without mutation.
- `cmd/reconcile read-model` detects missing/extra rows, journey-pair, fare,
  station, schedule, cancelled-status, duplicate, receipt/projection, and
  source-timestamp mismatches.
- `cmd/reconcile cache-versions` detects missing search/availability version
  keys and invalid version-token formats without reading arbitrary data keys.
- Production reconciliation is read-only by default. Only an explicit admin
  rebuild repairs the disposable model; no command repairs authoritative
  railway or booking data implicitly.

## Observability Requirements

Read-model metrics are `read_model_event_total`,
`read_model_duplicate_event_total`, `read_model_rebuild_total`,
`read_model_rebuild_failure_total`, `read_model_rebuild_duration_seconds`,
`read_model_rows_written_total`, `read_model_fallback_total`,
`read_model_reconciliation_mismatch_total`, and
`read_model_projection_lag_seconds`.

Cache metrics are `cache_request_total`, `cache_hit_total`,
`cache_miss_total`, `cache_failure_total`, `cache_invalidation_total`,
`cache_invalidation_failure_total`, `cache_fill_total`,
`cache_fill_failure_total`, `cache_singleflight_shared_total`, and
`cache_fill_duration_seconds`. `cache_source_query_total` is the bounded
load-evidence counter for authoritative station/search/availability queries.

Only bounded allowlisted `cache_type`, `operation`, `result`, `reason`, and
`event_type` labels are permitted. IDs, station or route input, reservation,
user, passenger, event ID, cache key, token, query hash, origin, destination,
and raw query data are prohibited labels.

The read-model worker exposes `/livez`, `/readyz`, and `/metrics` through the
existing private worker server. Readiness checks PostgreSQL, Redis when the
stream consumer requires it, migration version, and process-owned config. It
does not require JWT or admission-token secrets. A disabled worker executes no
initial pass and remains process-healthy until clean shutdown.

## Load-Test Requirements

Provide k6 scenarios for station cache, cold/warm search cache, availability
cache, mixed search and booking, invalidation storm, Redis cache outage,
read-model worker pause, and multi-replica shared search cache. Measure actual
station/search/availability throughput and latency percentiles, hit/miss/fill/
fallback/source-query/singleflight observations, rebuild duration and lag,
invalidation rate, dependency usage, booking conflicts, unexpected 5xx, and
seat/read-model reconciliation.

Healthy steady state permits no unexpected 5xx. Tests must demonstrate cache
behavior and safe fallback without presenting functional smoke runs as
sustained capacity. The benchmark report contains only measurements captured
from the current environment and makes no national-scale claim.

## Implementation Decisions

- Preserve the modular monolith and add a Query/Read Model module plus separate
  worker/admin executables that reuse internal packages.
- Use PostgreSQL rather than a search engine because current query semantics
  are relational, source state already resides in PostgreSQL, and this
  milestone needs rebuildable reads rather than a new distributed system.
- Add explicit migration 7 for the projection, receipts, constraints, indexes,
  and readiness version; never use AutoMigrate.
- Reuse `ResolveJourney` anchoring or a shared extracted deep module so time
  calculations cannot diverge across source and projected reads.
- Keep projection replacement and receipt insertion inside explicit short
  PostgreSQL transactions; use batch inserts and bounded fan-out.
- Extend the existing Redis Streams/outbox transport contract. Do not add
  Kafka, synchronous publication, or a distributed PostgreSQL/Redis transaction.
- Use exact version keys, secure tokens, stable cache serialization, bounded
  TTL/jitter, and in-process singleflight.
- Keep the existing source query as the correctness fallback and preserve the
  Milestone 2 hot-run Redis failure policy unchanged.
- Add configuration only for process-owned worker and cache parameters with
  secure production validation and least-privilege secret mounting.

## Testing Decisions

- Pure tests cover projection generation, all ordered journey/class rows,
  reverse rejection, overnight/non-zero offset anchoring, deterministic repeat,
  canonical keys, SHA-256 query identity, sort rejection, secure version-token
  recovery/rotation, availability masks, hint semantics, and singleflight.
- Real PostgreSQL tests cover migration 7, atomic complete replacement,
  rollback, durable duplicate receipts, current-state handling of out-of-order
  events, live reads during rebuild, source fallback, and reconciliation.
- Real Redis tests cover atomic namespace creation/rotation, expiry,
  invalidation, flush recovery, failure/retry, pending-event recovery, and
  shared multi-replica visibility.
- Concurrency tests use barriers and bounded polling, not arbitrary sleeps. They
  cover identical cold misses, unrelated keys, multiple API replicas, worker
  duplicates/restarts, booking with a stale positive hint, and goroutine safety.
- Critical cases run repeatedly and under the race detector. All prior
  milestone unit, integration, reconciliation, security, deployment, and CI
  gates remain acceptance requirements.

## Acceptance Criteria

- [ ] Work is isolated on `feat/milestone-3-read-model-cache`.
- [ ] This PRD and ADRs 019 through 026 agree with the implementation.
- [ ] Migration 7 creates the projection and durable receipt schema with all
      required constraints and search indexes and is verified fresh/repeated/
      populated-v6/down-up where safe.
- [ ] One train-run rebuild deterministically creates every valid ordered
      journey and active fare class and replaces the old set atomically.
- [ ] Repeated rebuild is idempotent; partial failure exposes no partial set.
- [ ] Duplicate receipts prevent duplicate effective work, and out-of-order
      events rebuild latest authoritative state.
- [ ] Worker processing is bounded, recoverable, disabled by default,
      lifecycle-safe, metrics-safe, and poison-event tolerant.
- [ ] Station and search caches use versioned random namespaces, bounded TTL and
      jitter, stable serialization, and no request-path keyspace scan.
- [ ] Search canonicalization includes every result-shaping input and rejects
      unsafe sort before hashing or SQL selection.
- [ ] Availability cache is per train run, short-lived, computed from current
      authoritative masks, and documented and returned only as a hint.
- [ ] Booking never accepts cached availability as proof and still safely
      conflicts when the hint is stale and PostgreSQL is full.
- [ ] Missing version keys cannot resurrect stale namespaces.
- [ ] Identical local misses are coalesced while unrelated keys remain
      independent and failed fills remain retryable without leaks.
- [ ] Redis failure, projection failure, worker pause/restart, cache loss, and
      rebuild interruption follow the documented safe degradation policy.
- [ ] Multi-replica APIs share namespace state and need no sticky sessions.
- [ ] Read-model and cache-version reconciliation are detect-only by default
      and pass on the accepted fixture; seat/quota/admission checks still pass.
- [ ] Required bounded metrics, worker health, configuration, Compose,
      Kubernetes, Docker, load tests, and operational documents exist.
- [ ] All Milestone 1, 1.1, and 2 regressions, focused integration tests,
      repeated critical tests, race detector, static/security scans, image
      gates, and required GitHub Actions pass.
- [ ] Independent review reports zero Critical and zero High findings.
- [ ] A non-draft pull request into `main` is open and mergeable with green CI;
      it remains unmerged and no tag is created.

## Out of Scope

- Milestone 4 and any payment, refund, or rescheduling workflow.
- Train-run database sharding or shard routing implementation.
- Regional cache replication or globally consistent cache state.
- Multi-region active-active writes or cross-region transaction coordination.
- Kafka, Service Mesh, Kubernetes Operators, Elasticsearch, or OpenSearch.
- A search/cache microservice or any modular-monolith split.
- Redis reservation authority, cache-backed booking acceptance, synchronous
  invalidation as a booking requirement, or PostgreSQL/Redis distributed
  transactions.
- Frontend UI, real identity verification, or an anti-bot platform.
- National-scale, unlimited-scaling, zero-lag, or exact-real-time claims.

## Further Notes

- A future regional design may use independent regional read projections and
  caches fed from an authoritative event stream, but it remains a design-only
  topic until ownership, recovery, and consistency evidence exists.
- Initial backfill must document batch sizing, locks, disk growth, rollback,
  and observed operational impact. Zero downtime is not claimed without
  evidence.
- Dedicated search infrastructure is justified only by measured PostgreSQL
  projection limits and a proven independent operational boundary; it is not a
  Milestone 3 prerequisite.
