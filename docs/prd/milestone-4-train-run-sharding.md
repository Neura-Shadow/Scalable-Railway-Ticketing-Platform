# Milestone 4: Train-Run Sharding and Single-Writer Ownership

Status: Implementation present in this work-in-progress branch; final controlled runtime, CI, independent review, and release acceptance remain pending.

Target: Milestone 4

Last updated: 2026-07-23

## Problem Statement

The booking write path currently assumes one public PostgreSQL schema. That
layout preserves atomic seat allocation, reservation lifecycle, idempotency,
quota enforcement, ticket creation, and outbox intent, but it does not prove
that a train run can be routed to an explicit ownership boundary or moved
without allowing two writers. Process-local routing, Redis hints, or a cached
mapping cannot safely establish ownership because stale API replicas and
workers can continue using old state.

Milestone 4 must establish a reversible sharding-readiness boundary around
`train_run_id`. Selected train runs can remain in legacy storage, move into one
of two schema-isolated logical booking shards, or move between those shards.
Every mutation must lock and validate both the durable assignment generation
and the selected shard's local fence in the same PostgreSQL transaction as the
booking change. A stale route must therefore fail before it can allocate a
seat, complete idempotency, consume quota, write a locator, or append outbox
intent.

Migration must preserve the established segment-mask correctness model and all
durable booking state without dual writing. It uses a bounded quiesced copy,
explicit validation, atomic cutover, a retained read-only source, and rollback
rules that distinguish whether the target has accepted any mutation. Customer
lookups must not scan every shard, and operational fanout must remain bounded.

The accurate project description after this milestone is:

> A reversible, single-region train-run sharding-readiness proof of concept
> using fixed schema-isolated logical shards, explicit routing, monotonic
> PostgreSQL fencing generations, bounded quiesced migration, and controlled
> rollback while PostgreSQL remains the authoritative writer.

This is not completed production-grade physical database sharding. It does not
promise transparent online rebalancing, zero interruption, independent shard
failure domains, unlimited shard count, global transactions, multi-region
ownership, or production capacity.

## Solution

Add a fixed control-plane catalog in the public schema with three allowlisted
storage targets: `legacy`, `shard-0`, and `shard-1`. The two logical shard IDs
map internally to `booking_shard_0` and `booking_shard_1`; identifiers are never
accepted from customer input or interpolated from unvalidated configuration or
database values. Migration 8 bootstraps an explicit `legacy` assignment with a
positive initial generation for every existing train run, eliminating an
ambiguous implicit legacy route.

Encapsulate route resolution and schema selection in a routed transaction
module. It accepts only a validated shard handle, sets an allowlisted
transaction-local `search_path`, locks the assignment row and local train-run
fence, and verifies shard, generation, write enablement, and migration state
before exposing booking repositories. The setting is local to the transaction
and cannot leak through a pooled connection. HTTP handlers and application
services never receive schema names or construct schema-qualified SQL.

Place train-run booking state, booking idempotency, and write fences in the
selected storage. Keep one centrally indexed `public.outbox_events` table in
this same PostgreSQL database so booking and offering intent still commit in
their source transaction without publisher/copy races. A minimal public
idempotency-key claim preserves ADR 005 uniqueness across shards, but is written
only after routing/fence selection, stores no completed result, and cannot route
or replay a command. The public booking tables remain the authoritative legacy
storage and are not a second write target for an assigned schema shard. A
public quota-claim ledger remains authoritative across all logical schemas so
one user's or passenger's limit cannot be bypassed by spreading holds across
shards. All global claims and central outbox intent commit in the same
cross-schema PostgreSQL transaction as reservation state.

Create public reservation, ticket-order, and ticket locator indexes. Creation
or transition of the referenced resource and its locator commits atomically.
ID-based operations resolve exactly one authoritative route through these
indexes, then revalidate ownership and generation in PostgreSQL. Owner-scoped
ticket-order lists page through the global locator index first and fetch only
the bounded set of routes represented by that page.

Move a selected train run with a durable, resumable state machine: plan, drain,
quiesce, copy, validate, cut over, resume, and retain the source for a bounded
rollback window. Source and target are never writable simultaneously and no
dual writes are introduced. A successful target mutation records durable
target-write evidence; after such evidence exists, direct mapping rollback is
forbidden and recovery requires a validated reverse migration with a newer
generation.

Keep the Milestone 3 journey projection global and disposable. Search remains
global. Availability reads resolve the current assignment and use a
generation-aware cache namespace; cutover rotates that namespace without a
global Redis scan. Redis remains a cache and waiting-room dependency, never the
ownership authority.

## Why `train_run_id` Is the Boundary

- Inventory ownership, segment masks, holds, reservations, ticket lifecycle,
  expiration, and availability all converge on one dated train run.
- A train run provides a natural unit for locking, reconciliation, migration,
  rollback, and operator inspection.
- Assigning the whole run keeps overlapping-seat correctness within one
  PostgreSQL transaction and one authoritative inventory.
- `user_id` is unsuitable because one train run's seats would be fragmented
  across user shards and allocation would require cross-shard coordination.
- Passenger-based sharding has the same problem and would expose identity data
  as a physical routing concern.
- Hashing `reservation_id` cannot route creation before the reservation exists
  and would split one train run's inventory and lifecycle across writers.
- The boundary is an extraction seam for a future physical PostgreSQL pilot,
  not evidence that cross-database foreign keys or transactions are solved.

## Actors and Journeys

- **Customer** creates, reads, confirms, or cancels a reservation and reads
  ticket state without selecting or learning a shard.
- **API replica** may cache a route briefly, but begins every mutation through
  a fenced routed transaction and refreshes an authoritative stale route at
  most once.
- **Hold-expirer** enumerates bounded authoritative worksets and expires holds
  only through a validated route.
- **Outbox worker** continues the central bounded claim/publish/finalize
  protocol. Booking events carry bounded internal train-run/shard provenance,
  but consumers do not depend on shard identity for correctness.
- **Admission worker** manages queue state without deciding booking ownership;
  the booking path resolves the current route when a token is used.
- **Read-model worker** consumes at-least-once events and reloads current routed
  authoritative state while maintaining the global disposable projection.
- **Railway operator** inspects catalog, health, assignments, migrations, and
  reconciliation; explicitly confirms cutover, rollback, or cleanup.
- **Reviewer/test harness** proves atomicity, fencing, rollback restrictions,
  failure isolation, bounded resource use, and preservation of Milestones 1–3.

### Write-routing journey

1. A create request supplies a train run, never a shard.
2. The router reads or briefly caches the public assignment's shard and
   generation.
3. Admission is evaluated as already defined, then the routed transaction
   begins on the fixed storage handle.
4. The transaction locks the assignment and its local fence and validates the
   expected shard/generation, storage/fence write enablement, and migration
   state.
5. It inserts or validates the global idempotency-key claim, acquires local
   idempotency, locks the canonical global quota claim scope, allocates
   inventory, creates reservation state and the public reservation locator,
   appends central booking outbox intent, completes local idempotency, and
   commits atomically.
6. A stale assignment rolls back all effects. The caller refreshes from the
   catalog once and may retry once; repeated staleness fails safely.

Confirm, cancel, expiration, and reconciliation begin from a locator or a
bounded train-run workset, enter the same routed transaction boundary, and
perform the same assignment/fence validation before mutation.

### Reservation and ticket lookup journey

1. A reservation-ID request looks up the public reservation locator.
2. The API checks its owner against the authenticated user before returning
   protected data and does not expose locator topology.
3. The route is revalidated against the assignment. A stale locator is
   refreshed authoritatively rather than causing a shard scan.
4. The repository reads only the selected storage.
5. Ticket-order and ticket-ID reads use their corresponding public locator.
6. An owner-scoped order list first reads one bounded locator-index page,
   groups only that page by allowlisted route, and performs bounded fetches.
   Partial internal failure is never silently represented as complete data.

### Migration journey

1. The operator plans a source-to-target move after verifying source
   assignment, target health/capability, and absence of another active
   migration.
2. The catalog reserves a newer target generation and records a durable
   migration row.
3. Draining rejects new holds retryably while allowing only the documented
   bounded existing lifecycle work on the source.
4. Quiescence locks the authoritative assignment, disables the source fence,
   and waits through bounded database mechanisms for in-flight transactions.
5. Deterministic bounded batches copy all train-run-local authoritative state
   and record a resumable cursor and counts. The global quota ledger and global
   locator indexes are validated but not duplicated into a shard.
6. Validation proves counts, identities, masks, lifecycle states, quotas,
   idempotency, tickets, outbox intent, and locator relationships.
7. Cutover atomically installs the target generation/fence, updates assignment
   and all affected locators, leaves the source fence disabled, rotates the
   availability generation, and records the rollback window.
8. Writes resume only on the target. Source rows remain retained and read-only
   until a separately confirmed cleanup after the rollback window.

### Operator cutover journey

1. The operator inspects a bounded validation report and dry-run output.
2. The command verifies the migration is `cutover_ready`, the source is still
   authoritative and fenced, the target copy still matches, and no conflicting
   migration exists.
3. Explicit confirmation starts one bounded cutover transaction.
4. A commit makes the target assignment and locator switch visible together;
   a rollback leaves the source assignment intact and target fenced.
5. Post-commit health and reconciliation establish that only the target is
   writable before normal traffic is considered resumed.

### Rollback journey

- Before cutover, the target copy may be retained or discarded while the
  source fence is re-enabled and the assignment returns to stable.
- After cutover but before any successful target mutation, the operator may
  restore source ownership atomically using a newer generation, disable the
  target, update locators, and record an audit entry.
- Once durable target-write evidence exists, a direct mapping flip is rejected.
  The operator must plan a reverse migration, copy and validate the target's
  current state, and cut over with a still newer generation.

### Shard outage and stale-router journey

- Catalog loss fails new writes closed; cached routes cannot bypass the
  assignment lock and fence check.
- A request for an unavailable logical shard receives a bounded retryable
  service response without shard identity. Other logical shards may continue.
- A target outage before cutover leaves the source authoritative and the
  migration resumable or safely failed.
- Source loss before validated cutover never promotes the target.
- A stale replica is rejected by PostgreSQL, refreshes once, and either reaches
  the target or fails without probing other shards.
- Because both logical shards share one PostgreSQL cluster, this milestone can
  demonstrate logical routing/fencing degradation but not independent physical
  database failure isolation.

### Cross-shard administration journey

1. The operator chooses an allowlisted, bounded scope.
2. The command enumerates a fixed workset with per-shard and global deadlines,
   deterministic serial traversal, effective concurrency `1`, stable ordering,
   and bounded memory.
3. Results are explicitly `complete`, `partial`, or `unavailable`; an opaque
   cursor may continue a page without encoding schema names or credentials.
4. Reconciliation is detect-only by default. Cleanup and other mutations
   require explicit confirmation and revalidation.

## User Stories

1. As a customer, I want a reservation routed by its train run so that all
   competing seat writes use one authoritative inventory.
2. As a customer, I want shard topology hidden so I cannot accidentally or
   maliciously choose a storage target.
3. As a customer, I want stale routing to produce a bounded retryable response
   rather than a duplicate or conflicting reservation.
4. As a customer, I want the same idempotency key and request to replay one
   durable result after cutover.
5. As a customer, I want reuse of that key with another request to remain a
   conflict regardless of migration.
6. As a customer, I want confirm and cancel routed by locator without scanning
   every shard.
7. As a customer, I want reservation and ticket ownership checked against my
   authenticated identity after routing.
8. As a customer, I want owner-scoped ticket-order pages complete and bounded,
   never silently partial.
9. As a customer, I want one user and passenger quota enforced across all
   logical shards.
10. As a customer, I want migration rejection to leave seat masks, quota,
    idempotency, and outbox unchanged.
11. As a waiting-room customer, I want a token issued before cutover to remain
    safely usable after cutover.
12. As a waiting-room customer, I want a fenced attempt not to consume my token
    permanently or leak an in-flight slot.
13. As an API replica, I want a bounded route cache whose stale entries are
    harmless because the database fence is authoritative.
14. As an API replica, I want exactly one authoritative refresh and retry after
    stale-assignment rejection.
15. As an API replica, I want an unknown or disabled shard to fail closed.
16. As a booking repository, I want a validated routed transaction so schema
    selection and fence enforcement cannot be bypassed by individual queries.
17. As a database operator, I want schema identifiers fixed and allowlisted so
    no request, token, Redis value, or catalog corruption can inject SQL.
18. As a database operator, I want transaction-local routing state to reset
    automatically when pooled connections are returned.
19. As a railway operator, I want existing train runs bootstrapped to explicit
    legacy assignments so every locator references a durable assignment.
20. As a railway operator, I want selected train runs explicitly assigned to
    either logical shard without moving unrelated runs.
21. As a railway operator, I want every generation positive and monotonically
    increasing so an old writer can never regain authority.
22. As a railway operator, I want source and target never write-enabled at the
    same time.
23. As a railway operator, I want a bounded zero-writer quiescence interval so
    final copy and cutover do not require dual writes.
24. As a railway operator, I want migration copy resumable from a deterministic
    cursor after a process or transaction failure.
25. As a railway operator, I want validation to cover every authoritative
    booking relation, not just row counts.
26. As a railway operator, I want cutover of assignment and locators atomic so
    readers cannot observe a partial route switch.
27. As a railway operator, I want source data retained read-only for a bounded
    audit and rollback window.
28. As a railway operator, I want cleanup separate, explicitly confirmed, and
    blocked until the window expires and authority is revalidated.
29. As a railway operator, I want a safe pre-cutover rollback without data
    loss or generation decrease.
30. As a railway operator, I want direct post-cutover rollback permitted only
    when durable evidence proves the target accepted no mutation.
31. As a railway operator, I want a reverse migration after target writes so
    successful target state is never discarded.
32. As a railway operator, I want availability namespaces rotated at cutover
    without scanning the Redis keyspace.
33. As a railway operator, I want search to remain a global read-model query
    rather than fan out across booking shards.
34. As a hold-expirer operator, I want bounded per-shard batches and continued
    progress on healthy shards when one route fails.
35. As an outbox operator, I want the existing bounded central relay to retain
    at-least-once safety without source/target lease races during migration.
36. As a read-model operator, I want events to reload current authoritative
    routed state so duplicate and out-of-order delivery remains safe.
37. As an administrator, I want bounded fanout, explicit partial results, and
    opaque pagination.
38. As an administrator, I want migration commands unavailable on public
    customer APIs.
39. As an administrator, I want dry-run and explicit confirmation for
    destructive operations and nonzero failure exits.
40. As an administrator, I want output free of DSNs, credentials, passenger
    PII, raw reservations, and internal schema topology.
41. As an operator, I want catalog failure to fail writes closed without a
    random or Redis-selected fallback.
42. As an operator, I want optional logical-shard degradation visible while
    unrelated serving routes remain available.
43. As an operator, I want each long-running process to expose liveness,
    dependency-aware readiness, and bounded-label metrics.
44. As a security reviewer, I want customer errors and logs to hide shard IDs,
    schema names, generations, migration IDs, SQL, payloads, and secrets.
45. As a security reviewer, I want metrics limited to bounded operation,
    result, reason, phase, and configured shard labels.
46. As a reviewer, I want real PostgreSQL evidence for three stale replicas,
    100 concurrent routed-transaction/fencing attempts, and a separate 100-call
    end-to-end `CreateHold` gate proving one target writer, no source mutation,
    no duplicate reservation, and no overlapping allocation.
47. As a reviewer, I want real Redis tests proving admission and cache behavior
    across migration without making Redis authoritative.
48. As a reviewer, I want all Milestone 1–3 behavior and race tests to remain
    green.
49. As a reviewer, I want load results to measure overhead and interruption
    honestly without asserting production or national-scale capacity.
50. As a future architect, I want the logical boundary documented as an
    extraction seam while cross-database transactions and foreign keys remain
    explicitly unresolved future work.

## Consistency and Authority Boundaries

- The public shard catalog, assignments, migrations, minimal global
  idempotency-key claims, global quota claims, locator indexes, and central
  outbox are durable control/global state.
- Public legacy booking tables are authoritative only for train runs assigned
  to `legacy`.
- Each schema shard is authoritative only for train runs whose current
  assignment and matching local fence select it.
- Railway reference and identity records remain public and may participate in
  cross-schema foreign keys during this same-cluster PoC.
- Seat inventory, reservations, reservation seats, order/ticket state, local
  idempotency completion, and reconciliation observations are train-run scoped.
- The central outbox carries bounded train-run and assignment provenance for
  booking events, but remains same-database public storage in this PoC.
- The global quota-claim ledger is the cross-shard source of truth for active
  per-user and per-passenger claims. It is locked and mutated in the same
  PostgreSQL transaction as the associated booking lifecycle transition.
- Reservation, ticket-order, and ticket locators are global routing indexes,
  not substitutes for authorization or booking state.
- The Milestone 3 journey projection, event receipts, and Redis cache entries
  are global derived state and may be rebuilt.
- Redis waiting-room and cache state never grants shard ownership and cannot
  override PostgreSQL assignment or fencing.
- Same-cluster cross-schema atomicity is intentional for this milestone and is
  a stated limitation for future physical extraction.

## Shard Catalog and Topology

- The catalog contains exactly the fixed PoC storage targets `legacy`,
  `shard-0`, and `shard-1`; arbitrary shard creation is not a customer feature.
- `shard-0` and `shard-1` map through compiled or otherwise immutable validated
  metadata to `booking_shard_0` and `booking_shard_1`.
- Storage and assignment changes are transactional. Disabled storage cannot
  receive new assignments.
- Migration 8 creates the schemas and required shard-local tables and
  bootstraps all existing train runs to explicit `legacy` assignment rows.
- New train runs receive an explicit legacy assignment until an operator plans
  another placement.
- One active assignment exists per train run. Its generation is positive,
  never decreases, and advances for every cutover or ownership rollback.
- Stable serving state has exactly one write-enabled matching fence. An
  explicitly documented migration phase may have zero writers, but no state
  permits more than one.

## Routing, Fencing, and Transaction Boundaries

- Route resolution returns an opaque shard handle plus assignment generation,
  never a free-form schema identifier.
- The optional route cache has bounded entries and TTL. It is keyed by train
  run and stores only allowlisted shard identity and generation.
- The routed transaction module applies a validated transaction-local
  `search_path`; tests must prove pooled connections revert afterward.
- Before any mutation, the transaction locks the public assignment row and the
  selected storage's local fence, then verifies matching shard and generation,
  both write-enabled states, and an allowed migration state.
- A stale route cannot create or complete idempotency, reserve quota, change a
  mask, create a locator, append outbox intent, or record target-write evidence.
- Database guards on retained public booking mutations derive the affected
  train run and reject writes whose assignment is no longer `legacy` or whose
  legacy fence is disabled. Schema-PoC rollout also requires an explicit
  minimum-writer-version drain so no known pre-M4 writer remains active.
- Create, confirm, cancel, expire, and reconcile mutations use the same module.
- No caller probes multiple shards after failure. At most one catalog refresh
  and one bounded retry follows `shard_assignment_stale`.
- Public failures collapse internal stale, fenced, unavailable, and migration
  categories into bounded safe responses such as
  `service_temporarily_rebalancing` where appropriate.

### Create transaction

The transaction validates routing/fencing, inserts or validates the global
idempotency-key claim, acquires local create idempotency, serializes and records
global quota claims, validates global reference data, allocates a local seat
mask, creates reservation and seat rows, creates the public reservation
locator, appends central booking outbox intent, completes local idempotency, and
commits all effects together.

### Confirm transaction

The reservation locator selects the route. The transaction validates owner,
assignment, and fence; validates the global key claim and acquires local
confirm idempotency; transitions the reservation; closes its active-hold quota
claim; creates order/tickets and their public locators; appends all central
booking outbox intents; completes idempotency; and commits atomically.

### Cancel and expiration transactions

The reservation locator selects the route. Each transaction validates the
assignment/fence, locks reservation and inventory state, transitions lifecycle
and releases masks and global quota claims where applicable, updates order or
ticket state, appends outbox intent, and commits atomically. Expiration uses a
bounded shard-aware workset rather than scanning every storage in one query.

## Locator and Cross-Shard Read Design

- Locator uniqueness prevents one logical resource from resolving to several
  storages.
- Locators reference an existing train-run assignment and carry the observed
  assignment generation and owner needed for bounded lookup/security checks.
- Resource creation and locator creation are one same-cluster transaction.
- Migration changes all affected locator routes in the atomic cutover
  transaction, never during an externally visible partial-copy phase.
- A stale locator triggers authoritative assignment reconciliation. An absent
  locator is a not-found or explicit legacy-compatibility condition, never an
  excuse for an unbounded shard scan.
- Owner lists query a bounded ordered page of the global locator index, then
  fetch only the distinct allowlisted routes in that page under configured
  concurrency and deadlines.
- The ticket-order locator index stores the owner, status, total, currency, and
  created time needed for the existing exact global sort and count contract;
  confirmation creates it and cancellation updates its status atomically.
- Customer responses do not reveal whether rows came from legacy, shard-0, or
  shard-1.

## Idempotency and Outbox Design

- Booking idempotency is stored beside authoritative booking state: public for
  legacy-assigned runs and shard-local for schema-assigned runs.
- A minimal global key claim with uniqueness on user, operation, and key hash
  is inserted only inside the already-routed transaction. It stores the request
  fingerprint and train run but no completion/result, so it preserves the
  existing cross-shard conflict contract without becoming pre-routing replay
  authority or diverging from local booking state.
- The claim and local idempotency record share one database-derived expiry. A
  key may be atomically reacquired on any current shard only after that expiry;
  bounded cleanup cannot remove the global claim earlier than the local retry
  window, and migration does not extend or shorten it.
- Idempotency acquisition follows routing and fence validation and is completed
  only in the same transaction as the booking result.
- Migration copies fingerprints, statuses, responses, expiry metadata, and
  resource references. Retry after cutover reaches the target's copied record.
- Same key/same request replays; same key/different request conflicts; failure
  before commit leaves no completed record.
- Booking and offering outbox rows remain centrally indexed in
  `public.outbox_events` because every logical shard shares one database.
  Booking state and outbox intent still commit atomically without Redis or a
  distributed transaction; event IDs remain globally unique.
- Migration validates central event provenance and publication state but does
  not copy outbox rows. The existing bounded lease/finalize worker therefore
  has no source/target claim race.
- Bounded shard provenance may support operations but is not part of external
  consumer correctness and never creates unbounded metric labels.

## Migration State Machine and Copy Process

Durable states are `planned`, `draining`, `copying`, `validating`,
`cutover_ready`, `cutting_over`, `rollback_window`, `completed`, `failed`, and
`rolled_back`. Only one active migration exists per train run; transitions are
explicit and invalid transitions fail closed.

- **Plan:** lock and verify source assignment, target health and capability,
  uniqueness, and reserve a newer target generation.
- **Drain:** prevent new reservation creation for the run with a retryable
  response. Existing lifecycle operations may continue only according to a
  documented bounded policy while target remains fenced.
- **Quiesce:** lock assignment, disable source writes, and wait with a bounded
  database timeout for in-flight work. Arbitrary sleeps are prohibited.
- **Copy:** process deterministic primary-key order in bounded batches selected
  per private operator invocation and
  persist a resumable cursor and row counts. Copy inventory, reservations,
  reservation seats, associated orders/tickets, local idempotency,
  fence/reconciliation state, and any other classified local authority. Global
  key claims, quota claims, locators, and central outbox rows are validated but
  not copied.
- **Validate:** compare keys/counts, foreign-key relationships, status counts,
  exact segment masks, quota claims, idempotency responses, order/ticket links,
  outbox state, locator coverage, and reconciliation invariants. The target is
  never routable from partial-copy data.
- **Cut over:** preflight a documented locator-row cap, supporting indexes, and
  a statement timeout; revalidate immediately; then install the newer target
  fence, update assignment and locators, retain the disabled source, rotate
  availability generation, and enter the rollback window in one transaction.
- **Resume:** permit writes only after the target assignment/fence pair is
  committed. Record durable target-write evidence with every first and later
  mutation under that target generation.
- **Complete:** retain source data for the window persisted by the migration
  plan. Cleanup is an
  explicit later operation and never an automatic state transition.

Raw SQL errors, DSNs, credentials, payloads, stack traces, or PII are never
stored in migration state; only bounded error categories are durable.

## Rollback Semantics

- Pre-cutover rollback preserves the source assignment, re-enables its fence,
  leaves target non-authoritative, records failure/rollback state, and never
  loses committed source data.
- Post-cutover direct rollback takes assignment and both fences under exclusive
  locks that conflict with normal mutation fence locks, then checks migration,
  the bounded locator set, and durable target-write evidence in that same
  transaction. It is allowed only when no target mutation committed after
  cutover.
- A permitted direct rollback uses a new generation, never restores an old
  generation, atomically switches assignment and locators, disables target,
  enables source, and records an audit outcome.
- Any target-write evidence makes direct rollback permanently unsafe for that
  cutover. The command rejects it and requires a reverse migration with a new
  migration ID, newer generation, bounded copy, full validation, and cutover.
- Source cleanup requires completed migration, expired rollback window,
  explicit confirmation, current assignment/fence revalidation, and clean
  cancellation. Interrupted cleanup must not change authority.

## Admission, Read Model, and Cache Interaction

- Admission tokens bind train run and request identity, not shard or schema.
- Token use resolves the current assignment, validates admission, acquires a
  bounded processing slot, and then enters the fenced booking transaction.
- Draining/fenced rejection does not permanently consume the token, complete
  idempotency, mutate quota, or leak the processing lease. Token TTL and retry
  behavior are explicit and bounded.
- The global journey projection remains disposable. Search never queries every
  booking shard and does not use locators.
- Availability resolves the authoritative route, reads only that storage, and
  places assignment generation in its internal cache envelope/namespace.
- Cutover rotates the affected run's availability namespace and schedules the
  relevant global read-model invalidation/rebuild without `KEYS`, broad
  `SCAN`, or global cache deletion.
- Old-generation cache entries are rejected or naturally expire. Booking never
  trusts cached availability when allocating a seat.

## Shard-Aware Workers and Administration

- Each worker obtains an allowlisted bounded workset rather than discovering
  arbitrary schemas.
- Hold expiration and outbox polling use bounded batches, stable fair order,
  per-shard result categories, and continue healthy routes after one failure.
- The read-model worker resolves locators/routes and reloads current state;
  duplicate and out-of-order event delivery remains safe.
- Admission queue operations remain shard-neutral.
- Reconciliation runs per selected shard and aggregates a bounded detect-only
  summary. Production repair is never automatic by default.
- Administrative list, inspect, plan, start, resume, validate, cutover,
  rollback, cleanup, reconcile, and health operations are operator-controlled
  commands, not customer APIs.
- Meaningful operations support dry-run; mutations require an explicit
  confirmation flag, honor cancellation, bound output, and return nonzero on
  failure.
- Cross-shard queries enforce per-shard/global timeouts, deterministic serial
  traversal with effective concurrency `1`, bounded memory, and explicit
  partial status.

## Failure and Degradation Policy

- Catalog/control PostgreSQL unavailable: train-run writes fail closed and
  readiness reports the required dependency; cached or Redis routes cannot
  authorize writes.
- Optional logical shard unavailable: requests for it return a bounded safe
  error, healthy routes continue, workers continue their remaining bounded
  workset, and admin aggregation reports `partial`.
- Required/default serving storage unavailable: readiness follows the
  explicitly configured required-storage policy rather than silently falling
  back elsewhere.
- Target unavailable during migration: source remains authoritative and no
  cutover occurs.
- Source unavailable before cutover: target is not promoted without complete
  source validation.
- Source unavailable after successful cutover: target remains authoritative;
  loss of rollback evidence is reported as an operational limitation.
- Redis unavailable: ownership is unchanged; Milestone 3 reads use their
  documented fallback and Milestone 2 hot-train admission remains fail-closed.
- Liveness is process health, not dependency health. API readiness may remain
  available with healthy catalog and at least one serving storage when failed
  storage is classified optional; degradation is visible and tested.
- Every long-running worker exposes `/livez`, `/readyz`, and `/metrics` and
  validates only its required configuration and dependencies.

## Configuration Requirements

- `booking_shard_mode`: `legacy` or `schema_poc`; default `legacy`.
- `booking_shard_ids`: fixed configured subset of the allowlisted catalog.
- `booking_route_cache_enabled`
- `booking_route_cache_ttl_seconds`
- `booking_route_cache_max_entries`
- `booking_shard_query_timeout`

Worker traversal is serial and bounded by the configured subset of the three
fixed storage IDs. Migration controls belong to the private operator command:
each invocation accepts a bounded `--timeout`, copy accepts bounded
`--batch-size`, and planning accepts bounded `--rollback-window`. These are not
application runtime environment settings.

Startup rejects duplicate/unknown shard IDs, unsafe schema identifiers,
invalid limits, and a disabled required/default storage. Schema mode requires
explicit production opt-in; ordinary production examples remain legacy by
default. Full configuration and secrets are never logged. Operator commands do
not receive JWT or admission keyring secrets unless an owned function requires
them.

## Observability Requirements

Bounded metrics cover route resolution/cache/refresh, stale assignment and
fence rejection, request duration, unavailability, bounded/partial fanout,
migration lifecycle/duration/copy counts/validation failures, cutover,
rollback, and reconciliation mismatch.

Allowed labels are only bounded values such as `operation`, `result`,
`reason`, `phase`, and allowlisted `shard_id`. Metrics never label train run,
reservation, ticket, user, passenger, migration, generation, schema, host,
DSN, idempotency key, admission token, or cache key.

Logs use bounded sanitized categories and never reveal schema-qualified SQL,
topology, credentials, DSNs, raw booking identifiers or payloads, admission or
idempotency material, passenger PII, migration internals, or full config.
Migration and fanout reports distinguish complete, partial, retryable, and
failed outcomes without embedding arbitrary database errors.

## Security Requirements

- Public requests, JWT claims, Redis values, and cache entries cannot select a
  shard or schema.
- Assignment authority and current role checks remain in PostgreSQL.
- Every schema name is mapped from a fixed allowlist before SQL execution;
  malicious shard IDs and identifier injection fail before a transaction.
- Transaction-local `search_path` is reset by transaction completion and
  cannot persist through pooling.
- Public errors do not disclose shard identity, schema, generation, migration,
  database topology, or DSN.
- Locator lookup never bypasses owner authorization, and locator tampering is
  detected by assignment/fence validation.
- Migration replay and invalid state transitions fail closed. Cutover,
  rollback, and cleanup are operator-controlled and explicitly confirmed.
- Fanout bounds protect against administrative denial of service.
- Cleanup cannot run before the rollback window or against current authority.
- No production secret, database data, passenger PII, migration dump, Redis
  data, local absolute path, or raw operational output is committed.

## Migration Availability Impact

- Draining can reject new reservation creation for the selected train run.
- Quiescence and final copy/cutover intentionally create a bounded interval in
  which neither source nor target accepts train-run writes.
- Existing lifecycle operations are allowed only until the documented drain
  boundary and may then receive a retryable response.
- Admission and read browsing may continue according to their independent
  policies, but successful booking remains subject to the fence.
- Bounded timeouts abort or safely fail migration rather than waiting without
  limit.
- Measurements must report rejection-window and validation duration honestly.
  The design does not claim zero downtime or online no-disruption migration.

## Implementation Decisions

- Build a **catalog/router deep module** that resolves fixed shard handles and
  hides schema identities, cache policy, authoritative refresh, and bounded
  shard enumeration behind a small interface.
- Build a **routed transaction deep module** that owns transaction-local
  schema selection, assignment/fence locking, migration permission, stale-route
  categorization, and rollback behavior. Booking repositories cannot bypass it.
- Build a **locator module** for atomic reservation/order/ticket locator writes,
  owner-index paging, generation refresh, and bounded route grouping.
- Build a **global quota-claim module** that serializes canonical user and
  passenger claims across legacy and both logical schemas inside the booking
  transaction.
- Build a minimal **idempotency-key-claim module** that preserves the existing
  global key conflict contract but has no route or completed replay interface.
- Adapt booking repositories so their SQL is storage-relative under a validated
  transaction; local completion idempotency remains beside authoritative state
  while the minimal global key claim and central outbox participate atomically
  in the same database transaction.
- Build a **migration coordinator** that owns the durable state machine,
  deterministic cursor, quiescence, copy groups, validation report, cutover,
  target-write evidence, rollback decisions, and cleanup gate.
- Build a **bounded fanout/workset module** shared by workers and operator
  commands for deadlines, concurrency, stable aggregation, cancellation, and
  partial-result semantics.
- Keep offering/reference storage and the global Milestone 3 projection outside
  booking shards. Their existing authority and fallback behavior remain.
- Use migration 8 for explicit catalog/bootstrap and schema-local structures;
  do not use runtime AutoMigrate or dynamically create schemas.
- Preserve existing PostgreSQL segment-mask allocation predicates unchanged.

## Testing Decisions

Tests assert externally observable authority, atomicity, routing, errors, and
bounded behavior rather than private helper structure. Real PostgreSQL is
required for cross-schema transactions, locks, constraints, generations,
cutover, and rollback; real Redis is required for waiting-room and cache
interaction. Concurrency tests use deterministic barriers, advisory locks,
channels, and test clocks rather than arbitrary sleeps.

This section separates bounded implemented coverage from final acceptance.
The presence of a test or runner does not imply controlled runtime, CI,
independent review, or release acceptance.

- Routing tests cover explicit legacy bootstrap, shard-0/shard-1, unknown or
  disabled shards, bounded cache/expiry, one stale refresh, and identifier
  rejection.
- Fencing tests cover matching route, stale generation, wrong shard, disabled
  fence, generation monotonicity, allowed zero-writer migration phases, and the
  invariant that two storages are never write-enabled.
- Locator tests cover atomic create, confirm/cancel/read routing, owner checks,
  stale refresh, ticket locators, bounded owner pages, and no scan on missing.
- Idempotency/outbox/quota tests prove same-transaction success and rollback,
  cross-shard key uniqueness, replay after cutover, conflict behavior, no
  wrong-shard completion, synchronized expiry, bounded cleanup, cross-shard
  reacquisition after expiry, global quota enforcement across shards, central
  outbox provenance, and globally unique event identity.
- Migration tests cover every valid/invalid state transition, one active move,
  deterministic resumable copy, exact state validation, failed copy, failed
  cutover, pre-cutover rollback, post-cutover rollback before writes, durable
  target-write restriction, and reverse migration.
- Migration fixtures cover copied held, confirmed, cancelled, and expired
  state. The post-cutover probe covers create, get, replay, confirm, cancel, and
  ticket reads.
- The runner also creates a target-side hold, arms expiry only in the target
  schema, waits for the shard-aware hold-expirer, and confirms `expired` through
  the locator read. Final read-model reconciliation remains indirect.
- Admission/cache tests cover tokens issued before cutover, draining/fenced
  submissions, retry without duplicate or lease/quota leak, availability
  generation rotation, and rejection of old cache envelopes.
- Current operator reconciliation traverses the fixed storage set serially,
  with effective concurrency `1`, deterministic order, bounded pages/time, and
  explicit complete, partial, or unavailable status.
- SQL security tests prove only allowlisted identifiers are used, no input
  reaches identifiers, `search_path` cannot leak across pooled transactions,
  and retained-public guards reject old or bypassing writers after cutover.
- The deterministic PostgreSQL suite keeps its routed-transaction/fencing test
  and a separate 100-call full `CreateHold` barrier test. The full gate uses
  distinct users across three stale caches and requires exactly one target
  reservation for a one-seat fixture, no source mutation, no duplicate ID, no
  overlap, exact per-replica refresh counts, and clean reconciliation.
- The bounded runtime runner separately exercises three API replicas with stale
  caches and checks target writes plus post-run integrity. These observations
  must not be merged into a throughput or production-capacity claim.
- Failure tests cover crashes before/after booking commit, catalog/lock/schema
  outages, copy/validation/cutover failure, worker/admin interruption, Redis and
  projection delays, and cleanup interruption without authority ambiguity,
  infinite retries, goroutine leaks, or credential leaks.
- Migration tests cover fresh up, repeated up, populated v7-to-v8, constraints,
  compatibility fixtures, safe one-step down/up where supported, and a clean
  migration version.
- Full Milestone 1–3 regression, race, static analysis, vulnerability, secret,
  filesystem/image, Compose, and multi-replica gates remain required.
- The bounded runner records route/cache observations, strict k6 checks,
  request/sample/iteration counts and achieved rates, stale refresh, both
  cutovers' generation rotation, pre-baseline read-model catch-up, exact
  `shard_cutover` event receipts, retained-source fingerprints,
  catalog-disabled-route behavior, serial admin fanout, aggregate migration
  timing, reconciliation, connection samples, and Redis PING latency.
- A sustained benchmark still requires repeated request-rate measurements,
  per-copy-group throughput, independent warm-up, host limits, and complete
  PostgreSQL/Redis telemetry without fabricated capacity claims.

## Acceptance Criteria

- A complete data-dependency map and ADR set approve the boundary before
  booking implementation begins.
- Migration 8 creates public control/locator/quota structures, both fixed shard
  schemas, required local tables, constraints/indexes, and explicit legacy
  assignments for existing train runs without moving existing booking data.
- Legacy-assigned train runs preserve all Milestone 1–3 behavior.
- Selected train runs can be explicitly assigned and routed to either schema
  shard.
- Every create, confirm, cancel, expire, reconcile, and applicable worker
  mutation locks and validates assignment plus local fence in its transaction.
- A stale generation or wrong shard cannot commit any booking side effect.
- Generation never decreases; stable ownership has one writer; migration may
  have a bounded zero-writer phase; source and target are never both writable.
- Reservation, ticket-order, and ticket locators are atomic, owner-safe, and
  eliminate unbounded customer shard scans.
- Owner ticket-order listing begins from a bounded global locator-index page and
  fetches only that page's routes.
- Booking completion idempotency resides in authoritative storage; the minimal
  global key claim and central same-database outbox commit atomically with it
  and booking state.
- The public quota-claim ledger enforces limits atomically across legacy and
  both logical schemas.
- Migration is bounded, resumable, validated across all authoritative state,
  and contains no dual-write phase.
- Cutover changes assignment, fences, locators, and availability generation
  atomically or leaves source authority unchanged.
- Pre-cutover rollback is safe; direct post-cutover rollback requires proof of
  zero target mutations; target-write evidence forces reverse migration.
- Source data remains retained/read-only through a rollback window and is
  never cleaned automatically.
- Admission rejection during migration leaves tokens recoverable and creates
  no duplicate, quota mutation, or in-flight leak.
- The global projection remains correct/disposable and old availability cache
  generations are not reused after cutover.
- Workers and administrative queries operate on bounded worksets; one logical
  route failure does not corrupt or starve another; partial results are
  explicit.
- Reconciliation detects assignment/fence, locator, duplicate, mask, quota,
  ticket/order, idempotency, outbox, copy, and stale-source violations without
  production auto-repair.
- Required health endpoints, bounded metrics, sanitized logs/errors, operator
  authorization, SQL identifier protections, and secret boundaries are tested.
- Multi-replica, concurrency, failure/recovery, load-smoke, migration, race,
  CI, security, and container evidence is recorded without unsupported claims.
- Independent review has no unresolved Critical or High finding.
- A non-draft, mergeable, green pull request is opened but not merged, and no
  tag is created.

## Out of Scope

- Payment or refund saga
- Milestone 5 implementation
- Physical PostgreSQL shard deployment or transparent database sharding
- Multi-region ownership transfer or active-active writes
- Global consensus, Raft, Paxos, XA, or two-phase commit
- Kafka, CDC platform, Service Mesh, or Kubernetes Operators
- Splitting the modular monolith into booking/router microservices
- User-, passenger-, or reservation-hash sharding
- Redis-authoritative routing or durable Redis outbox
- Dual-write migration or a generic distributed transaction coordinator
- Zero-downtime online migration
- Automatic shard autoscaling, split, merge, or source cleanup
- Global secondary indexes, Elasticsearch, or OpenSearch
- Unbounded cross-shard fanout
- Changes to PostgreSQL segment-mask correctness
- Production/national-scale capacity claims
- Automatic PR merge or release tag

## Further Notes

- Schema-isolated shards preserve the same-cluster transactions and
  cross-schema foreign keys needed for this proof. Those guarantees do not
  carry automatically to separate databases.
- The fixed three-target topology is deliberate. It proves routing, fencing,
  migration, rollback, and bounded operations without implying arbitrary or
  elastic shard counts.
- Logical schema degradation tests are not evidence of independent physical
  shard fault domains because both schemas share one PostgreSQL cluster.
- Cross-schema public locators and quota claims are purposeful PoC
  dependencies whose extraction or redesign is required before physical
  sharding.
- Cutover can temporarily reject writes. Source retention amplifies disk use,
  and operator backup, abort, rollback-window, and cleanup procedures are part
  of deployment readiness.
- The recommended next milestone, only after this one is fully evidenced, is
  **Physical PostgreSQL Shard Pilot and Online Rebalancing**.
