# ADR 028: Fixed Shard Catalog and Explicit Routing

- Status: Accepted
- Date: 2026-07-23

## Context

Multiple API and worker replicas can retain different routing observations.
An implicit default route, process-local owner map, Redis hint, or shard probing
cannot tell a stale caller whether its target still owns a train run. The
routing design also needs a compatible legacy path, bounded caching and
fanout, safe schema selection, and a durable lookup for requests that start
with reservation or ticket identifiers.

Allowing arbitrary schema identifiers would turn routing metadata into SQL
syntax. Catalog rows alone cannot make such identifiers safe because catalog
corruption, unsafe configuration, or an unauthorized write could introduce an
identifier outside the deployed topology.

## Decision

Migration 8 creates a public `booking_shards` catalog with the fixed Milestone
4 storage IDs:

- `legacy`, mapped to the existing public booking tables;
- `shard-0`, mapped internally to `booking_shard_0`; and
- `shard-1`, mapped internally to `booking_shard_1`.

Only `legacy` and `schema` storage kinds are accepted. Catalog state records
enabled and write-enabled policy plus `active`, `degraded`, `draining`, or
`disabled` lifecycle state. A disabled or unknown storage cannot receive a new
assignment.

Create one public `train_run_shard_assignments` row per train run. Each row
identifies one fixed storage, a positive monotonic generation, assignment
state, and optional active migration. Migration 8 bootstraps every populated
version-7 train run to an explicit generation-1 `legacy` assignment without
moving its booking rows. Creation of a new train run also creates its explicit
legacy assignment transactionally unless an authorized workflow selects
another fixed storage.

Retained public booking tables are protected by database guards. Ordinary
inventory and lifecycle DML is rejected unless the train run is currently
assigned to `legacy` and its legacy fence is write-enabled. Migration copy is
read-only, and later cleanup uses a separately authorized, revalidated operator
path rather than bypassing the ordinary guard. These guards stop an old writer
from mutating public source rows after drain or cutover.

Database guards cannot make a pre-Milestone-4 binary supply and validate an
expected generation if ownership later returns to legacy. Enabling
`schema_poc` therefore has two deployment preconditions: every incompatible
API and booking worker is drained, and every serving writer satisfies the
catalog/configured minimum fencing-protocol version. Readiness rejects an
incompatible writer; the operator rollout gate prevents mixed old/new writers
from entering schema mode.

Expose a deep catalog/router module whose interface resolves:

- a train run to an opaque shard handle and assignment generation;
- a reservation, ticket order, or ticket through its global locator;
- a fresh authoritative route after stale-assignment rejection; and
- a bounded allowlisted workset for workers and operator commands.

The interface never returns a free-form SQL identifier. The HTTP module accepts
domain IDs only and does not know schema names. PostgreSQL adapters receive a
validated shard handle. The handle maps to a schema only inside the routed
transaction module through a compiled allowlist; HTTP input, JWT claims, Redis,
arbitrary environment strings, and unvalidated database values cannot extend
that allowlist.

An optional train-run route cache has a short configured TTL and bounded entry
count. It stores only the fixed shard ID and observed generation. A hit is a
routing hint, not authority. On `shard_assignment_stale`, a caller discards the
entry, refreshes from PostgreSQL once, and may retry once. Repeated staleness,
an unknown shard, a disabled shard, or catalog failure fails safely without
probing other shards.

Ticket-order locators carry `created_at`, `status`, `total_amount_minor`, and
`currency` in addition to identity, ownership, train run, route, and
generation. Lifecycle changes update this summary atomically with the local
order. Owner-scoped ticket-order lists therefore obtain the exact stable
ordering and response fields from a bounded global locator-index page, then
route only that page's resource IDs when authoritative detail verification is
needed. Administrative fanout enumerates only the fixed enabled catalog set and
returns explicit `complete`, `partial`, or `unavailable` status.

All reservation, ticket-order, and ticket locator tables have a train-run index;
the ticket-order locator also has the owner-and-created-at pagination index.
Before cutover, the migration coordinator counts affected locator rows through
those indexes and refuses to enter the cutover transaction above the configured
row cap. The atomic locator update runs under a bounded statement timeout. A
cap or timeout failure leaves assignment and both fences unchanged and records
a retryable bounded migration result; partial locator switching is impossible.

Schema mode defaults off. `legacy` is the default booking mode; `schema_poc`
requires explicit opt-in, and ordinary production examples do not enable it
accidentally.

## Consequences

- Every train run, including legacy runs, has a durable assignment that a
  locator and fence can reference.
- Legacy-table guards and the schema-mode minimum-writer gate make rolling
  deployment requirements explicit rather than assuming old binaries fence
  themselves.
- Route lookup complexity, cache refresh, allowlist validation, and bounded
  enumeration remain local to one module instead of leaking into every caller.
- Stale caches affect latency or produce a bounded retryable error but cannot
  grant write authority.
- Ordinary customer operations select exactly one storage and never scan the
  topology.
- The fixed three-target topology makes configuration, metrics labels, tests,
  and operational fanout bounded.
- Locator cutover duration is bounded by indexed preflight counts, a hard row
  cap, and a statement timeout; oversized train runs require an explicit
  re-plan rather than an unbounded transaction.
- Catalog availability is a required write dependency. Cached routes do not
  make writes available when authoritative control state cannot be validated.
- Adding arbitrary or physical shards later requires a new catalog, health,
  credential, routing, and migration decision; it is not implied here.

## Rejected alternatives

- Implicit legacy fallback when no assignment exists: rejected because locators
  could not reference a durable assignment and missing control state could be
  mistaken for authorization.
- Compute ownership by hashing `train_run_id`: rejected because it does not
  support explicit placement, reversible migration, or retained-source audit.
- Let callers pass shard or schema names: rejected because it permits ownership
  bypass and identifier injection.
- Trust arbitrary catalog schema names: rejected because database metadata is
  not a substitute for the deployed identifier allowlist.
- Use Redis as the route directory: rejected because Redis state is not atomic
  with booking mutations and may be stale, unavailable, or compromised.
- Keep only process-local ownership: rejected because replicas can disagree and
  no database transaction can prove which process-local value was current.
- Retry against every shard: rejected because it is unbounded, leaks topology,
  and can turn stale routing into multiple write attempts.
- Dynamically create a schema per train run: rejected because topology,
  migrations, connections, metrics, and administration would become unbounded.
- Enable schema routing while incompatible writers still serve traffic:
  rejected because an old binary cannot carry the expected generation even
  though database guards can block writes after ownership leaves legacy.
