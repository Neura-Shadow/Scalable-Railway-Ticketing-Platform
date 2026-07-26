# Train-Run Sharding Design

## Positioning

Milestone 4 is a reversible, single-region sharding-readiness proof of concept.
It keeps one PostgreSQL cluster and database, and separates booking state into
three fixed logical storage targets:

| Logical storage | PostgreSQL schema | Purpose |
|---|---|---|
| `legacy` | `public` | Compatible storage for existing and unmigrated train runs |
| `shard-0` | `booking_shard_0` | Opt-in logical booking shard |
| `shard-1` | `booking_shard_1` | Opt-in logical booking shard |

The schema names are internal constants. Customer input, JWT claims, Redis,
catalog values, and arbitrary configuration cannot introduce another schema.
This topology is not independent physical PostgreSQL sharding: all three
targets share the database engine, primary, connection pool, disk, and failure
domain.

## Ownership boundary

`train_run_id` is the shard key because competing seat writes, segment masks,
holds, lifecycle transitions, tickets, expiration, and reconciliation all
converge on one dated train run. Sharding by user, passenger, or reservation ID
would split the inventory that must serialize overlapping seat allocations.

Exactly one logical storage owns writes for a train run in a stable serving
state. A bounded migration interval may intentionally have zero writers. No
state may have two writable owners, and migration never dual writes.

## Data boundary

Train-run-local authority moves as one unit:

- `seat_inventory`;
- `reservations` and `reservation_seats`;
- associated `ticket_orders` and `tickets`;
- local booking idempotency completion records; and
- the local train-run write fence.

The following remain in `public`:

- accounts, passengers, railway offering/reference data, and train runs;
- shard catalog, assignments, migrations, and target-write evidence;
- reservation, ticket-order, and ticket locators;
- the minimal global idempotency-key uniqueness claim;
- cross-shard active-hold quota claims;
- the central transactional outbox; and
- the disposable Milestone 3 projection and receipts.

The same-database global claims, locators, quota ledger, outbox, and
cross-schema foreign keys are deliberate constraints. They preserve current
atomicity but must be redesigned before any physical-database extraction.
See the [data-dependency map](architecture/milestone-4-data-dependency-map.md).

## Write path

Every train-run mutation follows one deep transaction boundary:

1. Resolve a train run or resource locator to a fixed shard ID and observed
   positive assignment generation.
2. Begin one PostgreSQL transaction and select only the internally allowlisted
   schema with transaction-local state.
3. Lock the public assignment and verify catalog, assignment, and migration
   policy.
4. Lock the selected storage's fence and verify the same generation and
   `write_enabled = true`.
5. Acquire idempotency and quota state, mutate booking rows, maintain locators,
   append central outbox intent, and record target-write evidence where
   applicable.
6. Commit or roll back all effects together.

A stale API replica or worker cannot become authoritative through its route
cache. PostgreSQL rejects the stale generation before the booking mutation.
The caller may refresh once and retry once; it never probes or writes every
shard.

## Read path

- Availability resolves one current train-run assignment and treats Redis as
  a generation-aware hint only.
- Reservation, ticket-order, and ticket reads use global locators and then
  access one authoritative storage.
- Owner-scoped ticket-order listing pages the global locator index first and
  fetches only routes represented by that bounded page.
- Journey search remains a global read-model query and does not scan booking
  shards.
- Only operator workflows may traverse the fixed allowlisted topology. Current
  reconciliation is serial, with effective concurrency `1`, bounded time and
  output, and explicit partial status.

## Migration model

A selected train run moves through a durable state machine. New creates drain,
the source fence is disabled under the same locks used by normal writers, data
is copied in deterministic resumable batches, and all authoritative invariants
are validated before an atomic cutover. The source remains retained and
read-only during a rollback window.

Quiescence and cutover may reject train-run writes temporarily. This is not a
zero-downtime or disruption-free migration claim. If the target accepts a
successful mutation, direct mapping rollback becomes unsafe; recovery then
requires a fully validated reverse migration with a newer generation.

## Authority and non-goals

PostgreSQL remains the only booking authority. Redis admission and caches may
carry train-run or request identity, but never assignment or fencing authority.
The existing segment-mask allocation predicate is unchanged.

Milestone 4 does not implement payment, active-active writers, multi-region
ownership, physical shard failover, distributed transactions, arbitrary shard
creation, automatic shard splitting, zero-downtime rebalancing, or national-
scale capacity. The next possible step is an evidence-gated physical
PostgreSQL shard pilot, not an automatic production rollout.

## Related decisions

- [Milestone 4 PRD](prd/milestone-4-train-run-sharding.md)
- [ADR 027: train-run boundary](adr/027-train-run-shard-boundary.md)
- [ADR 028: catalog and routing](adr/028-shard-catalog-and-routing.md)
- [ADR 029: fencing generation](adr/029-single-writer-fencing-generation.md)
- [ADR 030: schema-isolated logical shards](adr/030-schema-isolated-logical-shards.md)
- [ADR 031: migration and cutover](adr/031-train-run-migration-cutover.md)
- [ADR 035: future physical shards](adr/035-future-physical-postgresql-shards.md)
