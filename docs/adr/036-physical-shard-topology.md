# ADR 036: Fixed Three-PostgreSQL Physical Shard Pilot

- Status: Accepted for Milestone 5 implementation
- Date: 2026-07-29

## Context

Milestone 4 routes a train run to `public`, `booking_shard_0`, or
`booking_shard_1`, but all three schemas still share one PostgreSQL transaction
manager, connection pool, failure domain, and migration history. ADR 035
therefore treats physical extraction as unproven. Milestone 5 needs real
database and failure isolation without turning the modular monolith into
network microservices or allowing catalog data to introduce arbitrary database
endpoints.

Physical routing also changes the safety properties of the catalog. A schema
identifier can be selected from a compile-time allowlist, while a PostgreSQL
connection includes credentials, network location, pool limits, protocol
compatibility, and independent health. Persisting a DSN in the catalog would
let catalog corruption or an unauthorized catalog change create an arbitrary
outbound connection and could disclose secrets through logs or diagnostics.

## Decision

Run a fixed, single-region pilot with three independent PostgreSQL instances:

1. `control-postgres`;
2. `booking-shard-0-postgres`; and
3. `booking-shard-1-postgres`.

The instances use separate databases, credentials, connection pools, health
checks, migration histories, volumes, and failure injection. They do not share
a PostgreSQL data directory. Redis remains an admission and cache dependency;
it is not a source of shard ownership.

Control PostgreSQL owns global identity and passenger data, the railway catalog
and global train-run read model, hot-train policy, the shard catalog,
assignments, the migration ledger, booking-command ledger, quota leases,
reservation directory, global ticket/order read projections, control-plane
outbox, read-model receipts, and global operational state.

Each physical booking PostgreSQL owns the train-run booking snapshot, local
seat/coach and fare snapshots, seat inventory, reservations, reservation seats,
ticket orders, tickets, shard-local idempotency execution, booking-command
receipts, shard-local outbox, train-run write fences, migration capture and
mutation journal, migration apply receipts, target-write evidence, and
shard-local reconciliation state.

Extend the catalog with bounded metadata including `storage_kind`,
`connection_ref`, protocol and schema versions, enablement, write enablement,
health, and lifecycle state. Allowed storage kinds are `legacy_schema`,
`logical_schema`, and `postgres`. The pilot physical IDs are fixed to
`physical-shard-0` and `physical-shard-1`; configured physical shard count and
IDs are bounded.

The catalog stores neither raw DSNs, credentials, hosts, nor ports.
Application configuration maps an allowlisted `connection_ref` to a secret
DSN. The physical connection registry validates the complete mapping at
startup, rejects unknown or duplicate identities, constructs bounded pools,
and exposes only sanitized health. Customer and operator HTTP input cannot add
a connection, connection reference, shard ID, or SQL identifier. DSNs and
connection references are absent from public errors, logs, metrics labels, and
readiness responses.

Each pool has configured maximum open and idle connections, connection and
query timeouts, maximum lifetime, and maximum idle time. Startup validates the
sum of control, physical-shard, worker, and administrative pool budgets against
the configured process budget. Exceeding either a per-shard or total budget
fails closed before serving physical writes.

Preserve the proven legacy and logical-schema adapters. A selected train run
may opt into a physical adapter through its control assignment; unassigned and
existing logical assignments keep their current behavior. Physical sharding is
disabled by default and requires explicit opt-in. No route falls back from an
unavailable physical shard to retained or unrelated data.

## Consequences

- The pilot provides real data, connection, and failure isolation for two
  booking databases while retaining one deployable modular monolith.
- One failed booking shard can be isolated without describing logical-schema
  failure tests as physical-shard evidence.
- Control availability remains necessary for new command reservation and route
  authority; shard-local commit and recovery semantics are separate decisions.
- A fixed topology keeps discovery, pool demand, worker enumeration, metrics,
  and administrative fanout bounded.
- Separate control and booking-shard migration histories are required. A shard
  with an incompatible protocol or schema version cannot receive writes.
- Existing logical routes remain compatible, but their shared-database
  atomicity is not attributed to physical routes.
- This is a single-region pilot, not production-certified sharding, automatic
  balancing, physical failover, or national-scale capacity evidence.

## Rejected alternatives

- Store DSNs in the catalog: rejected because catalog contents must not create
  arbitrary network connections or persist secrets.
- Discover an unbounded set of shards dynamically: rejected because connection,
  fanout, readiness, and operational work would be unbounded.
- Hash train-run IDs directly to endpoints: rejected because placement must
  support explicit ownership, migration, source retention, and fencing.
- Fall back to another shard on outage: rejected because only the assigned
  database owns the train run.
- Split routing or booking into network microservices: rejected because the
  milestone changes storage placement, not the modular-monolith deployment.
- Use Redis as the connection registry or ownership authority: rejected because
  it cannot authorize PostgreSQL booking writes and may be stale or unavailable.
