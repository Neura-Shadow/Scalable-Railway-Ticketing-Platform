# Physical Shard Topology

## Scope

Milestone 5 is a fixed, single-region pilot with three independent PostgreSQL
instances: one control database and two booking databases named
`physical-shard-0` and `physical-shard-1`. The existing legacy and logical-
schema paths remain available for train runs not enrolled in the pilot.

This topology demonstrates independent transaction and failure boundaries. It
does not demonstrate multi-region operation, automatic failover, automatic
balancing, national-scale capacity, or production readiness.

## Ownership

- **Control PostgreSQL:** owns users/passengers, catalog/search, assignment,
  customer and operator command ledgers, quota, directory, operator booking
  projections, migration ledger, and the control outbox. It does not own
  shard-local seat mutation or booking receipts.
- **Booking shard 0/1:** owns snapshots, inventory, bookings, receipts, local
  outbox, fences, and migration evidence. It does not own global users,
  assignment authority, or global quota.

There are no cross-database foreign keys, transactions, joins, callbacks, XA,
or two-phase commit. Redis remains an admission and cache aid and is never
booking, quota, routing, or migration authority.

## Route and connection boundary

The control catalog stores only a fixed shard ID and bounded `connection_ref`.
It never stores a DSN, host, port, password, or arbitrary SQL identifier.
Configuration maps the two allowlisted references to separately bounded pools.
A process rejects unknown references, incompatible protocol/schema versions,
disabled or unhealthy routes, and a total pool budget over its configured cap.

The deployment budget is calculated before physical mode starts:

`control_database_max_open_conns * control_database_pool_count`
`+ api_replica_count * physical_shard_count * physical_shard_max_open_conns`
`+ physical_worker_replica_count * physical_shard_count * physical_shard_max_open_conns`
`+ migration_admin_reserve + operational_reserve`.

`POSTGRES_MAX_CONNECTIONS_LIMIT`, when non-zero, is the approved combined
deployment ceiling for that explicit formula; startup fails when demand exceeds
it. This is configuration arithmetic, not an observed capacity measurement.
The local Compose profile budgets 40 control, 18 API-shard, 18 worker-shard, 8
migration/admin-reserve, and 16 operational-reserve connections: 100 total.
Operators must additionally apportion that total per PostgreSQL instance below
each server's `max_connections`, preserving superuser/maintenance headroom.

Every ordinary physical write requires both a current control assignment and a
matching enabled database-local fence for the same train run and generation.
A cached route, Redis value, health result, request value, or successful prior
request cannot replace either condition.

Operator fare, seat-booking-state, and booking-policy mutations use the same
route and fence boundary as customer commands. The operator first reads the
authoritative shard-local source version, submits that version with a bounded
idempotency key, and reserves one immutable `operator_booking_commands` row in
control. The shard mutation and receipt commit atomically; a later control
transaction advances both the bounded projection and ledger state. Recovery
can finalize from that receipt without opening a cross-database transaction.

## Deployment roles

API replicas may use control plus both approved shard pools. Shard-local workers
receive only control state required for ownership and the one shard credential
they need. Global relays enumerate the fixed allowlist with bounded concurrency
and fair rotation. Migration tooling receives separate least-privilege control,
source, and target credentials only for the duration of an operator action.

See [ADR 036](adr/036-physical-shard-topology.md),
[ADR 037](adr/037-control-plane-and-shard-transaction-boundary.md), and
[the failure policy](physical-shard-failure-policy.md).
