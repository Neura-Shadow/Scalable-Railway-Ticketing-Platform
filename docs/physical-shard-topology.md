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
  commands, quota, directory, migration ledger, and the control outbox. It does
  not own shard-local seat mutation or booking receipts.
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

Every ordinary physical write requires both a current control assignment and a
matching enabled database-local fence for the same train run and generation.
A cached route, Redis value, health result, request value, or successful prior
request cannot replace either condition.

## Deployment roles

API replicas may use control plus both approved shard pools. Shard-local workers
receive only control state required for ownership and the one shard credential
they need. Global relays enumerate the fixed allowlist with bounded concurrency
and fair rotation. Migration tooling receives separate least-privilege control,
source, and target credentials only for the duration of an operator action.

See [ADR 036](adr/036-physical-shard-topology.md),
[ADR 037](adr/037-control-plane-and-shard-transaction-boundary.md), and
[the failure policy](physical-shard-failure-policy.md).
