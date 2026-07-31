# ADR 037: Control Plane and Booking Shards Never Share a Transaction

- Status: Accepted for Milestone 5 implementation
- Date: 2026-07-29

## Context

Milestone 4 deliberately uses one routed PostgreSQL transaction to lock the
public assignment, validate a schema-local fence, mutate inventory and booking
rows, update global idempotency and quota state, maintain locators, append the
central outbox, and record generation-write evidence. That interface is safe
only because every schema shares one transaction manager.

Independent PostgreSQL instances remove cross-schema foreign keys, common row
locks, and atomic commit. Hiding remote control writes behind the existing
routed-transaction interface would make a partial commit look atomic. XA,
two-phase commit, source/target dual writes, and a generic distributed
transaction coordinator are outside the pilot and would add blocking and
coordinator-recovery semantics without removing the need for explicit repair.

## Decision

No transaction spans control PostgreSQL and a physical booking PostgreSQL, or
two physical booking PostgreSQL instances. There are no cross-database foreign
keys. A callback, repository helper, trigger, or database function running in
one transaction must not query or mutate another database.

The control transaction owns control facts only: current authentication and
passenger ownership validation, assignment resolution, booking-command
reservation, conservative quota leasing, globally unique resource-ID
reservation, pending directory state, migration checkpoints, and control
outbox intent.

The shard transaction owns booking authority only: validation of its local
storage identity, expected monotonic generation, write fence, migration
permission, and booking snapshots; shard-local command receipt and idempotency
execution; seat-mask allocation; reservation/order/ticket lifecycle; local
outbox intent; target-write evidence; and mutation-journal capture.

Callers pass immutable command data and an expected opaque route to exactly one
approved shard adapter. The shard transaction validates the expected generation
against its database-local fence. It does not query control assignment state to
make the commit valid. A stale router therefore fails at the selected database
before inventory, receipt, outbox, or journal mutation.

Likewise, a control transaction does not synchronously query a physical shard.
Durable shard receipts and separately bounded observations are consumed before
or after a control transaction and are revalidated while finalizing. Control
state may be conservatively pending while an outcome is unknown.

All booking-critical reference data needed to decide a shard mutation is local
and versioned as decided in ADR 040. Global IDs are application-generated and
collision-safe across databases; no shared PostgreSQL sequence is required.

The legacy and logical-schema adapters may retain their established
same-database implementation, but their shared transaction is an adapter detail.
The physical adapter exposes a local shard transaction only. Higher-level
booking coordination uses the durable saga in ADR 038 for every storage kind so
that physical failure modes are explicit and testable.

Event delivery and repair are at least once. The system does not claim an
exactly-once distributed transaction. Idempotent command and event identities,
local receipts, state transitions, and reconciliation make repeated work
converge without repeating authoritative seat mutation.

## Invariants

- Exactly one physical shard is write-enabled for a train run in a stable
  serving state; a migration may temporarily have zero writers, never two.
- A shard commit is authoritative for booking state even if control
  finalization is delayed.
- Control state never treats an unknown shard outcome as proven failure.
- No cache, Redis value, process-local lock, or control-only observation can
  bypass a shard-local generation fence.
- No distributed rollback attempts to undo a committed seat mutation. Recovery
  finalizes or explicitly compensates only control state according to proven
  shard evidence.
- No cross-database foreign key, join, or hidden synchronous query is required
  for booking correctness.

## Consequences

- Partial failure becomes an explicit normal state with durable recovery rather
  than an exception hidden behind a transaction abstraction.
- Shard transactions remain short, local, and authoritative for seat inventory.
- Control commands, quota, and directory state may temporarily over-reserve or
  remain pending, causing safe false rejection.
- The physical adapter cannot reuse the Milestone 4 `RoutedTx` interface as if
  it still owned assignment, fence, control indexes, and booking state in one
  commit.
- Integration tests must use independent databases and inject failures before
  and after each durable commit.
- The decision preserves the modular monolith and does not imply microservices,
  global serializability, or production RPO/RTO beyond the bounded pilot.

## Rejected alternatives

- Preserve the routed transaction and perform hidden remote writes: rejected
  because its interface would make partial commits indistinguishable from
  atomic success.
- XA or PostgreSQL prepared transactions: rejected because blocking,
  coordinator loss, recovery ownership, and operational evidence are not
  justified for the pilot.
- Source-and-target dual writes: rejected because one-sided commits can diverge
  masks, idempotency, receipts, outbox, and lifecycle state.
- Query control inside a shard transaction: rejected because control failure or
  latency would enter the shard's commit path and still provide no common
  transaction.
- Query a shard inside a control transaction: rejected because a remote timeout
  cannot determine whether the shard committed and would hold control locks
  across an independent failure domain.
