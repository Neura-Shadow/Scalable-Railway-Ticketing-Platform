# ADR 032: Global Locators Route Resource Reads Without Shard Scans

- Status: Accepted
- Date: 2026-07-23

## Context

Reservation creation starts with `train_run_id`, but later reservation,
ticket-order, and ticket requests start with resource IDs. Hashing those IDs
does not recover a migrated train run's current storage, and probing legacy plus
every logical shard would make customer latency and database work grow with the
topology. It would also multiply authorization and failure-disclosure risks.

Owner-scoped ticket-order listing has an existing exact global ordering,
pagination, count, status, amount, currency, and creation-time contract. A
bounded page cannot be reconstructed correctly by taking unrelated pages from
each shard and merging them without a global index.

## Decision

Maintain three durable public locator relations:

- a reservation locator keyed uniquely by reservation ID, carrying train run,
  fixed shard ID, assignment generation, owner user ID, and timestamps;
- a ticket-order locator keyed uniquely by order ID, carrying reservation and
  train-run IDs, fixed shard ID, assignment generation, owner user ID,
  `created_at`, `status`, `total_amount_minor`, `currency`, and update time; and
- a ticket locator keyed uniquely by ticket ID, carrying its order,
  reservation, train run, fixed shard ID, assignment generation, owner user ID,
  and timestamps.

Locator shard IDs reference the fixed catalog and their train run references an
explicit assignment. They never contain a schema name or DSN. Migration 8
bootstraps locators for populated legacy resources and validates completeness
before `schema_poc` can serve ID-based operations.

### Atomic creation and maintenance

Create reservation inserts the reservation and its locator in the same
PostgreSQL transaction after route/fence validation. Confirm inserts the local
ticket order and tickets plus every corresponding public locator in its routed
transaction; it also closes the active global quota claim and appends central
outbox intent before commit. Cancellation updates local order state and the
ticket-order locator's status atomically when applicable.

The ticket-order locator summary is part of the exact global owner-list
contract, not a best-effort cache. Every lifecycle transition that changes a
listed field updates it in the resource transaction. Detect-only reconciliation
compares locator metadata with authoritative local state.

### ID-based routing and authorization

A reservation-ID command or read first resolves one locator, verifies the
locator owner against the authenticated caller, and resolves its train-run
assignment. The routed transaction then locks and rechecks assignment/fence and
rechecks owner on the authoritative local reservation. Locator ownership is an
early rejection aid, never the final authorization authority.

Ticket-order and ticket reads follow their locators to the same routed storage.
The ticket locator provides indirection through ticket order and reservation so
event reload and customer reads do not infer a shard from ticket ID. The local
resource and current authenticated owner are rechecked before protected data is
returned.

A cached locator with an old generation triggers one authoritative assignment
refresh. Missing or malformed locator state fails safely and records a bounded
reconciliation category; it never triggers a scan. Public responses do not
expose shard ID, generation, schema, or whether a locator was stale.

### Owner-scoped list and bounded reads

The ticket-order locator has an index equivalent to owner ID, descending
creation time, and deterministic order-ID tie-break. A count and bounded page
from this index provide exact global ordering, pagination, total, status,
amount, currency, and creation time. Only resource IDs in that page may be
grouped by their allowlisted route and fetched under configured concurrency and
per-shard/global timeouts when ticket detail or authoritative verification is
required.

Customer hot paths never enumerate enabled shards. Operator summaries may use
bounded allowlisted fanout and must distinguish `complete`, `partial`, and
`unavailable` results. Partial customer data is never labeled complete.

### Migration cutover

Reservation, ticket-order, and ticket locator tables have `train_run_id`
indexes. Validation proves the source resources and current locator set agree.
Cutover preflights the indexed affected-row count against a configured hard cap
and verifies required indexes before it starts.

The cutover transaction locks migration, assignment, both fences, and the
bounded locator set; updates every locator to the target and new generation;
and switches assignment/fences atomically under a statement timeout. If the
count changes unexpectedly, the cap is exceeded, an index is absent, or the
statement times out, the whole transaction rolls back. Locators are never
updated during partial copy and no externally visible state has a mixed
source/target locator set.

A permitted direct rollback applies the same locks, row cap, indexes, and
timeout and changes locators with a newer source generation only after the
target-write evidence is proven zero in that transaction. A reverse migration
uses the normal cutover protocol.

## Consequences

- Ordinary reservation, ticket-order, and ticket operations perform one global
  locator lookup and one routed storage access rather than a shard scan.
- Owner authorization is enforced both at the locator seam and against the
  authoritative resource, preventing locator tampering from granting access.
- Ticket-order locator rows duplicate a small set of list fields and must be
  updated and reconciled transactionally.
- The exact global owner-list contract remains stable with bounded page work.
- Atomic locator creation and cutover are available because logical shards
  share one PostgreSQL database.
- Locator indexes and the cutover cap add storage and operational limits but
  prevent unbounded assignment-lock duration.
- Global locators are an explicit physical-shard extraction constraint; a
  future multi-database design needs a new atomicity and repair protocol.

## Rejected alternatives

- Scan every shard on a missing locator: rejected because cost and failure
  exposure grow with topology and a scan can hide corrupted control state.
- Hash reservation, order, or ticket ID: rejected because migration changes
  ownership and creation already follows train-run inventory.
- Trust locator owner without local recheck: rejected because a stale or
  tampered index must not bypass authoritative authorization.
- Put only shard IDs in customer tokens or URLs: rejected because routing would
  become user-controlled and expose topology.
- Merge independent per-shard owner-list pages: rejected because it cannot
  preserve exact global ordering, total count, or stable pagination cheaply.
- Treat ticket-order locator metadata as an eventually consistent cache:
  rejected because the existing exact list contract would return stale status
  or money values.
- Switch locators before or after assignment in another transaction: rejected
  because readers could route to the wrong authority.
