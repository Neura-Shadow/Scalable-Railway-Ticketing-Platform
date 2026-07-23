# ADR 027: Train Run Is the Booking Shard Boundary

- Status: Accepted
- Date: 2026-07-23

## Context

The current booking transaction protects seat allocation by locking and
updating `seat_inventory` for one dated train run. Reservations, reservation
seats, ticket orders, tickets, lifecycle transitions, idempotency, and booking
outbox intent all depend on that same inventory authority. Expiration and
reconciliation also converge on the train run whose segment masks are being
released or checked.

Milestone 4 needs a sharding-readiness seam that preserves those PostgreSQL
transactions while allowing selected data to move reversibly. A user may hold
or confirm travel on many runs, while many unrelated users compete for seats on
one run. A reservation ID does not exist until creation has already selected
inventory. Neither user nor reservation hashing therefore identifies the
single writer that must serialize overlapping seat allocations.

The existing account, passenger, railway-offering, hot-train policy, and
Milestone 3 projection data has different ownership and access patterns. Moving
all of it with booking state would duplicate global authority and make the
proof of concept less honest about future cross-database work.

## Decision

Use `train_run_id` as the booking shard key. Exactly one assigned storage owns
all authoritative booking writes for a train run at a time.

The assigned booking storage contains:

- `seat_inventory`;
- `reservations` and `reservation_seats`;
- train-run-associated `ticket_orders` and `tickets`;
- booking idempotency completion records;
- the local train-run write fence; and
- local reconciliation observations needed to validate that state.

Keep these concerns global in the public schema:

- users and passengers;
- stations, routes, route stops, trains, coaches, seats, train runs, and fares;
- hot-train admission policy;
- the shard catalog, assignments, and migration control state;
- reservation, ticket-order, and ticket locator indexes, with ticket-order
  locators carrying the exact owner-list fields `created_at`, `status`,
  `total_amount_minor`, and `currency`;
- the minimal booking idempotency key-claim relation that preserves ADR 005's
  `(user_id, operation, key_hash)` uniqueness without storing route or result
  authority;
- the authoritative cross-shard reservation quota-claim ledger;
- one central transactional outbox for all booking and railway-offering events,
  with only bounded allowlisted storage provenance; and
- the disposable Milestone 3 journey projection, receipts, and progress state.

Create reservation begins with `train_run_id` and resolves one assignment.
Confirm, cancel, and reservation reads begin with a reservation ID and use the
global reservation locator to recover the same train-run assignment. Ticket
and ticket-order reads use their corresponding locators. Expiration enumerates
a bounded set of authoritative shard work, and reconciliation resolves one
current route unless an explicit migration authorizes comparison with the
recorded source and target.

The global quota-claim ledger is locked and changed in the same PostgreSQL
transaction as its shard-local reservation transition. Create opens the claim;
confirm closes it, and cancel or expiration closes an active claim. This
preserves user and passenger hold limits across logical shards without leaving
confirmed reservations counted as active holds. It is intentionally identified
as a future physical-extraction constraint rather than hidden behind a false
claim of independent shard autonomy.

Booking idempotency is split deliberately. A minimal public key claim preserves
ADR 005's global key uniqueness and request-fingerprint conflict semantics. The
authoritative completion and resource reference live with the assigned booking
state. Claim, local completion, booking mutation, locator, quota transition,
and central outbox intent still commit in one PostgreSQL transaction. The claim
does not choose a route and cannot answer a replay by itself.

The global claim and its local completion share one database-time-derived
`expires_at`. The unique key can be reacquired atomically only after that time;
bounded cleanup removes neither record before the documented local retry
window. Migration preserves the local completion's exact expiry while the
global claim remains in place, so moving a train run neither extends nor
shortens key ownership.

All domain outbox rows remain in one public same-database outbox. A booking
transaction inserts its central outbox intent atomically with shard-local state.
Migration validates relevant intent but does not copy outbox rows. Optional
provenance is limited to the fixed `legacy`, `shard-0`, `shard-1`, or `global`
categories and is not required by consumers for correctness.

Do not change the established PostgreSQL segment-mask allocation predicate.
Sharding changes placement and routing, not seat-overlap semantics.

## Consequences

- All writes that can overlap on one train run remain colocated behind one
  authoritative routed-transaction interface.
- Customer ID-based operations avoid shard scans through global locators.
- Owner-scoped ticket-order listing can page the global locator index and fetch
  only the bounded routes represented by that page.
- Global identity and offering rows may remain cross-schema foreign-key targets
  in this same-cluster proof of concept.
- The global key claims, quota ledger, locator/outbox atomicity, and
  cross-schema foreign keys are explicit blockers that require new decisions
  before physical database extraction.
- Moving one train run copies its shard-local authoritative state and local
  idempotency completions. It does not copy the central outbox, public key
  claims, global journey projection, or Redis data.
- Cross-shard expiry tests must prove conflict/replay before expiry, atomic
  reacquisition only afterward, and identical behavior across migration.
- This decision proves a logical ownership seam, not unlimited shard count,
  physical failure isolation, global transactions, or production capacity.

## Rejected alternatives

- Shard by `user_id`: rejected because competing users for one train run would
  update separate inventories and require cross-shard seat coordination.
- Shard by passenger: rejected for the same inventory-splitting reason and
  because passenger identity is not the allocation aggregate.
- Hash `reservation_id`: rejected because the ID is unavailable before create
  chooses inventory and because one train run would have several writers.
- Put global identity and offering tables in every booking shard: rejected
  because it creates replicated authority and undefined update consistency.
- Move the Milestone 3 projection with booking state: rejected because search
  is global and disposable and must not fan out across booking shards.
- Use Redis waiting-room locality as the shard key: rejected because admission
  is ephemeral control state and cannot authorize PostgreSQL writes.
