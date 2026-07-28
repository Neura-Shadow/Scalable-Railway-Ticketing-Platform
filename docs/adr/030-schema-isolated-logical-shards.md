# ADR 030: Use Schema-Isolated Logical Booking Shards First

- Status: Accepted
- Date: 2026-07-23

## Context

Milestone 4 must prove explicit routing, fencing, reversible migration,
locator-based reads, and multi-replica stale-route rejection while preserving
the existing atomic booking transaction. Physically separate PostgreSQL
databases would remove cross-database foreign keys and make reservation
locators, global quota claims, minimal global idempotency claims, and the
central outbox impossible to commit through the existing one-database
transaction seam without a new coordination protocol.

The proof also needs a gradual legacy path. Existing populated data must remain
authoritative until a train run is explicitly migrated, and source data must be
retained read-only after cutover. Dynamic schema selection introduces SQL
identifier risk and connection-pool state leakage unless it is isolated behind
one validated module.

## Decision

Keep one PostgreSQL cluster and database. Use these fixed schemas:

- `public` for global/control/reference/derived state and the `legacy` booking
  storage;
- `booking_shard_0` for logical booking storage `shard-0`; and
- `booking_shard_1` for logical booking storage `shard-1`.

Migration 8 creates both schemas and every required local table explicitly.
It does not use AutoMigrate, dynamic schema creation, or automatic movement of
existing bookings. It inserts fixed catalog rows and explicit generation-1
legacy assignments for populated version-7 train runs. New runs remain
explicitly legacy until an operator migrates them.

Each booking storage contains the train-run-scoped inventory, reservation,
reservation-seat, associated order/ticket, local booking idempotency completion,
and write-fence structures. Public completion rows serve legacy-assigned runs;
schema-local completion rows move with schema-assigned booking state.

A minimal public idempotency key-claim relation preserves ADR 005 uniqueness on
`(user_id, operation, key_hash)` and the request-fingerprint conflict check.
The claim is inserted or reused in the same booking transaction, but stores no
route, generation, resource result, or replay response. The current routed
local completion is the only result authority, and migration copies that local
completion rather than the public claim.

The claim and local completion use one database-time-derived `expires_at`.
Before expiry, the tuple remains owned and bounded cleanup cannot remove either
record earlier than ADR 005's local retry window. After expiry, reacquisition is
one atomic transaction rather than delete-then-insert steps. Migration retains
the completion's exact expiry and does not rewrite the global claim. Real
PostgreSQL tests cover same-key/same-request replay, different-fingerprint
conflict, concurrent expiry reacquisition, cleanup bounds, and key reuse after
legacy-to-shard and shard-to-shard moves.

All booking and railway-offering events use one public central outbox. Inserts
from a schema-local booking mutation remain atomic because this is one database.
Rows may carry only bounded fixed storage provenance for operations; consumers
do not depend on it for correctness. The outbox is validated during migration
but never copied or switched between schemas.

Keep users, passengers, railway reference data, train runs, hot-train policy,
catalog/migration state, reservation/order/ticket locators, the minimal
idempotency key claims, authoritative quota-claim ledger, central outbox, and
the Milestone 3 projection in `public`. Cross-schema foreign keys to global
identity/reference data are permitted where they preserve current validation.
Global claims, locators, quota transitions, and outbox intent commit in the same
PostgreSQL transaction as shard-local booking state. Confirm closes the active
quota claim; cancel and expiration close one when present.

The ticket-order locator contains `created_at`, `status`,
`total_amount_minor`, and `currency`, and is updated atomically with order
lifecycle. Together with an `(owner_user_id, created_at, ticket_order_id)`
index, it supplies the exact stable global owner-list contract before only the
bounded page's routes are consulted.

Hide storage selection behind the routed transaction module. Its interface
accepts only a validated fixed shard handle. The implementation maps that
handle through an internal allowlist and applies the corresponding
transaction-local `search_path` before any storage-relative repository query.
The HTTP and application modules never receive schema names, and individual
repositories do not interpolate identifiers.

The `search_path` value cannot come from HTTP input, JWT claims, Redis, arbitrary
configuration, or an unvalidated catalog field. Tests must prove:

- every fixed handle resolves to the intended schema and no other identifier;
- malicious or unknown identifiers are rejected before SQL execution;
- the setting is local to the current transaction;
- commit and rollback both restore pooled-connection behavior;
- concurrent transactions on one pool cannot observe each other's schema; and
- trigger/function resolution remains fixed to the intended local and public
  relations.

Schema-local triggers and functions are installed with explicit safe relation
resolution. Mutable session-wide `search_path` and caller-managed reset are
prohibited.

Retained public booking tables install database guards that derive the affected
train run and reject ordinary DML unless its assignment is `legacy` and the
legacy fence is write-enabled. Migration copy reads the source; explicitly
authorized cleanup follows its own assignment/fence/window checks. Schema mode
is not enabled until incompatible pre-fencing APIs and workers are drained and
every serving writer meets the configured minimum fencing-protocol version.

Migration uses a bounded quiesced copy from one storage to another, never dual
writes. It copies local completion idempotency but not the central outbox or
minimal public key claims. Same-database transactions permit atomic assignment,
fence, locator, quota, outbox, and cutover updates. Source tables remain
available but fenced and read-only throughout the rollback window; cleanup is
a separate explicitly confirmed operation.

Reservation, order, and ticket locators are indexed by train run. Cutover
preflight counts all affected locator rows through those indexes and enforces a
configured maximum. The atomic locator/assignment/fence switch runs with a
bounded statement timeout; cap or timeout failure rolls back the entire switch.

## Consequences

- Existing PostgreSQL atomicity is preserved while routing and ownership become
  explicit and independently testable through one deep transaction interface.
- Cross-schema foreign keys, global locators, and global quota claims make the
  PoC reversible and keep current integrity checks local to one database.
- Minimal global key claims and the central outbox preserve ADR 005 and ADR 007
  semantics without becoming routing authorities or migration copy targets.
- The public legacy path and two opt-in schemas allow gradual per-train-run
  migration without destructive bulk movement.
- `search_path` complexity and identifier safety have locality in one module
  instead of being repeated across booking repositories.
- The design consumes extra schema objects and disk during source retention and
  requires schema-aware migrations, backups, reconciliation, legacy guards,
  and a mixed-version deployment gate.
- Both logical shards share connections, storage engine, primary availability,
  and physical failure domain. The bounded harness disables a catalog route;
  it is not schema, process, host, disk, network, or physical-shard isolation.
- Cross-schema foreign keys, global key/locator/quota/outbox atomicity, relay
  discovery, and control-plane availability must be redesigned or coordinated
  before extracting separate databases.
- This decision is a single-region logical-sharding proof of concept, not
  physical shard completion, online zero-downtime rebalancing, distributed
  transactions, or production-capacity evidence.

## Rejected alternatives

- Separate PostgreSQL databases now: rejected because cross-database foreign
  keys, locator and quota atomicity, idempotency/outbox commit, failure recovery,
  and distributed-transaction assumptions are unresolved.
- One dynamically named schema per train run: rejected because migrations,
  identifiers, metrics, fanout, and operational state would be unbounded.
- Copy global identity, offering, or projection tables into each shard:
  rejected because it creates replicated authority and unrelated migration
  work.
- Direct schema-qualified SQL assembled throughout repositories: rejected
  because identifier selection and reset rules would spread across many
  callers, reducing locality and making injection review harder.
- Session-wide mutable `search_path`: rejected because pooled connections can
  leak routing state across requests.
- Redis as a shard store or ownership directory: rejected because eviction,
  partitions, and non-atomicity with PostgreSQL cannot protect booking writes.
- Dual-write source and target during migration: rejected because one-sided
  commits would corrupt masks, idempotency, quota, locators, or outbox state.
- Shard-local outbox tables in this PoC: rejected because one central
  same-database outbox already preserves transactional intent, avoids copying
  publication state during migration, and keeps the existing bounded relay;
  future physical extraction still requires a new outbox topology decision.
