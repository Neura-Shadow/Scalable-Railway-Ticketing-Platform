# ADR 033: Local Idempotency Completion and Central Same-Database Outbox

- Status: Accepted
- Date: 2026-07-23

## Context

ADR 005 gives booking commands durable uniqueness on
`(user_id, operation, key_hash)` and commits completion with the resource.
ADR 007 commits event intent with each domain mutation. Sharding must preserve
both contracts when a train run moves and when stale replicas race cutover.

A completion stored only in a global pre-routing table would become a second
result authority and could diverge from routed booking state. Completely local
keys, however, could allow the same ADR 005 tuple to be acquired independently
on two storages during migration or an operational error. Outbox rows present a
different tradeoff: all logical shards share one database, so moving existing
publication state provides no correctness benefit and creates avoidable worker
claim races.

## Decision

Split idempotency integrity from completion authority while keeping both in one
routed PostgreSQL transaction.

### Minimal global key claim

Maintain a public key-claim relation unique on ADR 005's
`(user_id, operation, key_hash)`. Store only the request fingerprint,
train-run integrity reference, database-derived `expires_at`, and bounded
lifecycle timestamps needed for acquisition and cleanup. Raw keys are never
stored or logged.

The claim contains no shard route, schema, generation, resource ID, completion
status, or replay response. It cannot select storage and cannot satisfy a
replay. Create already has a train run; confirm/cancel first resolve the
reservation locator. Only after route and fence validation does the booking
transaction insert, validate, or atomically reacquire the global claim.

### Local completion authority

Store the authoritative idempotency completion beside booking state: public
legacy completion for a legacy-assigned run and schema-local completion for a
schema-assigned run. It retains ADR 005 operation, key hash, fingerprint,
in-progress/completed state, resource type/ID, and bounded expiry metadata.
It never stores a sensitive full response.

The global claim and local completion receive the same `expires_at` from
database time in the transaction. Before expiry, same key and fingerprint
resolves the routed completion and a different fingerprint conflicts. Only
after expiry may one transaction atomically replace/reacquire both the claim
and current local completion. There is no separately committed global claim or
delete-then-insert ownership gap.

Cleanup uses an indexed, bounded, `SKIP LOCKED` workset and database time. It
does not remove the global claim earlier than the local retry window or leave a
live local completion without its claim. Cleanup failure is retryable and does
not change a resource.

Migration copies local idempotency records in deterministic batches, including
their exact fingerprint, completion/resource fields, and original `expires_at`.
The global claim is not copied and migration neither extends nor shortens its
expiry. After cutover, retry resolves the current assignment and finds the
copied target completion. Tests cover same-key replay, different-fingerprint
conflict, concurrent reuse before/after expiry, bounded cleanup, and reuse
across legacy-to-shard and shard-to-shard moves.

### Central transactional outbox

Keep one `public.outbox_events` relation for all booking and railway-offering
events. Every routed booking transaction inserts its central outbox row in the
same PostgreSQL transaction as local booking state, key claim, idempotency
completion, quota transition, locators, and target-write evidence. No Redis
write or distributed transaction participates in commit.

Event IDs remain globally unique. Booking rows carry bounded provenance needed
for operations: train run, observed assignment generation, and a storage ID
from the fixed `legacy`, `shard-0`, or `shard-1` allowlist. Global offering
events use the bounded global category. Provenance is sanitized in logs, shard
ID is bounded in metrics, high-cardinality IDs are never metric labels, and
external consumers do not depend on shard provenance for correctness.

Migration validates relevant central event intent, aggregate references,
provenance, and publication state but does not copy or relabel outbox rows. The
existing bounded claim/publish/finalize worker continues to lease from one
queue. Its at-least-once semantics, stale-lease recovery, conditional finalize,
globally unique event IDs, and consumer deduplication remain unchanged. There is
no source/target outbox ownership race.

## Consequences

- ADR 005 uniqueness survives routing changes without making a global record a
  route or result authority.
- Completion, booking state, global claim, quota, locator, and event intent
  still commit or roll back together in one database.
- Database-derived synchronized expiry prevents early cleanup or key reuse on a
  different shard.
- Copying local completion lets replay follow current ownership; leaving the
  key claim and outbox global avoids duplicate global state.
- A single central outbox retains the current bounded worker and avoids
  migration of processing leases and publication history.
- Global key claims and a central outbox are deliberate same-database
  dependencies. They do not prove that a future physical shard can commit
  global uniqueness or event intent atomically.
- Physical extraction requires a new key-allocation protocol and an outbox/
  relay topology, with explicit failure, replay, and reconciliation semantics.

## Rejected alternatives

- Store raw idempotency keys: rejected because they are replay credentials.
- Use only local key uniqueness: rejected because ADR 005's tuple could be
  acquired independently across storages.
- Store completion/result in the global claim: rejected because it would become
  a pre-routing replay authority that can diverge from current booking state.
- Commit the global claim before routing or in another transaction: rejected
  because stale or failed booking could strand command ownership.
- Give global claim and local completion different expiry: rejected because
  one shard could reuse a key while another still owns its result.
- Delete claims without bounded local-record coordination: rejected because it
  can violate the documented retry window and uniqueness contract.
- Use shard-local outbox tables in this logical PoC: rejected because the
  central same-database table already preserves atomic intent and avoids
  migration of leases/publication state. Physical extraction remains future
  work rather than being simulated.
- Use Redis for durable idempotency or outbox: rejected because Redis is not
  atomic with PostgreSQL booking state and may evict or become unavailable.
- Publish during the booking transaction: rejected because network I/O extends
  locks and still cannot make external delivery atomic.
- Add a distributed transaction coordinator: rejected because physical shards
  and cross-database transactions are explicitly outside Milestone 4.
