# ADR 029: PostgreSQL Monotonic Generation Fences Every Writer

- Status: Accepted
- Date: 2026-07-23

## Context

A route lookup occurs before a booking transaction begins and can become stale
before mutation. During migration, different API replicas and workers may hold
the old source route while the catalog has moved to the target. Cache expiry,
process coordination, or a Redis lock cannot close that race because none is
atomic with PostgreSQL inventory, idempotency, quota, locator, and outbox
changes.

Single-writer ownership therefore needs a durable fencing token that every
mutation proves inside its transaction. It must remain safe across process
crashes, connection reuse, stale caches, retry, cutover, and rollback.

## Decision

Use the positive monotonic assignment generation as the fencing token. A
generation never decreases or returns to an earlier value, including when
ownership returns to a previous storage.

Every storage has a local `train_run_write_fences` row for each train run it may
own. Before create, confirm, cancel, expiration, reconciliation repair if ever
explicitly enabled, or another train-run mutation can touch booking state, the
routed transaction module performs this ordered protocol:

1. begin one PostgreSQL transaction;
2. lock the public assignment row;
3. verify its storage, expected generation, catalog write enablement, and
   migration state;
4. lock the selected storage's local fence row;
5. verify the same train run and generation and `write_enabled = true`;
6. acquire the minimal public ADR 005 key claim, the local idempotency
   completion record, and required global quota locks;
7. perform booking, locator, central outbox, and target-write-evidence changes;
   confirm also closes the active global quota claim; and
8. commit all effects together.

Assignment and fence locks are held until commit or rollback. The check is not
a preflight query that can race with the mutation. A stale or fenced route
rolls back before idempotency is completed, quota is claimed, a mask changes, a
locator appears, or outbox intent is appended.

The public key claim stores only the globally unique hashed key tuple, request
fingerprint, and bounded lifecycle metadata. It carries no shard route,
generation, completed resource, or replay result. The routed local completion
record remains authoritative. Both records are created or reused inside the
same booking transaction, preserving ADR 005 uniqueness across cutover without
creating a global pre-routing result authority.

Claim and local completion receive the same `expires_at` from database time in
that transaction. The uniqueness protocol may replace an expired claim and
local completion atomically only after the deadline. Cleanup is bounded and
cannot remove either side before the local idempotency retry window. Migration
copies the completion with its original expiry and leaves the public claim
unchanged. Tests exercise concurrent reuse before and after expiry when the
train run remains in one shard and when it moves between legacy, shard-0, and
shard-1.

All booking and offering outbox intent uses the central public same-database
outbox. Cross-schema insertion remains atomic with the booking mutation and
carries at most fixed allowlisted provenance. Because the outbox is not
shard-local, migration neither copies nor switches outbox ownership.

Stable serving state has exactly one matching write-enabled fence. Drain may
allow bounded source lifecycle work while the target remains disabled.
Quiescence disables the source under the assignment/fence locks and permits a
bounded zero-writer interval. Cutover installs a newer target fence and changes
assignment while the source remains disabled. No state permits source and
target to be writable simultaneously, and migration never dual writes.

The database reports bounded internal categories such as
`shard_assignment_stale`, `shard_write_fenced`, `shard_unavailable`, and
`train_run_migrating`. The caller may refresh authority once and retry once;
there is no blind fallback. Customer errors collapse topology details into a
safe retryable response where appropriate.

Cutover also creates and locks a durable target-generation evidence row with a
zero successful-mutation count. Every successful non-replay target mutation
locks and increments that row in its booking transaction. Direct post-cutover
rollback is allowed only when the locked row remains zero. A rollback still
uses a newer generation. Any positive target-write evidence requires a reverse
migration and prevents a simple assignment flip.

A direct rollback uses one transaction that locks the assignment row, source
fence, target fence, migration row, and target-generation write-evidence row in
a fixed order. It checks absence of successful target mutation while those
locks are held, then installs a newer source generation, switches locators,
disables target, and enables source. Any evidence insert racing rollback must
serialize on the same locked evidence key; it either commits first and blocks
rollback or observes the newer fenced assignment and fails. A separate
preflight check is not sufficient.

Retained public booking tables also enforce a database guard that rejects
ordinary DML when assignment is non-legacy or the legacy fence is disabled.
Before `schema_poc` is enabled, all incompatible writers are drained and every
serving API/worker passes the configured minimum fencing-protocol version gate.
The guard protects source data after drain; the version gate prevents an old
writer from becoming valid if a later generation returns ownership to legacy.

Redis admission/cache state may carry train-run or request identity but never
the authority generation needed to bypass this protocol. Process-local locks
may reduce duplicate work but are not correctness mechanisms.

## Consequences

- Stale API replicas and workers are rejected by the same database transaction
  that would otherwise mutate inventory.
- Route caches may be short-lived performance hints without becoming trusted
  ownership state.
- Every mutation adapter must enter through the routed transaction interface;
  direct booking SQL outside it is prohibited.
- Cutover and ownership rollback advance generation, so an old writer can never
  become valid again merely because ownership returns to its storage.
- Quiescence can temporarily reject writes. This is an explicit availability
  cost and is not a zero-downtime migration claim.
- Lock ordering, timeouts, stale refresh, and target-write evidence require
  deterministic concurrency and failure tests.
- Mixed-version deployment must drain old writers before schema mode; this is a
  documented availability and rollout constraint.
- Same-cluster locks prove single-writer behavior for this logical-shard PoC;
  they are not distributed consensus or evidence for independent databases or
  regions.

## Rejected alternatives

- Route-cache TTL as protection: rejected because a write can race assignment
  change before TTL expiry.
- Process-local ownership or mutexes: rejected because replicas and workers do
  not share memory and restart loses state.
- Redis lock, lease, or Cluster slot ownership: rejected because it is not
  atomic with PostgreSQL booking commit and Redis may fail independently.
- Advisory lock without durable generation: rejected because it serializes
  contenders but does not prove that a stale caller holds current ownership.
- Check assignment before opening the booking transaction: rejected because
  cutover can occur between the check and mutation.
- Check target-write evidence before opening the rollback transaction: rejected
  because a target mutation could commit between that check and the mapping
  switch.
- Reuse or decrement a generation during rollback: rejected because delayed
  commands from the old generation could become valid again.
- Active-active source and target with conflict repair: rejected because two
  successful overlapping seat allocations cannot be repaired safely after
  confirmation.
- Dual-write migration: rejected because partial commits and divergent
  idempotency/outbox state would require a distributed transaction protocol.
