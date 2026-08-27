# High-Concurrency Design

## Correctness target

The platform must prevent overlapping active reservations for the same seat and train run while allowing that seat to be reused on non-overlapping route segments. A multi-passenger request receives all seats or none.

For an enabled hot policy, the platform must also prevent an uncontrolled
request burst from entering the booking transaction. Admission permits one
attempt and does not guarantee inventory. Redis orders and bounds attempts;
PostgreSQL remains the only seat and durable-quota authority.

## Serialization points

- Train-run row: compatible shared locks allow holds to proceed concurrently while still serializing them against operator status updates.
- Passenger row: deterministic locks serialize the one-active-reservation-per-passenger/train-run rule.
- Seat-inventory row: serializes occupancy changes for one physical seat and train run.
- Reservation row: serializes confirm, cancel, and expire.
- Idempotency unique key/row: serializes retries of one customer command.
- Outbox row: serializes claim/finalize ownership.
- Redis policy generation: version and continuity latch fail closed on missing or stale hot-run control state.
- Redis waiting-room Lua scripts: atomically serialize duplicate join, monotonic sequence, queue capacity, global issue rate, inflight count, and token leases across replicas.
- Booking per-user advisory transaction lock: serializes authoritative held-reservation quota counts.
- API execution slot: provides a non-blocking local instance bound and never becomes a queue.
- Physical booking command row and conservative quota lease: serialize one
  cross-database intent before any shard-local allocation.
- Physical shard command receipt: serializes retry execution by globally unique
  command ID and fingerprint in the same local transaction as the seat change.
- Physical train-run fence: rejects stale routes and wrong-database writers at
  the shard-local transaction boundary.
- Operator command identity and optimistic source version: serialize one
  bounded booking-state change across retries before its local receipt commits.
- Payment operation row and provider idempotency hash: serialize one checkout,
  capture, void, or full-refund identity while the provider call runs outside
  every database transaction.
- Payment saga/inbox leases: `FOR UPDATE SKIP LOCKED` assigns bounded work;
  expired pre-call claims return to pending, while expired in-flight financial
  calls become uncertain and must be queried before retry.
- Shard-local payment/issuance/refund/compensation receipts: serialize stable
  command replay in the same fenced transaction as reservation, ticket, outbox,
  and exact inventory mutation.

## Allocation algorithm

Create-hold uses one Read Committed transaction. It locks the idempotency record, takes a shared train-run lock, and locks canonical owned passenger rows before reading fare and mutating inventory. It rejects a passenger already present in a held/confirmed reservation on that run. A data-modifying CTE selects candidates in `(coach_number, seat_number, seat_id)` order with `FOR UPDATE OF seat_inventory SKIP LOCKED`. The update applies `occupied_segments | requested_mask` only to active-seat candidates whose equal-length intersection is zero.

The returned row count must exactly equal passenger count. Any shortfall returns a bounded availability conflict by rolling back the transaction. Passenger-to-seat assignment follows returned deterministic seat order and canonical passenger order.

`SKIP LOCKED` avoids waiting behind seats another request is deciding. It can conservatively reject a request when enough seats are temporarily locked; it cannot partially commit. This tradeoff is measured under the hot-train k6 scenario.

The core VARBIT allocation SQL is unchanged by admission. For hot requests, a
completed durable idempotency replay is checked first; otherwise the current
policy and admission token are validated, the token is acquired, and a local
execution slot is obtained before entering the existing transaction. Inside
that transaction, durable idempotency acquisition precedes the per-user quota
lock/count and existing booking locks. PostgreSQL commit precedes token
finalization.

## Lock ordering

All commands use the ordering in ADR 002. Multi-row inventory operations sort by seat ID before mutation. Outbox workers order by ready time and ID. Lock acquisition never depends on untrusted sort input.

## Isolation and retry

Atomic predicates and row locks are sufficient at Read Committed. The application retries only PostgreSQL `40001` and `40P01`, with a small configured limit, context cancellation, and jitter. It never retries inventory conflicts, invalid states, authorization failures, or changed idempotency fingerprints.

## Deterministic concurrency tests

Tests coordinate starts with barriers/channels and expose transaction hooks at adapter seams. They do not assert timing using sleeps.

Required cases:

1. One seat and 100 overlapping attempts: one success.
2. Ten seats and 100 overlapping attempts: ten successes.
3. Same seat on non-overlapping A-C/C-D intervals: both may succeed; overlapping A-C/B-D may not.
4. Multi-passenger shortfall: no seat remains allocated.
5. Multiple expirer workers: one terminal transition/release/event.
6. Confirm versus expire: one valid state and consistent occupancy.
7. Same idempotency key: one resource.
8. Cancel versus confirm: valid terminal outcome with no leak/double release.
9. Train-run cancellation versus hold: no hold after non-bookable status is authoritative.
10. One user and one admission fingerprint under 100 concurrent joins: one active entry and one sequence.
11. Mismatched concurrent joins: one active entry and bounded conflicts.
12. Three admission workers: no double issue and no global rate/inflight breach.
13. One token under 100 same-key submissions: one durable reservation and stable replay.
14. Wrong owner or request: token rejected before inventory mutation.
15. One user under 100 quota attempts: configured held bounds are never exceeded.
16. API/worker termination and Redis-finalize failure: transaction is atomic, lease recovery is bounded, and durable replay prevents duplication.
17. One payment-intent key under 100 concurrent API requests: one intent, saga,
    and begin-payment receipt.
18. Duplicate/out-of-order webhooks and capture response loss: one capture,
    query-before-retry, one issuance receipt, and one ticket per seat.
19. Concurrent void/refund/issuance/compensation replays: no duplicate provider
    effect, ticket, receipt, outbox event, or seat release.
20. Payment during physical cutover/reverse: stale generations reject before
    effect and all v2 payment state/receipts survive on the current authority.

Critical cases run repeatedly and under the Go race detector. Reconciliation follows every database concurrency suite.

For the Milestone 5 pilot, concurrent requests on different physical shards
still serialize their global quota acquisition in the control database. The
shard transaction then validates only local snapshots, fence generation, the
unique command receipt, and deterministic VARBIT rows; it performs no control-
database read. A shard commit followed by control finalization failure leaves a
conservative pending quota and directory entry that retry/reconciliation can
finalize without touching inventory. This deliberately trades availability for
no quota undercount and no duplicate seat mutation.

Physical operator mutations follow the same single-writer rule. Their control
ledger fixes the route and generation; the shard transaction locks the matching
fence and snapshot row, rejects a stale source version, and records the result
version with its receipt. The final control projection uses the same expected
version, so a concurrent operator update cannot silently overwrite it.

## Deadlock controls

- One documented order across all lifecycle commands.
- Stable multi-row ordering before locks.
- Short transactions with no network publication or Redis writes inside.
- Small worker batches.
- Bounded deadlock retry and metrics by bounded reason.
- Database statement/lock timeouts configured below request deadlines.

## Honest performance scope

Milestone 2 measures local/single-region behavior only. FIFO fairness is scoped
to one policy generation in one Redis deployment. The system does not claim
global fairness, guaranteed post-admission inventory, national-scale
throughput, unlimited horizontal write scaling, or multi-region active-active
writes. No accepted sustained Milestone 2 benchmark is currently recorded.

## Milestone 3 read concurrency

Public station, search, and availability reads resolve exact random-generation
Redis keys. Identical local misses are coalesced by exact-key singleflight;
unrelated keys continue independently. Shared Redis lets a fill from one API
replica serve another, while TTL jitter spreads ordinary expiry. No distributed
cache lock or PostgreSQL/Redis transaction is introduced.

Search reads the disposable journey projection and uses the normalized source
query as a safe fallback. Availability batches use one PostgreSQL query for
cache misses, avoiding a per-result N+1 loop. Redis loss therefore raises
database read pressure but does not change the booking lock order or VARBIT
allocator. Enabled-hot admission retains its existing fail-closed behavior.

Projection replacement is one transaction per train run. Concurrent readers
observe an old complete or new complete committed set. Event receipts make
multiple worker replicas idempotent; pending claims, attempts, impact size, and
DLQ length are bounded. These mechanisms are correctness and amplification
bounds, not sustained capacity evidence. No accepted Milestone 3 benchmark is
recorded.

## Milestone 4 routed-write concurrency

Milestone 4 adds an outer serialization boundary without changing the VARBIT
allocation predicate. Before any idempotency completion, quota transition,
inventory mutation, locator write, or central outbox append, a routed booking
transaction locks the public train-run assignment and the selected storage's
local fence. It verifies the fixed shard ID, positive expected generation,
catalog write policy, migration state, and enabled matching fence under those
locks.

An ownership operation uses this global order:

1. migration/control row;
2. train-run assignment;
3. destination shard-catalog row, including atomic write-eligibility and
   fencing-protocol compatibility revalidation;
4. bounded locator rows in fixed table and primary-key order;
5. current-owner fence, then destination fence in the shared fixed role order;
6. target-generation write-evidence row when applicable.

A normal booking transaction instead locks assignment, the active local fence,
global idempotency-key/per-user quota serialization, and then the established
train-run, passenger, reservation, and inventory rows. It never locks migration
or locator control rows or an inactive fence. Customer writes therefore cannot
create a reverse cycle with cutover or rollback.

Normal mutations hold assignment and fence locks through commit. Quiescence
takes the same locks and disables the source, so it waits for preceding writers
through PostgreSQL serialization instead of sleep-based observation. A stable
state has one writer; the bounded copy/cutover interval may have zero writers;
no valid state has two.

A stale replica may refresh once and retry once after
`shard_assignment_stale`. It never probes multiple storages. Cutover creates a
zero-valued target-generation evidence row, and every successful non-replay
target mutation increments it in the same transaction. Direct rollback locks
that row together with assignment and both fences; a racing first target write
either commits evidence first and blocks rollback or observes the newer fenced
route and fails.

Required deterministic evidence extends the earlier suite with:

1. three replicas caching an old generation while cutover commits;
2. 100 concurrent routed-transaction/fencing attempts reject stale authority;
3. a separate full-booking barrier drives 100 concurrent `CreateHold` commands
   from distinct users through that assignment change, requires 100 stale-route
   refreshes, exactly one target reservation for one seat, no cross-storage
   duplicate or overlap, and no source booking mutation;
4. source/target fences never simultaneously enabled;
5. commit and rollback of routed transactions never leak `search_path` through
   the pool;
6. copy failure and retry preserve one source authority and an unroutable
   partial target;
7. locator cap/timeout or cutover failure exposes no partial switch;
8. direct rollback racing the first target mutation preserves committed state;
   and
9. logical-shard worker failure does not starve bounded healthy work.

The added assignment/fence locks and routing queries have measurable overhead,
and quiesced cutover has a measurable retryable interruption. Both remain
pending until controlled Milestone 4 runtime evidence is accepted. Logical
schema results are not physical-shard or production-capacity evidence.

## Milestone 7 concurrency boundaries

Regional authority adds a database-local lock before every control mutation and
before each shard's existing train-run generation fence. The order is regional
authority, assignment/catalog ownership, train-run fence, then existing domain
rows. A stale, passive, or recovery process fails before domain DML; this is
defense in depth and never replaces external fencing of an isolated old primary.

Payment action attempts are scoped to stable action identities instead of a
shared saga counter. One provider query or earlier shard step cannot exhaust a
later ticket-issuance retry budget. Mutating provider 5xx outcomes are uncertain
and query-before-retry, and one shared observation evaluator rejects
contradictory capture/refund totals before tickets, seats, or ledger postings.

Ledger event identity, settlement cursor pages, refund request keys, shard
commands, and receipt identities are exact-replay boundaries. Importers perform
provider I/O outside transactions, then atomically persist one bounded page and
its cursor. Complete-ticket refunds lock authoritative ownership, fare, prior
refund totals, regional authority, and shard generation; concurrent duplicate
requests replay or conflict without double provider, ticket, seat, or ledger
effects. These properties are correctness and amplification bounds, not a
sustained throughput or production-capacity result.
