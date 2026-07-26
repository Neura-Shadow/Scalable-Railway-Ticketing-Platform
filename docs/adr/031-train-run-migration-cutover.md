# ADR 031: Train-Run Migration Uses Bounded Quiesced Cutover

- Status: Accepted
- Date: 2026-07-23

## Context

Moving a train run changes the storage that owns its seat masks,
reservations, order and ticket lifecycle, and idempotency completions. Copying
while source writes continue would produce a target assembled from different
points in time. Writing source and target together would add partial-commit and
replay failure modes that the current one-database booking transaction does not
solve.

Cutover also changes every resource locator used by ID-based reads and commands.
An unbounded locator update could hold catalog and booking locks indefinitely.
A mapping switch performed separately from locator or fence changes could make
the system route to incomplete state. Finally, returning to the source after
the target has accepted a write would discard committed target state.

## Decision

Represent each move in a durable `train_run_shard_migrations` state machine.
Only one active migration exists per train run. The states and allowed forward
path are:

1. `planned` -> `draining`;
2. `draining` -> `copying` after bounded quiescence;
3. `copying` -> `validating` after all deterministic copy groups complete;
4. `validating` -> `cutover_ready` only after every required invariant passes;
5. `cutover_ready` -> `cutting_over` inside the cutover transaction;
6. `cutting_over` -> `rollback_window` on commit;
7. `rollback_window` -> `completed` after the retained-source window; and
8. any explicitly recoverable phase -> `failed` or the appropriate
   `rolled_back` outcome under its rollback rules.

Invalid transitions fail closed. A migration stores source and target shard,
source and reserved target generation, bounded copy cursor and counts,
timestamps, and a normalized error category. It never stores DSNs,
credentials, SQL text, stack traces, request payloads, or PII.

### Plan and drain

Planning locks and verifies the current assignment, source generation, target
catalog state and health, absence of another active migration, and a strictly
newer target generation. The target remains write-disabled.

Drain marks the assignment accordingly and rejects new reservation creation
with a retryable response. Existing confirm, cancel, and expire work may
continue on the source only for the documented bounded drain interval. An
admission attempt rejected by migration does not complete idempotency, mutate
quota, or permanently consume its token or processing lease.

### Quiesce and copy

Every normal mutation locks the assignment and local fence. Quiescence takes
those same locks, disables the source fence, and therefore waits for preceding
mutations to finish before it can commit the no-writer state. It uses bounded
database lock and statement timeouts, not sleeps or process-local observation.
Source and target may both be disabled during this phase; they are never both
write-enabled.

After quiescence, copy the train-run-local data in deterministic primary-key
order with configured batch size, documented transaction groups, an inclusive
or exclusive cursor contract, and durable progress after each committed batch.
Copied state includes inventory, reservations, reservation seats, associated
orders and tickets, local idempotency completions with their original expiry,
and required local fence/reconciliation state.

Public quota claims, minimal global idempotency key claims, reservation/order/
ticket locators, and the central public outbox are validated but never copied.
There is no source/target dual write. A failed batch leaves source authority
unchanged and target partial state unroutable; resume repeats or continues a
batch idempotently without duplicate corruption.

### Validate and preflight

Validation compares source and target row identities and counts, foreign-key
relationships, reservation state counts, exact segment masks, active quota
claims, local idempotency fingerprints/results/expiry, order-ticket links,
central outbox intent and bounded provenance, locator coverage, and detect-only
reconciliation results. A mismatch prevents `cutover_ready`.

All three locator tables have a supporting `train_run_id` index. Before opening
the cutover transaction, an indexed count establishes the complete affected
locator set and must not exceed the configured cutover row cap. The ticket-order
owner-page index is validated separately. Cutover uses a configured statement
timeout. Exceeding the cap, missing an index, or timing out is a bounded failure
that leaves source assignment/fence and all locators unchanged; operators must
re-plan rather than accept a partial switch.

### Atomic cutover

In one PostgreSQL transaction, cutover:

1. locks the migration, assignment, and target shard-catalog row;
2. revalidates source generation, target write eligibility and fencing-
   protocol compatibility, completed copy/validation, locator count, and the
   cutover deadline;
3. locks bounded locator rows, the source fence, and then the target fence;
4. creates and locks the target-generation write-evidence row at zero;
5. installs the reserved target generation and enabled target fence while
   retaining the disabled source fence;
6. updates reservation, ticket-order, and ticket locators through their
   train-run indexes;
7. switches the assignment to target and enters `rollback_window`;
8. records central cutover/invalidation outbox intent; the committed assignment
   generation immediately invalidates old availability envelopes and the
   worker rotates the disposable Redis namespace after commit; and
9. commits all routing, fencing, locator, and invalidation effects together.

The target accepts writes only after this commit. A stale replica remains
unable to write because its source fence is disabled and its expected
generation no longer matches. Source rows remain read-only for the rollback
window; cleanup is a later explicitly confirmed operation and is never an
automatic cutover side effect.

### Rollback

Before cutover, rollback keeps the source assignment, removes or retains
unroutable target copy state, re-enables the source fence with the current
generation, and records the outcome. No routing switch or data loss occurs.

Cutover pre-creates a target-generation evidence row. Every successful
non-replay target mutation locks and increments it in the same booking
transaction. A direct post-cutover rollback is permitted only in one
transaction that locks migration, assignment, the destination shard-catalog
row, the indexed bounded locator set, both fences, and the zero-valued evidence
row in that order. It atomically revalidates destination protocol compatibility
and write eligibility, installs a newer source generation, switches locators,
enables source, disables target, and records the audit outcome.

If target-write evidence is positive, direct mapping reversal is forbidden.
Recovery requires a new reverse migration from target to source or another
shard, a newer generation, complete bounded copy and validation, and another
quiesced cutover. Generation never decreases.

## Consequences

- The migration has a bounded interval with zero writers and therefore does not
  claim zero downtime or disruption-free online rebalancing.
- Source remains the only authority until an atomic cutover commit; partial
  target copies are never routable.
- Deterministic batches and durable cursors make copy restartable and auditable.
- Indexed locator preflight, a hard row cap, and statement timeout bound the
  atomic cutover transaction at the cost of rejecting oversized moves.
- Central claims and outbox state avoid copy ambiguity but remain same-database
  dependencies that block direct physical extraction.
- Retained source data increases disk use and backup scope during the rollback
  window.
- A successful target write deliberately removes the simple rollback option;
  preserving committed state is more important than a fast mapping reversal.
- This proves same-cluster logical ownership transfer, not physical shard
  failover, distributed consensus, or production migration capacity.

## Rejected alternatives

- Dual-write source and target: rejected because mask, locator, quota,
  idempotency, and event state can commit on only one side.
- Copy while source writes continue without a change stream: rejected because
  validation would not describe one consistent source state.
- Promote a partially copied target: rejected because row counts alone cannot
  establish booking invariants.
- Update assignment before locators or fences: rejected because observers could
  route to incomplete or simultaneously writable state.
- Update locators in unbounded batches after cutover: rejected because customer
  ID routes would be partially switched.
- Sleep for presumed quiescence: rejected because process timing cannot prove
  that PostgreSQL transactions have finished.
- Delete source at cutover: rejected because it removes bounded rollback and
  audit evidence.
- Flip directly back after a target mutation: rejected because it would discard
  committed target state; reverse migration is required.
