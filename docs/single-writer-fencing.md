# Single-Writer Fencing

## Invariant

For each train run:

- a stable serving state has exactly one write-enabled storage fence;
- a documented migration phase may have zero write-enabled fences;
- no valid state has more than one write-enabled fence; and
- the assignment generation is positive, monotonic, and never reused or
  decreased, including rollback to an earlier storage.

The generation is the fencing token. A route lookup, cache entry, process lock,
or Redis lease is insufficient because it cannot commit atomically with seat
inventory and reservation state.

## Mutation protocol

Every create, confirm, cancel, expiration, and other train-run booking mutation
uses this lock and validation order inside one PostgreSQL transaction:

1. Lock `public.train_run_shard_assignments` for the train run.
2. Verify the expected shard and assignment generation.
3. Verify catalog enablement, catalog write enablement, and assignment/migration
   state allow the operation.
4. Lock the selected storage's `train_run_write_fences` row.
5. Verify its train run, generation, and `write_enabled` flag.
6. Acquire global idempotency-key and quota locks, then local completion and
   booking locks.
7. Mutate inventory/lifecycle state, maintain locators, append central outbox
   intent, and record target-generation write evidence when required.
8. Commit all effects, or roll back all effects on any rejection.

The assignment and fence locks remain held until transaction completion.
Fence validation is not a separate preflight query.

## Stale writers

A stale API replica or worker receives a bounded internal stale/fenced result
before it can complete idempotency, claim quota, change a segment mask, insert
a locator, append outbox intent, or record a successful target write. It may
refresh authority once and retry once. Repeated staleness fails closed.

Retained `public` booking tables require database guards as a second boundary:
ordinary DML is rejected when the run is no longer assigned to `legacy` or the
legacy fence is disabled. Before schema mode is enabled, all pre-fencing writer
binaries must be drained and every serving writer must satisfy the catalog's
minimum fencing-protocol version. A table guard alone cannot make an old binary
carry an expected generation when ownership later returns to legacy.

The only exceptional retained-public mutations are reviewed migration copy or
source cleanup transactions. They use an internal transaction-local migration
or cleanup ID, and the database revalidates the corresponding migration state,
authority, fence, and rollback-window rules. Those settings are not public
configuration and are not general bypass switches.

## Cutover and target-write evidence

Ownership operations use one global lock order: migration row, assignment row,
bounded locators in fixed table/primary-key order, source fence, target fence,
then generation-write evidence. Cutover disables the source, installs a
strictly newer target generation, changes assignment and bounded locators
atomically, and leaves the source read-only.

Normal booking transactions lock only assignment and the active local fence;
they never lock migration or locator control rows. They therefore cannot form a
reverse lock cycle with cutover/rollback.

The cutover transaction creates a zero-valued durable target-generation
evidence row. Every successful non-replay target mutation locks and increments
that row in its booking transaction. A direct post-cutover rollback must lock
the same evidence key after migration, assignment, bounded locators, source
fence, and target fence. It is permitted only while the value remains zero and
still installs another newer generation.

If target-write evidence is positive, direct reversal is forbidden. A reverse
migration must copy and validate the target's current state before another
cutover. This protects committed target work from being discarded.

## Quiescence

The migration coordinator acquires the same assignment/fence locks and disables
the source fence. Because earlier mutation transactions hold those locks until
completion, this establishes a database-proven zero-writer state. The operation
uses bounded lock and statement timeouts; it does not infer quiescence from a
sleep, quiet metrics, or process-local observation.

The zero-writer interval can produce retryable customer failures. This is an
explicit availability tradeoff, not a zero-downtime claim.

## Required evidence

- matching route/generation succeeds;
- stale generation, wrong shard, disabled catalog, disabled fence, and invalid
  migration state fail before booking side effects;
- concurrent assignment and fence updates never leave two writers;
- three stale replicas refresh only after bounded stale rejection, and 100
  concurrent routed-transaction/fencing attempts cannot establish stale source
  authority after cutover;
- separate end-to-end create evidence remains required for duplicate-resource,
  inventory-overlap, and source-mutation claims;
- generation never decreases across migration and direct rollback;
- a target mutation racing direct rollback serializes safely; and
- retained-public guards reject bypassing or incompatible writers.

Use deterministic barriers and database locks in concurrency tests. Timing
sleeps are not evidence of serialization.

See [ADR 029](adr/029-single-writer-fencing-generation.md) and
[cutover and rollback](shard-cutover-and-rollback.md).
