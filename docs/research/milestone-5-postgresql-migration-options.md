# Milestone 5 PostgreSQL Migration Options

- Status: Accepted research decision for Milestone 5 architecture
- Date: 2026-07-29
- Runtime baseline: PostgreSQL 16 (`postgres:16-alpine` in the repository)

## Scope and evidence boundary

This note compares migration mechanisms for moving one approved train-run
booking boundary between independent PostgreSQL instances. It is based on
official PostgreSQL documentation and informs the Milestone 5 ADRs and
implementation. It does not by itself prove the repository implementation,
runtime behavior, migration duration, or production capacity.

The pilot remains single-region. "Online" means that the source can continue
serving writes during the base copy and journal catch-up. Final cutover still
requires a measured, bounded write pause. This document makes no claim of:

- zero-downtime rebalancing;
- production-certified sharding;
- distributed serializable transactions;
- exactly-once distributed processing;
- active-active writers;
- multi-region failover; or
- national-scale capacity.

## Decision

Use a **durable source-local, database-trigger-written mutation journal** for
the physical-shard pilot.

The source remains the only booking authority while a consistent base copy is
loaded into a non-writable target. Every committed mutation in the selected
train-run boundary records a bounded journal entry in the same source database
transaction. The target applies journal entries idempotently and records a
durable apply receipt in the same target transaction as the resulting state
change.

After base copy, the migration catches up while source writes continue. Final
cutover fences new source writes, waits boundedly for in-flight work, drains the
journal, validates the target, disables the source, enables the target with a
higher generation, and changes the control-plane assignment last. Source data
is retained read-only for the rollback window. Once the target has accepted a
booking write, direct rollback is prohibited; returning requires a new reverse
migration.

Application-written capture remains a documented fallback only if every
mutation path can be proven to append the journal atomically and tests fail
when any path omits capture. PostgreSQL logical replication, source-and-target
dual writes, and generic distributed transaction coordination are not selected.

## PostgreSQL 16 facts that constrain the design

### Transaction isolation and consistent copy

PostgreSQL's default `READ COMMITTED` isolation takes a new snapshot for each
statement. Two queries in one transaction can therefore observe different
committed states. `REPEATABLE READ` instead gives the transaction a stable
snapshot, although updating transactions must be prepared to retry after
serialization failures.

The source base copy must use a `REPEATABLE READ READ ONLY` transaction. If
copying is parallelized, one source transaction exports its snapshot and stays
open while every source-side copy worker:

1. begins a `REPEATABLE READ READ ONLY` transaction;
2. imports the snapshot before its first query, data-modification statement,
   or `COPY`; and
3. reads only from the source database.

An exported snapshot is valid only until the exporting transaction ends. It
synchronizes source sessions; it is not imported by the independent target
database.

Sources:

- [PostgreSQL 16 transaction isolation](https://www.postgresql.org/docs/16/transaction-iso.html)
- [PostgreSQL 16 snapshot synchronization functions](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION)
- [PostgreSQL 16 `SET TRANSACTION SNAPSHOT`](https://www.postgresql.org/docs/16/sql-set-transaction.html)

### Row locks and advisory locks

`SELECT ... FOR UPDATE` blocks conflicting writers and row lockers until the
current transaction ends. Durable capture state, migration phase, and local
write-fence rows should use row locks with a fixed acquisition order.

Advisory locks have application-defined meaning and PostgreSQL does not enforce
their use. Session-level advisory locks also survive transaction rollback until
explicitly unlocked or the session ends. They are local to one database server
and do not transfer ownership between independent instances. At most, a
transaction-level advisory lock may be a duplicate-operator optimization. It
must never be the authoritative shard fence, routing generation, or cutover
record.

Source: [PostgreSQL 16 explicit and advisory locking](https://www.postgresql.org/docs/16/explicit-locking.html)

### Trigger atomicity and coverage

PostgreSQL executes a trigger in the same transaction as the statement that
fired it. An error in either the statement or trigger rolls both back. This is
the core reason to select trigger capture: the authoritative mutation and its
journal record share one local commit boundary without a cross-database write.

Triggers can cover `INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE` as appropriate.
Row-level and statement-level designs have different cost and transition-table
behavior, so the implementation must explicitly test multi-row statements,
`ON CONFLICT`, cascading foreign-key actions, workers, and administrative DML.
`COPY FROM` invokes destination triggers and check constraints but does not
invoke rules.

Sources:

- [PostgreSQL 16 trigger behavior](https://www.postgresql.org/docs/16/trigger-definition.html)
- [PostgreSQL 16 `CREATE TRIGGER`](https://www.postgresql.org/docs/16/sql-createtrigger.html)
- [PostgreSQL 16 `COPY`](https://www.postgresql.org/docs/16/sql-copy.html)

### `COPY` behavior

Use `COPY (SELECT ...) TO STDOUT` to stream the approved, filtered source rows
and `COPY FROM STDIN` into target staging tables. The query form is required
where the full relation must be restricted to one train run and its dependent
rows. The copy must use deterministic keys and bounded batches or parallel
workers under the shared source snapshot.

The target load must not make the target writable. It must preserve explicit
identifiers, validate constraints, and avoid recursively creating migration
journal entries. Capture state should simply remain disabled on the target;
the design must not rely on globally disabling triggers or foreign-key checks.

A failed large `COPY FROM` can leave dead tuples that require `VACUUM`, so the
copy engine needs resumable staging, explicit cleanup, and bounded retry rather
than assuming an interrupted load is cost-free.

Source: [PostgreSQL 16 `COPY`](https://www.postgresql.org/docs/16/sql-copy.html)

### Sequence behavior and journal ordering

PostgreSQL sequence operations are deliberately not transactional: values
obtained with `nextval` are not reclaimed on rollback, and `setval` changes are
not undone. Crashes and conflicts can create gaps. Logical replication also
does not replicate sequence state.

Therefore a `BIGSERIAL` value must not be treated as a gapless journal, as proof
that no mutation is missing, or as commit order across concurrent transactions.
The selected design uses a normal, transactional capture-state row per train
run:

1. every relevant trigger locks the capture-state row even when capture is
   currently disabled;
2. the trigger checks `capture_enabled` only after acquiring that lock;
3. when enabled, it transactionally increments `next_mutation_seq` and inserts
   the journal row; and
4. the journal enforces `UNIQUE (train_run_id, mutation_seq)` and an immutable
   event identifier.

Because the row lock is held until transaction end, enabling capture waits for
older source mutations to finish. Mutations that begin afterward observe the
enabled state. A base-copy snapshot sees only the last committed counter; an
in-flight mutation that commits after the snapshot receives a later
transactional sequence and is eligible for catch-up.

Sources:

- [PostgreSQL 16 sequence functions](https://www.postgresql.org/docs/16/functions-sequence.html)
- [PostgreSQL 16 transaction isolation sequence caveat](https://www.postgresql.org/docs/16/transaction-iso.html)
- [PostgreSQL 16 logical-replication restrictions](https://www.postgresql.org/docs/16/logical-replication-restrictions.html)

### Replication slots and WAL retention

A replication slot prevents removal of WAL that its consumer still needs, but
an abandoned or lagging slot can retain enough WAL to fill `pg_wal`. PostgreSQL
16 permits `max_slot_wal_keep_size` to bound retention; once required WAL is
removed, the consumer may no longer be able to continue from that slot.

The selected trigger-journal approach does not require a logical replication
slot. If a future ADR revisits logical replication, it must budget and monitor
`max_replication_slots`, `max_wal_senders`, `restart_lsn`, slot activity, WAL
growth, disk headroom, and rebuild behavior after slot invalidation. Unlimited
retention is not an acceptable implicit safety mechanism.

Sources:

- [PostgreSQL 16 replication slots](https://www.postgresql.org/docs/16/warm-standby.html#STREAMING-REPLICATION-SLOTS)
- [PostgreSQL 16 replication configuration](https://www.postgresql.org/docs/16/runtime-config-replication.html)
- [PostgreSQL 16 logical subscription slot management](https://www.postgresql.org/docs/16/logical-replication-subscription.html#LOGICAL-REPLICATION-SUBSCRIPTION-SLOT)

### Failover semantics

PostgreSQL physical streaming replication is asynchronous by default. A
primary failure can lose transactions that committed before their WAL reached
the standby. Synchronous replication can strengthen durability, but increases
commit latency and can reduce availability when required standbys are absent.

Database failover is distinct from application shard cutover. Promotion does
not select the authoritative train-run shard, update the control assignment, or
replace the database-local generation fence. Milestone 5 must not imply an RPO
or failover guarantee that was not configured and tested.

PostgreSQL 16 also predates built-in failover logical-slot synchronization. The
feature was added in PostgreSQL 17, so the pilot's PostgreSQL 16 design must not
assume newer `failover` subscription/slot behavior.

Sources:

- [PostgreSQL 16 high-availability overview](https://www.postgresql.org/docs/16/high-availability.html)
- [PostgreSQL 16 streaming and synchronous replication](https://www.postgresql.org/docs/16/warm-standby.html)
- [PostgreSQL 17 release notes: logical replication failover](https://www.postgresql.org/docs/17/release-17.html#RELEASE-17-LOGICAL-REPLICATION)

### Connection, statement, and lock timeouts

PostgreSQL/libpq treats an absent or zero `connect_timeout` as an indefinite
wait, and applies the timeout separately to each host or address. PostgreSQL 16
also defaults `statement_timeout`, `lock_timeout`, and
`idle_in_transaction_session_timeout` to disabled.

The migration and cutover paths must therefore use explicit bounded deadlines:

- a finite DSN `connect_timeout` for every approved shard connection;
- application context deadlines around connect, copy, replay, validation, and
  cutover operations;
- transaction-local `lock_timeout` shorter than `statement_timeout`;
- a bounded `idle_in_transaction_session_timeout` for the migration role; and
- deterministic timeout handling that records a retryable or operator-visible
  migration phase rather than continuing ambiguously.

PostgreSQL documentation discourages setting broad statement and lock timeouts
globally because they affect every session. PostgreSQL 16 does not provide the
newer `transaction_timeout`, so application deadlines and phase-specific
timeouts remain required.

Sources:

- [PostgreSQL 16 libpq `connect_timeout`](https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-CONNECT-CONNECT-TIMEOUT)
- [PostgreSQL 16 client connection timeout settings](https://www.postgresql.org/docs/16/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-STATEMENT)

## Strategy comparison

### 1. Full quiesced copy

**Method:** reject all source writes, copy and validate the full boundary, then
switch ownership.

**Advantages:** this is the simplest consistency proof. There is no online
mutation gap and no replay engine.

**Disadvantages:** write unavailability lasts for the entire copy, index,
validation, and switch duration. Pause time grows with data volume and target
performance.

**Decision:** reject as the primary Milestone 5 method because it does not meet
the online base-copy goal. Retain it only as an explicit emergency/offline
fallback, without calling it online rebalancing.

### 2. Application-level source mutation journal

**Method:** every application mutation path appends a semantic journal record
inside the same source transaction.

**Advantages:** semantic events can be compact, versioned, and naturally free
of irrelevant database columns. Target apply logic can follow domain commands.

**Disadvantages:** correctness depends on complete application coverage.
Create, confirm, cancel, expiration, admission retry, operator state changes,
reconciliation, repair tools, and direct repository calls can silently omit
capture. Database-side cascades and administrative SQL require special care.

**Decision:** viable fallback, not the default. Selecting it requires a
machine-checked mutation-path inventory and tests that fail when any path
commits without its expected journal entry.

### 3. Database-trigger source mutation journal

**Method:** source-local triggers append bounded row mutations or tombstones to
the journal in the originating transaction.

**Advantages:** PostgreSQL provides the desired atomicity and enforcement at
the database write boundary. Capture covers independently implemented workers
and repository paths, and a trigger failure aborts the mutation.

**Disadvantages:** triggers add write overhead and operational complexity.
Multi-row statements, transition tables, cascades, `ON CONFLICT`, trigger
recursion, privileges, payload growth, and reverse-migration activation require
focused tests. Trigger payloads must not copy passenger PII.

**Decision:** selected for the bounded pilot. Install explicit coverage for
every approved boundary table and mutation operation. Keep payloads bounded,
versioned, and reconstructable; use primary keys and changed booking state,
not unrestricted row dumps.

### 4. PostgreSQL logical replication with filtered publications

**Method:** publish the selected train-run rows and subscribe the target.

**Advantages:** PostgreSQL supplies initial synchronization, ordered apply
within a subscription, and ongoing change delivery.

**Constraints:** PostgreSQL 16 row filters are per table and permit only simple
expressions. For publications containing `UPDATE` or `DELETE`, filtered columns
must be part of the replica identity. Filters do not apply to `TRUNCATE`.
Filters from multiple subscribed publications can be combined with `OR`, and
initial synchronization has publication-option caveats. Logical replication
does not replicate DDL or sequence state.

The approved train-run boundary includes dependent rows whose membership can
be reached through parent keys rather than a filterable `train_run_id` in each
table. PostgreSQL publication row filters cannot perform the joins needed to
prove the complete six-table boundary. Making this work would require further
schema denormalization, replica-identity changes, publication lifecycle, slot
monitoring, and independent cutover fencing. It would still not make the
subscriber safe for concurrent application writes.

**Decision:** reject for Milestone 5. It may be revisited only after proving
complete per-table membership, `UPDATE`/`DELETE` behavior, initial copy,
sequence repair, DDL coordination, slot failover, WAL budgets, conflicts, and
all dependent rows.

Sources:

- [PostgreSQL 16 logical replication](https://www.postgresql.org/docs/16/logical-replication.html)
- [PostgreSQL 16 row filters](https://www.postgresql.org/docs/16/logical-replication-row-filter.html)
- [PostgreSQL 16 restrictions](https://www.postgresql.org/docs/16/logical-replication-restrictions.html)
- [PostgreSQL 16 subscription conflicts](https://www.postgresql.org/docs/16/logical-replication-conflicts.html)

### 5. Source-and-target dual writes

**Method:** application requests independently update both PostgreSQL
instances during migration.

**Advantages:** it appears to keep the target current without a separate log.

**Disadvantages:** independent commits have an unavoidable partial-success
window under process crash, timeout, or network failure. Retrying can duplicate
or reorder effects. Allowing the target to accept these writes also conflicts
with the invariant that exactly one physical shard is writable for a train
run.

**Decision:** reject. A source-local journal is not a dual-authority write: the
source mutation and journal share one authoritative transaction, while later
target application is idempotent migration work against a non-writable target.

### 6. Generic distributed transaction coordination

**Method:** coordinate source, target, and possibly control-plane commits with
two-phase commit/XA or a generic transaction manager.

**Advantages:** a correctly operated coordinator can provide an atomic global
decision across transactional resources.

**Disadvantages:** PostgreSQL documents `PREPARE TRANSACTION` as an interface
for an external transaction manager, not ordinary application use. Prepared
transactions continue holding locks, interfere with `VACUUM`, and must be
resolved promptly. This adds a new coordinator and failure protocol, conflicts
with the milestone's no-XA/no-two-phase-commit boundary, and does not remove
the need for application routing and fencing.

**Decision:** reject. Cross-database booking finalization must use durable
command state, idempotent receipts, conservative leases, and reconciliation,
not a generic distributed transaction.

Source: [PostgreSQL 16 `PREPARE TRANSACTION`](https://www.postgresql.org/docs/16/sql-prepare-transaction.html)

## Selected migration protocol

### Capture activation

1. Create a disabled capture-state row and install triggers on every approved
   source boundary table.
2. Require every relevant trigger to lock that row before checking whether
   capture is enabled.
3. Enable capture in a transaction holding the same row lock. This waits for
   earlier mutation transactions and prevents the activation race in which a
   pre-enable write commits after the base-copy snapshot without a journal row.

### Base copy

1. Begin the source `REPEATABLE READ READ ONLY` exporter transaction.
2. Record the snapshot-visible transactional mutation sequence as the copy
   baseline.
3. Export the snapshot when parallel source readers are used.
4. Stream deterministic filtered queries with `COPY ... TO STDOUT`.
5. Load target staging with `COPY FROM STDIN`; the target remains disabled for
   application writes and migration capture.
6. Validate schema version, row counts, keys, checksums/fingerprints, and
   booking invariants before promoting staged data.
7. Close copy-worker transactions and then the exporter promptly to avoid an
   unnecessarily old source snapshot and vacuum pressure.

### Journal catch-up

1. Read committed journal entries after the snapshot-visible baseline.
2. Apply each event to the target in a transaction that also inserts a unique
   apply receipt.
3. Treat a duplicate receipt as an idempotent success.
4. Reject or defer an out-of-order event until the expected per-train-run
   predecessor has applied.
5. Persist checkpoints only after the target transaction commits.
6. Reconcile source and target repeatedly while source remains authoritative.

### Bounded final quiesce and cutover

1. Enter `quiescing` and activate the durable source-local write fence.
2. Reject new source writes and wait only up to the declared bound for in-flight
   source transactions.
3. Drain the committed journal to zero lag.
4. Run final validation and record its evidence.
5. Mark the source non-writable.
6. Enable the target with a strictly higher local generation.
7. Switch the control-plane assignment to that target generation.
8. Refresh stale routers; both old routing and old generation writes must fail
   closed at the database-local fence.
9. Measure and report the write-pause duration from the first source rejection
   until the new assignment is usable. A timeout leaves a durable, recoverable
   phase and must not be reported as success.

At every crash boundary, zero or one shard may be writable, never two. It is
acceptable for recovery to leave no writer temporarily.

## Required implementation and validation handoff

The architecture and test stages must prove:

- every approved boundary mutation fires capture exactly when enabled;
- journal and source mutation roll back together;
- capture activation cannot miss an in-flight source mutation;
- journal payloads are bounded, versioned, and free of passenger PII;
- a sequence gap is never interpreted as data loss or replay completion;
- parallel copy workers use one valid exported source snapshot;
- target application and apply receipt commit atomically;
- duplicate and out-of-order journal attempts converge safely;
- source remains writable during base copy and ordinary catch-up;
- target rejects application writes before cutover;
- final quiescence is bounded and measured rather than inferred;
- source is disabled before target enable;
- target is enabled before the control-plane assignment switch;
- stale routers cannot bypass the source or target generation fence;
- migration resumes after copy, replay, validation, and cutover crashes;
- pre-target-write rollback is safe;
- target writes prohibit direct rollback and require reverse migration;
- source data remains read-only and is never cleaned automatically;
- connection, lock, statement, and application deadlines fail safely; and
- no test result is generalized into zero-downtime or production-capacity
  evidence.
