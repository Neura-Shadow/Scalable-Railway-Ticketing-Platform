# Source Mutation Journal

## Selected mechanism

Milestone 5 uses database triggers on the source booking shard, not application
dual writes or PostgreSQL logical replication. Trigger capture shares the
ordinary mutation transaction, so a committed train-run change has a journal
record and a rolled-back change has neither. The rationale and rejected
alternatives are recorded in
[the PostgreSQL migration research](research/milestone-5-postgresql-migration-options.md)
and [ADR 042](adr/042-source-mutation-journal.md).

`migration_capture_state` binds one active migration to a train run and source
generation. A transactionally updated per-train-run counter assigns a committed
mutation sequence. It is not a PostgreSQL sequence, so rollback does not create
a false continuity promise. `train_run_mutation_journal` stores only approved
table name, operation, bounded primary-key object, row version/fingerprint,
bounded non-sensitive metadata, migration ID, and commit timestamp.

## Coverage

Version 1 installs capture triggers for booking snapshots, seat catalog, fare
snapshots, inventory, reservations, reservation seats, orders, tickets,
idempotency records, and command receipts. Event intent is validated separately
as part of source/target outbox reconciliation. Adding a mutable table to the
train-run boundary requires an explicit journal trigger, apply behavior,
tombstone behavior, coverage test, and validation rule before rollout.

## Apply and retention

The target applies bounded source-sequence batches and commits a unique
`migration_apply_receipts` row with a 32-byte fingerprint in the same target
transaction. Exact duplicate delivery is a no-op; a conflicting fingerprint,
missing sequence, unsupported table, or relationship mismatch fails closed.
The durable target commit precedes advancement of the control checkpoint.

Capture begins before the base-copy snapshot and remains enabled through final
catch-up. Journal retention continues through rollback/reverse-migration
evidence and is never removed automatically.
