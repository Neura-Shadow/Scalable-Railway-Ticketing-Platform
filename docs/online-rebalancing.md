# Online Physical Rebalancing

## Safety model

Online means that the source remains writable during capture, base copy, and
journal catch-up. It does not mean zero downtime: final quiescence intentionally
creates a measured, bounded write pause and may preserve a zero-writer recovery
state. Source and target are never simultaneously valid writers.

## Forward migration

1. Preflight the control assignment, source fence, target schema/protocol,
   pool budget, storage headroom, and detect-only reconciliation.
2. Create the control migration ledger with a new UUID and a strictly newer
   target generation. Bootstrap a disabled target fence and local snapshots.
3. Enable source-local trigger capture and record its start sequence.
4. Open a bounded PostgreSQL `REPEATABLE READ`, read-only source transaction;
   copy the approved dependency-ordered table set in deterministic batches.
5. Replay committed journal batches through target-local apply receipts while
   source writes continue. Persist a checkpoint only after target commit.
6. Run online validation at recorded watermarks. This is a progress gate, not
   final equality while the source remains writable.
7. When lag is within the configured limit, execute the ordered final cutover
   in [the cutover runbook](physical-cutover.md).

Copy and replay are resumable from bounded, durable checkpoints. A partial
target is disabled and unroutable. Process, source, target, timeout, validation,
or protocol failure stops safely; it never promotes by row count or health.

## Base-copy rules

Copy identity/version columns, not generated display results. Use a fixed
table allowlist, deterministic primary-key order, row/byte/time caps, streaming
`COPY` or bounded parameterized batches, and explicit cancellation. The export
snapshot and WAL pressure have hard duration/headroom limits. Checkpoints record
object, cursor, rows, bytes/checksum, journal watermark, and status without PII.
Every base-copy table has a `(train_run_id, assignment_generation, id)` cursor
index so each page can use a bounded keyset range instead of rescanning and
sorting the remaining train-run rows.

See [ADR 043](adr/043-online-copy-catchup-and-cutover.md) and
[the mutation journal](mutation-journal.md).
