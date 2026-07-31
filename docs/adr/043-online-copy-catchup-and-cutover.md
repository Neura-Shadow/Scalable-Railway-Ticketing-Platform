# ADR 043: Online Copy and Catch-Up End in a Bounded Quiesced Cutover

- Status: Accepted for the Milestone 5 bounded pilot
- Date: 2026-07-29

## Context

A selected train run must move from legacy or logical storage to a physical
booking shard, and between the two pilot shards, without stopping source writes
for the entire base copy. Independent PostgreSQL databases cannot atomically
change the source fence, target fence, control assignment, migration ledger,
and global locators. Cutover therefore needs durable checkpoints and explicit
zero-writer crash states rather than a nominal cross-database transaction.

Online in this milestone means that source writes continue during snapshot,
base copy, and journal catch-up. It does not mean zero downtime: final authority
transfer requires a bounded write pause.

## Decision

Use a resumable migration module with durable control-plane phase checkpoints,
source-local capture state, target-local apply receipts, and monotonic
generations. Only one active migration may exist for a train run. The target
has the reserved newer generation but remains disabled for normal booking
writes until final cutover.

### Snapshot and base copy

The ordered start protocol is:

1. validate source ownership, target allowlisting/schema/health, pool budget,
   absence of another migration, and a strictly newer reserved generation;
2. install the disabled target fence and empty target-local migration state;
3. enable source trigger capture for the selected train run and generation;
4. open a bounded `REPEATABLE READ`, read-only source transaction and export a
   snapshot; and
5. copy the approved train-run table groups from that snapshot using
   deterministic keys, configured batch/row/byte limits, and `COPY` or bounded
   parameterized inserts through the migration adapter.

Capture begins before the snapshot. Replay starts at the capture start rather
than trying to infer a fragile snapshot/sequence cut line. Entries whose final
state is already present in the snapshot are safe because target row versions,
tombstones, fingerprints, and apply receipts make replay idempotent. The
exporting transaction and replication resources have explicit timeouts and WAL
retention monitoring; an exceeded bound aborts the attempt without promoting
the partial target.

After every committed target batch, the migration records a durable cursor,
row count, and checksum. Resume verifies the checkpoint and repeats or
continues idempotently. Target data remains unroutable, its local normal-writer
fence remains disabled, and migration credentials cannot execute ordinary
booking commands.

### Journal catch-up and validation

Catch-up reads bounded committed journal batches in source sequence order and
applies each batch with target-local receipts. It reports source high
watermark, target applied watermark, rows and bytes, replay rate, lag, retries,
and conflicts. Duplicate delivery is accepted; missing, conflicting, or
out-of-order content fails closed.

Online validation compares the fixed table set, identities, versions,
reservation states, segment masks, idempotency completions, order/ticket
relationships, fence/capture state, journal/apply coverage, and shard-local
outbox expectations at recorded watermarks. Because the source is still
writable, this validation is a progress gate, not final equality proof.

### Final quiesce and cutover order

When lag is below the configured threshold, the migration begins a bounded
final write pause:

1. control routing marks the train run `quiescing` so new commands receive a
   bounded retryable response;
2. the source transaction locks the same local fence and capture-state rows as
   ordinary mutations, waits for preceding mutations, disables the source
   writer, and commits;
3. with source disabled and target still disabled, record the final journal
   watermark, replay through it, and run exact final validation and
   reconciliation;
4. enable the target-local fence at the reserved newer generation and commit;
5. switch the control-plane assignment and stable reservation-directory route
   to the target generation, then commit the cutover checkpoint and
   control-plane outbox intent; and
6. refresh routes and end the pause. Source capture and retained data remain
   available for audit and recovery.

Source disable always commits before target enable. Target enable always
commits before the control assignment switch. A normal routed write requires
both a current control assignment to that shard/generation and a matching local
enabled fence. Therefore target enable alone, before the control switch, does
not authorize a normal write. Source and target are never simultaneously
writable for the train run.

The runner measures the pause from the committed source-disable gate until the
control assignment permits target writes. It records source, target, and
control database timestamps plus an orchestrator monotonic duration, the final
lag, rejected-command count, and recovery steps. It reports the observed
bounded result without converting it into a zero-downtime or capacity claim.

### Crash windows

Every step is idempotent and recovery first reads all three durable states.

- Before source disable, source remains authoritative; resume or abandon the
  unroutable target.
- After source disable but before target enable, there is no writer. Recovery
  replays and resumes, or safely disables the target and re-enables source only
  while control still assigns source and no target write evidence exists.
- After target enable but before control switch, there is still no valid normal
  target route and source remains disabled. Recovery completes the switch or
  disables target before a safe source re-enable.
- After control switch, target is authoritative even if final checkpoint or
  outbox recording is delayed. Recovery finalizes control state from the
  assignment, target fence, command receipts, and target-write evidence.
- A timeout or validation mismatch never enables a target. It preserves the
  zero-writer state for explicit resume or the safe rollback rules above.

## Consequences

- Base copy and catch-up proceed while source serves, limiting the final pause
  to drain, final replay, validation, target enable, and control switch.
- Cross-database cutover is a recoverable protocol, not an atomic distributed
  transaction. Durable zero-writer windows are an intentional safety state.
- Target enable before control switch requires every normal write to verify
  both control assignment and the database-local fence.
- Long snapshots, WAL retention, copy throughput, journal lag, validation, and
  pause duration require explicit bounds and evidence.
- The pilot remains single-region and fixed-topology. It does not prove
  zero-downtime rebalancing, automatic balancing, failover, sustained capacity,
  or national-scale throughput.

## Rejected alternatives

- Quiesce for the full copy: rejected because it makes downtime proportional
  to the full data set and provides no online-copy evidence.
- Copy while writable without a journal: rejected because post-snapshot
  mutations can be lost.
- Enable target before disabling source: rejected because it creates a
  split-brain write window.
- Switch control before target enable: rejected because routing could select a
  database that still rejects the assigned generation.
- Treat target enable alone as write authority: rejected because recovery
  requires a safe pre-control-switch zero-writer window.
- Use dual writes or two-phase commit: rejected because both exceed the
  milestone and introduce blocking or one-sided failure semantics.
- Call the measured final pause zero downtime: rejected because customer writes
  are intentionally rejected while no writer is authorized.
