# Physical Cutover Runbook

## Preconditions

Use a disposable or explicitly approved environment. Record commit, image and
migration artifacts, migration/train-run IDs, source and target shard IDs,
source/target generations, backup/restore point, rollback deadline, and named
operator. Require healthy compatible control/source/target databases, clean
migration histories, complete base-copy checkpoints, catch-up within the
configured bound, and a passing online validation/reconciliation report.

The administration interface must support dry-run, bounded output, explicit
migration-bound confirmation, cancellation, and nonzero failure exits. Until a
specific command is implemented and verified, use the tested migration runner
for the build under review; do not improvise unrestricted SQL.

## Ordered write pause

1. Mark the assignment quiescing/draining. New commands receive a bounded
   retryable migration response; in-flight work drains to a database-visible
   bound rather than a sleep.
2. In the source transaction, lock the ordinary fence/capture rows, wait for
   preceding mutations, disable source writes, and commit.
3. With both writers disabled, record the final source journal watermark,
   replay through it, and run exact validation and reconciliation.
4. Enable the target-local fence at the reserved newer generation and commit.
5. Switch the control assignment/directory hints, commit the cutover checkpoint
   and control outbox intent, then rotate route and availability generations.

Measure pause from committed source disable until control permits target writes.
Record database timestamps, monotonic orchestrator duration, final lag,
retryable command count, and recovery actions. Report the observed result only;
do not call it zero downtime or extrapolate capacity.

## Crash windows

- Before source disable, the source remains authoritative; resume or abandon the
  disabled target.
- With source and target disabled, preserve the safe zero-writer state; resume
  final replay/validation or pass the rollback gates.
- With target enabled but assignment still at source, no normal target route is
  valid; finish the switch or disable target before source re-enable.
- After assignment switch, target is authoritative; repair delayed ledger,
  directory, outbox, and cache finalization.

A timeout or disagreement preserves zero writers and requires explicit
reconciliation. See [ADR 043](adr/043-online-copy-catchup-and-cutover.md).
