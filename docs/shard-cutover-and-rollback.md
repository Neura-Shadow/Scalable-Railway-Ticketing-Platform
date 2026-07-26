# Shard Cutover and Rollback

## Safety summary

Cutover is an explicitly confirmed, bounded PostgreSQL transaction. It may
temporarily reject writes for the selected train run. It does not dual write,
does not delete the source, and does not claim zero downtime.

`cmd/shard-admin` and its focused tests are present in the work-in-progress
branch. Final controlled runtime, CI, independent review, and release
acceptance remain pending. Do not substitute direct catalog DML.

## Before cutover

1. Confirm the migration is `cutover_ready` and its latest validation is
   complete, untruncated, current, and passing.
2. Confirm the source assignment/generation and disabled source fence still
   match the plan.
3. Confirm the target is healthy, disabled, fully copied, and carries the
   reserved newer generation.
4. Verify locator indexes, count the affected rows through those indexes, and
   remain below the configured hard cap.
5. Verify the cutover statement/lock timeout fits the approved maintenance
   window.
6. Record sanitized health, reconciliation, backup, disk, connection, lock,
   and outbox baseline evidence.
7. Announce that requests for the selected train run may receive bounded
   retryable failures during the zero-writer interval.

Preview the operation through the private CLI:

```text
shard-admin inspect-train-run --train-run-id <train-run-id>
shard-admin validate-migration --migration-id <migration-id> --dry-run
shard-admin cutover --migration-id <migration-id> --dry-run
```

## Atomic cutover

The confirmed command must lock in the shared fixed order and, in one
transaction:

1. lock the migration row, assignment row, and target shard-catalog row;
2. revalidate migration, assignment, target write eligibility and fencing-
   protocol compatibility, validation, locator count, health, and deadline;
3. lock bounded locators in fixed table/primary-key order, then the source
   fence and target fence;
4. create and lock the zero-valued target-generation write evidence as the
   final control lock;
5. install the reserved target generation and enable only the target fence;
6. keep the source fence disabled;
7. update reservation, ticket-order, and ticket locators through indexed
   train-run keys;
8. switch the assignment to the target and enter `rollback_window`;
9. advance the availability generation and append central invalidation/audit
   intent; and
10. commit every visible route/fence/locator change together.

Normal booking transactions lock only the assignment and active local fence.
They do not lock migration or locator control rows, so this ownership-operation
order does not create a reverse cycle with customer writes.

Run only with explicit confirmation:

```text
shard-admin cutover --migration-id <migration-id> --confirm
```

Timeout, cancellation before commit, cap failure, or a changed precondition
must roll back the whole transaction. The command exits non-zero and emits a
bounded category without credentials, topology, SQL, or resource payloads.

## After cutover

- Inspect the train run and prove exactly one writable fence.
- Exercise one bounded read and, only if approved, one synthetic/idempotent
  lifecycle smoke through the public API.
- Confirm stale source writes are rejected and a fresh route reaches target.
- Run shard assignment, locator, migration, seat, quota, idempotency, ticket,
  and outbox reconciliation.
- Drain earlier train-run events to durable read-model receipts before recording
  the availability namespace baseline. After cutover, identify the unique
  generation-bound `shard_cutover` outbox event, wait for its exact
  `railway-read-model` receipt, and only then confirm that the namespace advanced
  without a Redis key scan.
- Keep the source retained and immutable throughout the rollback window.

## Rollback matrix

| Point in lifecycle | Direct rollback | Required action |
|---|---|---|
| Before cutover | Allowed | Re-enable source, leave assignment on source, retain/discard only unroutable target copy |
| After cutover, target writes = 0 | Allowed under locks | Install newer source generation, switch bounded locators, disable target, enable source atomically |
| After any successful target mutation | Forbidden | Plan and execute a full reverse migration with a newer generation |
| Rollback window expired | Direct rollback unavailable | Treat target as authority; use a new migration plan |

### Pre-cutover rollback

The assignment remains on source. The operation revalidates that target is not
authoritative, re-enables the source fence at the current generation, and
records `rolled_back`. It never deletes source state and never exposes the
partial target.

### Direct post-cutover rollback

The rollback transaction follows the same global order: migration, assignment,
the destination shard-catalog row, bounded locators in fixed table/primary-key
order, current-owner fence, destination fence, then target-generation evidence.
It revalidates destination write eligibility and protocol compatibility while
holding the catalog lock. The evidence must still be zero under those locks.
The operation uses a generation newer than the cutover generation; generations
are never decremented or reused.

```text
shard-admin rollback --migration-id <migration-id> --dry-run
shard-admin rollback --migration-id <migration-id> --confirm
```

If a first target mutation races the rollback, the common locks serialize the
operations. Either the write records evidence first and rollback fails, or
rollback installs newer source authority and the stale target write fails.

### Reverse migration

Once target-write evidence is positive, do not flip a mapping or restore an old
generation. Create a new migration whose source is the current target, copy its
current state, validate every invariant, quiesce it, and cut over normally to
the earlier source or another fixed logical shard.

## Source cleanup

Cleanup is never automatic. It is eligible only after a completed migration,
rollback-window expiry, current target authority/fence revalidation, no active
conflicting migration, clean reconciliation, and an approved backup/retention
decision.

```text
shard-admin cleanup-source --migration-id <migration-id> --dry-run
shard-admin cleanup-source --migration-id <migration-id> --confirm
```

Interrupted cleanup must fail boundedly and remain auditable. It must not
delete current authority or weaken rollback rules. See
[Migration 8 production rollout](migrations/migration-8-production-rollout.md).
