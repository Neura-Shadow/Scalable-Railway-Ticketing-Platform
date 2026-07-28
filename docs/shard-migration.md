# Train-Run Shard Migration

## Scope

Migration moves one selected train run between `legacy`, `shard-0`, and
`shard-1` inside one PostgreSQL database. It is durable, bounded, resumable,
quiesced, and explicitly operator-controlled. It never dual writes and never
promotes an unvalidated partial copy.

## State machine

The forward path is:

```text
planned -> draining -> copying -> validating -> cutover_ready
        -> cutting_over -> rollback_window -> completed
```

An explicitly recoverable phase may enter `failed` or `rolled_back` according
to the rollback rules. Invalid transitions fail closed. Only one active
migration may exist for a train run.

The durable record contains the migration/train-run identity, fixed source and
target IDs, source and reserved target generations, copy phase/cursor and
bounded counts, validation state, timestamps, rollback deadline, and a bounded
error category. It must not contain DSNs, credentials, raw SQL errors, stack
traces, request payloads, or PII.

## Phase contract

### Plan

- lock and verify the current assignment and source generation;
- verify the target is a different fixed, enabled storage;
- verify there is no active migration;
- reserve a strictly newer target generation; and
- record the requested rollback window and operation bounds.

Planning is idempotent by migration ID only when every immutable field matches.

### Drain and quiesce

Draining rejects new reservation creation for the selected run with a
retryable response. Existing confirm, cancel, and expire operations may finish
on the source only within the documented drain interval. An admission attempt
rejected for migration must not complete booking idempotency, mutate quota, or
permanently consume its token/processing lease.

Quiescence locks assignment and source fence, disables the source fence, and
uses bounded database timeouts to wait for earlier mutation transactions. Both
source and target may be disabled; they are never both enabled.

### Copy

Copy proceeds in deterministic primary-key order and batches selected by the
bounded `shard-admin start-migration --batch-size` invocation:

1. inventory;
2. reservations;
3. reservation seats;
4. ticket orders;
5. tickets;
6. local idempotency completions with their original expiry; and
7. local fence/reconciliation observations needed for validation.

Each committed batch advances a durable cursor and count in the same control
transaction. Replaying a batch must be idempotent. A failed batch leaves source
authority unchanged and partial target state unroutable.

Public quota claims, global idempotency-key claims, resource locators, journey
projection data, Redis state, and central outbox rows are validated but not
copied.

### Validate

Validation is bounded by row and time limits and must compare:

- identities and counts for every copied relation;
- exact segment masks and reservation lifecycle counts;
- reservation-seat, order, ticket, and global-reference relationships;
- active global quota claims against source lifecycle state;
- local idempotency fingerprint, status, resource, and exact expiry;
- global key-claim relationships;
- central outbox intent/provenance and publication state;
- reservation, order, and ticket locator coverage; and
- detect-only inventory and duplicate-resource reconciliation.

Truncation, timeout, missing coverage, or any mismatch prevents
`cutover_ready`. Row-count equality alone is insufficient.

### Cut over and retain

Before cutover, indexed locator counts must remain within a hard cap and all
required indexes must exist. Assignment, fences, locators, target-write
evidence, availability generation, and migration state change atomically under
a statement timeout. The target accepts writes only after commit.

Source rows remain retained and read-only for the rollback window. Completion
does not delete them. Cleanup is a separate, explicitly confirmed operation
after window expiry and authority/fence revalidation.

Final runtime integrity rechecks all six retained source primary-key sets with
source-to-target anti-joins. Target row-count growth is allowed after cutover,
but a new row cannot conceal the loss of any copied source identity.

## Resumption and abort policy

- Target outage before cutover: retain source authority; resume or fail safely.
- Source outage before complete validation: do not promote the target.
- Copy timeout/failure: preserve cursor and unroutable target; retry boundedly.
- Validation mismatch: preserve evidence and stop before cutover.
- Locator cap/index/timeout failure: roll back the atomic step; re-plan.
- Cutover transaction failure: source assignment remains visible, target stays
  fenced, and no partial locator switch is accepted.
- Cancellation stops new bounded work and does not imply cleanup.

See [cutover and rollback](shard-cutover-and-rollback.md),
[production rollout](migrations/migration-8-production-rollout.md), and
[ADR 031](adr/031-train-run-migration-cutover.md).
