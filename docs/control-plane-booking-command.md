# Control-Plane Booking Command

## Purpose

The booking command is the durable coordinator for a cross-database booking.
It makes partial outcomes visible without pretending the control and booking
databases commit atomically. The immutable identity is the command UUID;
replay uniqueness is `(owner_user_id, operation, idempotency_key_hash)`, and a
SHA-256 request fingerprint rejects key reuse with different input.

Raw idempotency keys, passenger PII, tokens, DSNs, and shard endpoints are not
stored in command, receipt, event, or diagnostic payloads.

## Create-reservation flow

1. Authenticate the user and validate passenger ownership in control.
2. Lock the user's quota scope and resolve the current assignment.
3. In one control transaction, reserve a globally unique command and
   reservation ID, acquire a conservative quota lease, and create a `pending`
   directory row.
4. Execute once on exactly the assigned shard. The local receipt, inventory
   mutation, reservation, and local outbox intent commit together.
5. In a second control transaction, mark the command `finalized`, activate the
   directory entry, convert the quota lease, and write control outbox intent.

The possible durable states are `reserved`, `executing`,
`committed_on_shard`, `finalized`, `failed`, `expired`, and `needs_repair`.
A shard receipt is authoritative proof of the shard outcome. Failure to finish
step 5 returns an uncertain/retryable outcome and must not repeat step 4.

## Retry and reconciliation

A replay with the same fingerprint returns the same command/reservation. A
different fingerprint conflicts. A reconciler leases bounded command batches,
checks only the recorded shard, verifies receipt identity and fingerprint, and
repairs control state idempotently. It may finalize or record a proven failure;
it never allocates or frees seats. Unknown shard outcomes retain their pending
quota and directory state until evidence becomes decisive.

Confirm and cancel finalization also advances a control-local lifecycle ledger.
Its rank is monotonic (`confirmed=1`, `cancelled=2`), so a cancellation
tombstone may be recorded before a delayed confirm locator and can never be
downgraded by replaying the older confirm command. A finalized lifecycle replay
still reads the immutable shard receipt to rebuild the response, but does not
reapply the control projection.

No random shard fallback, all-shard scan, dual write, or generic distributed
transaction is permitted. See [ADR 038](adr/038-cross-database-booking-command-saga.md),
[quota leases](global-quota-leases.md), and
[reservation directory](reservation-directory.md).

## Operator booking-state commands

Physical fare, seat-eligibility, and booking-policy changes use a separate
durable operator ledger rather than the customer quota/directory flow. The
operator GET reads the currently assigned physical database and returns its
assignment generation and authoritative source version. PATCH requires that
version and a bounded `Idempotency-Key`; the control transaction reserves the
fixed shard, generation, operation, actor, key hash, request fingerprint, and
bounded non-PII payload before the shard is contacted.

The shard verifies the current control route and its local generation fence,
then commits exactly one mutation receipt with the snapshot change. The final
control transaction conditionally applies the resulting version to the control
projection and marks the ledger finalized. If that transaction is interrupted,
the operator-command reconciler validates the recorded shard receipt and
finishes it. A finalized receipt replays the same resource identity; a changed
fingerprint conflicts. Recovery never guesses a shard, scans all databases, or
re-executes a command whose receipt proves the shard commit.

A route mismatch or disabled local writer fence is a deterministic rejection
that occurs before shard mutation. A lease-owning recovery worker first checks
the recorded shard for an immutable receipt; only after an authoritative miss
and deterministic re-execution rejection may it atomically mark a `reserved`
command `failed` with the
bounded `shard_rejected` category. Timeouts, connection loss, and shard
unavailability remain recoverable because they do not prove whether a commit
occurred. Terminal failure compares the immutable command identity, route,
versions, fingerprint, state, and mandatory recovery lease before it can unblock a
migration.

Fare mutation is deliberately limited to an existing fare whose control row is
directly scoped to the selected train run. A route-level fare may affect many
train runs and is rejected before command reservation; broad catalog fanout is
outside this pilot.
