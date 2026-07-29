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

No random shard fallback, all-shard scan, dual write, or generic distributed
transaction is permitted. See [ADR 038](adr/038-cross-database-booking-command-saga.md),
[quota leases](global-quota-leases.md), and
[reservation directory](reservation-directory.md).
