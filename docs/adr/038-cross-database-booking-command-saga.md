# ADR 038: Cross-Database Booking Uses a Durable Command Saga

- Status: Accepted for Milestone 5 implementation
- Date: 2026-07-29

## Context

Create reservation currently commits global idempotency ownership, quota,
locator, local inventory and reservation state, and event intent together.
Physical extraction removes that atomic commit. A control commit can precede a
shard failure, and a shard commit can precede a control outage. Retrying an
ambiguous command must not allocate seats twice, strand the only route to a
committed reservation, or undercount global quota.

The goal is convergence to one durable booking result, not an exactly-once
distributed-processing claim. The shard's local commit is the authority for
seat allocation; control facts must be repairable from durable identities and
receipts.

## Decision

Coordinate booking through a durable control-plane `booking_commands` ledger.
Each command has an application-generated globally unique `command_id`,
operation, owner, train run, reserved resource ID, hashed idempotency identity,
request fingerprint, target shard and generation observation, bounded state,
execution lease, result resource ID, bounded error category, and timestamps.

The global uniqueness contract remains
`(owner_user_id, operation, idempotency_key_hash)`. Raw idempotency keys are
never stored. A matching fingerprint replays the same command; a different
fingerprint conflicts. Command and resource identifiers use the repository's
globally unique UUID representation and do not depend on a database-local
sequence. Reservation-seat, ticket-order, ticket, event, migration, and any
externally referenced journal identifiers obey the same cross-database
collision rule.

The command lifecycle includes `reserved`, `executing`,
`committed_on_shard`, `finalized`, `failed`, `expired`, and `needs_repair`.
State transitions are conditional and idempotent. Leases bound worker ownership
but do not prove that a shard failed; lease expiry only permits another observer
to determine the durable outcome.

### Control reservation transaction

In one control transaction:

1. validate the authenticated user and passenger ownership using current
   control data;
2. resolve the train run's current assignment;
3. create or replay the booking command and verify its fingerprint;
4. acquire the conservative quota lease from ADR 039;
5. reserve the globally unique reservation ID; and
6. create a pending reservation-directory entry linked to the command.

Commit before contacting a physical shard. No control transaction remains open
while shard work executes.

### Shard execution transaction

Route to exactly one approved shard. In one local transaction:

1. validate storage identity, expected assignment generation, local write
   fence, migration permission, and the required local booking snapshots;
2. acquire `booking_command_receipts.command_id`;
3. verify operation and request fingerprint;
4. execute or replay shard-local idempotency;
5. allocate seat masks and create the reservation and reservation-seat rows;
6. append shard-local outbox intent and any mutation-journal entry;
7. mark the receipt committed with the reserved result resource; and
8. commit all local effects together.

A same-command/same-fingerprint retry returns the receipt's same result. A
different fingerprint conflicts. A receipt cannot claim success without the
corresponding local booking state. Receipts contain no raw idempotency key or
passenger PII and move with their train run during migration.

### Control finalization transaction

After observing a committed receipt, one control transaction conditionally:

1. marks the command committed and finalized;
2. activates the reservation directory entry;
3. converts the pending quota lease to an active hold; and
4. appends control-plane event intent when required.

Every step is keyed by stable command/resource identity and is safe to repeat.
An acknowledged booking has zero local RPO within the authoritative shard:
inventory, reservation, receipt, idempotency result, and shard outbox committed
together. Control visibility may lag without losing that booking.

### Deterministic recovery

A booking-command reconciler inspects bounded commands and exactly one assigned
shard. It never mutates seat inventory.

- Reserved command, no receipt: while connectivity or execution outcome is
  uncertain, retain pending quota and directory state. Only after the execution
  lease expires and durable absence or permanent failure is proven may control
  release quota and mark the directory/command failed.
- Committed shard receipt, unfinished control: verify fingerprint and local
  resource, then idempotently finalize command, directory, and quota.
- Permanent shard failure receipt: idempotently release the lease, fail the
  directory and command, and retain only a bounded error category.
- Partial control finalization: repeat the same conditional control transition;
  never create duplicate lease or directory rows.
- Control unavailable after shard commit: retries and the reconciler use the
  receipt after control returns; they do not repeat seat allocation.

## Consequences

- A shard commit followed by control failure is recoverable without duplicate
  inventory mutation.
- A control reservation followed by shard failure can temporarily consume
  quota, but it does not permanently do so once failure is proven.
- Retry converges through command and receipt identities; this is idempotent
  at-least-once coordination, not one atomic or exactly-once distributed
  transaction.
- Command reconciliation becomes a required worker and operational surface.
- Affected train runs may return bounded retryable errors during uncertainty;
  healthy shards remain independent.
- Legacy and logical storage can implement the same command protocol while
  retaining their current local transaction adapter, avoiding two externally
  different idempotency contracts.

## Rejected alternatives

- Commit a global idempotency claim and treat it as the booking result:
  rejected because it can diverge from the authoritative shard result.
- Retry shard mutation without a local command receipt: rejected because an
  ambiguous first commit could allocate seats twice.
- Release quota when a request times out: rejected because timeout does not
  prove absence of a committed shard reservation.
- Compensate a committed booking by automatic seat mutation: rejected because
  the reconciler repairs control state and must not silently reverse authority.
- Claim exactly-once distributed execution: rejected because delivery and
  finalization are retried at least once across independent commits.
