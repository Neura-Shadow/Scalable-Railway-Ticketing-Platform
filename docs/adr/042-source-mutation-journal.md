# ADR 042: Source-Local Triggers Capture a Bounded Mutation Journal

- Status: Accepted for the Milestone 5 bounded pilot
- Date: 2026-07-29

## Context

Milestone 5 copies a selected train run while its current source remains
writable. A base snapshot alone cannot contain mutations committed after that
snapshot. Source-and-target dual writes are forbidden because independent
database commits can diverge. Application-only capture would also require
proving that every API, expiration, cancellation, reconciliation, and operator
mutation path always appends the same journal intent.

The journal must close the copy gap without becoming another booking authority
or an unbounded copy of passenger data.

## Decision

Use database triggers on every train-run-local authoritative booking table in
the approved migration set. Trigger installation and expected-table coverage
are part of the booking-shard migration history. Capture is disabled by
default and is enabled only for a selected train run and source generation by
a row in `migration_capture_state`.

### Transactional capture and ordering

When capture is enabled, a trigger locks that train run's capture-state row,
verifies the source generation, allocates the next train-run-local sequence,
and inserts a journal entry in the same transaction as the source mutation.
The row lock is held to commit or rollback. This creates a committed order for
the selected train run and prevents a replay cursor from skipping a lower
sequence that was allocated by a still-open transaction. A rolled-back source
mutation rolls back its sequence advance and journal entry as well.

Serialization on the capture-state row is an intentional, measured pilot
cost. It is enabled only during migration, has bounded lock and statement
timeouts, and fails the source mutation closed if capture cannot be written.
The pilot records journal-capture latency and contention rather than assuming
the overhead is negligible.

### Bounded journal contract

Each entry contains a journal UUID, migration ID, train-run ID, source
generation, ordered sequence, allowlisted table and operation, bounded primary
key fields, row version or deterministic fingerprint, database timestamp, and
a versioned size-limited payload needed to apply the change. Payloads are
constructed from an explicit per-table column allowlist; arbitrary
`row_to_json`, SQL text, request bodies, and unbounded values are prohibited.

The journal contains no passenger name, email, identity document, contact
data, credential, DSN, token, or raw idempotency key. Opaque surrogate IDs are
included only when required to preserve shard-local relationships. Access is
limited to the booking writer, migration reader, and reconciliation roles;
customer and ordinary operator queries cannot read or alter the journal.

The source booking tables remain authoritative. The journal cannot serve a
booking read, authorize a write, choose a shard, repair inventory by itself,
or replace source validation. It is a durable migration input, not a second
authoritative write and not an event outbox.

### Target apply and completeness

The target remains unavailable to normal writers. A restricted migration
adapter applies journal entries in order in a target-local transaction. The
same transaction changes target rows and inserts a unique
`migration_apply_receipts` row keyed by migration and journal entry. A retry
with the same fingerprint is a no-op; the same identity with different content
fails closed. Row versions, tombstones, foreign-key ordering, and deterministic
conflict rules make duplicate application safe and reject out-of-order or
inconsistent state.

Coverage tests enumerate the required trigger set and exercise every booking
mutation path. They prove that commit creates exactly the expected bounded
journal entries, rollback creates none, disabled capture creates none, the
wrong generation is rejected, payload limits hold, and forbidden passenger
fields never appear. Reconciliation compares source maximum sequence, target
apply receipts, missing or duplicate sequences, and fingerprints.

Journal cleanup is not automatic. Entries remain until cutover or rollback is
terminal, the source-retention requirements in ADR 044 pass, and an explicitly
confirmed bounded cleanup records an audit result.

## Consequences

- All SQL mutation paths share one capture implementation with strong
  locality; adding a new authoritative table requires migration and coverage
  updates before it can participate in a move.
- Capture failure rolls back the source mutation, preserving copy completeness
  at an availability and latency cost.
- Per-train-run ordering simplifies resumable replay and final lag proof but
  serializes mutations for the selected run while capture is active.
- Apply receipts make replay idempotent without making the journal a booking
  or event authority.
- Payload allowlists and role separation reduce PII and topology exposure, but
  the journal still contains integrity-sensitive booking identifiers and must
  be protected and retained deliberately.
- The measured bounded pilot does not establish production journal throughput
  or national-scale migration capacity.

## Rejected alternatives

- Source-and-target dual writes: rejected because no transaction can make the
  two database commits atomic.
- Application-only capture: rejected for the pilot because worker, operator,
  and future SQL paths could omit capture without a database-enforced seam.
- PostgreSQL logical replication by default: rejected because the full
  train-run boundary, filtering, sequence, DDL, slot retention, cutover, and
  reverse-migration semantics are not proven by the existing system.
- Store complete row or request JSON: rejected because it is unbounded and can
  replicate passenger PII or secrets.
- Best-effort journal insertion: rejected because an unjournaled committed
  source mutation makes convergence unprovable.
- Use the journal as an event queue: rejected because migration replay and
  business event delivery have different identities and retention contracts.
