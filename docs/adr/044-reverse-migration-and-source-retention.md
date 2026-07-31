# ADR 044: Retain the Source and Reverse-Migrate After Target Writes

- Status: Accepted for the Milestone 5 bounded pilot
- Date: 2026-07-29

## Context

Deleting source data at cutover would remove rollback evidence and make an
operator mistake or target failure harder to diagnose. Conversely, treating a
retained source as current after the target accepts a booking would discard
target-era reservations, inventory masks, idempotency results, orders, tickets,
and outbox intent. Independent databases provide no transaction that can make a
direct assignment flip also copy those writes back.

The pilot must distinguish safe pre-cutover rollback, a narrowly safe direct
post-cutover rollback before any target write, and reverse migration after the
target becomes active.

## Decision

Cutover never deletes source rows. The source fence remains disabled and source
data remains read-only through a configured rollback and audit window. The
retained source is evidence and a possible destination for a future reverse
migration; it is not a fallback writer or an implicitly current replica.

### Rollback classes

Before control assignment switches, rollback leaves or restores source
authority and discards or quarantines the unroutable target copy. If final
quiescence already disabled source, recovery first disables target, verifies
that control still assigns source and target-write evidence is zero, then
enables source under a newer generation when required. It never enables both.

After control assignment switches, direct rollback is allowed only when all
authoritative evidence proves that no normal target mutation committed. The
operator protocol verifies and locks the control assignment, both local
fences, the target generation's write-evidence row, booking-command receipts,
and migration state in their respective databases. It disables target before
installing and enabling a strictly newer source generation, then switches the
control assignment. A timeout or disagreement leaves a safe zero-writer state
and requires reconciliation; uncertainty is not treated as zero writes.

Every successful non-replay target booking transaction increments durable
target-generation write evidence locally. Once that evidence is positive, or
the command/directory ledgers show a committed target result, direct rollback
is permanently forbidden for that cutover generation.

### Reverse migration

After any target-era write, returning to the former source is a new migration
with the current target as source and the retained database as a disabled
destination. It receives a new migration ID and a generation greater than all
previous assignments. It uses the same trigger capture, consistent base copy,
journal replay, apply receipts, online validation, bounded quiesce, cutover
ordering, outbox drain, and reconciliation defined by ADRs 041–043.

The destination is rebuilt or deterministically reconciled from current target
authority; old retained rows are never assumed current merely because their
IDs match. Final validation proves that source-era and target-era writes,
segment masks, lifecycle, idempotency, receipts, orders, tickets, and event
intent are preserved. At least one physical-shard-to-physical-shard reverse
migration after successful target writes must run in a disposable acceptance
environment. Documentation alone is not evidence.

### Retention and cleanup

Retention ends only after the configured time window and all of these gates:

- migration and any reverse migration are terminal and reconciled;
- target authority and target-write evidence are unambiguous;
- source-local outbox rows are drained or explicitly accounted for;
- journal and apply-receipt coverage is complete;
- command ledger, quota leases, reservation directory, read-model receipts,
  masks, and row fingerprints reconcile;
- backup/restore evidence and the documented recovery point exist; and
- no active operator command references the retained source.

Cleanup is a separate, operator-controlled command with dry-run inventory,
bounded row and time caps, an exact migration-bound confirmation value,
least-privilege credentials, and a durable sanitized audit result. It is never
automatic, never part of cutover, never triggered by health alone, and never
falls back to unbounded table deletion. Failure is resumable and leaves the
disabled fence in place.

## Consequences

- Retained source data provides rollback, audit, reconciliation, and reverse-
  migration input at the cost of storage, backup scope, and cleanup work.
- Direct rollback is intentionally narrow; uncertainty or any target write
  chooses data preservation over rapid reassignment.
- Reverse migration exercises the same protocol in the opposite direction and
  advances generation, so delayed writers never become valid again.
- Cleanup requires explicit evidence and authority and cannot silently remove
  the only recoverable copy.
- The acceptance reverse migration is bounded disposable evidence, not proof
  of production recovery time, zero data loss under every failure, or an RPO/
  RTO SLA.

## Rejected alternatives

- Delete source at cutover: rejected because it destroys rollback and audit
  evidence before the target has operating history.
- Read or write retained source as a fallback: rejected because it becomes
  stale after the first target mutation and can create split brain.
- Flip assignment back after target writes: rejected because committed target-
  era state would be lost.
- Decrement or reuse a generation: rejected because an old delayed writer could
  become valid again.
- Automatically clean up after a timer: rejected because time alone does not
  prove outbox drain, reconciliation, backup, or recovery readiness.
- Treat a copied backup as completed reverse migration: rejected because the
  live journal, cutover, fencing, and target-era invariants remain unproven.
