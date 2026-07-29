# Reverse Migration Runbook

## Decision boundary

The cutover source is retained read-only. Direct post-cutover rollback is
allowed only when locked, durable evidence proves zero successful target writes
and no contradictory command receipt, reservation, outbox, or control result
exists. Unavailable or ambiguous evidence is not zero.

After any successful target-era write, returning to the former source is a new
reverse migration. Flipping the catalog back would discard committed inventory,
reservations, idempotency outcomes, tickets, and event intent and is forbidden.

## Procedure

1. Create a new migration UUID with current target as source, retained database
   as disabled destination, and a generation greater than every prior value.
2. Reconcile or rebuild the destination from current authority; never assume
   matching retained row IDs mean the data is current.
3. Enable capture on the current source, take the bounded consistent snapshot,
   copy in dependency order, replay with apply receipts, and validate online.
4. Execute the same bounded quiesce and ordered source-disable, final catch-up,
   validation, target-enable, control-switch sequence.
5. Validate source-era and target-era reservations, masks, lifecycle,
   idempotency, receipts, orders, tickets, outbox, directory and quota state.

Acceptance requires a physical-to-physical reverse migration after at least one
successful target write in a disposable three-database environment. A written
plan, backup copy, or direct route flip is not evidence. Runtime evidence is
currently pending until that scenario is executed and its artifact is linked.

## Cleanup

Cleanup is a separate, dry-run-first, explicitly confirmed operation after the
retention deadline, complete reconciliation, outbox/journal accounting, and a
verified recovery point. It is bounded and resumable and never runs as part of
cutover or from a health signal. See
[ADR 044](adr/044-reverse-migration-and-source-retention.md).
