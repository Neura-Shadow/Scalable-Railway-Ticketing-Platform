# Booking-Shard Version 3 Partial-Refund Rollout

## Scope

Booking-shard version 3 extends the existing physical booking authority with
whole-ticket partial-refund states, exact selected-ticket and seat-mask receipt
evidence, regional write authority, DR reconciliation checkpoints, and mutation
journal coverage. It preserves Milestone 6 payment/ticket/full-refund rows and
does not contact a provider or change inventory during migration.

## Preconditions

- The shard is clean at schema version 2, belongs to the fixed allowlisted
  physical-shard-0 or physical-shard-1 connection, and has a verified backup.
- No booking, issuance, void, refund, migration-replay, or reconciliation write
  is in flight. The control catalog remains at version 2 until both shard
  upgrades succeed.
- Record row counts and digests for reservations, reservation seats, ticket
  orders, tickets, all M6 command/refund/compensation receipts, outbox rows, and
  the physical-migration journal.
- Confirm no retained row already uses a version-3-only partial-refund state or
  relation and no prohibited payment or database secret is present.
- Version 3 marks every pre-existing snapshot departure as PostgreSQL
  `-infinity`. This non-null sentinel is deliberately ineligible for refund or
  physical migration; it must never be treated as a real timetable value.

## Rehearsal

Rehearse fresh `0 -> 3`, populated `1 -> 3`, populated `2 -> 3`, repeat up,
empty one-step down, and re-up on PostgreSQL 16. Assertions must prove legacy
ticket codes and all M6 evidence are byte-for-byte preserved and that the
upgrade invents no refund receipt.

Exercise the new receipt contract with synthetic identities:

- one refund request selects one or more complete active tickets only;
- immutable fare amounts, currency, ticket IDs, reservation-seat IDs, segment
  masks, provider proof, route generation, region, and epoch match;
- provider refund success is durable before shard compensation;
- one fenced transaction changes only selected tickets, clears exactly selected
  masks, appends receipts/outbox/journal evidence, and leaves unselected tickets
  and masks unchanged;
- concurrent replay creates one receipt and never releases the same ticket or
  segment twice;
- stale generation, stale epoch, passive/recovery authority, changed money,
  duplicate ticket, missing receipt, or conflicting command fails closed.

## Deployment order

1. Drain and remove old shard writers from readiness.
2. Upgrade physical-shard-0 and run the version-3 schema/data assertions.
3. Upgrade physical-shard-1 and run the same assertions.
4. While writers remain drained, rematerialize `scheduled_departure_at` for
   every routed snapshot from the control `train_runs` authority, binding each
   update to the exact train run, shard, and assignment generation. Verify the
   copied value equals control and that `NOT isfinite(scheduled_departure_at)`
   returns zero on both shards. Physical-migration preflight rejects a shard
   until this proof is complete.
5. Verify both databases report `version=3 dirty=false`, matching active
   region/epoch seeds, and complete M6/v3 mutation-capture triggers.
6. Apply control migration 11, allowing it to advance exactly the two catalog
   rows to schema 3.
7. Start only v11/v3-aware processes with regional writes disabled or in
   recovery mode. Enable writes after the three-database authority gate and
   detect-only reconciliation are clean.

If one shard fails, stop. Do not advertise a mixed v2/v3 topology, edit the
catalog to hide it, route to a random shard, or enable partial refunds.

## Downgrade and forward recovery

Prefer forward repair. Version-3 down is allowed only when regional authority
is still the seed and no partial-refund receipt/checkpoint or v3-only ticket,
order, or reservation state remains. The migration must reject retained
evidence and leave its transaction intact.

Before any permitted downgrade, disable M7 processes, drain leases, reconcile
provider refund operations against shard receipts, and prove no selected seat
mask needs compensation. Never turn a partially refunded booking into a full
M6 refund merely to satisfy downgrade constraints.

This runbook is a bounded migration procedure, not a production DR exercise,
capacity result, provider settlement statement, or zero-loss guarantee.
