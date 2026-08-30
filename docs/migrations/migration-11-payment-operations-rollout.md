# Migration 11 Payment Operations Rollout

## Scope and safety boundary

Control migration 11 adds the bounded Milestone 7 payment-operations contract:
provider capability metadata, action-scoped retry state, an immutable balanced
operational ledger, normalized settlement and payout evidence, whole-ticket
partial-refund coordination, webhook-key rotation metadata, regional authority,
failover checkpoints, and backup/restore verification metadata. It stores no
provider credential, webhook secret, raw provider report, database address,
backup encryption key, PAN, CVC, track data, or customer-supplied money.

Applying migration 11 does not start Stripe, settlement imports, refunds,
regional promotion, backup deletion, or customer writes. It seeds the durable
regional authority for the existing region A deployment at epoch 1 as
`active` with `writes_enabled=true`; this preserves the pre-migration writer
identity. Deployment/process write flags and every new M7 process must remain
disabled until the control and both physical booking databases pass the same
evidence gate.

## Preconditions

- Record the exact source commit and checksums of both migration files. Verify
  control is clean at version 10 and both catalogued physical shards are clean
  at version 2.
- Use a dedicated migration role and bounded lock/statement timeouts. Capture a
  verified backup and complete an isolated restore before changing authority or
  financial schemas.
- Drain payment/shard claims and settlement imports. Keep provider mutations,
  partial-refund requests, webhook key changes, failover, and backup expiration
  disabled.
- Verify the seven ledger account codes, provider account/API-version profile,
  deployment region/role/epoch, three-database identity set, and selected
  booking-shard migration order.
- Confirm no direct SQL writer can bypass the reviewed regional-authority or
  financial-evidence transaction seams.

A timeout, dirty schema, missing shard, truncated inspection, invalid backup,
or inconclusive reconciliation blocks rollout.

## Rehearsal and validation

In an isolated PostgreSQL 16 topology, rehearse fresh `0 -> 11`, populated
`10 -> 11`, repeated up, empty `11 -> 10`, and `10 -> 11` again. Run the
version-11 schema assertion and verify `version=11 dirty=false` after each up.

The rehearsal must prove:

- action leases and attempts are independent of the parent payment-saga budget;
- every committed ledger transaction has at least two positive postings, one
  currency, checked minor-unit totals, equal debit/credit totals, immutable
  evidence, and at most one balanced reversal;
- provider imports use stable provider/account/record identities, signed
  gross/net values, non-negative fees, exact content hashes, durable cursors,
  and retained changed-hash conflicts;
- settlement review acknowledgements append evidence without modifying a run,
  mismatch, provider fact, or ledger fact;
- partial-refund requests derive owner, provider, currency, amount, shard, and
  ticket fare only from authoritative server data;
- regional epoch cannot decrease or change region at the same epoch, and writes
  cannot be enabled outside active authority;
- failover checkpoints are bounded, versioned, fixed to control/shard-0/shard-1,
  and contain no address, command, path, credential, or raw WAL payload;
- retained M7 evidence or changed regional authority makes schema down fail
  closed while leaving evidence and schema intact.

## Rollout order

1. Enter the reviewed maintenance boundary and stop old replicas from serving.
2. Apply booking-shard version 3 independently to both physical shard databases;
   do not update the control catalog manually.
3. Apply control migration 11, which atomically advances the two fixed catalog
   entries from schema 2 to schema 3.
4. Run control/shard schema, data-preservation, privilege, secret-column, and
   regional-authority assertions. Confirm all three migrations are clean.
5. Start new binaries in `recovery` or with regional writes disabled. Verify
   exact database identity, primary/standby role, timeline, region, epoch, and
   schema agreement through fresh connections.
6. Run detect-only payment, ledger, settlement, refund, routing, and DR
   reconciliation. Resolve every mismatch without inventing financial effects.
7. Enable API ingress, payment workers, settlement worker, and partial-refund
   traffic only in the documented canary order. Provider live-test mode remains
   disabled unless its separate protected gate was actually executed.

## Rollback and recovery

Operational rollback disables new M7 requests and worker claims, preserves all
durable provider/import/refund/ledger/DR evidence, and repairs forward. Do not
delete a ledger entry, settlement conflict, refund receipt, webhook event,
failover checkpoint, or backup verification to make rollback possible.

Schema down is allowed only when every M7 evidence relation is empty, the seven
seed accounts are unchanged, regional authority remains the original
`region-a`/epoch-1 active row, and both catalog rows can return exactly from
schema 3 to 2. Otherwise down must fail closed. A failed application through
the migration runner may mark its migration metadata dirty even when the SQL
transaction preserved the schema; inspect and repair that metadata only under
the migration runbook, never by deleting evidence.

This runbook is not production authorization, PCI certification, a live-provider
result, statutory accounting evidence, or a zero-downtime/zero-RPO claim.
