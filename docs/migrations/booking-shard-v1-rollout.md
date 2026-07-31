# Booking-Shard Schema Version 1 Rollout

## Scope

The independent booking-shard history starts at version 1 and must be applied
separately to both physical booking databases. It creates local snapshots,
VARBIT inventory, reservations/seats, orders/tickets, idempotency and command
receipts, shard outbox, monotonic write fences, target-write evidence, capture
state, trigger journal and target apply receipts. It creates no global users,
passengers, assignment catalog, quota leases, directory or cross-database
foreign keys.

## Preconditions

- Create two empty independent databases with distinct volumes and separate
  migration/runtime roles; do not clone credentials from control.
- Record PostgreSQL version, commit and migration checksum; take a disposable
  recovery point and define command timeouts.
- Confirm the control migration is clean at version 9 and both physical catalog
  rows remain disabled.
- Ensure ordinary runtime roles cannot create schema, disable triggers, mutate
  journal/apply evidence, or enable fences outside the reviewed protocol.

## Apply both histories

For each shard, place only its URL in `DATABASE_URL` through the secret
mechanism, then run:

```powershell
go run ./cmd/migrate -path migrations/booking-shard up
go run ./cmd/migrate -path migrations/booking-shard up
go run ./cmd/migrate -path migrations/booking-shard version
```

Do not pass or print URLs in evidence. Expect version `1`, `dirty=false`, and a
second `up` reporting no change.

Validate every expected table, index, constraint, update trigger, monotonic
fence/evidence guard, and mutation trigger. New databases contain no train-run
writer; installation alone never authorizes booking. Bootstrap local snapshots
and an explicitly disabled fence through the reviewed migration workflow.

## Rehearsal and compatibility gates

In disposable databases, exercise fresh up, repeated up, populated down/up,
and simultaneous independent shard migration. Before any assignment, verify
protocol/schema version 1, fixed allowlist identity, pool budget, no PII in
journal/outbox/receipts, exact duplicate apply idempotency, conflicting apply
failure, trigger coverage, and generation monotonicity.

The version-1 down removes only version-1 objects without `CASCADE`. Run it only
on an unrouted disposable/retired shard after retained data and recovery
requirements are explicitly satisfied. Operational rollback keeps schema and
data, disables the local fence, and repairs forward; it never points the train
run at another database because a migration command failed.

This installation is not proof of routing, cutover, reverse migration,
production capacity or zero downtime. Continue with
[the online rebalancing runbook](../online-rebalancing.md).
