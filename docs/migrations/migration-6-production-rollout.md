# Migration 6 Production Rollout

Migration 6 adds the durable Milestone 2 hot-train policy and supporting
indexes. This is an operator runbook, not authorization to modify production.

## Change summary

The up migration:

- creates `hot_train_policies` with train-run foreign key, unique
  `(train_run_id, seat_class)`, optimistic version, Redis initialization latch,
  safe numeric checks, and timestamps;
- adds an enabled-policy lookup index;
- adds partial/indexed paths used to derive active held-reservation quotas;
- extends outbox aggregate/event constraints for policy create, update, and
  disable events; and
- leaves waiting-room entries and admission tokens in Redis only.

It does not rewrite `seat_inventory`, `reservation_seats.segment_mask`, or any
existing VARBIT allocation data.

## Preconditions

1. Record the release commit and expected migration checksum.
2. Verify a recent PostgreSQL backup and restore rehearsal.
3. Confirm migration version 5 is clean and every Migration 5 populated-upgrade
   gate remains satisfied.
4. Rehearse Migration 6 on a recent production-like restore and record lock
   time, index time, table size, and outbox constraint validation.
5. Confirm no existing custom outbox type conflicts with the versioned
   constraint set.
6. Confirm the target train-run foreign keys use valid UUIDs and that the
   application will create only `standard`, `business`, or `first` policies.
7. Prepare externally managed admission keyrings, but do not expose values in
   logs, tickets, command lines, or this runbook.
8. Keep admission workers disabled until migration and application readiness
   both pass.

A timeout, dirty migration state, failed restore rehearsal, or unexpected
constraint/index cost blocks the rollout.

CI also performs a deterministic populated rehearsal from clean version 5:
it seeds a held reservation plus its pending `reservation.held` outbox event,
upgrades to version 6, verifies the new table/index/constraint set and existing
data, moves one step down to version 5, and reapplies version 6. This fixture is
a regression gate, not a substitute for the production-like restore rehearsal
and lock-duration measurements above.

## Apply and validate

Use a dedicated migration identity and secret-managed libpq configuration:

```powershell
$env:PGSERVICE = 'railway-migrations'
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations version
```

Expected version is `6` and dirty must be false. Validate with read-only catalog
queries that:

- `hot_train_policies` and its unique/check/foreign-key constraints exist;
- the enabled-policy and held-reservation indexes are valid;
- outbox constraints admit only the documented prior events plus the three
  policy event types;
- no policy row exists unless deliberately created after application rollout;
  and
- existing seat reconciliation remains clean.

Deploy APIs with the new admission accept keyring and quota/backpressure
configuration. Prove `/readyz` before traffic. Deploy admission workers
disabled, prove their PostgreSQL/Redis/migration/keyring readiness, then enable
one. Create or enable policies only after the worker has initialized the
expected Redis generation. Scale workers only after multi-replica evidence.

## Application rollback

Prefer an application rollback while leaving schema version 6 in place. The
older version-5 application ignores the additive policy table and quota
indexes. Before rolling back:

- disable new policy mutations and admission workers;
- decide how enabled hot runs will be traffic-gated while the old API cannot
  enforce admission;
- retain Redis evidence and policy outbox rows;
- verify the previous binary against a version-6 restore; and
- continue PostgreSQL seat reconciliation.

Do not route enabled hot-run traffic through a binary that cannot enforce the
policy. Disabling a policy is a deliberate operator/product decision, not an
automatic outage fallback.

## Exceptional schema down

The down migration is destructive: it deletes all
`hot_train_policy` outbox events, drops policy/quota indexes, and drops the
policy table because version-5 outbox constraints cannot represent those
events. It does not repair or delete Redis waiting-room state.

Run down only after a separately reviewed decision that explicitly accepts loss
of policy configuration and audit intent, all new binaries are stopped, no
enabled policy protects traffic, evidence is exported securely, and rollback
has been rehearsed on a restore. Never run it automatically during an incident.

After any exceptional down, verify clean version 5, seat reconciliation,
outbox constraints/backlog, and application readiness. Redis generation keys
must be treated as orphaned control state and handled through a reviewed,
bounded operational procedure; never use `KEYS`.
