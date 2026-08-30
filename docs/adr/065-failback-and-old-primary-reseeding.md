# ADR 065: Failback by Restore and Old-Primary Reseeding

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

After failover, the former primary may contain transactions absent from the new
timeline and lacks later active-region writes. Starting it because it is
healthy can fork or overwrite payment, ticket, ledger, and seat authority.

## Decision

Keep the former region externally fenced. Archive or destroy divergent PGDATA.
Restore fresh empty control/shard volumes from a verified current-active
pgBackRest backup and WAL chain, create new physical slots, catch up, and verify
system identity, source timeline/LSN provenance, schema, regional state, and all
financial/shard reconciliation.

To return authority, fence the current active region first, promote the reseeded
region, verify each database, and install a strictly newer regional epoch. Reset
pools, reconcile, activate workers/ingress/customer writes in the same order as
failover, and leave the former writer passive.

`pg_rewind` is not the required Milestone 7 path. It may be researched later as
an optimization, but failback acceptance is restore/reseed.

## Invariants

- `pg_isready`, old configuration, or prior primary role is never freshness
  evidence.
- Old PGDATA is never directly promoted after divergence.
- Failback never decrements or reuses a regional epoch.
- The restored source, repository, backup identity, timeline, and achieved LSN
  are recorded and verified.

## Consequences

- Failback is slower but easier to prove than reusing divergent storage.
- Fresh slot creation and backup provenance are part of every failback drill.
- A failed or incomplete restore cannot become an application target.

## Rejected alternatives

- Restart the old primary and reconcile later: rejected because conflicting
  authoritative writes may already escape.
- Require `pg_rewind`: rejected because its WAL, checksum/hints, stop-state, and
  failure prerequisites make it a weaker bounded acceptance path.

