# PostgreSQL Disaster Recovery

## Selected design

The disposable topology initializes every cluster with data-page checksums and
refuses startup if checksum state is disabled. Physical replication uses one
`NOSUPERUSER LOGIN REPLICATION` role and one external password secret per
database pair. PostgreSQL server TLS is mandatory: the evidence runner creates
a short-lived CA plus a distinct private key and hostname-scoped leaf
certificate for every primary, Region B standby, and Region A reseed endpoint
outside the repository. The primary HBA requires `hostssl`, and standbys use
`sslmode=verify-full`.

The bounded topology pins PostgreSQL 16.14 and uses asynchronous physical
streaming for control, shard 0, and shard 1. Each pair has one dedicated
LOGIN+REPLICATION identity, one finite physical replication slot, bounded
`max_slot_wal_keep_size`, WAL archive fallback, TLS/authentication controls in a
production deployment, and independent primary/standby data volumes.

Asynchronous replication preserves write availability when the sole standby or
link is unavailable, but it can lose acknowledged writes. Evidence therefore
records source WAL positions, standby replay positions, elapsed lag, lost-record
count where measurable, and the worst required-database RPO. It never claims
zero RPO.

## Promotion and clients

PostgreSQL does not provide failure detection, fencing, or safe orchestration.
The old primary is externally fenced first. The typed DR runner promotes the
fixed database set, verifies `pg_is_in_recovery() = false`, database identity,
timeline/provenance, schema version, and regional epoch, then resets every pool.
Fresh connections require a read-write target and repeat role/epoch checks
before write readiness.

A planned or unplanned failback never reconnects a divergent old primary. It is
discarded or archived, restored/reseeded from the current writer, catches up,
and can be promoted only after a later fence with a strictly newer epoch.
`pg_rewind` is not required for the Milestone 7 contract.

## Retry and observability contract

Standby bootstrap is retry-safe around interrupted `pg_basebackup`: it writes a
bounded source/database/user/slot marker, removes an incomplete data directory
before retry, reuses a healthy inactive physical slot, and recreates a lost
slot. Replacing a non-standby initialized directory still requires explicit
destructive reseed authority. `restore_command` uses pgBackRest `archive-get`
and follows the latest timeline when streaming WAL is unavailable.

Health and drill evidence observe checksum/TLS state, the fixed physical slot,
`wal_status`, safe and retained WAL bytes, archive success/failure timestamps,
WAL receiver state, replay LSN/timestamp, and live timeline. A process ping is
not sufficient. Primary archive health recovers only after a later successful
archive supersedes the most recent failure; standby health requires a streaming
receiver and replay within the configured staleness bound.

Observed RPO is calculated independently for each database from an acknowledged
source marker and final source LSN to the promoted target's marker and replay
observations. The fence path does not wait for catch-up. It reports derived
missing records, missing WAL bytes, and the observation window; the bounded
laboratory acceptance limit is one marker and 512 MiB per database. End-to-end
failover/failback time is RTO and is never reused as RPO. A zero-loss drill is
an observation for that run, not a zero-RPO claim.

Replication credentials rotate one database pair at a time. Provision the new
external pair-specific secret and recreate only that primary so the new file is
mounted, run `sh /etc/railway/configure-replication.sh` inside it to rotate the
role and reload the managed HBA include, then recreate its standby and prove a
TLS streaming connection under the fixed application/slot name. Existing
streaming sessions remain valid only during this bounded handoff; a failed
reconnect stops the rotation before another pair is touched.
