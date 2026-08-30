# ADR 063: PostgreSQL Streaming Replication and Encrypted PITR

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

Streaming replication can reduce failover time but mirrors destructive changes
and has no historical recovery. Periodic backup alone has a larger recovery
window. Milestone 7 needs both a promotion path and an independently restorable
history for all three databases.

## Decision

Require authenticated PostgreSQL server TLS and a dedicated external
replication password/role for each control, booking-shard-0, and
booking-shard-1 pair. Sharing one cross-database replication credential is
outside this decision. The same-host lab certificate contains only the fixed
Compose database DNS names and is generated outside the source tree;
production issuance and trust distribution remain deployment responsibilities.

Startup must fail closed on disabled data checksums, an active/wrong-type/lost
slot that cannot be safely recovered, unavailable TLS material, or stale
standby replay. Interrupted base backups restart from a clean partial directory
while retaining a healthy slot; archive recovery uses pgBackRest `archive-get`
on the latest timeline. Evidence records slot WAL headroom, retained WAL,
archive state, negotiated TLS, receiver/replay freshness, and timeline.

RPO and RTO are separate observations. Per-database RPO spans final source LSN
observation to target replay observation and includes missing WAL/record counts;
RTO spans service recovery. Neither is inferred from the other.

Pin PostgreSQL 16.14 with data checksums. Use asynchronous physical streaming,
one standby and one bounded physical replication slot for each control/shard
pair. Keep local `synchronous_commit=on` but do not configure a sole synchronous
standby. Measure and report nonzero-capable RPO per database.

Use pgBackRest 2.59.0 for client-side AES-256-CBC encrypted full/WAL backup,
retention, PITR, check, JSON inventory, and repository verification. Repository,
primary PGDATA, standby PGDATA, and isolated restore PGDATA are distinct. The
encryption key is supplied separately and never stored with repository or
application data. Key rotation creates and verifies a new repository.

Slots have finite `max_slot_wal_keep_size`, monitored safe size and disk use.
Lost WAL requires reseed. Restore acceptance requires checksum/repository
verification plus boot, timeline, schema, regional, payment, ticket, refund,
ledger, and settlement validation on an allowlisted isolated target.

## Invariants

- Replication and backup credentials are separate, narrow, and TLS restricted.
- Promotion never occurs before external fencing.
- Backup success is not restore proof.
- Evidence reports streaming RPO, archive/PITR RPO, and end-to-end RTO
  separately.

## Consequences

- Async replication favors write availability and avoids mandatory regional
  RTT, while accepting possible data loss that must be measured and reconciled.
- A future synchronous design requires separate latency/availability evidence.
- Passphrase loss is terminal and repository encryption cannot rotate in place.

## Rejected alternatives

- Streaming only: rejected because corruption and deletion replicate.
- Native backup only: rejected because encryption, retention, and repository
  operations would require bespoke tooling.
- WAL-G for M7: viable but not selected because the chosen bounded POSIX design
  gets one integrated backup/WAL/verify interface from pgBackRest.
- Logical replication/application dual writes/multi-primary: rejected as
  incomplete or unsafe authority models.
