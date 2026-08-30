# Active-Passive Regional Topology

Milestone 7 models exactly one active writer region and one passive recovery
region for a fixed database set: control, booking shard 0, and booking shard 1.
Each active PostgreSQL primary streams asynchronously to one read-only standby
with independent PGDATA. Each region has separate app and data networks:
regional proxies join only the app network, database containers join only the
data network, and application processes explicitly join both. Redis stays on
the app network. This prevents an ingress container from bypassing the
application boundary and connecting directly to PostgreSQL. These same-host
Docker networks remain only a fault-injection model, not production
multi-region infrastructure.

The passive region does not advertise customer-write readiness. It may run
recovery-only probes, receive WAL, verify backups, and stage processes with
writes disabled. Every mutating worker checks configured role/write enablement
and the database-local authority/primary identity before each pass. During an
authority transfer the runner starts recovery APIs only; mutating workers and
the regional proxy remain stopped until control and both shard authorities are
active at the same region and epoch. Health alone never promotes a database or
moves ingress.

Promotion is manual and typed. External process, network, credential, and
ingress fencing of the old region must succeed before any target promotion.
Database-local region/role/epoch guards then reject stale or passive writers in
the same transaction as every control or shard mutation. Those guards are
defense in depth, not distributed consensus.

DR activation requires every required database to be promoted, writable,
current-schema, and on the selected region/epoch. After activation, a later
single-shard outage degrades only assignments on that shard and never routes to
an unrelated database. Redis remains non-authoritative and is rebuilt rather
than promoted as payment evidence.

Application runtime credentials are non-owner, non-superuser roles. They may
read the authority row for readiness but cannot update it, mutate the recovery
journal, or mutate backup, verification, restore, or backup-expiration
operation records. Migration and DR administration retain separate bounded
operator credentials.
