# Milestone 4 Limitations

- The logical `legacy`, `shard-0`, and `shard-1` storages are schemas in one
  PostgreSQL database. They share the primary, engine, disk, connections,
  backup boundary, and physical failure domain.
- The bounded harness disables one logical catalog route. It does not inject a
  schema permission/query failure, PostgreSQL process or host outage, disk
  failure, network partition, independent availability, RTO, RPO, or failover.
- All authoritative writes remain single-region. There is no multi-region
  ownership transfer, active-active writer, distributed consensus, or regional
  admission continuity.
- Migration is quiesced and may reject creates and lifecycle writes for the
  selected train run. There is no zero-downtime, transparent, or disruption-
  free online rebalancing claim.
- Source and target are never dual written. This avoids divergence but requires
  a bounded zero-writer interval for final copy/validation/cutover.
- A direct mapping rollback is permitted only before a target mutation. Once
  durable target-write evidence is positive, recovery requires a full reverse
  migration with a newer generation.
- Source data is retained read-only during the rollback window, increasing
  database size, backup scope, vacuum work, and operational inspection cost.
  Cleanup is separate and explicitly confirmed.
- Locator cutover is one bounded transaction. A hard row cap, required indexes,
  and statement timeout intentionally reject an oversized move rather than
  claiming unbounded online cutover.
- Fixed shard IDs and schemas keep routing, metrics, tests, and fanout bounded.
  Milestone 4 does not support arbitrary shard count, automatic addition,
  autoscaling, split, merge, or hash redistribution.
- Global idempotency-key claims, reservation quota claims, resource locators,
  and the central outbox commit atomically with schema-local state only because
  this is one PostgreSQL database.
- Cross-schema foreign keys to users, passengers, train runs, seats, and other
  reference data do not work across future independent databases.
- The central outbox avoids source/target lease races in this PoC, but physical
  extraction needs shard-local transactional intent, relay discovery, globally
  unique IDs, and explicit migration/recovery semantics.
- The global locator index preserves one-resource lookup and exact owner order
  pagination. A future physical topology needs a new atomic creation/cutover/
  repair protocol.
- The global active-hold quota ledger is authoritative across logical shards.
  It is not a physically independent shard protocol.
- PostgreSQL catalog and assignment availability is a mandatory write
  dependency. Cached routes cannot authorize writes during catalog loss.
- Route caches are process-local hints. They can be stale and may add a refresh
  round trip; fencing, not cache TTL, preserves correctness.
- Logical-shard degradation can leave some routes available only while the
  shared PostgreSQL engine remains usable. It cannot isolate an engine, disk,
  primary, or cluster outage.
- Search remains a global disposable read model and does not become a global
  secondary index over physical shards. Availability remains a cacheable hint,
  never booking authority.
- Redis remains an ephemeral admission/cache dependency. Its loss follows the
  existing fail-closed hot-admission and bounded read-fallback policies and
  cannot change shard ownership.
- Admission allows an attempt, not a seat. Account quotas do not prevent Sybil
  identities, and token delivery remains subject to the documented Milestone 2
  limitations.
- No payment authorization, capture, refund, anti-fraud platform, or real
  passenger identity verification is implemented.
- Operational CLI identity, approval, database-role separation, secret
  delivery, network policy, backup service, PITR rehearsal, and audit retention
  remain deployment responsibilities.
- Reconciliation is detect-only. A mismatch stops acceptance and requires a
  reviewed repair; no production auto-repair is provided.
- The committed benchmark report remains pending until a controlled Milestone 4
  runtime run is accepted. No production-capacity, national-scale, or
  12306-equivalent claim is supported.

Future work is an evidence-gated
**Physical PostgreSQL Shard Pilot and Online Rebalancing**. It must replace the
same-database assumptions explicitly rather than treating this logical PoC as
already physical.
