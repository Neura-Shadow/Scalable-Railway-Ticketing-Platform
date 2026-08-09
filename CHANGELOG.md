# Changelog

All notable changes to this project will be documented in this file. The project has not published a Milestone 4 release or version tag.

## Unreleased

### Added

- Milestone 6 provider-neutral payment intents and sagas, stable checkout/
  capture/void/full-refund operations, signed durable webhook inbox, bounded
  uncertainty/manual-review state, and a deterministic fault-injectable local
  sandbox. No raw card data or live production gateway is included.
- Payment-aware booking-shard schema v2 with fenced begin-payment, ticket
  issuance, refund-pending, void cancellation and refund compensation commands;
  their receipts, exact inventory effects, tickets and outbox events commit
  shard-locally and replay idempotently.
- Two-replica payment worker, detect-first reconciler, private bounded admin
  inspection, payment-aware physical migration/reverse migration, owner-scoped
  ticket retrieval, payment metrics/readiness, Compose topology, documentation,
  and ten bounded k6 scenario drivers with evidence still explicitly pending.

- Milestone 5 bounded single-region physical PostgreSQL pilot with one control
  database, two fixed booking databases, allowlisted connection references,
  bounded pools, and database-local monotonic writer fences.
- Durable cross-database booking-command saga with conservative global quota
  leases, repairable reservation directory, shard-local command receipts and
  outbox events; no XA, two-phase commit, or cross-database transaction.
- Independent booking-shard migration history, local booking snapshots,
  source mutation journal, idempotent apply receipts, resumable online base
  copy/catch-up, measured final quiesce, crash-safe cutover, retained source,
  and reverse-migration controls.
- Physical-shard Compose topology, operational runbooks, bounded failure/load
  scenarios, and an evidence-pending benchmark report. These are not zero-
  downtime, production-capacity, multi-region, or national-scale claims.

- Milestone 4 fixed `legacy`, `shard-0`, and `shard-1` booking storages inside
  one PostgreSQL database, with explicit train-run assignments, monotonic
  writer generations, database fences, and retained-public guards.
- Global reservation/order/ticket locators, bounded route caching, same-
  transaction cross-shard quota/idempotency integrity, and a central outbox
  that preserve existing booking atomicity without Redis routing authority.
- Durable bounded train-run migration, deterministic resumable copy,
  validation, quiesced atomic cutover, retained-source rollback window,
  target-write evidence, reverse-migration rules, and detect-only shard
  reconciliation.
- Private bounded shard administration, shard-aware lifecycle work, explicit
  partial-result policy, Migration 8 rollout guidance, bounded measurement and
  failure scenarios, prewarm/lifecycle probes, and an evidence-pending report.

- The historically named `cross-shard-admin.js` is a customer cross-route batch
  read. Admin fanout is the private serial reconciliation CLI, with effective
  concurrency `1` and explicit complete/partial/recovered results.

- Milestone 3 disposable PostgreSQL journey projection with atomic per-run rebuild, durable event receipts, current-state event handling, bounded backfill, lag inspection, and detect-only reconciliation.
- Versioned station, normalized train-search, and short-lived availability-hint Redis caches with CSPRNG generations, TTL jitter, exact-key invalidation, local singleflight, PostgreSQL fallback, and multi-replica sharing.
- Read-model worker lifecycle, bounded Redis Stream pending/retry/DLQ handling, process-scoped secrets, bounded metrics, migration 7 rehearsal, nine k6 scenarios, and honest benchmark/limitation documentation.

- Milestone 2 durable hot-train policies, Redis-backed bounded waiting-room admission, policy-local monotonic FIFO sequencing, stable duplicate joins, and owner-scoped status/cancellation.
- Self-authenticating, one-time-delivered admission tokens with 256-bit randomness, hash-only Redis storage, request/idempotency binding, bounded processing leases, and durable replay recovery.
- PostgreSQL-authoritative active-hold quotas and non-blocking per-API booking backpressure without changing the VARBIT seat allocator.
- Admission-worker lifecycle/health surfaces, global Redis admission rate/inflight enforcement, a three-API/two-worker non-sticky Compose topology, eight focused k6 scenarios, and honest benchmark/limitation documentation.
- Milestone 1.1 process-specific runtime configuration, Redis-aware outbox readiness, and a production rollout runbook for Migration 5.

- Modular-monolith architecture for Accounts, Railway Offering, Booking, Query, Event Relay, and Platform boundaries.
- Variable-length route-segment inventory with PostgreSQL `BIT VARYING`, deterministic locks, exact-count allocation, exact-mask release, and reconciliation.
- Temporary reservation hold, confirm, cancel, and expiration state transitions with concurrency tests.
- PostgreSQL-backed durable idempotency and transactional outbox records.
- JWT/RBAC foundations, bounded Prometheus metrics, environment validation, health endpoints, hold-expirer, and outbox worker lifecycle.
- Explicit ordered SQL migrations and local Docker Compose dependencies.
- Nine environment-driven k6 scenarios and a benchmark report template with no fabricated results.
- Read-only CI permissions and quality, migration, integration, secret, vulnerability, and container gates.
- Single-region production deployment guidance and hardened Kubernetes baseline manifests.

### Security

- Fixed shard-to-schema mappings and transaction-local `search_path` prevent
  public, Redis, configuration, or corrupted metadata from introducing an SQL
  identifier; database fencing rejects stale routes inside the mutation
  transaction.
- Public errors and telemetry hide topology, generations, migration IDs, SQL,
  DSNs, identifiers, and PII. Destructive migration/rollback/cleanup remains a
  private explicitly confirmed operator workflow.
- Source and target are never dual writable. A successful target mutation is
  recorded durably and forbids a direct mapping rollback that could discard
  committed state.

- Read caches remain outside booking interfaces; stale availability cannot bypass PostgreSQL segment-overlap, status, admission, quota, or idempotency checks.
- Cache generations and query hashes are bounded, version loss creates a fresh random namespace, production paths never enumerate cache keys, and cache/event payloads are excluded from logs and labels.
- Read-model workers receive PostgreSQL/Redis settings only, admin rebuilds default to dry-run, and reconciliation remains detect-only.

- Hot-run Redis and continuity failures fail closed; no waiting-room bypass can flood PostgreSQL. Admission and idempotency headers are excluded from logs/metrics by contract.
- Admission keyrings require exact 32-byte external keys, explicit issue/accept IDs, and API-first rotation. Committed Compose material is synthetic and local-only.
- Token theft or route, class, passenger-count, passenger, owner, fingerprint, and idempotency mismatch is rejected before inventory mutation.
- Registration returns one enumeration-resistant accepted response for both new and existing valid email addresses; registration no longer auto-logs in.
- Migration and startup database errors use bounded categories that discard credential-bearing DSN details.
- Production rejects an enabled log outbox publisher unless an explicit emergency override is set; worker manifests no longer mount unused JWT or Redis secrets.
- Raw passwords, JWTs, idempotency keys, passenger identifiers, DSNs, Redis credentials, and full event payloads are excluded from logs/metrics by contract.
- Runtime containers run non-root with a read-only root filesystem, dropped capabilities, and no privilege escalation in the deployment baseline.

### Known limitations

- Milestone 4 uses logical schemas in one PostgreSQL cluster and does not prove
  independent physical shard failure isolation, distributed transactions,
  zero-downtime rebalancing, or production capacity.
- Quiesced cutover may reject selected train-run writes, retained source data
  amplifies disk/backup scope, and post-cutover target writes require a reverse
  migration rather than a simple rollback.
- Milestone 4 load and benchmark evidence remains pending until controlled
  runtime results and complete reconciliation are accepted.

- Admission permits an attempt and does not guarantee a seat. Redis AOF does not guarantee queue continuity, token delivery is at-most-once, and account-level quota does not prevent Sybil identities.
- Projection/cache state can lag or disappear; availability is hint-only and cold generations can amplify PostgreSQL read load across replicas.
- No live production payment gateway/settlement integration, complete anti-bot
  platform, multi-region active-active writes, national-scale capacity
  evidence, or real passenger identity verification.
- Sustained load and multi-replica benchmark results remain unmeasured.
