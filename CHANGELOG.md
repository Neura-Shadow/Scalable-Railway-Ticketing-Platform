# Changelog

All notable changes to this project will be documented in this file. The project has not published a Milestone 3 release or version tag.

## Unreleased

### Added

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

- Admission permits an attempt and does not guarantee a seat. Redis AOF does not guarantee queue continuity, token delivery is at-most-once, and account-level quota does not prevent Sybil identities.
- Projection/cache state can lag or disappear; availability is hint-only and cold generations can amplify PostgreSQL read load across replicas.
- No real payment integration, complete anti-bot platform, multi-region active-active writes, national-scale capacity evidence, or real passenger identity verification.
- Sustained load and multi-replica benchmark results remain unmeasured.
