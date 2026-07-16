# Changelog

All notable changes to this project will be documented in this file. The project has not published a Milestone 1 release or version tag.

## Unreleased

### Added

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

- Raw passwords, JWTs, idempotency keys, passenger identifiers, DSNs, Redis credentials, and full event payloads are excluded from logs/metrics by contract.
- Runtime containers run non-root with a read-only root filesystem, dropped capabilities, and no privilege escalation in the deployment baseline.

### Known limitations

- No real payment integration, waiting room, multi-region active-active writes, national-scale capacity evidence, or real passenger identity verification.
- Sustained load and multi-replica benchmark results remain unmeasured.
