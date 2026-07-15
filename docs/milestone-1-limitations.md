# Milestone 1 Limitations

Milestone 1 proves the core reservation correctness model in a production-minded single-region backend. It deliberately does not claim product or infrastructure completeness.

## Explicit boundaries

- **Single region:** all authoritative booking writes target one regional PostgreSQL primary.
- **PostgreSQL authority:** train-run status, seat masks, reservations, tickets, idempotency, and outbox rows are not recoverable from Redis.
- **Availability is a hint:** search and availability may be cached or stale; create-hold always rechecks PostgreSQL.
- **No real payment integration:** confirmation represents a simulated successful domain confirmation. There is no authorization, capture, refund, settlement, chargeback, or payment saga.
- **No waiting room:** there is no admission queue, fair ordering, pre-sale token, or hot-event traffic shaping beyond ordinary rate controls.
- **No multi-region active-active writes:** regional write ownership, failover fencing, conflict resolution, and cross-region reservation writes are not implemented.
- **No national-scale claim:** current tests and scripts do not establish national throughput, fleet size, traffic peaks, or unlimited horizontal write scaling.
- **No real passenger identity verification:** passengers have minimal owner-scoped records; there is no government-ID validation, document verification, or external identity provider check.

## Product limitations

- No real payment gateway or payment state machine.
- No seat preference, adjacent-seat optimization, accessibility workflow, group booking, waitlist, exchange, partial cancellation, refund, promotion, coupon, tax, or dynamic pricing workflow.
- No notification, email, or SMS consumer.
- No complete operator-facing UI or customer frontend.
- Train-run cancellation blocks new holds but does not automatically mass-cancel or re-accommodate existing passengers.

## Scale and resilience limitations

- PostgreSQL is a single authoritative write boundary and therefore the booking write-scaling boundary.
- A sustained multi-replica benchmark, chaos test, regional failover exercise, recovery-time measurement, and disaster-recovery certification have not been completed.
- `SKIP LOCKED` can conservatively return an allocation conflict while suitable rows are temporarily locked.
- The repository provides load scenarios but currently records no accepted sustained capacity numbers.
- Kubernetes resource requests and replica counts are operational starting points, not measured recommendations.
- Holding/hoarding controls are limited; durable customer/train-run quotas and waiting-room admission are deferred.

## Operational limitations

- Reconciliation is an acceptance/audit invariant, not an automatic production repair mechanism.
- Production ingress, certificate management, cloud database/Redis provisioning, secret-manager integration, network policy, backups, restore automation, alert routing, and dashboards are deployment-environment responsibilities.
- Redis outage behavior depends on route class: cache reads may fall back, while protected write-route controls fail closed in production.
- Outbox publication is at least once; consumers must deduplicate by event ID.
- No payment, email, SMS, or notification side effects are emitted.

## Security limitations

- JWT validation and RBAC do not constitute real-world passenger identity proofing.
- Rate limiting is not a full anti-bot, device-risk, fraud, or denial-of-wallet system.
- Production security still depends on TLS, least-privilege identities, managed secret rotation, network isolation, dependency patching, scanning, backup protection, and operational review.

## Deferred direction

Milestone 2 may evaluate waiting-room admission, hot train-run protection, reservation quotas, train-run shard ownership, read-model optimization, regional caches, cache invalidation, sustained multi-replica benchmarks, chaos/failover testing, an optional payment saga design, and operational reconciliation tooling. None of those capabilities is implemented or implied by Milestone 1.
