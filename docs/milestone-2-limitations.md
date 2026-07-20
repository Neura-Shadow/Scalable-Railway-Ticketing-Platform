# Milestone 2 Limitations

Milestone 2 is a single-region modular monolith with a Redis-backed hot-train
waiting room and PostgreSQL-authoritative booking. Its boundaries are:

- Admission permits a booking attempt; it does not guarantee a seat.
- PostgreSQL remains the only authority for segment overlap, seat allocation,
  reservations, durable idempotency, quotas, tickets, and outbox state.
- Redis is ephemeral control-plane state. AOF reduces loss risk but does not
  guarantee perfect queue continuity. Total Redis loss can lose entries,
  ordering, delivery opportunity, and tokens.
- Hot-run Redis failure is deliberately fail-closed. Customers may receive
  bounded `503` responses until operators restore or deliberately advance a
  policy generation.
- Fairness is FIFO within one policy generation by Redis sequence. It is not
  global, cross-policy, cross-region, identity-proof, or bot-resistant
  fairness.
- Token delivery is at-most-once over the network. A response lost after the
  Redis claim may require cancellation/expiry and rejoin.
- Per-instance backpressure bounds booking execution but is not a globally
  uniform API scheduling guarantee.
- Durable quotas limit held reservations per authenticated account. They do not
  prove real-world identity or prevent Sybil accounts.
- The system may admit more attempts than eventual inventory supports.
  PostgreSQL can safely reject after admission.
- The local multi-replica Compose topology is evidence scaffolding, not
  production capacity, high availability, or disaster-recovery architecture.
- No accepted sustained Milestone 2 benchmark is recorded. There is no
  national-scale or 12306-equivalent throughput claim.
- The system remains single-region with one authoritative PostgreSQL primary.
  It has no multi-region active-active writes, global consensus, shard
  migration, or regional Redis replication.
- No real payment gateway, refund workflow, rescheduling, email, SMS, real
  identity verification, CAPTCHA, frontend, mobile app, or complete anti-bot
  platform exists.
- No Kafka, service mesh, Kubernetes Operator, database sharding, or
  microservice extraction is introduced.
- Station, search, and availability Redis read caches remain deferred to
  Milestone 3. Current PostgreSQL read paths remain valid.
- The existing PostgreSQL VARBIT inventory model is unchanged; Redis never
  becomes inventory authority.
- Admission-state and quota reconciliation is intended to be detect-only.
  Production auto-repair is not provided.
- Policy mutation responses do not include a point-in-time queue/inflight
  impact preview for the invalidated generation. Operators currently use
  bounded counters and detect-only admission-state reconciliation; a
  partial-failure-safe preview is deferred.

These limitations are product and evidence boundaries, not promises to
implement every deferred feature next.
