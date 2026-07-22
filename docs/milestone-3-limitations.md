# Milestone 3 Limitations

- PostgreSQL source tables and seat inventory remain the only authorities. The
  projection and Redis caches are disposable observations.
- Projection updates and cache invalidations are eventually consistent. Lag is
  expected and observable; it is not promised to be zero.
- Availability is a short-lived hint. A later booking can legitimately
  conflict, and Redis failure never permits a booking bypass.
- Local singleflight bounds identical misses within one API process. It is not
  a distributed global lock; simultaneous cold misses can still occur across
  replicas.
- Search invalidation rotates one global generation. This is deliberately
  simple and bounded but can cold-start unrelated queries after an offering
  change.
- A source search fallback is correct but can be more expensive than a complete
  projection. PostgreSQL pool and query budgets remain deployment concerns.
- `rebuild-all` commits per train run/batch. It is resumable, not one global
  snapshot across every train run.
- Projection row growth is quadratic in route stop count per fare class. No
  production dataset sizing or lock-duration evidence is included.
- The repository includes load scripts and a benchmark template, but no
  accepted sustained Milestone 3 throughput or latency measurement.
- The system is a single-region modular monolith. There is no search
  microservice, OpenSearch, regional cache replication, multi-region
  active-active write, train-run database sharding, Service Mesh, Kafka,
  Kubernetes operator, or payment implementation.
- The multi-replica Compose topology is local functional evidence, not a
  production high-availability design. PostgreSQL and Redis remain single
  local instances in that topology.
- Redis persistence does not turn cache data into authority. A flush can reduce
  performance until refill and can disrupt existing Milestone 2 ephemeral
  admission state according to its fail-closed policy.
- Operator commands rely on deployment RBAC and a least-privilege database
  role. This repository does not provision a production secret manager,
  network policy, backup service, or audited approval workflow.
- No zero-downtime migration or backfill claim is made. Operators must measure
  locks, disk growth, and backfill duration on a production-like restore.
- The shared source stream is not blindly length-trimmed because Redis trimming
  can remove entries still present in a consumer-group PEL. Backlog capacity,
  alerting, and a group-floor-aware retention procedure remain deployment
  responsibilities; an extended worker outage can therefore increase Redis
  storage until recovery.
- A repeatedly failing event is DLQ-bound after bounded attempts, but its
  PostgreSQL progress intentionally keeps search on correct source fallback.
  Recovery requires the documented `read-model-admin resume-event` redrive
  after the dependency or data defect is repaired.
- No national-scale, 12306-equivalent, global fairness, or production capacity
  claim is supported.
