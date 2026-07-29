# Milestone 5 Limitations

Milestone 5 is deliberately bounded. Its acceptance evidence, when complete,
applies only to the recorded single-region disposable topology and workload.

- Topology is fixed at one control PostgreSQL and two booking PostgreSQL
  instances. There is no service discovery, split/merge, autoscaling or
  automatic rebalancer.
- The architecture remains a Go modular monolith with independently addressed
  databases. It is not a microservice, service-mesh, Kubernetes-operator or
  generic workflow-platform migration.
- Each train run has one writer. There is no active-active, multi-region,
  replica promotion or health-only failover.
- Cross-database booking is a durable saga with uncertainty windows. There is
  no XA, two-phase commit, distributed serializability or dual write.
- Event and journal delivery is at least once with receipts/deduplication. No
  exactly-once distributed processing claim is made.
- Base copy and catch-up are online, but final cutover has a measured bounded
  write pause. The design does not promise zero downtime.
- Source retention supports evidence, narrow rollback and reverse migration;
  it is not a current fallback replica. Cleanup is manual and gated.
- PostgreSQL row locks and local fences remain authority. Redis and caches are
  never booking, quota, routing or migration authority.
- The existing VARBIT segment inventory model is retained; no national-scale
  seat-layout redesign is included.
- The pilot does not add payment orchestration, durable payment compensation,
  ticket issuance saga, or other Milestone 6 behavior.
- Load tests on one recorded host do not establish sustained capacity,
  production sizing, RPO/RTO, disaster recovery, or national-scale throughput.
- Security review and synthetic failure tests are not production certification
  or a substitute for deployment-specific threat, backup and access reviews.
- Physical operator fare mutation supports only existing fares directly scoped
  to one train run. Route-level shared-fare fanout is rejected before shard
  execution and requires a separately designed workflow.
- Application route and generation fences constrain the normal runtime roles;
  a PostgreSQL superuser or equally privileged direct database actor can bypass
  application invariants. Deployment must use least-privilege roles, private
  database networks, audited administrative access, and separate migration
  credentials.
- API and reconciler route, fence, command, quota and repair metrics are
  persistently scrapeable. Migration administration remains a bounded
  one-shot CLI, so its phase/copy/replay/write-pause counters are durable
  structured evidence rather than a persistent Prometheus exporter. Startup
  pool-open failures likewise occur before a metrics listener exists. A
  long-running migration collector/controller is intentionally not introduced
  in this pilot.

Current runtime/load evidence is tracked in
[the benchmark report](benchmark-report-milestone-5.md). Any incomplete or
blocked scenario must remain explicit rather than converted into a readiness
claim.
