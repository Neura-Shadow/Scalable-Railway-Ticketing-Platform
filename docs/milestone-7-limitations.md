# Milestone 7 Limitations

- Stripe is a production-oriented adapter selection, not a live production
  deployment. Standard CI uses the synthetic sandbox; optional Stripe test mode
  is secret-gated and must be reported separately. The DR drill's Stripe-shaped
  signature/rotation probe uses generated local secrets and a contract adapter;
  it is not evidence that a live Stripe endpoint secret was rotated.
- Hosted checkout minimizes payment-data scope but does not certify PCI DSS or
  SAQ eligibility. The repository accepts no PAN, CVC, track data, PIN, or raw
  payment credential.
- The financial ledger is operational reconciliation evidence, not statutory,
  tax, GAAP, or IFRS accounting.
- Partial refunds cover complete selected ticket fare snapshots only. There is
  no fee, exchange-rate adjustment, arbitrary amount, or partial fare refund.
- Provider and webhook delivery remain at-least-once. Stable identities,
  provider queries, and durable receipts provide convergence, not global
  exactly-once execution.
- The DR design is one active writer plus one passive region. It has no
  active-active writes, consensus service, automatic health-based promotion,
  or automatic failback.
- PostgreSQL streaming is asynchronous, so acknowledged data loss is possible.
  RPO and RTO are measured and environment-specific; zero is never inferred.
- The drill commits one bounded source marker per database immediately before
  each failover/failback fence, does not wait for that marker or final source
  LSN to replay, and derives missing records from the promoted target. Its
  acceptance bound is one missing drill marker and 512 MiB of missing WAL per
  database; those are laboratory release bounds, not production SLOs.
- External network, process, credential, and ingress fencing is required.
  Database epoch checks alone cannot stop an isolated old primary.
- pgBackRest verification is not restore proof; only an isolated boot and
  invariant run is accepted. Backup key loss is terminal for that repository.
- The Docker DR topology is a bounded same-host fault model, not production
  multi-region infrastructure or capacity evidence.
- Redis is rebuilt and is not payment, ticket, refund, ledger, settlement, or
  regional-authority durability.
- No Kafka, service mesh, Kubernetes operator, XA, cross-database transaction,
  generic distributed coordinator, or monolith decomposition is introduced.
