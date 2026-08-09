# Milestone 6 Limitations

Milestone 6 is deliberately bounded. Acceptance evidence, once recorded,
applies only to its named single-region disposable topology, synthetic payment
sandbox, fixtures, fault schedule, tool versions, and workload.

- The payment adapter is provider-neutral at the application boundary, but the
  only implementation is a deterministic local sandbox. There is no live
  gateway, merchant account, settlement, payout, dispute, chargeback, or
  production-provider evidence.
- Hosted/tokenized synthetic semantics reduce the designed card-data boundary;
  they are not PCI assessment, certification, or proof of a production checkout
  integration.
- Provider calls, control PostgreSQL, and booking-shard transactions are a
  durable saga. There is no XA, two-phase commit, distributed serializability,
  exactly-once external side effect, or zero-failure settlement guarantee.
- Webhooks and outbox delivery are at least once. Inbox IDs, operation
  idempotency, receipts, fingerprints, and reconciliation prevent duplicate
  logical effects; they do not create exactly-once transport.
- Unknown capture/void/refund outcomes can retain seats in `payment_review` or
  `refund_pending`. This conservative policy reduces double-sale risk at an
  inventory cost and requires alerting and staffed manual review.
- Automatic reconciliation is bounded and detect-first. It does not directly
  repair inventory, mint/cancel tickets, charge, refund, or bypass a shard
  command. The scheduled process is domain-detect-only; it writes only bounded
  reconciliation checkpoints and manual-review evidence. The operator CLI supports only
  explicitly confirmed replay/finalization from existing deterministic
  commands and immutable receipts; other mutations fail closed. Some
  mismatches still require explicit operator judgment.
- Ticket codes have an immutable control-plane identity directory. The M6
  rollout remains fail closed until `payment-admin backfill-ticket-codes
  --confirm` has claimed and verified every pre-existing locator. New payment
  issuance first reserves deterministic ID/code pairs in that directory and
  the shard then validates and uses exactly the reserved plan. Abandoned claims
  remain tombstones; they are not recycled.
- Milestone 6 supports full capture and full refund only. It omits partial
  capture/refund, split tender, installments, cancellation fees, FX, tax,
  invoices, accounting ledgers, disputes, chargebacks, and settlement matching.
- Payment is single-region. There is no multi-region active-active work,
  cross-region webhook ingress, regional provider routing, disaster-recovery
  payment failover, or RPO/RTO evidence.
- The modular monolith, PostgreSQL/Redis boundaries, two physical booking-shard
  pilot, and VARBIT inventory remain. There is no Kafka, service mesh,
  Kubernetes operator, generic workflow engine, or microservice split.
- Physical migration includes payment/ticket state and receipts, but final
  cutover still has the existing bounded write-pause/fencing model. It is not a
  zero-downtime or automatic-rebalancing claim.
- Ticket codes are opaque owner-facing identifiers, not signed offline boarding
  credentials. Offline validation, PDF/email/SMS delivery, Apple Pay, Google
  Pay, bank transfer, convenience-store payment, and frontend payment UI remain
  out of scope.
- Zero-fare reservations remain valid historical booking data but are not sent
  through the M6 provider workflow. Payment-intent creation rejects a zero
  amount before control or shard mutation; a future provider-free issuance
  policy must be designed explicitly rather than sending a zero-value charge.
- Only API ingress receives webhook-verification keys; worker,
  reconciler, and admin provider clients are outbound-only. Process
  configuration and application controls cannot protect against a
  compromised runtime host, database superuser, secret provider, payment
  provider, or authorized operator. Production needs separate least-privilege,
  egress, TLS, secret rotation, audit retention, backup, incident, and access
  reviews.
- Provider endpoint SSRF and redirect controls are designed at the adapter
  boundary. Their production adequacy depends on actual DNS, proxy, network and
  deployment configuration and must be revalidated for a real adapter.
- Local fault/load runs cannot establish production capacity, pool sizing,
  provider rate limits, financial SLOs, national-scale throughput, availability,
  or resource costs. Pool peaks and acquire waits are evidence for the recorded
  run only.
- Security scans, deterministic fault tests, independent reviews, and synthetic
  reconciliation are not formal verification, compliance certification, or a
  substitute for provider/deployment penetration and incident testing.

Runtime, migration, failure, load, security, container, and CI evidence belongs
in the Milestone 6 benchmark/QA artifacts. Until those artifacts contain direct
passing results, these documents describe intended design rather than verified
implementation. Any blocked or incomplete scenario must stay explicit.
