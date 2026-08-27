# Milestone 7 Production Payment and Disaster-Recovery Options

- Research date: 2026-08-10
- Scope: official and primary sources only
- Repository baseline: PostgreSQL 16, Go 1.25.13, pgx v5.10.0, deterministic
  sandbox provider, control schema v10, booking-shard schema v2
- Decision status: selected for bounded Milestone 7 implementation and evidence

## Executive decision

Milestone 7 will implement exactly one production-oriented payment adapter:
Stripe Checkout Sessions with PaymentIntents using manual capture, initially
restricted to card payments, plus Refunds, Balance Transactions, and Payouts.
The implementation pins `github.com/stripe/stripe-go/v86` v86.2.0 and Stripe
API/webhook version `2026-07-29.dahlia`. The deterministic sandbox remains the
mandatory fault and CI oracle. There is no automatic provider selection or
provider failover.

The database design uses PostgreSQL 16.14 asynchronous physical streaming with
one standby for control, shard 0, and shard 1. Each pair has one bounded
physical replication slot. pgBackRest 2.59.0 provides client-side encrypted
full/WAL backup, retention, PITR, repository verification, and isolated restore
validation. External fencing is mandatory before promotion. Failback restores
and reseeds the former region from the current writer and uses a newer regional
epoch; stale primary data is never started as authority.

These choices support a bounded disposable active-passive drill. They are not
evidence of production PCI compliance, zero RPO, geographic isolation,
capacity, settlement completeness, or live-provider suitability.

## Phase 0 repository findings

Four Milestone 6 defects must be corrected before new provider, refund, ledger,
or DR work can depend on the payment state machine:

1. A lost authorization webhook can strand `awaiting_customer` because the
   reconciliation candidate query omits it and current status comparison does
   not converge a provider-side authorization.
2. Provider uncertainty and shard actions increment the same saga attempt
   counter, so unrelated provider queries can exhaust the first ticket-issuance
   retry budget.
3. Mutating HTTP 500/502/503/504 responses are classified retryable rather than
   outcome-unknown, which can cause a second POST before status observation.
4. A normalized `captured` or `refunded` enum can advance state without exact
   cumulative captured/refunded totals.

The implementation gate is one shared financial-observation evaluator, one
normal convergence path for synchronous responses/webhooks/queries, and
purpose-built action attempts with independent identity, lease, backoff, and
counter. No second payment state machine is introduced.

## Provider comparison

| Capability | Existing sandbox | Stripe | Adyen | Decision |
|---|---|---|---|---|
| Hosted checkout | Synthetic hosted reference/token | Provider-hosted Checkout URL | Hosted Checkout redirect | Both candidates keep payment entry off-platform |
| Manual authorize/capture | Explicit synthetic authorize/capture | PaymentIntent manual capture; hosted completion can yield `requires_capture` | Manual capture for eligible methods | Select card-only methods with proven support |
| Server status lookup | Direct normalized status | Direct PaymentIntent retrieval plus Charge/Refund facts | Hosted session result does not refresh; provider directs integrations to webhooks | Stripe best matches query-before-retry |
| Idempotency retention | Durable local snapshot | Keys may be pruned after at least 24 hours | At least seven days | Stripe needs persisted object IDs and no key-only recovery |
| Partial refund | Deliberately unsupported in M6 | Repeated partial refunds up to remaining amount | Eligible-method dependent | Stripe cleanly supports whole-ticket partial refund |
| Settlement/payout | None | BalanceTransaction and Payout object APIs | Settlement report files and report credentials | Stripe has the smaller bounded importer seam |
| Webhook signing | Synthetic HMAC key ID and timestamp | Raw `Stripe-Signature`, timestamp, overlapping secrets | HMAC; propagation overlap has no published fixed duration | Provider-specific verifier required |
| Go/version fit | Repository code | stripe-go v86.2.0 requires Go 1.22 and pins current API | Adyen v21.2.2 requires Go 1.23, but documents Checkout v71 while hosted flow needs v72+ | Both are Go-compatible; Stripe has exact current API alignment |
| Test environment | Deterministic, restartable, fault-injectable | Stripe test/sandbox environments | Adyen test environment | Local sandbox remains mandatory; provider test mode is optional/secret-gated |

### Stripe selection

Stripe was selected because hosted Checkout, manual capture, direct
PaymentIntent retrieval, partial refunds, and object-addressable balance/payout
evidence align with the existing durable operation and query-before-retry
model. The adapter owns authentication, API version, request mapping,
idempotency, response bounds, webhook verification, pagination, state
normalization, and bounded error classification. Provider-specific types do not
enter saga, ledger, refund, booking, or public HTTP state.

Checkout uses `mode=payment`, a provider-hosted UI, card-only payment methods,
server-derived amount/currency, and
`payment_intent_data[capture_method]=manual`. The adapter returns separate
checkout-session and payment-object identities because a PaymentIntent may not
be available at initial session creation. Authorization is observed from
provider facts and a fresh PaymentIntent query; the sandbox-only synthetic
token does not become a production contract.

Capture uses the persisted PaymentIntent identity. Void cancels a
`requires_capture` PaymentIntent. Refund uses a stable request identity and the
exact server-derived whole-ticket amount. Status normalization combines
PaymentIntent, Charge, and Refund facts because a PaymentIntent can remain
`succeeded` after refunds.

Automatic network retries in the SDK are disabled or otherwise exposed to the
durable operation implementation. Every dispatched mutating timeout, reset,
truncated response, and 5xx is outcome-unknown. The next action is an object
status query, never a blind second POST.

### Strongest counterargument

Stripe guarantees idempotency retention only until a key is at least 24 hours
old, while Adyen documents at least seven days. This is material for delayed
recovery. The mitigation is architectural:

- persist Checkout Session, PaymentIntent, Charge, Refund,
  BalanceTransaction, and Payout IDs as soon as observed;
- query a known provider object before retrying;
- never treat the idempotency key as durable provider identity after its
  retention window;
- if an old unknown mutation has no provider identity, stop automatic progress
  and require reconciliation/manual review; and
- never generate a new key because the original operation remains unresolved.

### Why Adyen is not selected for this milestone

Adyen remains a viable future adapter. It is not selected because the current
tagged Go SDK documents Checkout v71 while Hosted Checkout requires v72+, its
hosted session result is explicitly non-refreshing, hosted checkout expands
shopper-email handling, modification outcomes are more asynchronous, and
settlement is a report-file pipeline with a separate report credential. These
facts increase M7 conformance and privacy scope. Go-version incompatibility is
not a rejection reason: the tagged module requires Go 1.23 and is compatible
with this repository.

## Provider security and conformance rules

- Never accept, proxy, persist, log, trace, queue, back up, or return PAN, CVC,
  track data, or PIN. A full provider-hosted redirect preserves the narrowest
  platform data path; it does not self-certify SAQ A or PCI compliance.
- Verify webhook signatures over provider-prescribed raw bytes, validate
  provider/environment/account/type, commit immutable deduplicated evidence,
  then return 2xx. Business processing is asynchronous.
- Stripe uses a nonzero five-minute signed timestamp tolerance, synchronized
  time, event-ID dedupe, and object/type business idempotency. Event order is
  never authority.
- Stripe endpoint secret rotation can sign with old and new secrets for up to
  24 hours. The local keyring stores activation/retirement metadata and accepts
  exactly one valid match.
- Provider selection, endpoint, version, credential, proxy policy, return URL,
  and webhook secrets are startup configuration, never request input.
- Test-mode credentials are optional protected CI secrets. Their absence is a
  truthful skip, not a pass. Provider test mode is not a load target or live
  settlement/compliance proof.

## Database replication comparison

| Strategy | Benefit | Material problem | Decision |
|---|---|---|---|
| Streaming standby only | Small failover delay | Mirrors corruption/deletes and has no historical recovery | Reject alone |
| Periodic backup only | Independent history | Longer restore RTO and backup-cadence RPO without WAL | Reject alone |
| Streaming plus encrypted full/WAL backup | Fast promotion plus independent PITR | More operational assets and restore testing | Select |
| Logical replication | Filtering and major-version flexibility | Does not replicate a complete cluster, DDL, sequences, or all object types | Reject |
| Application dual writes | Appears provider-independent | Partial commits require forbidden distributed coordination | Reject |
| Multi-primary | Writes in either region | Conflicts violate one-writer ticket/inventory/financial authority | Reject |

### Asynchronous mode decision

Milestone 7 uses asynchronous streaming because there is one standby per
database. Configuring it as the sole synchronous standby would make every write
wait for cross-region flush and could stop writes when that standby or link is
unavailable. Local `synchronous_commit=on` remains enabled for primary
durability, but no `synchronous_standby_names` is configured.

The strongest counterargument is that payment and ledger commits benefit from
synchronous durability. If a future deployment accepts regional RTT and write
unavailability when no synchronous standby exists, synchronous replication is
safer. It requires separate benchmarks and availability policy; it cannot be
inferred from the one-host Docker drill.

The evidence reports a nonzero-capable RPO. Per-database commit markers and LSN
positions measure control, shard 0, and shard 1 independently. Regional
observed RPO is the maximum observed gap, while all three values remain in the
artifact. RTO begins at incident declaration and ends only after fencing,
promotion, epoch advancement, reconciliation, worker/webhook activation,
ingress switch, and customer-write readiness.

### Replication rules

- Pin PostgreSQL 16.14 to an exact evidence image digest and enable data
  checksums.
- Use one dedicated TLS `LOGIN REPLICATION` identity and one bounded physical
  slot per database pair.
- Configure finite `max_slot_wal_keep_size` from measured peak WAL and the
  intended repair window; alert on `wal_status`, safe bytes, disk use, receiver
  state, replay position, and archive failures.
- Configure WAL archive fallback through pgBackRest and use the latest timeline.
- A lost slot or irrecoverable WAL gap requires a fresh standby reseed.
- PostgreSQL promotion is not fencing. External ingress, process, credential,
  database-network, and old archive-writer fencing succeeds first.
- Multi-host pgx connection strings and `target_session_attrs=read-write` only
  select a connection. They do not prove authority. Pools reset after route
  change and fresh connections verify database identity, primary role,
  timeline, schema, region, and epoch.

## Backup-tool comparison

| Tool | Reviewed version | Relevant strengths | Material limits | Decision |
|---|---|---|---|---|
| pgBackRest | 2.59.0 | Client-side AES-256-CBC, full/diff/incr, WAL, PITR, retention, JSON, check, repository verify | Passphrase loss is terminal; repository cipher setting cannot rotate in place | Select |
| WAL-G | 3.0.8 | Encrypted compressed backups and cloud-storage support | Encryption defaults off; verification and retention are more fragmented | Viable, not selected |
| Native PG16 | 16.14 | Physical base backup, SHA manifest, `pg_verifybackup` | No built-in encryption, retention, repository catalog, or complete runbook | Reject for M7 |

The pgBackRest key is injected from a separately protected secret source and is
never committed, logged, placed on the repository volume, or mounted into
application processes. Key rotation requires a new encrypted repository,
overlapping fresh backups, independent restore validation, then retirement of
the old repository. Checksum or repository verification alone is not restore
evidence. The isolated target must boot and pass schema, timeline, regional,
payment, ticket, refund, ledger, and settlement invariants.

Backup writer, retention/deletion, restore/decryption, promotion/fence,
application, settlement, and audit-reader identities remain distinct. Routine
processes cannot both promote a database and decrypt/delete recovery evidence.

## Failover and failback decision

Failover uses one typed resumable operation over exactly control, shard 0, and
shard 1. The sequence is external fence; record positions and potential RPO;
promote; verify primary roles/timelines; install one newer epoch; reset pools;
start recovery mode; reconcile financial and shard facts; enable workers and
webhook ingress; enable customer writes; record RPO/RTO; and leave the source
fenced. Health cannot auto-authorize any phase.

Failback never starts divergent old PGDATA. It restores fresh empty volumes
from the current active region, creates new slots, catches up, validates and
reconciles, fences the current active region, promotes the reseeded region with
a strictly newer epoch, and leaves the former writer passive. `pg_rewind` is
not the required M7 path because its prerequisites and failure modes are
stricter than a fresh restore/reseed acceptance path.

## Audit integrity

Database grants and immutable triggers are insufficient against a database
owner. Operational roles are insert-only, corrections append reversals, and
audit rows use a monotonic sequence plus previous/current hash. A periodically
signed checkpoint/root is exported with the records to separately protected
append-only/WORM storage. Its signing key is unavailable to the database,
application, backup writer, and evidence runner. Audit events include actor,
operation, database/shard, region epoch, fence decision, provider operation,
ledger/refund/reconciliation identity, bounded reason, UTC time, and key
version; they exclude secrets, raw webhook authorization material, and card
data.

## Official sources

### Stripe

- Hosted Checkout: <https://docs.stripe.com/payments/checkout>
- Separate authorization and capture:
  <https://docs.stripe.com/payments/place-a-hold-on-a-payment-method>
- PaymentIntent retrieval: <https://docs.stripe.com/api/payment_intents/retrieve>
- Idempotent requests: <https://docs.stripe.com/api/idempotent_requests>
- Advanced error handling: <https://docs.stripe.com/error-low-level>
- Webhooks and key rotation: <https://docs.stripe.com/webhooks>
- Partial refunds: <https://docs.stripe.com/api/refunds/create>
- Reporting and reconciliation:
  <https://docs.stripe.com/plan-integration/get-started/reporting-reconciliation>
- Payout reconciliation: <https://docs.stripe.com/payouts/reconciliation>
- Rate limits: <https://docs.stripe.com/rate-limits>
- stripe-go v86.2.0:
  <https://github.com/stripe/stripe-go/releases/tag/v86.2.0>

### Adyen

- Hosted Checkout: <https://docs.adyen.com/standard/integration/hosted-checkout>
- Idempotency: <https://docs.adyen.com/development-resources/api-idempotency>
- Webhook handling:
  <https://docs.adyen.com/development-resources/webhooks/handle-webhook-events>
- Webhook HMAC and rotation:
  <https://docs.adyen.com/development-resources/webhooks/secure-webhooks/verify-hmac-signatures>
- Settlement Detail report:
  <https://docs.adyen.com/reporting/settlement-reconciliation/transaction-level/settlement-details-report>
- Go v21.2.2:
  <https://github.com/Adyen/adyen-go-api-library/releases/tag/v21.2.2>

### PostgreSQL and pgx

- Warm standby and streaming replication:
  <https://www.postgresql.org/docs/16/warm-standby.html>
- Failover and fencing guidance:
  <https://www.postgresql.org/docs/16/warm-standby-failover.html>
- Continuous archiving and PITR:
  <https://www.postgresql.org/docs/16/continuous-archiving.html>
- Replication configuration:
  <https://www.postgresql.org/docs/16/runtime-config-replication.html>
- `pg_rewind`: <https://www.postgresql.org/docs/16/app-pgrewind.html>
- Multi-host connections:
  <https://www.postgresql.org/docs/16/libpq-connect.html>
- `pg_verifybackup`: <https://www.postgresql.org/docs/16/app-pgverifybackup.html>
- pgxpool v5.10.0: <https://pkg.go.dev/github.com/jackc/pgx/v5@v5.10.0/pgxpool>

### Backup and security

- pgBackRest 2.59.0 user guide: <https://pgbackrest.org/user-guide.html>
- pgBackRest commands: <https://pgbackrest.org/command.html>
- pgBackRest releases: <https://pgbackrest.org/release.html>
- PCI sensitive-authentication-data rule:
  <https://www.pcisecuritystandards.org/faqs/1533/>
- PCI hosted-page eligibility:
  <https://www.pcisecuritystandards.org/faqs/if-a-merchant-s-e-commerce-implementation-meets-the-criteria-that-all-elements-of-payment-pages-originate-from-a-pci-dss-compliant-service-provider-is-the-merchant-eligible-to-complete-saq-a-or-saq-a-ep/>
- NIST SP 800-53 Rev. 5.1, AU-9/CP-9/AC-5/AC-6:
  <https://csrc.nist.gov/pubs/sp/800/53/r5/upd1/final>

## Evidence and honesty limits

- No provider account, credential, contract, jurisdiction, enabled payment
  method, webhook endpoint, or settlement configuration was inspected.
- No Stripe/Adyen test-mode or live call was made during research.
- Authorization windows, card/network eligibility, disputes, chargebacks,
  merchant-of-record status, pricing, support, legal, privacy, accessibility,
  acquirer, and QSA decisions remain deployment work.
- A same-host Compose standby is not a regional failure domain.
- A local named-volume backup repository is not offsite, immutable, or durable
  production storage.
- A zero-loss asynchronous drill does not prove zero RPO.
- Restore success, provider test mode, synthetic load, and security scans do not
  establish live settlement correctness, capacity, compliance, or production
  readiness.
