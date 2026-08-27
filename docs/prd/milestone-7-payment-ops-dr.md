# Milestone 7: Production Payment Operations and Disaster Recovery

Status: Draft implementation baseline; official-source research, provider
selection, architecture, and threat-model gates complete

Target: Milestone 7

Depends on: Milestone 6 payment saga and durable ticket issuance

Last updated: 2026-08-11

## Problem Statement

Milestone 6 proves provider-neutral payment coordination, deterministic
synthetic checkout, durable payment intents and sagas, signed webhook inbox
processing, query-before-retry financial operations, shard-local ticket
issuance, full-refund compensation, and payment-safe physical-shard migration.
It does not connect a production-oriented provider, model provider settlement
evidence, support refunds for selected complete tickets, or recover the
authoritative control and booking databases after a regional failure.

Without those extensions, an operator cannot demonstrate that a concrete
hosted or tokenized provider behaves within the existing payment contract,
trace captured and refunded effects through an immutable operational ledger,
reconcile provider fees, settlements, and payouts, or recover payment and
ticket workflows through a controlled regional promotion. A provider timeout,
webhook retry, interrupted settlement import, partial-refund request, or failed
region could therefore leave correct behavior dependent on undocumented manual
judgment.

The platform needs a bounded next step that adds these operational capabilities
without changing its fundamental ownership model. Provider, control
PostgreSQL, and the authoritative physical booking shard still cannot share a
transaction. One region must remain the sole writer. The passive region must
not become active until operators have externally fenced the old region,
promoted all required databases, reconciled durable state, and explicitly
enabled the new regional epoch.

## Solution

Extend the Milestone 6 design with one production-oriented hosted or tokenized
payment-provider adapter selected only after official-source research. The
adapter will implement the existing provider-neutral contract, publish a
bounded capability profile, pin an explicit API version, and pass a reusable
deterministic conformance suite. The synthetic sandbox remains the default CI
provider. The production-oriented adapter remains disabled unless explicitly
configured, and optional provider test-mode validation runs only through a
manually triggered, secret-gated workflow.

Add an immutable, balanced operational financial subledger and idempotent
ingestion of normalized provider balance transactions, settlement batches, and
payout reports. Reconciliation will compare local payment operations, provider
evidence, ledger postings, settlements, and payouts. Mismatches create visible
manual-review evidence; reconciliation never charges, refunds, issues or
cancels tickets, or edits seat inventory directly. This subledger supports
operations and reconciliation only and is not statutory accounting.

Add whole-ticket partial refunds. An authenticated owner selects one or more
complete active tickets, while the server derives ownership, immutable fare
snapshots, amount, currency, current shard, and request fingerprint. Provider
refund success must be durably known before one fenced shard-local transaction
marks the selected tickets refunded and releases exactly their seat masks.
Unselected tickets remain active. Unknown provider outcomes retain inventory
and are queried before retry.

Harden webhook acknowledgement so a provider receives 2xx only after signature
and replay checks pass and the normalized inbox event commits durably. Support
bounded overlapping webhook verification keys and documented provider
credential rotation without logging or persisting key material.

Add a single-writer, active-passive disaster-recovery topology for control
PostgreSQL and both physical booking shards. The passive region uses independent
database storage, separate secrets, and write-disabled application processes.
Streaming standbys provide bounded promotion evidence; encrypted, checksummed
backups plus WAL recovery provide an independent restore path. Regional
failover and failback are operator-controlled, externally fenced, journaled,
idempotent, and resumable. The old region is reseeded from the current active
region before failback and receives a newer monotonic epoch; an old divergent
primary is never promoted directly.

The milestone records environment-specific observed RPO and RTO, replication
lag, webhook unavailability, saga recovery, ticket accessibility, refund
correctness, reconciliation, backup restore, and failback. It does not claim
zero data loss, zero downtime, active-active writes, exactly-once external
financial processing, production capacity, or production merchant readiness.

## Scope and Positioning

Milestone 7 extends rather than rewrites the Milestone 6 provider boundary,
sandbox, payment intent and saga state, signed webhook inbox,
query-before-retry operations, `payment_pending` reservation behavior,
shard-local ticket issuance, full-refund compensation, migration receipts,
control migration version 10, and booking-shard migration version 2.

The resulting system is:

- provider-adapter-ready, with one selected production-oriented adapter after
  an official-source research and ADR gate;
- single-writer across regions, with one active region and one passive DR
  region;
- active-passive for control PostgreSQL and the two bounded physical booking
  shards;
- operationally auditable through immutable financial and DR operation
  evidence;
- capable of complete-ticket subset refunds before a configurable cutoff; and
- validated through deterministic local and CI evidence, with optional
  secret-gated provider test-mode evidence kept distinct.

It is not PCI certification, a live production merchant deployment, statutory
financial accounting, exactly-once external financial processing, automatic
multi-region consensus, active-active payment or booking writes, a zero-RPO or
zero-RTO guarantee, zero-downtime regional failover, production SLO
certification, or national-scale capacity certification.

## Delivery Contract

Implementation starts from synchronized `main` commit
`7770c4f888c72508bba972fab9d27c93a23f29e4` and is performed only on
`feat/milestone-7-payment-ops-dr`. If the remote base advances before delivery,
the branch is reconciled deliberately without resetting or discarding user
work, and the actual reviewed base is recorded.

Changes are delivered as small logical commits after status, working diff, and
staged diff inspection. The final pull request targets `main`, is non-draft,
uses title `feat: add production payment operations and disaster recovery`, and
must be mergeable with every required check green. This workflow opens and
verifies the pull request but does not merge it, create a tag, or create a
GitHub Release. It does not begin Milestone 8.

## Phase 0: Milestone 6 Prerequisite Remediation

Milestone 7 must not place a production-oriented adapter, partial refunds, or
regional recovery on top of unresolved Milestone 6 correctness findings. Before
provider selection is implemented, the following four prerequisite gaps must be
fixed on the Milestone 7 branch and receive focused regression evidence:

1. **Lost authorization-webhook convergence:** a payment left in
   `awaiting_customer` after a missed authorization webhook must be discovered
   by bounded provider status reconciliation and converge through the normal
   authorization/capture path. Recovery cannot require the webhook to be
   replayed manually.
2. **Independent retry and decision budgets:** provider uncertainty attempts,
   ticket-issuance retries, and compensation decisions must not share one budget
   that can convert a first transient issuance failure into an immediate refund.
   A captured payment retains issuance retry/reconciliation opportunity unless
   an explicitly classified irrecoverable condition authorizes compensation.
3. **Ambiguous mutating-HTTP failures:** a timeout, disconnect, response loss,
   or provider 5xx/503 after a request may have reached the provider must become
   an uncertain financial outcome. Only a failure proven to occur before
   provider commit may retry directly; every ambiguous mutation requires status
   query before replay.
4. **Contradictory provider financial evidence:** captured or refunded status
   whose amount, currency, captured total, or refunded total contradicts the
   immutable intent and operation evidence must fail closed into visible manual
   review. It must not authorize ticket issuance, refund completion, shard
   compensation, ledger posting, or seat release.

Each remediation must include a deterministic failing-before/passing-after test,
concurrency and crash coverage where applicable, and explicit code/security
review. All four are release prerequisites, not Milestone 7 nice-to-have work.

## Actors

- **Customer:** authenticated reservation owner who pays through a hosted or
  tokenized provider surface, retrieves tickets, and requests refunds for
  selected complete tickets.
- **Payment worker:** advances durable payment and refund sagas using stable
  operation identities and query-before-retry semantics.
- **Webhook ingress:** authenticates provider events and acknowledges only
  after durable inbox persistence.
- **Settlement worker:** imports normalized provider financial reports through
  durable pagination checkpoints.
- **Payment operations operator:** inspects adapter, ledger, settlement,
  payout, refund, and manual-review state without directly mutating financial or
  booking authority.
- **Security operator:** provisions and rotates provider credentials, webhook
  verification keys, replication credentials, and backup encryption keys.
- **DR operator:** fences a failed region, promotes the passive region,
  reconciles it, controls ingress, and later reseeds and fails back.
- **Auditor or reviewer:** verifies immutable evidence, bounded claims,
  migration compatibility, security boundaries, and direct acceptance results.

## Required Journeys

### Production-provider journey

1. Official or primary provider documentation is compared against the bounded
   selection criteria before any provider is named in an ADR or implemented.
2. One hosted or tokenized provider is selected with explicit support and
   limitations for checkout, authorization, capture, status query, void,
   refunds, webhooks, settlement evidence, payouts, versioning, limits, and
   timeouts.
3. Startup accepts only an allowlisted provider type, explicit API version,
   fixed HTTPS endpoint, bounded timeouts and response limits, process-specific
   credentials, and a validated capability profile.
4. The adapter normalizes provider-specific requests, observations, and errors
   without leaking arbitrary provider state into the core domain or customer
   APIs.
5. Stable idempotency and query-before-retry remain mandatory. An uncertain
   financial mutation never causes an automatic switch to another provider.

### Provider test-mode journey

1. Standard CI exercises the sandbox and the production-oriented adapter
   against deterministic contract servers without provider credentials.
2. A live provider test-mode check is available only as a manual,
   protected-environment workflow when explicit test credentials are present.
3. The workflow uses only synthetic low-value test transactions, never selects
   live production mode, cleans up or refunds where practical, and emits only
   sanitized evidence.
4. Missing credentials cause a truthful skip rather than a failure or a
   fabricated pass. The milestone reports whether a real test-mode run occurred.

### Payment-ledger journey

1. A captured, issued, refunded, fee, settlement, or payout effect creates one
   immutable operational ledger transaction with at least two postings.
2. All postings use checked integer minor units, one currency, non-negative
   amounts, bounded account codes, and globally unique correlations.
3. Debit and credit totals must balance before commit. Committed postings cannot
   be edited or deleted through normal application paths.
4. Corrections append a balanced reversal referencing one existing unreversed
   transaction; history is never rewritten.
5. Ledger data excludes passenger PII, provider secrets, raw webhook bodies,
   raw settlement files, and raw credentials.

### Settlement-ingestion journey

1. A settlement worker claims one due provider account and retrieves bounded
   pages outside database transactions.
2. It normalizes balance transactions, settlement batches, settlement lines,
   payouts, fees, currencies, timestamps, and payload hashes.
3. Each page commits idempotently with a durable pagination checkpoint.
   Duplicate pages and restarts are harmless.
4. The worker applies bounded rate-limit and retry-after behavior, never loops
   indefinitely, and never mutates booking or ticket state.
5. Same provider identity with a changed normalized payload is retained as
   conflict evidence rather than overwritten.

### Payout-reconciliation journey

1. Reconciliation compares local capture and refund operations, provider
   balance transactions, settlement lines, payouts, and ledger postings.
2. It detects missing, duplicate, amount, currency, fee, aging, mapping,
   imbalance, and payout-total mismatches.
3. A clean result records bounded evidence. A mismatch records a durable
   manual-review case with sanitized correlations.
4. Reconciliation defaults to detect-only and cannot charge, refund, alter
   ledger history, issue or cancel tickets, or modify seat masks.

### Partial-ticket-refund journey

1. An authenticated owner submits selected opaque ticket identities and an
   idempotency identity; no amount, currency, fee, exchange rate, provider
   refund identity, or shard selection is accepted.
2. The server locks and validates the selected active tickets, owner,
   departure cutoff, immutable fare snapshots, currency, provider capability,
   current shard assignment, and cumulative captured/refunded totals.
3. It creates or replays one durable partial-refund request and provider refund
   operation using a stable fingerprint and identity.
4. Unknown provider outcomes enter uncertainty and are queried before retry.
   Selected seat masks remain occupied.
5. After refund success, one fenced shard-local transaction marks only the
   selected tickets refunded, releases only their exact segment masks, updates
   order and reservation aggregates, records receipts, and emits events.
6. Unselected tickets remain active. Selecting every remaining ticket converges
   to the existing full-cancellation outcome.

### Webhook-acknowledgement journey

1. Ingress identifies the allowlisted provider, enforces method, content type,
   body, parsing, and deadline bounds, then verifies signature, key identity,
   timestamp, and replay window over the exact request bytes.
2. It normalizes the bounded event and commits the inbox row before returning
   2xx.
3. A control-database failure, failed commit, or uncertain persistence returns
   a retryable failure so the provider can redeliver.
4. An exact duplicate returns success without another processing effect. A
   changed payload under the same event identity records a security conflict
   and cannot mutate payment state.
5. Acknowledgement never waits for a payment worker, provider query, booking
   shard, ticket issuance, or settlement worker.

### Webhook-secret-rotation journey

1. Security operators provision a new verification key to both regions through
   separate secret delivery while the current key remains accepted.
2. Readiness validates one primary identity, a bounded accepted set, and a
   bounded retirement grace period without exposing key material.
3. Old and new signatures work during overlap. After the grace period, the old
   identity fails closed.
4. Key material never enters the database, repository, logs, traces, metrics,
   health output, or evidence.

### Region-failure journey

1. Operators declare an incident, remove the old region from ingress, stop or
   isolate its writer processes, revoke or isolate write access, and confirm
   database fencing before promotion.
2. They inspect standby replay positions, expected data-loss window, backup
   health, uncertain financial operations, shard assignments, and DR-region
   configuration.
3. If external fencing cannot be proven, promotion stops and reports split-brain
   risk; database epoch checks alone are not presented as sufficient fencing.

### PostgreSQL-promotion journey

1. The DR operation records source positions and promotes control, shard 0, and
   shard 1 standbys in a deterministic, resumable order.
2. It verifies new timelines and write roles, increments the regional epoch
   exactly once, and installs consistent regional authority in control and
   physical shards.
3. The new application region starts in recovery mode. Customer writes remain
   disabled until control, shard, payment, ticket, ledger, and regional
   reconciliation pass.
4. Missing required shards yield an explicit bounded degraded state; requests
   never fall back to a random database or reveal topology.

### Webhook-recovery journey

1. The provider uses one stable global endpoint. During the bounded switchover
   it receives retryable failure rather than an in-memory acknowledgement.
2. The DR endpoint becomes eligible only after promoted control PostgreSQL can
   commit inbox rows and the correct keyring is present.
3. Retried events deduplicate against replicated inbox state. Processing may
   remain paused until affected physical shards reconcile.

### Payment-worker-recovery journey

1. DR workers enumerate checkout, authorization, capture, ticket issuance,
   refund, partial-refund, webhook, status-query, and settlement work at each
   durable transition.
2. They reuse original operation and command identities, resolve the current
   physical assignment, reject stale epochs, and query the provider after
   uncertainty.
3. No capture or refund is retried blindly. Shard receipts preserve internal
   one-effect behavior without claiming exactly-once provider execution.

### Ticket-retrieval-after-failover journey

1. After promotion and reconciliation, ticket lookup resolves through the
   replicated control directory and current physical shard.
2. Issued ticket identities, immutable fare snapshots, order state, and
   migration receipts remain unchanged across regional promotion.
3. Missing or inconsistent ticket evidence produces bounded unavailability or
   manual review, never a duplicate ticket or topology leak.

### Failback journey

1. The former primary remains fenced and its divergent data is archived or
   discarded according to runbook policy.
2. Fresh control and shard standbys are seeded from the current active region,
   catch up WAL, verify schema and timelines, and pass full reconciliation.
3. The current active region is externally fenced before the reseeded region is
   promoted under a strictly newer epoch.
4. Workers and ingress switch only after recovery checks. The former active
   region remains fenced and becomes passive.

### Backup-restore journey

1. Control, shard 0, and shard 1 produce encrypted, checksummed backups and WAL
   archives in storage independent of PostgreSQL data directories.
2. Encryption keys arrive through secret delivery and remain separate from
   backup storage and metadata.
3. A selected backup is restored into allowlisted, independent validation
   instances that cannot join the active topology.
4. Verification checks checksum, schema version, timeline, expected data,
   ledger balance, payment/ticket/refund consistency, and reconciliation before
   recording success.

### RPO/RTO-measurement journey

1. Before a bounded failover run, the evidence plan records topology,
   replication mode, workload, acceptance bounds, clocks, last durable markers,
   database positions, and start conditions.
2. Observed RPO is calculated from durable source markers and promoted state,
   reporting missing records and elapsed loss window even when both are zero.
3. Observed RTO is measured from the declared failure or service-unavailable
   boundary to the first reconciled, fenced, write-ready active-region result.
4. Webhook unavailability, payment-saga repair, ticket access, refund recovery,
   and failback have distinct timestamps and results.
5. Results remain environment-specific and are never generalized into zero-RPO,
   zero-RTO, zero-downtime, production SLO, or capacity guarantees.

## User Stories

### Provider onboarding and conformance

1. As a payment architect, I want provider selection gated by official or primary documentation, so that implementation decisions are evidence-based rather than inferred.
2. As a customer, I want payment entry hosted or tokenized by the selected provider, so that raw payment credentials never enter the railway platform.
3. As a payment operator, I want one explicit production-oriented provider type, so that deployments cannot silently select arbitrary adapters.
4. As a platform operator, I want the deterministic sandbox to remain the default CI provider, so that standard tests stay reproducible and credential-free.
5. As a security operator, I want the production-oriented adapter disabled unless explicitly configured, so that an accidental deployment cannot initiate provider operations.
6. As a developer, I want the provider API version pinned, so that upstream behavior cannot drift invisibly.
7. As a payment worker, I want provider-specific states normalized into bounded domain states, so that core saga behavior stays provider-neutral.
8. As a customer, I want unsupported provider capabilities to fail safely, so that the platform never pretends a refund or settlement feature exists.
9. As a payment operator, I want an explicit bounded capability profile, so that checkout, capture, refund, webhook, settlement, and payout support are inspectable.
10. As a test engineer, I want the sandbox and selected adapter to share an applicable conformance suite, so that contract differences are detected before deployment.
11. As a test engineer, I want deterministic coverage of timeouts before and after provider commit, so that unknown outcomes cannot be mistaken for definite failures.
12. As a payment worker, I want stable provider idempotency identities and identical retry parameters, so that safe retries converge on one financial operation.
13. As a payment worker, I want status retrieval before retrying uncertain mutations, so that response loss cannot cause a blind duplicate charge or refund.
14. As a security reviewer, I want provider endpoints fixed by startup configuration and HTTPS policy, so that customer input cannot turn the adapter into an SSRF client.
15. As a platform operator, I want bounded connection, request, redirect, header, and response limits, so that a slow or malformed provider cannot exhaust the service.
16. As a payment operator, I want rate limits and retry-after responses classified explicitly, so that workers back off without retrying forever.
17. As a security operator, I want process-specific provider credentials, so that compromise of one process does not expose unnecessary capabilities.
18. As a release reviewer, I want live provider test-mode results distinguished from deterministic contract results, so that no unsupported live-integration claim is made.
19. As a release reviewer, I want a missing test-mode secret to produce a truthful skip, so that absence of evidence is never reported as a pass.
20. As a customer, I want uncertain operations pinned to their original provider, so that automatic provider switching cannot create duplicate financial effects.

### Operational financial ledger and settlement

21. As a payment operator, I want every captured effect represented in an immutable operational ledger, so that local financial state is traceable.
22. As a payment operator, I want every refund effect represented in the ledger, so that refunded totals can be reconciled to the provider.
23. As a settlement operator, I want provider fees represented separately from ticket fare, so that fare history remains immutable.
24. As an auditor, I want every ledger transaction to balance debits and credits in one currency, so that malformed operational evidence cannot commit.
25. As an auditor, I want committed postings to be immutable, so that operational history cannot be silently rewritten.
26. As an auditor, I want corrections recorded as balanced reversals, so that both the original and correcting evidence remain visible.
27. As a developer, I want checked integer minor-unit arithmetic, so that floating-point rounding cannot change financial totals.
28. As a privacy reviewer, I want ledger records free of passenger PII and raw provider payloads, so that reconciliation does not expand sensitive-data exposure.
29. As a settlement operator, I want provider balance transactions imported idempotently, so that repeated pages do not duplicate evidence.
30. As a settlement operator, I want settlement batches and lines normalized, so that provider-specific reports can be compared with local operations.
31. As a settlement operator, I want payout reports ingested without initiating payouts, so that received provider evidence remains separate from bank actions.
32. As a settlement worker, I want durable pagination checkpoints, so that an interrupted import resumes without skipping or duplicating records.
33. As a settlement worker, I want external provider I/O outside database transactions, so that slow calls do not hold database locks.
34. As a settlement operator, I want duplicate provider identities with changed content retained as conflicts, so that tampering or provider corrections are visible.
35. As a payment operator, I want captures and refunds reconciled against provider balance transactions, so that missing financial effects are detected.
36. As a payment operator, I want fees reconciled against settlement evidence, so that net settlement differences are explainable.
37. As a payment operator, I want payout totals reconciled to their settlement lines, so that payout mismatches cannot be hidden.
38. As a payment operator, I want aged unsettled operations surfaced, so that delayed provider settlement receives bounded attention.
39. As a reviewer, I want settlement reconciliation to default to detect-only, so that an inspection cannot charge, refund, or change bookings.
40. As an operator, I want each mismatch to create a durable manual-review case, so that unresolved differences remain visible and auditable.
41. As an auditor, I want one currency per ledger transaction and no currency conversion, so that FX assumptions cannot enter this milestone.
42. As a release reviewer, I want the ledger labelled operational rather than statutory, so that the platform does not claim legal-accounting compliance.

### Whole-ticket partial refunds

43. As a customer, I want to refund one complete ticket from a multi-ticket reservation, so that the remaining passengers can still travel.
44. As a customer, I want to refund several selected complete tickets, so that I can cancel only the affected passengers.
45. As a customer, I want selecting every remaining ticket to converge on full cancellation, so that aggregate state remains understandable.
46. As a customer, I want the server to derive the refund amount from immutable fare snapshots, so that I cannot submit an arbitrary amount.
47. As a customer, I want the refund cutoff applied consistently before departure, so that eligibility is deterministic.
48. As a reservation owner, I want only tickets from my reservation to be selectable, so that another customer cannot refund them.
49. As a customer, I want a duplicate identical request to return one stable refund resource, so that client retries are safe.
50. As a customer, I want a changed ticket set under the same idempotency identity to conflict, so that one identity cannot represent two refunds.
51. As a payment worker, I want one stable provider refund identity per request, so that retries cannot duplicate the refund.
52. As a payment worker, I want an uncertain refund queried before retry, so that response loss cannot over-refund the customer.
53. As a payment operator, I want cumulative refunds bounded by captured amount, so that multiple partial refunds cannot exceed funds received.
54. As a customer, I want each ticket refundable at most once, so that duplicate selection fails safely.
55. As a customer, I want selected seats retained while the provider outcome is unknown, so that an unpaid release cannot resell inventory.
56. As a customer, I want selected seat masks released only after durable refund success, so that financial and inventory state stay ordered.
57. As a customer, I want unselected tickets and seats unchanged, so that a partial refund cannot cancel the rest of the reservation.
58. As a booking worker, I want ticket state and exact seat-mask release committed together, so that no ticket is refunded without its inventory transition.
59. As a booking worker, I want duplicate shard compensation to replay one receipt, so that retries cannot release a seat twice.
60. As a payment operator, I want partial-refund state preserved through physical-shard migration, so that cutover cannot repeat provider or seat effects.
61. As a customer, I want bounded processing and review states for unresolved refunds, so that uncertainty remains visible rather than appearing complete.
62. As a policy owner, I want no cancellation fee, partial fare refund, post-departure refund, or client-supplied fee in this milestone, so that the refund rule stays narrowly testable.

### Webhook durability and key rotation

63. As a provider, I want 2xx only after durable inbox commit, so that acknowledgement means the event can survive a process crash.
64. As a provider, I want a retryable failure when inbox persistence fails, so that I can redeliver the event.
65. As a payment worker, I want exact duplicate events to be harmless, so that provider retry cannot repeat a financial effect.
66. As a security operator, I want changed payloads under one event identity recorded as conflicts, so that replay or tampering remains visible.
67. As a security operator, I want signatures verified over exact bounded bytes with timestamp and replay checks, so that parsed-body ambiguity cannot bypass authentication.
68. As a platform operator, I want webhook acknowledgement independent of booking shards, so that a shard outage does not lose provider observations.
69. As a platform operator, I want webhook acknowledgement independent of payment workers, so that a worker backlog does not cause unnecessary redelivery.
70. As a security operator, I want overlapping old and new verification keys, so that planned rotation has no webhook downtime.
71. As a security operator, I want retired keys rejected after a bounded grace period, so that old credentials do not remain valid indefinitely.
72. As a DR operator, I want the same accepted key identities provisioned independently in the passive region, so that failover does not weaken verification.
73. As a security reviewer, I want key material absent from databases and observability, so that rotation metadata cannot disclose secrets.
74. As an operator, I want keyring consistency included in readiness, so that an invalid rotation blocks unsafe activation.
75. As a security operator, I want a documented provider credential rotation sequence, so that revocation follows verified deployment of the replacement.
76. As a security operator, I want no automatic fallback to a revoked credential, so that failures remain explicit and fail closed.

### Active-passive disaster recovery

77. As a DR operator, I want exactly one active writer region, so that control and booking authority cannot split across regions.
78. As a DR operator, I want the passive region write-disabled by role and epoch, so that standby processes cannot claim work accidentally.
79. As a database operator, I want independent standby storage for control and both physical shards, so that loss of primary volumes does not remove every copy.
80. As a database operator, I want replication lag and replay positions visible through bounded operator metrics, so that promotion risk is measurable.
81. As a DR operator, I want external fencing before database promotion, so that an isolated old primary cannot remain an application writer.
82. As a DR operator, I want failover to stop when fencing cannot be proven, so that automation does not hide split-brain risk.
83. As a DR operator, I want a monotonic regional epoch, so that stale processes and commands fail after promotion.
84. As a booking worker, I want both regional and physical-shard generation fences checked, so that regional recovery cannot bypass train-run ownership.
85. As a DR operator, I want failover steps durably journaled, so that a process crash can resume from a proven point.
86. As a DR operator, I want each promotion step idempotent, so that rerunning the command cannot increment epochs or promote databases twice.
87. As a DR operator, I want customer writes disabled during recovery reconciliation, so that inconsistent state cannot receive new mutations.
88. As a customer, I want healthy shard service to fail boundedly rather than route randomly when another promoted shard is unavailable, so that ownership remains correct.
89. As a provider, I want a stable webhook endpoint through failover, so that retry reaches the new active control database.
90. As a payment worker, I want replicated operation identities reused after failover, so that recovery cannot create a second charge or refund.
91. As a customer, I want captured-but-unissued work to issue one ticket after recovery, so that a regional outage does not strand paid travel.
92. As a customer, I want issued tickets retrievable after promotion, so that regional recovery preserves travel evidence.
93. As a customer, I want full and partial refunds to resume safely after promotion, so that regional failure does not lose financial state.
94. As a settlement operator, I want import checkpoints preserved across promotion, so that settlement ingestion resumes without duplication.
95. As a DR operator, I want old-region writers rejected after promotion, so that a stale process cannot mutate divergent state.
96. As a DR operator, I want Redis excluded from payment, booking, ledger, and regional authority, so that cache loss cannot corrupt durable correctness.
97. As an admission operator, I want hot-train admission to fail closed until DR Redis is ready, so that recovery never bypasses waiting-room controls.
98. As a product owner, I want waiting-room continuity limitations disclosed, so that Redis recovery is not overstated.

### Backup, restore, failback, and evidence

99. As a database operator, I want encrypted and checksummed backups for control and both shards, so that independent restore evidence exists.
100. As a security operator, I want backup encryption keys separate from backup storage, so that theft of one does not immediately disclose data.
101. As a database operator, I want WAL continuity monitored, so that point-in-time recovery gaps are visible.
102. As an auditor, I want restore performed into independent allowlisted validation instances, so that a backup is proven usable without touching active systems.
103. As an auditor, I want restore validation to include schema and cross-domain reconciliation, so that a booting database alone is not treated as success.
104. As a database operator, I want backup age and last successful restore-test age measured, so that stale protection is visible.
105. As a backup operator, I want deletion or expiration to require dry run and explicit confirmation, so that destructive administration is bounded.
106. As a DR operator, I want the old primary reseeded from the current active region before failback, so that divergent pre-failure data cannot return as authority.
107. As a DR operator, I want failback to use a newer regional epoch, so that authority remains monotonic.
108. As a DR operator, I want the current active region fenced before failback promotion, so that failback cannot create two writers.
109. As an auditor, I want observed RPO to report both missing records and elapsed loss window, so that a zero in one bounded run is not generalized.
110. As an auditor, I want observed RTO measured from a declared outage boundary to reconciled write readiness, so that the result has a reproducible definition.
111. As an auditor, I want webhook outage, saga repair, ticket retrieval, refund recovery, and failback timings reported separately, so that one aggregate cannot hide slow recovery.
112. As a release reviewer, I want exact source, configuration, database, provider-adapter, and evidence hashes, so that results bind to the tested tree and topology.
113. As a release reviewer, I want evidence summaries to be strict machine-readable data and transcripts labelled as logs, so that provenance and content type are honest.
114. As a security reviewer, I want evidence scanned for secrets, credentials, payment data, PII, DSNs, and topology details, so that validation artifacts do not widen exposure.
115. As a test engineer, I want deterministic barriers, fault hooks, database positions, and test clocks, so that correctness does not depend on arbitrary sleeps.
116. As a test engineer, I want failover crashes injected after every authority-changing boundary, so that restart behavior is proven rather than assumed.
117. As a migration operator, I want populated upgrades from control version 10 and shard version 2, so that Milestone 6 data remains usable.
118. As a migration operator, I want downgrade safety to block loss of retained financial evidence, so that rollback cannot silently discard irreversible state.
119. As a release reviewer, I want all Milestone 1 through 6 regressions and race tests preserved, so that new operations do not weaken prior guarantees.
120. As a release reviewer, I want all Critical and High independent findings resolved before PR opening, so that known release blockers are not deferred.

### Phase 0 prerequisite stories

121. As a customer, I want a missed authorization webhook recovered by provider status reconciliation, so that payment cannot remain indefinitely awaiting customer action.
122. As a paid customer, I want transient ticket-issuance retries separated from provider-attempt budgets, so that one temporary shard failure cannot trigger an unnecessary refund.
123. As a payment worker, I want ambiguous mutating HTTP failures classified as uncertain, so that a possible provider commit is queried before any retry.
124. As a payment operator, I want contradictory captured or refunded amount and currency evidence to fail closed, so that inconsistent provider data cannot issue tickets or release seats.

## Implementation Decisions

### 0. Phase 0 correctness gate

The four Milestone 6 prerequisite gaps are repaired through existing payment
provider, saga, worker, and reconciliation boundaries before new provider,
ledger, refund, or DR behavior depends on them. The remediation must not fork a
second payment state machine. Status reconciliation gains the missing
`awaiting_customer` convergence; operation execution records whether a mutation
is definitely uncommitted or uncertain; issuance retry policy has its own
bounded decision state; and one normalized financial-evidence validator rejects
amount, currency, captured-total, and refunded-total contradictions before any
authoritative downstream action. Existing stable identities and receipts remain
the only replay mechanisms.

The shared financial-observation evaluator is a pure bounded decision seam used
by synchronous responses, webhook processing, uncertain-operation recovery,
and reconciliation. For a full capture it requires the expected currency,
captured total equal to the immutable intent amount, and no unexplained refund.
For a full refund it requires captured and refunded cumulative totals equal to
the immutable capture. Partial-refund evaluation requires the exact requested
delta, monotonic cumulative totals, and a cumulative result no greater than the
capture. Contradiction produces manual review and no ticket, seat, ledger, or
compensation effect.

Provider-operation attempts and each shard action use separate durable
identities, leases, backoff, and counters. In particular, an issuance action's
budget is keyed by its saga and action identity and cannot inherit exhausted
provider-query attempts. Only the action's own bounded, safely classified
failure history may authorize its terminal compensation policy.

### 1. Official-source provider-selection gate

The official-source comparison selects Stripe Checkout Sessions backed by
PaymentIntents with manual capture, initially card-only, plus Refunds, Balance
Transactions, and Payouts. The adapter is pinned to stripe-go v86.2.0 and API
version `2026-07-29.dahlia`; the sandbox remains the deterministic CI and fault
oracle. Adyen was evaluated and rejected for this milestone, and no automatic
provider switching is permitted. The research document and ADR 056 record the
capability matrix, strongest counterargument, idempotency-retention limitation,
and the rule that recovery retrieves durable provider object identities before
considering same-key replay.

The completed comparison covers checkout, authorization and capture,
idempotency, status retrieval, webhooks and retries, full and partial refunds,
balance transactions, settlements, payouts, API versioning, limits, timeouts,
test mode, key rotation, and production secret handling. Any future provider
requires a new accepted capability/conformance review rather than a runtime
fallback path.

### 1a. Official-source PostgreSQL and security research gate

The same research document must cover PostgreSQL streaming replication,
replication slots, WAL retention, synchronous and asynchronous modes, lag,
base backups, point-in-time recovery, backup verification, standby promotion,
timeline changes, failback and standby reseeding, split-brain risk, client
reconnection, and connection-pool behavior during failover. It compares a
streaming standby alone, periodic backup alone, streaming standby plus
encrypted base backup and WAL recovery, logical replication,
application-level dual writes, and multi-primary active-active. It must explain
why the selected bounded design keeps one primary region, uses streaming
replication plus an independent encrypted restore path, and rejects dual writes
and multi-primary operation.

Security research must use primary sources to cover PCI sensitive
authentication-data restrictions, payment-webhook replay protection, backup
encryption, provider and webhook secret rotation, regional credential
separation, and audit-log integrity. PostgreSQL and security conclusions are
research gates equal to provider selection; ADRs 063 and 064 cannot be accepted
from design preference alone.

### 2. Deep provider adapter

The provider adapter remains a deep module: its small normalized interface
covers hosted checkout, payment observation, authorization, capture, void,
full and capability-gated partial refund, webhook verification, and optional
settlement and payout listing. Internally it owns provider authentication,
version headers, request mapping, idempotency headers, bounded HTTP behavior,
pagination, response validation, state normalization, and error
classification. Provider-specific types cannot leak into payment saga, ledger,
refund, booking, or public API state.

The adapter cannot write payment, reservation, ticket, ledger, settlement,
migration, or outbox storage. It returns normalized observations to the
existing coordinators. It cannot accept a request-controlled endpoint or
automatically choose another provider after an uncertain operation.

### 3. Bounded capability profile

Each adapter exposes one typed, startup-validated capability profile for hosted
checkout, authorization, capture, void, full refund, partial refund, payment
status query, settlement transactions, payout reports, webhook signatures, and
webhook key rotation. Capability values and metric labels come from fixed
allowlists, not an arbitrary string map. Required but absent capabilities fail
startup or disable the corresponding worker/API safely. The selected adapter's
profile is an ADR-backed fact, not a customer-controlled setting.

### 4. Reusable conformance harness

A provider-neutral conformance harness drives any adapter through deterministic
HTTP contract behavior. It owns fault injection for pre-commit timeout,
post-commit response loss, rate limit, server failure, malformed response,
unknown state, duplicate request, pagination replay, and webhook signature/key
rotation. It asserts normalized external behavior and stable identities rather
than internal implementation calls. The sandbox and selected adapter run all
applicable cases; unsupported optional capabilities must report bounded
unsupported results rather than silently pass.

### 5. Immutable operational financial ledger

The ledger is a deep module with one append-only transaction interface and one
reversal interface. Callers provide a bounded financial event identity,
correlation, currency, and typed postings. The module validates uniqueness,
checked minor-unit arithmetic, allowed accounts, non-negative values, same
currency, at least two postings, and equal debit/credit totals in one local
transaction. Committed transactions and postings have no normal update/delete
path. Reversal appends a new balanced transaction and atomically prevents
double reversal.

The initial bounded account model covers pending customer funds, ticket sales,
provider receivable, provider refund receivable, provider fee expense,
settlement cash or clearing, and reconciliation suspense. Exact capture,
issuance, refund, fee, settlement, and refund-settlement mappings require the
ledger ADR after provider research. No mapping may imply GAAP, IFRS, tax, legal,
or merchant-of-record accounting.

### 6. Settlement import boundary

The settlement importer is a deep resumable module. Its public operation is a
bounded `run once` for one configured provider account. Internally it claims a
due account in a short transaction, performs bounded provider pagination
outside transactions, normalizes records, commits idempotently, records payload
hash conflicts, and advances a durable cursor only with the corresponding
page. It handles bounded rate limits, partial failures, graceful cancellation,
and restarts without spawning unbounded work. It has no booking mutation
capability.

Normalized evidence includes provider transaction identity, payment
correlation where available, operation type, gross, fee, net, currency,
availability and creation times, settlement identity and period, payout
identity and arrival status, payload hash, and import time. Raw provider reports
are not stored by default; any later raw-report requirement needs a separate
encrypted design and data-retention review.

### 7. Detect-only settlement reconciliation

The reconciliation module accepts bounded scopes such as payment, period,
settlement, or payout and produces immutable run results and mismatch records.
It compares provider evidence, existing payment operations, ledger
transactions, and settlement/payout totals. It can mark reviewed through a
separate role-controlled audit action, but it cannot change provider state,
ledger history, ticket state, or inventory. Corrections must flow through
normal idempotent domain commands and ledger reversals.

### 8. Whole-ticket partial-refund orchestrator

The partial-refund orchestrator owns owner authorization, selected-ticket
normalization, deterministic locking order, cutoff policy, immutable fare
snapshot calculation, checked cumulative-refund validation, capability checks,
idempotency fingerprinting, saga creation, stable provider operation identity,
uncertainty handling, and finalization. Its interface accepts only selected
ticket identities plus an idempotency identity. Amount, currency, fee, provider
identity, and shard remain server-derived.

The control model records one immutable refund request, selected items, saga,
provider operations, and manual-review state. Selected tickets move to
`refund_pending`; unresolved provider state moves the saga to uncertainty or
manual review without inventory release. Provider success authorizes one
shard-local compensation command. A refund request never becomes its own
general workflow engine.

### 9. Shard-local selected-ticket compensation

The shard command resolves the current assignment, verifies storage and
generation fences plus the regional epoch, locks reservation and selected
tickets deterministically, validates the exact selected set, refund proof,
amount, and currency, and acquires one unique compensation receipt. In the same
transaction it changes selected ticket state, releases exactly their segment
masks, updates ticket-order and reservation aggregates, journals migration
state, and appends bounded outbox events. Replay returns the committed result;
any mismatched set or proof conflicts before mutation.

### 10. Durable webhook acknowledger

Webhook ingress remains a deep module whose successful result means one
authenticated normalized event is durable. It combines request bounds,
provider allowlisting, exact-byte signature verification, timestamp/replay
validation, bounded parsing, event normalization, payload hashing, durable
deduplication, and conflict evidence. It performs no provider call, financial
mutation, ticket work, or shard access inline. Only a committed inbox row or
proven exact duplicate receives 2xx; persistence failure receives a retryable
response.

### 11. Rotating webhook keyring and provider credentials

Webhook key material is supplied only through the process secret mechanism.
The typed keyring has one primary identity where outbound synthetic signing is
needed, a bounded accepted set, activation and retirement metadata, and a
bounded grace policy. Rotation follows add, accept, verify, retire, and remove.
Unknown or ambiguous identities fail closed.

Provider credentials are provisioned separately for active and DR regions and
separately from sandbox keys. Controlled process restart is the default reload
mechanism unless later implementation proves safe hot reload. Rotation verifies
readiness and a status query before old credentials are revoked; there is no
automatic fallback after revocation.

### 12. Regional write-authority module

Regional authority is a deep, local-transaction validation module used by every
control and physical-shard write path. It validates bounded deployment region,
deployment role, configured epoch, durable active region, durable current
epoch, writes-enabled state, and existing physical-shard generation fence. The
epoch can only increase. Passive and recovery roles reject normal customer and
worker writes until explicitly enabled.

This database check is defense in depth, not consensus. A disconnected old
primary can retain stale authority; therefore external ingress, process,
credential, and network fencing is a mandatory promotion precondition.

### 13. Active-passive database recovery boundary

The bounded topology contains one primary and one streaming standby for control,
shard 0, and shard 1, with independent data directories and region networks.
Standbys remain read-only and are never application write targets before
promotion. PostgreSQL 16.14 uses asynchronous physical streaming with one
bounded physical replication slot per control or shard pair, finite retained
WAL, and archive recovery through pgBackRest 2.59.0. Replication lag, replay
position, timeline, slot retention, archive health, and credential separation
are monitored. The design exposes possible loss and reports measured per-
database and worst-case RPO; it never infers zero loss from an asynchronous or
same-host drill.

### 14. Encrypted backup and restore verifier

One reviewed backup mechanism will provide full backups and WAL recovery for
all three databases. The backup module records bounded database role, shard
identity, source position, checksum, encryption state, creation time, retention
state, and restore-test evidence without keys or storage credentials. Backups
live outside database volumes and are encrypted at rest with separately
provisioned keys.

The restore verifier permits only allowlisted isolated targets, verifies
checksum before restore, blocks active-topology destinations, performs schema
and timeline checks, and runs payment, ticket, refund, ledger, settlement, and
regional reconciliation. A copied Docker volume or a command exit code without
data verification is not acceptance evidence.

### 15. Resumable DR operation runner

Failover and failback use one durable operation record with source/target
regions, source/target epochs, per-database positions, bounded state, timestamps,
and bounded error category. Every authority-changing step has a precondition,
idempotent effect, durable completion marker, and safe resume behavior.

Failover order is: prove external fencing; record positions and expected RPO;
promote control and required shards; verify roles and timelines; install one
new epoch; start applications in recovery; reconcile; enable workers; switch
webhook ingress; enable customer writes; record RPO/RTO; mark the target active;
and keep the source fenced. Customer writes cannot precede reconciliation.

Failback never reuses stale primary data. It keeps the old region fenced,
re-seeds every database from the current active region, catches up and
reconciles, externally fences the current writer, promotes the reseeded region
under a newer epoch, switches ingress and workers, and leaves the former writer
passive.

### 16. Payment and ticket recovery reconciler

The recovery reconciler inspects regional authority, payment intents and sagas,
provider operations, webhook inbox, shard receipts, ticket issuance, full and
partial refunds, ledger postings, settlement checkpoints, reservation
directory, outboxes, migration journals, and backup/replication evidence. It
repairs only proven control finalization or invokes the same fenced idempotent
commands used normally. All other discrepancies become manual review. It never
edits seat masks, invents tickets, or blindly calls the provider.

### 17. Redis remains non-authoritative

Each region has separate Redis state for waiting-room coordination, cache,
rate limiting, and configured event transport. Redis is never payment, booking,
ledger, settlement, webhook-inbox, or regional-write authority. Caches rebuild;
payment and ticket recovery comes from PostgreSQL. Waiting-room continuity may
be lost, and hot-train admission fails closed until the active-region Redis
policy is ready. Redis restore is not a prerequisite for financial correctness.

### 18. Schema evolution and compatibility

Control schema evolution advances from version 10 to version 11 for provider
capabilities, immutable ledger evidence, normalized settlement and payout
evidence, partial-refund coordination, regional authority and DR operations,
and backup/restore metadata. Booking-shard evolution advances from version 2 to
version 3 for selected-ticket refund states and receipts, regional epoch
fencing, migration journal coverage, and reconciliation evidence.

Fresh install, repeat application, populated upgrade, readiness, and safe
rollback or explicit downgrade blocking are required. Existing Milestone 6
sandbox, payment, ticket, full-refund, and migration data remain compatible.
No automatic schema migration, cross-database foreign key, dual write, or
network-service split is introduced.

### 19. Bounded observability

Metrics cover adapter operations and classifications, ledger transactions and
imbalances, settlement imports and mismatches, partial-refund results, webhook
acknowledgement and key rotation, regional epoch and write rejections,
replication lag, failover/failback duration, observed RPO/RTO, backup age,
restore-test age, and checksum failures. Labels are restricted to bounded
provider, operation, result, reason, capability, currency, region, database
role, shard, and phase values.

Identifiers, users, passenger data, provider transactions, settlement/payout
identities, event identities, DSNs, hosts, IPs, WAL positions, backup paths,
key identities from arbitrary input, credentials, and secrets are forbidden
labels and public health fields. Public readiness exposes only bounded service
state; detailed topology and positions are operator-only.

### 20. Modular-monolith boundary

All modules remain in the existing modular monolith and worker/administration
process model. This milestone does not split network services or introduce
Kafka, a service mesh, Kubernetes operators, XA, two-phase commit, a generic
distributed transaction coordinator, application-level database dual writes,
global distributed locks, or a redesign of PostgreSQL VARBIT seat inventory.

## Required Interface and Configuration Contract

The following names are part of the Milestone 7 acceptance surface. Research
may determine provider-specific values, but implementation must not silently
rename, omit, or replace these bounded contracts.

### Provider capabilities

The typed capability set contains exactly these Milestone 7 capability names:

- `hosted_checkout`;
- `authorize`;
- `capture`;
- `void`;
- `full_refund`;
- `partial_refund`;
- `payment_status_query`;
- `settlement_transactions`;
- `payout_reports`;
- `webhook_signatures`; and
- `webhook_key_rotation`.

The selected provider must support every capability required by the Milestone
7 customer and operations journeys, including partial refund, settlement
transactions, and payout reports. Capability gating remains mandatory so a
misconfigured or future adapter fails safely, but it cannot be used to waive a
Milestone 7 acceptance requirement.

The provider configuration surface includes:

- `payment_provider_type` with only `disabled`, `sandbox`, and the selected
  provider enum;
- `payment_provider_api_version`;
- `payment_provider_base_url`;
- `payment_provider_account_id`;
- `payment_provider_api_key`;
- `payment_provider_webhook_keyring`;
- `payment_provider_connect_timeout`;
- `payment_provider_request_timeout`; and
- `payment_provider_max_response_bytes`.

The source contract names both `payment_provider_webhook_keyring` and the
existing `payment_webhook_keyring`. They are compatibility aliases for one
typed ingress verification keyring, not two independent secret stores.
Sandbox/M6 configuration may continue to use `payment_webhook_keyring`; the
selected production adapter configuration uses
`payment_provider_webhook_keyring`. If both names are supplied, their parsed
key IDs and bytes must be identical or startup fails; neither has precedence.
Milestone 7 adds `payment_webhook_primary_key_id`,
`payment_webhook_accept_key_ids`, and
`payment_webhook_key_retirement_grace_seconds` over that one parsed keyring.

### Settlement, refund, and regional configuration

Settlement configuration includes `settlement_worker_enabled`,
`settlement_worker_interval_seconds`, `settlement_worker_page_size`,
`settlement_worker_max_pages_per_run`, `settlement_worker_max_attempts`, and
`settlement_reconciliation_lookback_days`.

Whole-ticket refund policy uses
`ticket_refund_cutoff_minutes_before_departure`. The customer request carries
only selected ticket IDs and an idempotency key. The intended customer surface
is `POST /api/v1/ticket-orders/{order_id}/refunds` and
`GET /api/v1/ticket-refunds/{refund_request_id}`. Amount, currency, fee,
provider identity, provider refund identity, exchange rate, and shard identity
are not request fields.

Regional configuration includes `deployment_region`, `deployment_role`,
`region_epoch`, `regional_writes_enabled`, `dr_failover_enabled`, and
`dr_required_database_count`. Deployment role is one of `active`, `passive`,
or `recovery`. Public regional health is limited to `active`, `passive`,
`recovery`, `degraded`, or `fenced`.

### Active-passive Compose process topology

`docker-compose.dr.yml` is self-contained about the bounded process shape.
Region A contains three API replicas, payment workers, the payment reconciler,
the settlement worker, admission workers, read-model workers, the hold-expirer,
the outbox worker, control PostgreSQL primary, shard-0 PostgreSQL primary,
shard-1 PostgreSQL primary, Redis A, and proxy A. Region B contains three
passive API replicas, passive payment workers, a passive payment reconciler, a
passive settlement worker, control PostgreSQL standby, shard-0 PostgreSQL
standby, shard-1 PostgreSQL standby, Redis B, and proxy B.

Shared bounded test infrastructure contains the payment sandbox, selected
provider contract server, an independent backup destination or object store,
and global test ingress/router. PostgreSQL data volumes and regional networks
are separate; either region can be isolated; no sticky sessions are required;
old ingress, database promotion, and webhook routing are explicit controls;
backups are outside database volumes; teardown is project-scoped; and rendered
evidence exposes no credential. This one-host Compose topology is never called
production multi-region infrastructure.

### Required administration interfaces

`cmd/settlement-worker` exposes a deterministic bounded `RunOnce(ctx)` path.
`cmd/settlement-admin` supports `inspect-batch`, `inspect-payout`,
`inspect-transaction`, `reconcile-period`, `reconcile-payment`,
`reconcile-payout`, `export-sanitized-report`, and `mark-reviewed`; none may
perform a destructive financial mutation.

`cmd/dr-admin` supports the confirmed failover form
`dr-admin failover --from <region> --to <region> --confirm` plus
`prepare-failback`, `reseed-region`, `validate-failback`, and `failback`.
`cmd/backup-admin` supports `backup-control`, `backup-shard`, `verify-backup`,
`list-backups`, `restore-validation`, `inspect-wal-archive`,
`inspect-retention`, and `expire-backup`. Destructive expiration requires dry
run plus explicit confirmation.

Detect-only DR reconciliation exposes `regional-authority`, `payment-ledger`,
`settlements`, `partial-refunds`, `dr-control`, `dr-shards`, `backups`, and
`dr-all` scopes. No scope directly repairs seats or invents financial effects.

## Required Persistence Contract

Control migration version 11 adds the following bounded relations, with
application-generated identities, immutable evidence, checked minor-unit
money, bounded states, and no key material, raw provider reports, passenger PII,
or raw webhook bodies:

- `financial_ledger_accounts`;
- `financial_ledger_transactions`;
- `financial_ledger_postings`;
- `financial_ledger_reversals`;
- `provider_balance_transactions`;
- `provider_settlement_batches`;
- `provider_settlement_lines`;
- `provider_payouts`;
- `provider_payout_lines`;
- `settlement_reconciliation_runs`;
- `settlement_reconciliation_mismatches`;
- `ticket_refund_requests`;
- `ticket_refund_request_items`;
- `ticket_refund_sagas`;
- `ticket_refund_operations`;
- `ticket_refund_manual_reviews`;
- `regional_write_authority`; and
- `regional_failover_operations`.

Provider capability metadata, webhook rotation metadata without secret
material, durable import checkpoints, and backup/restore verification metadata
are also control-owned. Booking-shard migration version 3 adds selected-ticket
refund states and receipts, exact selected-mask compensation evidence,
shard-local regional authority and epoch fencing, reconciliation metadata, and
mutation-journal coverage.

M7 does not reuse the M6 one-full-refund-per-intent uniqueness rule for
ticket-subset refunds. Each partial refund is keyed by
`ticket_refund_request_id`, while transaction-safe cumulative constraints still
prevent total refunds from exceeding capture. Shard receipts bind the request,
selected ticket identities, immutable fares, exact seat masks, money, currency,
provider proof, generation, region, and epoch.

The initial ledger account allowlist is `customer_funds_pending`,
`ticket_sales`, `provider_receivable`, `provider_refund_receivable`,
`provider_fee_expense`, `settlement_cash`, and `reconciliation_suspense`.
Postings use a positive integer amount plus an explicit debit or credit side;
negative postings are forbidden. ADR 058 freezes the exact balanced flows.

Selected tickets terminate as `refunded`. Ticket `cancelled` retains its
existing non-refund meaning. The reservation becomes `cancelled` and the order
becomes `refunded` only when no active ticket remains; otherwise their
terminal usable state is partially refunded. Ticket-selection refunds apply
only to issued active tickets and debit ticket-sales evidence. The existing
captured-but-unissued full-compensation path remains separate.

The bounded partial-refund states are:

- reservation: `confirmed`, `partially_refund_pending`,
  `partially_refunded`, `refund_pending`, `cancelled`, or `payment_review`;
- ticket order: `issued`, `partial_refund_pending`, `partially_refunded`,
  `refund_pending`, `refunded`, or `manual_review`;
- ticket: `active`, `refund_pending`, `refunded`, or `cancelled`; and
- refund saga: `created`, `validating`, `refund_pending`,
  `provider_uncertain`, `refund_succeeded`, `shard_compensating`, `completed`,
  `manual_review`, or `failed`.

`regional_write_authority` uses bounded authority states `active`, `draining`,
`fenced`, `promoting`, `recovery`, and `failed`. A failover operation uses
`planned`, `source_fencing`, `source_fenced`, `promoting_control`,
`promoting_shards`, `reconciling`, `enabling_workers`, `switching_ingress`,
`active`, `failed`, or `manual_review`. The durable record includes source and
target region and epoch, per-database positions, actor, reason, phase
timestamps, and a bounded error category.

The operator-controlled failover protocol is fixed in this order:

1. verify external fencing;
2. record control and shard replication positions;
3. remove passive write-readiness advertisement;
4. promote control;
5. promote shard 0;
6. promote shard 1;
7. verify roles and timelines;
8. allocate exactly one newer epoch;
9. install target control authority in `recovery`;
10. install matching local shard authority and fence state;
11. start APIs in recovery mode;
12. reconcile control, shards, payments, tickets, refunds, ledger, and routing;
13. enable payment workers;
14. enable settlement and reconciliation workers;
15. switch durable webhook and global ingress;
16. enable the customer-write process configuration while readiness remains
    gated;
17. record observed RTO;
18. record observed RPO;
19. atomically mark the target `active`, making customer writes ready; and
20. retain the source as externally fenced.

Every phase is idempotent and resumable. Crash tests cover after source
fencing, after control promotion, after one and all shard promotions, before
and after epoch installation, before worker enablement, and before and after
ingress switch. No crash point may reopen the source, authorize the target
early, or allocate the epoch twice.

## Specification Clarifications

The source request leaves several choices intentionally open. The following
decisions prevent implementation-time guesswork while preserving the required
research gates.

1. Global provider default remains `disabled`. CI and test configuration
   explicitly select `sandbox`; production rejects sandbox unless the existing
   disposable-test-only override is deliberately enabled.
2. Stripe is the selected provider and must meet the partial-refund,
   settlement-transaction, and payout-report requirements through capability-
   gated interfaces. The pinned SDK and API version are part of the conformance
   contract, not ambient deployment defaults.
3. The official-source sandbox, Stripe, and Adyen comparison and ADR 056 are
   accepted inputs to implementation. A second production adapter is out of
   scope and cannot be introduced as an automatic fallback.
4. ADR 063 selects pgBackRest 2.59.0 as the single encrypted backup, WAL archive,
   verification, and PITR mechanism. Milestone 7 does not build a generic
   multi-backend backup platform.
5. ADR 063 selects asynchronous PostgreSQL 16.14 physical streaming. Every
   report includes possible and observed loss; synchronous zero-loss is never
   claimed without separate direct proof.
6. Bounded evidence means a project-scoped runner with hard phase and total
   timeouts, not a production SLO. RPO is reported per database from the last
   confirmed source marker and promoted replay position as both lost-record
   count and elapsed window; the aggregate is the worst required database.
   RTO begins at the earliest recorded incident declaration or
   service-unavailable boundary, before fencing work, and ends only when the
   target is reconciled and customer-write-ready. Detection, fencing,
   promotion, reconciliation, and activation time therefore remain visible.
7. The corrupted phrase concerning prior tests means all Milestone 1 through 6
   regression suites and gates.
8. A provider without a native webhook key ID is verified by the selected
   adapter against a bounded accepted key set; exactly one valid match is
   required. Provider-native signature parsing does not leak into generic
   ingress.
9. Same webhook event identity with changed content returns 2xx only after the
   conflict evidence commits, preventing retry amplification while producing
   no payment effect. Failure to persist the conflict returns retryable 5xx.
10. Refund states that can still have a provider effect, including uncertainty
    and manual review, retain active-request uniqueness and selected inventory.
    Only a durable operator resolution proving `failed_no_effect` permits a new
    idempotency key to select those tickets; the original key always replays its
    original resource.
11. Each booking shard stores and locks a local regional-authority singleton.
    Every shard mutation verifies region, epoch, writes-enabled state, and the
    existing generation fence in its local transaction. No cross-database
    atomicity is claimed.
12. External fencing evidence is a one-operation attestation bound to the
    failover identity, source region and epoch, incident, operator identity,
    timestamp, and bounded hashes of ingress, process, credential, and
    database-network fencing observations. It cannot be reused for another
    failover.
13. Target authority becomes `recovery` after promotion and epoch installation.
    Enabling a process does not authorize customer writes. One final control
    transaction changes authority to `active`; only that commit permits write
    readiness and ingress activation.
14. Recovery mode may accept authenticated webhook inbox writes after promoted
    control persistence and the keyring are ready. Payment effects, ticket and
    seat mutations, settlement work, and normal customer writes remain paused
    until active authority.
15. Administrative authority uses separate least-privilege database roles and
    protected runner credentials; no new customer-facing IAM system is added.
    The injected operator identity is audited, and the ordinary application
    role cannot promote, expire backups, or mutate DR evidence.
16. Adapter provenance is the tuple of adapter contract version, adapter type,
    pinned provider API version, and exact source commit. Database provenance
    records PostgreSQL `server_version_num`, schema migration version, timeline,
    and primary, standby, or restore role for every database.
17. Source evidence is built from the clean exact commit's Git objects: ordinal
    relative paths, byte counts, and per-file SHA-256 values form an LF-joined
    manifest whose bytes are SHA-256 hashed. Rendered Compose bytes are hashed
    in temporary secret-safe storage; only the digest is published.
18. Every canonical evidence JSON has a schema version and a committed verifier
    that checks required fields, types, bounded enums, hash relationships,
    source binding, reconciliation, secret scan, and teardown. Mutation tests
    prove missing or altered evidence fails closed.
19. If one required promoted shard fails, control and verified healthy shards
    may expose a documented `degraded` policy, but requests owned by the failed
    shard receive a bounded service-unavailable error. They are never routed to
    another shard, and no topology detail is public.
20. Canonical DR evidence starts Redis B empty and rebuilds non-authoritative
    state. AOF restore is optional operational optimization, not financial or
    booking correctness evidence. Waiting-room continuity may be lost and
    admission fails closed.
21. The bounded topology uses three passive API replicas in Region B, matching
    the Region A process shape without claiming capacity parity.
22. Migration version 11 and shard version 3 support fresh, repeat, and
    populated upgrade. Down migration succeeds only when M7 evidence-bearing
    tables and states are empty and the epoch remains at its baseline; otherwise
    it rejects without deleting evidence.
23. Critical and High independent findings must be zero. A Medium that violates
    an explicit invariant or acceptance criterion is a release blocker; any
    retained Medium, Low, or Nice-to-have is disclosed with impact and tracking.
24. The repository PRD is the authoritative TO-PRD artifact. No issue tracker,
    label, or other external state is created without explicit authorization.
25. The final delivery response always contains exactly the requested 56
    sections. If no manual action remains, section 56 contains only `None`; if
    one remains, it contains only the single directly executable command.

## Required Artifact Inventory

The implementation and delivery stages must create or update the following
named artifacts. A design document cannot substitute for working code or direct
evidence.

Research and operational documentation:

- `docs/research/milestone-7-production-payment-and-dr-options.md`;
- `docs/production-provider-adapter.md`;
- `docs/provider-conformance.md`;
- `docs/operational-financial-ledger.md`;
- `docs/settlement-and-payout-reconciliation.md`;
- `docs/partial-ticket-refunds.md`;
- `docs/production-webhook-operations.md`;
- `docs/webhook-key-rotation.md`;
- `docs/active-passive-regional-topology.md`;
- `docs/postgresql-dr.md`;
- `docs/backup-and-pitr.md`;
- `docs/regional-write-fencing.md`;
- `docs/regional-failover.md`;
- `docs/regional-failback.md`;
- `docs/payment-recovery-after-failover.md`;
- `docs/dr-reconciliation.md`;
- `docs/milestone-7-load-testing.md`;
- `docs/benchmark-report-milestone-7.md`;
- `docs/milestone-7-limitations.md`;
- `docs/migrations/migration-11-payment-operations-rollout.md`; and
- `docs/migrations/booking-shard-v3-partial-refund-rollout.md`.

Decision records:

- `docs/adr/056-production-payment-provider-selection.md`;
- `docs/adr/057-provider-capability-and-conformance-contract.md`;
- `docs/adr/058-operational-financial-ledger.md`;
- `docs/adr/059-settlement-and-payout-reconciliation.md`;
- `docs/adr/060-whole-ticket-partial-refunds.md`;
- `docs/adr/061-production-webhook-acknowledgement.md`;
- `docs/adr/062-active-passive-regional-topology.md`;
- `docs/adr/063-postgresql-streaming-replication-and-pitr.md`;
- `docs/adr/064-regional-write-fencing-and-promotion.md`;
- `docs/adr/065-failback-and-old-primary-reseeding.md`; and
- `docs/adr/066-payment-saga-recovery-after-regional-failure.md`.

Deployment, runtime, and evidence artifacts include `cmd/settlement-worker`,
`cmd/settlement-admin`, `cmd/dr-admin`, `cmd/backup-admin`,
`docker-compose.dr.yml`, and `scripts/run-milestone-7-dr-evidence.ps1`.
`README.md`, `CHANGELOG.md`, `docs/production-deployment.md`,
`docs/high-concurrency-design.md`, `docs/security-threat-model.md`,
`docs/future-multi-region-design.md`, and `deploy/README.md` must reflect the
implemented and directly evidenced result.

The ten required bounded k6 modules are:

- `loadtest/k6/production-provider-contract.js`;
- `loadtest/k6/settlement-import.js`;
- `loadtest/k6/partial-ticket-refund.js`;
- `loadtest/k6/partial-refund-idempotency.js`;
- `loadtest/k6/webhook-ack-failure.js`;
- `loadtest/k6/webhook-key-rotation.js`;
- `loadtest/k6/regional-failover.js`;
- `loadtest/k6/payment-during-failover.js`;
- `loadtest/k6/refund-during-failover.js`; and
- `loadtest/k6/regional-failback.js`.

## Domain Invariants

### Provider and external-operation invariants

- Sandbox remains deterministic CI default and is rejected by production
  configuration unless explicitly allowed for a non-production environment.
- Production-oriented provider configuration is explicit, allowlisted,
  HTTPS-only, version-pinned, capability-validated, and secret-provisioned.
- Stable idempotency and identical retry parameters are retained for every
  provider mutation.
- Timeout or response loss after a possible provider commit becomes unknown;
  the next action is status query, not blind retry or provider switch.
- A provider observation alone cannot bypass amount, currency, state,
  ownership, shard, regional epoch, receipt, or reconciliation checks.
- External exactly-once execution is never claimed.

### Ledger and settlement invariants

- Every committed ledger transaction has at least two postings and equal debit
  and credit totals.
- One ledger transaction uses one currency, integer minor units, non-negative
  postings, and bounded account codes.
- Committed postings are immutable; corrections use one balanced reversal of
  an existing unreversed transaction.
- One provider capture or refund effect maps to at most one corresponding local
  ledger effect.
- Ticket revenue and cumulative refunds cannot exceed captured amount.
- Settlement cannot exceed provider-confirmed net evidence under the selected
  provider semantics.
- Provider fees never mutate immutable ticket fare.
- Duplicate imported records are harmless; changed content under the same
  provider identity is conflict evidence.
- Reconciliation mismatches append evidence and never rewrite prior ledger or
  provider records.

### Partial-refund invariants

- The client selects complete ticket identities only; the server derives
  owner, fare, amount, currency, cutoff, provider, shard, and fingerprint.
- Each selected ticket belongs to the authenticated reservation, is active,
  uses the same currency, and is not already pending, refunded, or cancelled.
- Each ticket is refunded at most once, and cumulative refunds never exceed
  captured amount.
- Refund uncertainty retains the selected seat masks and makes the case
  visible.
- Selected masks release only after durable provider refund success and in the
  same shard transaction as selected-ticket state.
- Unselected tickets and masks do not change.
- Duplicate identical requests and compensation commands replay stable
  resources; changed fingerprints conflict.
- Selecting every remaining active ticket converges to full cancellation.

### Webhook invariants

- Signature, key identity, timestamp, replay window, body bounds, normalized
  parsing, and durable inbox commit precede 2xx.
- Persistence failure or uncertainty receives a retryable response.
- Exact duplicate event identity and hash has one effect; changed hash becomes
  durable conflict evidence and cannot mutate payment.
- Webhook acknowledgement does not depend on Redis, payment workers, provider
  status query, booking shards, ticket issuance, or settlement processing.
- Key rotation uses a bounded overlap and retirement window and never stores or
  exposes key material.

### Regional authority and recovery invariants

- Exactly one region is write-authorized; passive and recovery regions reject
  normal writes.
- Control and both physical shards agree on active region and monotonic epoch
  before customer writes are enabled.
- Existing physical-shard generation fencing remains required in addition to
  regional authority.
- External fencing of the old region precedes promotion. Database epoch alone
  is not represented as split-brain prevention.
- Required databases use independent primary and standby storage and are
  promoted explicitly.
- A failover crash cannot enable target writes early, re-enable the source, or
  increment the target epoch twice.
- The old region remains fenced after promotion and must be reseeded from the
  active region before failback.
- Regional epoch never decreases, and a divergent old primary is never reused
  directly.
- Provider operation, payment saga, refund, ticket, ledger, settlement,
  migration, and outbox identities survive regional recovery.
- Redis loss cannot alter durable payment, booking, ticket, ledger, webhook,
  settlement, or regional-authority state.

## Security Requirements

- Never accept, transmit, store, log, trace, metric-label, fixture, or place in
  evidence a card number, PAN, CVV/CVC/CID, PIN or PIN block, magnetic-stripe or
  track data, raw bank credential, raw payment credential, or equivalent
  sensitive authentication data.
- Hosted or tokenized collection is mandatory. An opaque provider token is
  bounded metadata, not permission to accept arbitrary provider request bodies.
- Production provider credentials, webhook keys, replication credentials,
  backup encryption keys, database DSNs, and Redis credentials come only from
  secret provisioning and are purpose-separated by process and region.
- No real credential, payment data, settlement/payout report, database dump,
  backup, WAL archive, or customer PII enters the repository or published
  evidence.
- Provider endpoints are fixed startup configuration, HTTPS-only, DNS/IP safe
  for the deployment, redirect-disabled or exactly allowlisted, and protected
  by strict connect, request, header, and response limits.
- Provider API version and capability profile fail closed when missing,
  inconsistent, or unsupported.
- The application never automatically switches providers after an uncertain
  mutation and never retries a possible committed capture or refund before
  status query.
- Webhook verification uses exact bounded bytes, constant-time comparison,
  timestamp and replay tolerance, allowlisted key identities, and bounded key
  overlap. Verification details and bodies are not logged.
- Same webhook or provider report identity with changed content is retained as
  security conflict evidence and cannot mutate domain state.
- Refund amount, currency, ownership, selected active tickets, provider,
  current shard, and cutoff are server-controlled and revalidated under locks.
- Ledger transactions and postings are append-only, balanced, role-protected,
  and auditable; corrections are reversals.
- Settlement and DR administration is role-controlled, bounded, auditable,
  confirmable for destructive actions, and unable to bypass normal financial or
  booking commands.
- Backup artifacts are encrypted and checksummed; encryption keys remain
  separate from backup storage; restore targets are allowlisted and isolated.
- Replication credentials are not mounted into application processes, and DR
  credentials are provisioned separately from active-region credentials.
- Failover requires explicit confirmation and evidence of ingress, process,
  credential, and database-network fencing before promotion.
- Public APIs, errors, health, and logs do not disclose region topology, hosts,
  IP addresses, DSNs, storage locations, WAL positions, replication slots,
  backup paths, or provider internals.
- Metrics accept only bounded labels and never include high-cardinality or
  sensitive identities.
- Evidence runners inventory and hash source/configuration, sanitize output,
  scan for secrets and PII, record interruption-safe teardown, and never publish
  temporary or failed-run bundles as acceptance evidence.

## Operational Requirements

- Payment, settlement, reconciliation, backup, and recovery workers provide a
  deterministic bounded `run once` operation, short claims, recoverable leases,
  external I/O outside transactions, bounded batches/pages, bounded backoff and
  attempts, graceful cancellation, and no goroutine leaks.
- Startup validates provider type, API version, HTTPS endpoint, capabilities,
  keyring, timeouts, response limits, deployment region, deployment role,
  regional epoch, writes-enabled state, migration versions, and required
  database/storage identities.
- Liveness remains process-local. Active readiness proves current regional
  authority and writable required databases. Passive readiness proves passive
  role, read-only recovery state, replication connectivity, bounded lag, backup
  health, and absence of customer-write readiness.
- Webhook readiness depends on authenticated durable control persistence, not
  payment worker, settlement worker, Redis, or booking-shard readiness.
- Settlement import uses bounded account scopes and durable cursors. An outage
  or rate limit remains visible and cannot produce infinite retry.
- Reconciliation is detect-first and bounded by explicit payment, period,
  settlement, payout, shard, or regional scope.
- Partial refunds enforce a configurable pre-departure cutoff and preserve
  selected inventory during financial uncertainty.
- Streaming replication monitors slots, WAL retention, byte/time lag, replay
  timestamp, timelines, and standby read-only status for all three databases.
- Backups record checksum, encryption state, source role/position, age,
  retention, and restore-test status; a failed or stale backup is visible in
  readiness and alerts according to the final runbook policy.
- DR operations have exclusive bounded ownership, durable state, explicit
  confirmation, idempotent phases, safe restart, and non-zero failure exits.
- Promotion cannot advertise write readiness until external fencing, all
  required database promotion, epoch installation, and reconciliation pass.
- Webhook ingress switches only after promoted control persistence is ready.
  Customer write ingress switches after workers and required shards are safe.
- Failback always uses fresh standbys seeded from the active region and a newer
  epoch; automatic failback is forbidden.
- Redis recovery and waiting-room continuity are documented separately from
  financial and booking correctness. Hot-train admission fails closed.
- Observed RPO and RTO definitions, thresholds for the bounded test, topology,
  workload, and clock source are declared before evidence execution.
- Evidence records exact source commit and inventory digest, rendered topology
  digest, provider-adapter version, database versions, replication mode,
  database positions, failover phases, RPO/RTO, webhook outage, saga/ticket and
  refund recovery, ledger/settlement reconciliation, backup checksum/restore,
  failback, final epoch/region, final reconciliation, and teardown.
- Capacity and latency measurements describe only the tested environment and
  workload. They are not production sizing or certification.

## Testing Decisions

### Test philosophy

Good tests assert externally observable state, durable receipts, database
invariants, normalized provider requests and responses, bounded errors,
regional authority, and emitted evidence. They do not assert private helper
calls or depend on implementation layout. Correctness uses deterministic
barriers, fault hooks, provider contract fixtures, replication positions, and
test clocks. Arbitrary sleeps are never the sole proof of ordering or recovery.

The primary prior art is Milestone 6: deterministic provider fault injection,
stable operation identities, query-before-retry, webhook inbox deduplication,
worker `run once` behavior, shard command receipts, exact seat-mask validation,
physical migration fences and replay, multi-replica claims, and detect-first
reconciliation. Milestone 7 extends those patterns instead of creating a second
workflow or authority model.

### Module-level tests

- **Provider adapter and capability profile:** selection/config validation,
  supported and unsupported capabilities, API version requirement, endpoint
  allowlisting, normalized states and errors, strict response limits, and no
  request-controlled endpoint.
- **Conformance harness:** checkout replay, query, authorization, capture,
  pre/post-commit timeout, response loss, duplicate capture, void, full and
  supported partial refund, duplicate refund, settlement/payout pagination,
  webhook signatures and rotation, 429/5xx/malformed/unknown classification.
- **Operational ledger:** balanced and unbalanced transactions, mixed currency,
  negative/overflow amounts, immutable commits, valid reversal, missing or
  double reversal, and balanced capture/issuance/refund/fee/settlement flows.
- **Settlement importer:** duplicate and reordered pages, cursor restart,
  same-identity conflict, rate limit, partial-page failure, cancellation,
  bounded runs, and absence of booking mutations.
- **Settlement reconciliation:** missing local/provider effects, amount,
  currency, fee, aging, duplicate, settlement, payout, event conflict, and
  ledger imbalance cases; every mismatch is detect-only and visible.
- **Partial-refund orchestrator:** one, several, and all remaining tickets;
  owner and cutoff checks; server-derived amount; rejected client money;
  idempotent replay/conflict; cumulative bounds; provider capability; unknown
  result; and one-refund-per-ticket behavior.
- **Shard compensation:** deterministic lock order, exact selected-set proof,
  one receipt, atomic ticket/mask state, no unselected change, replay/conflict,
  control-finalization recovery, and regional/generation fence rejection.
- **Webhook acknowledger and keyring:** commit-before-2xx, retryable persistence
  failure, duplicate and changed-payload conflict, body/signature/replay bounds,
  old/new overlap, retirement, missing key, and passive-region consistency.
- **Regional authority:** correct region/epoch succeeds; passive, wrong region,
  stale/future epoch, writes-disabled, inconsistent shard authority, and epoch
  decrease fail; old-region attempts fail after promotion.
- **Backup and restore verifier:** checksum, encryption marker, missing key,
  stale backup/restore evidence, target allowlist, active-target rejection,
  schema mismatch, reconciliation mismatch, explicit deletion confirmation,
  and interruption-safe cleanup.
- **DR runner and reconciler:** every phase's precondition, repeat invocation,
  crash/resume, one epoch increment, required-shard degradation, no early write
  readiness, and detect-only mismatch handling.

### Integration and concurrency tests

The bounded integration topology includes active control, shard 0, and shard 1
primaries; passive independent standbys for each; separate Redis instances;
the deterministic sandbox; selected-provider contract server; independent
backup storage; and switchable test ingress. It does not represent production
multi-region infrastructure merely because it uses isolated networks or hosts.

Required scenarios include:

1. Production-oriented adapter conformance without live-mode transactions.
2. Optional manually triggered provider test mode, accurately skipped when
   credentials are absent and sanitized when present.
3. Settlement page duplication, pagination restart, changed-payload conflict,
   rate limit, and durable checkpoint resume.
4. Capture/refund/fee/settlement/payout mismatch detection with manual-review
   evidence and no mutation.
5. One-ticket, multi-ticket, and all-ticket partial refunds with balanced
   ledger results and exact selected-mask release.
6. One hundred concurrent identical partial-refund requests producing one
   provider refund, one shard compensation, and one seat release.
7. Conflicting ticket subsets under one idempotency identity producing no
   additional refund.
8. Partial refund during forward and reverse physical migration, preserving
   receipts and executing compensation once on the current target.
9. Webhook control-database outage returning retryable failure, followed by
   provider retry and one inbox effect.
10. Webhook old/new key overlap and bounded retirement without downtime.
11. Encrypted backup of all databases and restore into an independently created
    validation topology, followed by full reconciliation.
12. Region failure after known payment/ticket/refund/ledger/settlement markers,
    proven external fencing, explicit database promotion, one epoch increment,
    reconciliation, worker/ingress activation, and measured RPO/RTO.
13. Webhook response loss immediately before region failure and duplicate retry
    to the promoted region, producing one durable event effect.
14. Captured-but-unissued payment through failover, issuing one ticket with no
    second capture.
15. Full- and partial-refund uncertainty through failover, querying provider
    status before one compensation.
16. One required promoted shard unavailable, with explicit degraded behavior,
    healthy-shard policy, and no random route fallback.
17. DR operation crashes after source fencing, control promotion, one-shard
    promotion, all promotions, before/after epoch installation, before worker
    activation, and before/after ingress switch.
18. Old-region writer attempts after promotion, rejected without creating
    divergent accepted state in the bounded topology.
19. Failback only after re-seeding all databases from the active region,
    catching up, reconciling, fencing the current writer, and installing a
    newer epoch.
20. Redis loss before, during, and after DR activation, preserving financial
    and booking correctness while admission fails closed and caches rebuild.
21. Provider outage, provider rate limit, settlement interruption, corrupt
    backup, restore failure, replication disconnect, WAL pressure, database
    failure, promotion failure, epoch failure, ingress failure, worker failure,
    reseed failure, and backup-destination outage.
22. Full Milestone 1 through 6 regression, race, static, vulnerability, secret,
    filesystem/image, migration, Compose, container, load, and failure suites.

### Performance and bounded evidence tests

Measure provider-contract latency and classifications, settlement import rate
and lag, partial-refund latency and duplicates, durable webhook acknowledgement
latency and retries, database pool pressure, replication lag, failover and
failback duration, observed RPO, payment/ticket/refund recovery, regional write
rejections, ledger imbalance, settlement mismatches, and unexpected server
errors. Acceptance requires zero duplicate charge, refund, ticket, or selected
seat release; a balanced ledger; visible settlement mismatches; exactly one
writer region; a passing independent restore; reseeded failback; and honest
environment-specific RPO/RTO. No throughput or latency result is a production
capacity claim.

### Phase 0 regression tests

- A missed authorization webhook followed by bounded status reconciliation
  advances one `awaiting_customer` intent without fabricating or duplicating a
  provider event.
- A captured payment experiencing its first transient issuance failure remains
  eligible for the same idempotent issuance command and does not enter refund
  solely because provider-attempt counters are exhausted.
- Capture, void, full refund, and partial refund treat ambiguous 500/503,
  disconnect, timeout, and response-loss cases as uncertain, perform status
  query first, and issue no blind second mutation.
- Captured/refunded status with wrong amount, wrong currency, impossible totals,
  or contradictory terminal state enters manual review and produces no ticket,
  compensation, ledger, or seat-release effect.

## Evidence Contract

The bounded Milestone 7 DR evidence run must bind its results to the actual
tested tree and record, at minimum:

- exact source commit and scoped source-inventory hash;
- rendered active-passive topology hash;
- provider adapter and explicit API version;
- PostgreSQL and supporting component versions;
- replication mode, per-database source and replay positions, timelines, and
  measured lag before promotion;
- external-fencing observations and every durable failover phase;
- observed RPO in missing records and elapsed window, plus observed RTO under
  the declared definition;
- webhook unavailable interval and retry/deduplication result;
- payment-saga, ticket issuance/retrieval, full refund, and partial-refund
  recovery results;
- operational-ledger and settlement/payout reconciliation results;
- encrypted-backup metadata, checksum, independent restore validation, and
  restore reconciliation;
- failback re-seeding, catch-up, promotion, and result;
- final active region, final monotonic epoch, final regional/control/shard
  reconciliation, and project-scoped teardown; and
- secret/PII scan, source-state verification at start and end, and explicit
  limitations.

Canonical summaries are strict JSON. Human-readable progress transcripts use
honest log or text content types rather than a JSON extension. Temporary full
bundles, failed-run directories, database data, backups, WAL, credentials,
local absolute paths, and raw provider/settlement/payout data are not committed.
Only sanitized bounded summaries and intentional documentation may be
published. A successful command exit without source binding, artifact hashes,
invariant checks, and teardown is insufficient evidence.

## Verification and CI Contract

The implementation must preserve all existing required jobs and pass the
following repository gates on the final feature head:

- `go mod tidy`, with no unexplained module drift;
- formatting of every changed Go source file;
- `go vet ./...`;
- `go test ./... -count=1 -timeout 900s`;
- `go test -race ./... -count=1 -timeout 1080s`;
- `staticcheck ./...`;
- `govulncheck ./...`;
- validation of every GitHub Actions workflow with `actionlint`;
- repository secret scanning with Gitleaks;
- filesystem scanning with Trivy;
- rendered validation of the default, multi-replica, physical-shard, payment,
  and DR Compose configurations;
- a clean Milestone 7 container image build; and
- Trivy scanning of that exact built image.

The exact final gate surface includes `go mod tidy`, formatting all changed Go
files, `go vet ./...`, `go test ./... -count=1 -timeout 900s`,
`go test -race ./... -count=1 -timeout 1080s`, `staticcheck ./...`,
`govulncheck ./...`, `actionlint .github/workflows/*.yml`, Gitleaks, and
`trivy fs .`. Compose validation runs `docker compose config` plus the
`docker-compose.multi-replica.yml`, `docker-compose.physical-shards.yml`,
`docker-compose.payment.yml`, and `docker-compose.dr.yml` configurations. The
image gate builds `scalable-railway-ticketing-platform:milestone-7` and scans
that exact tag with Trivy.

Focused CI jobs must separately cover provider adapter conformance, the optional
secret-gated provider test mode, ledger invariants, settlement and payout
reconciliation, whole-ticket partial refunds, webhook acknowledgement and key
rotation, control migration version 11, booking-shard migration version 3,
backup/restore, standby replication, regional failover, webhook delivery during
failover, payment/refund recovery, failback, and all prior milestone
regressions.

The optional live-provider job is manual-only, uses a protected environment,
never runs on an untrusted fork pull request, skips cleanly when credentials are
absent, and cannot block standard CI merely because optional credentials are
missing. Any required CI failure must be diagnosed from authoritative logs and
fixed before delivery; tests are not weakened, skipped, or deleted to obtain a
green result.

## Independent Review Decisions

Independent lanes must review the final feature head for:

1. production-provider adapter correctness;
2. provider conformance;
3. provider credential and SSRF boundary;
4. financial-ledger invariants;
5. settlement ingestion;
6. settlement and payout reconciliation;
7. partial-refund eligibility;
8. partial-refund money invariants;
9. partial-refund seat release;
10. webhook acknowledgement durability;
11. webhook key rotation;
12. payment-saga compatibility, including all four Phase 0 prerequisite fixes;
13. physical-shard migration compatibility;
14. regional topology;
15. PostgreSQL replication;
16. backup and point-in-time recovery;
17. external regional fencing;
18. failover ordering;
19. failover crash recovery;
20. failback and re-seeding;
21. webhook failover;
22. payment and refund recovery after failover;
23. Redis DR boundary;
24. RPO/RTO evidence;
25. security and secrets;
26. metrics cardinality;
27. migration safety;
28. multi-replica concurrency;
29. load-test quality;
30. documentation and claim honesty; and
31. test quality.

Findings are classified as Critical, High, Medium, Low, or Nice-to-have.
Independent means a reviewer other than the implementation lane. Every Critical
and High finding must be fixed and re-reviewed before the non-draft pull request
is opened; self-review is not represented as independent evidence.

## Acceptance Criteria

Milestone 7 is complete only when implementation and direct recorded evidence,
not design text alone, prove all of the following:

### Requirements and provider gates

- The PRD, official-source research, threat model, deployment plan, migration
  plan, and provider/ledger/settlement/refund/webhook/DR ADRs are accepted.
- Research compares the sandbox and at least two hosted or tokenized providers
  with official test environments, selects one adapter, and records exact
  capability and operational limitations using official or primary sources.
- The same official-source research compares the required PostgreSQL DR
  options and covers replication, WAL/PITR, promotion, timelines, reseeding,
  split-brain risk, client reconnection, pool behavior, PCI sensitive-data
  restrictions, webhook replay, encryption, secret separation/rotation, and
  audit-log integrity before the DR and security ADRs are accepted.
- The sandbox remains the deterministic standard-CI provider.
- One production-oriented adapter exists, is disabled without explicit secure
  configuration, pins its API version, validates a bounded capability profile,
  and passes the applicable conformance suite.
- Live provider test-mode status is reported truthfully. No claim is made unless
  an actual protected, secret-gated test-mode run occurred.
- No raw payment credential or real provider credential, payment, settlement,
  payout, refund, backup, or customer dataset enters code, tests, logs, metrics,
  evidence, images, or repository history.

### Financial operations

- Every captured, issued, refunded, fee, settlement, and payout effect in the
  acceptance scenarios has matching immutable operational ledger evidence.
- Every ledger transaction balances, uses one currency and checked minor units,
  and is immutable after commit; corrections use one balanced reversal.
- Duplicate capture/refund/import identities are harmless, changed content is
  visible conflict evidence, and cumulative refunds never exceed capture.
- Settlement and payout ingestion is idempotent and resumes from durable
  checkpoints after interruption.
- Reconciliation detects every required missing, duplicate, amount, currency,
  fee, age, settlement, payout, event-conflict, and ledger-imbalance case,
  creates review evidence, and performs no destructive mutation.
- The ledger and reconciliation documentation explicitly disclaim statutory,
  GAAP, IFRS, tax, invoice, merchant-of-record, and bank-payout functionality.

### Whole-ticket partial refunds

- An authenticated owner can refund one or more complete selected active
  tickets before the configured cutoff using one stable idempotent resource.
- Amount and currency come only from immutable server-side fare snapshots; a
  client-supplied amount, fee, provider identity, currency, or shard is rejected.
- Each ticket refunds at most once; cumulative refunds remain within capture;
  unknown provider outcomes are queried before retry and retain selected masks.
- Durable provider refund success precedes one fenced shard compensation that
  atomically changes selected ticket state and releases exactly selected masks.
- Unselected tickets and masks remain active and unchanged. Selecting every
  remaining active ticket converges to full cancellation.
- Forward/reverse physical migration preserves request, saga, provider
  operation, receipt, ticket, mask, journal, outbox, and finalization state
  without repeating provider or inventory effects.

### Webhook and secret operations

- Valid signature, key identity, replay window, bounded parse, normalization,
  and durable inbox commit precede webhook 2xx.
- Persistence failure returns retryable failure; exact duplicate delivery is
  harmless; changed content under one event identity is retained as conflict
  evidence and cannot mutate payment.
- Webhook acknowledgement remains available independently of payment workers,
  Redis, and physical booking-shard availability when control persistence is
  healthy.
- Old and new webhook keys work during a bounded overlap, the old key fails
  after retirement, both regions receive separately provisioned accepted keys,
  and no key material appears in persistence or observability.
- Provider credential rotation follows a verified controlled sequence and does
  not fall back automatically to revoked credentials.

### Active-passive disaster recovery

- One active region and one passive region exist in the bounded evidence
  topology. Control, shard 0, and shard 1 each have independent read-only
  standby storage; application writes never target a standby before promotion.
- Replication mode, slots, WAL retention, lag, replay position, timelines, and
  possible data-loss behavior are recorded accurately.
- Control and both shards enforce bounded region identity, role, monotonic epoch,
  writes-enabled state, and existing generation fences.
- External ingress, process, credential, and database/network fencing of the
  old region is proven before promotion. Failure to prove it blocks promotion.
- Failover promotes required databases explicitly, verifies timelines, installs
  one newer epoch, starts in recovery, reconciles, enables workers, switches
  webhook ingress, and enables customer writes in the required order.
- Crash injection at every required failover boundary proves resumability,
  source fencing, no early target writes, no duplicate epoch increment, and no
  duplicate payment/refund/ticket effects.
- Provider webhooks recover through the stable endpoint and durable replicated
  inbox; retries create one event effect.
- Payment sagas, capture uncertainty, ticket issuance, full and partial
  refunds, settlement checkpoints, ledger, routing, migration receipts, and
  issued-ticket lookup recover correctly after promotion.
- Old-region writer attempts are rejected after promotion in the bounded
  topology. No consensus or automatic split-brain-prevention claim is made.
- Redis loss does not corrupt payment, booking, ticket, ledger, settlement,
  webhook, or regional authority. Waiting-room limitations are explicit and
  hot-train admission fails closed.
- Failback archives or discards divergent old-primary data, re-seeds all three
  databases from the active region, catches up and reconciles, fences the
  current writer, promotes under a newer epoch, and leaves the former writer
  passive. Stale pre-failure primaries are never reused directly.

### Backup, RPO/RTO, and release evidence

- Control, shard 0, and shard 1 have encrypted, checksummed backups and WAL
  continuity in storage independent of primary and standby data directories.
- Backup keys are separately provisioned; backup age, failures, checksum, and
  restore-test age are visible.
- A restore succeeds in an isolated allowlisted validation topology and passes
  schema, timeline, payment, ticket, refund, ledger, settlement, routing, and
  regional reconciliation.
- Evidence declares measurement definitions and acceptance bounds before each
  run and reports observed replication lag, missing records, elapsed RPO window,
  failover RTO, webhook outage, saga/ticket/refund recovery, failback, and
  teardown.
- A zero observed loss or short recovery in one bounded run is reported only as
  that observation, never as zero-RPO, zero-RTO, zero-downtime, production SLO,
  or production-capacity evidence.
- Fresh/repeat/populated control version 11 and booking-shard version 3
  migrations pass, with explicit safe rollback or retained-evidence downgrade
  blocking and no automatic schema migration.
- All Milestone 1 through 6 regression, unit, integration, concurrency, race,
  static analysis, vulnerability, secret, filesystem/image, Compose, container,
  load, failure, recovery, and migration gates pass.
- Independent provider, ledger, settlement, refund, webhook, architecture,
  replication, backup, fencing, failover, failback, migration, security,
  concurrency, metrics, evidence, and QA review has zero unresolved Critical or
  High findings.
- A non-draft, mergeable, green pull request is opened from the Milestone 7
  feature branch, is not merged automatically, and creates no tag or GitHub
  Release.

## Out of Scope

Milestone 7 does not implement or claim:

- raw card collection, a card vault, CVV storage, PIN or magnetic-stripe data,
  raw payment credentials, or direct handling of sensitive authentication data;
- live production charges, a live production merchant deployment, PCI
  certification, or provider/compliance certification;
- automatic provider switching, multi-provider payment routing, or retry of an
  uncertain financial mutation through a different provider;
- chargeback or dispute lifecycle, statutory or tax accounting, invoices,
  merchant-of-record behavior, bank payout initiation, or legal settlement
  accounting;
- foreign-exchange conversion, split tender, installment payment, partial-fare
  refund for one ticket, arbitrary refund amount, cancellation or dynamic
  refund fees, or post-departure refunds;
- multi-region active-active payment or booking writes, automatic PostgreSQL
  multi-primary, automatic global consensus, application-level database dual
  writes, automatic DNS failover without operator fencing, automatic failback,
  or global distributed locks;
- zero-RPO, zero-RTO, zero-downtime regional failover, consensus-based
  split-brain prevention, or exactly-once external financial processing;
- Kafka, a service mesh, Kubernetes operators, XA, two-phase commit, a generic
  distributed transaction coordinator, a generic workflow engine, or splitting
  the modular monolith into independent network services;
- redesign of PostgreSQL VARBIT seat inventory;
- frontend payment UI, email or SMS ticket delivery, offline signed boarding
  credentials, or unrelated customer-experience work;
- active customer reads from the passive region unless separately designed and
  proven safe;
- production SLO certification, production sizing, national-scale capacity
  certification, or generalized performance claims; or
- any Milestone 8 implementation.

For this milestone, partial refund means refunding one or more complete tickets
before a configurable pre-departure cutoff, using each ticket's immutable fare
snapshot, with no additional fee and no arbitrary client-supplied amount.

## Further Notes

- The concrete provider is deliberately **not selected in this PRD**. Selection
  is blocked on the requested official/primary-source research comparison and
  an accepted ADR. No vendor capability should be treated as fact before that
  gate.
- The exact PostgreSQL replication mode and encrypted backup/PITR mechanism are
  also research and ADR decisions. The required outcome is one writer,
  independent standbys, explicit promotion, external fencing, independently
  verified restore, reseeded failback, and honest bounded evidence.
- Live provider test-mode validation is optional and secret-gated. Until an
  actual run occurs, the truthful status is "not executed"; deterministic
  contract-server results cannot be relabelled as live provider evidence.
- The operational ledger supports railway payment operations and provider
  reconciliation only. It is not a general ledger or statutory accounting
  system.
- External provider effects remain at-least-once observations coordinated by
  idempotency, status query, receipts, and reconciliation. Exactly-once is only
  asserted for bounded internal effects where database uniqueness and local
  transactions directly prove it.
- Active-passive evidence may use a bounded disposable topology, but such a
  topology is not a live multi-region deployment and does not certify provider,
  infrastructure, latency, availability, RPO/RTO, or capacity in production.
- The recommended later milestone is "Sustained Capacity, Chaos, and SLO
  Certification," but Milestone 8 must not begin as part of this work.
