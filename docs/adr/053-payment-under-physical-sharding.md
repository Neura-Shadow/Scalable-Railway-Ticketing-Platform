# ADR 053: Payment Under Physical Sharding

- Status: Accepted for Milestone 6 implementation
- Date: 2026-08-05

## Context

Milestone 5 assigns a train run to one authoritative physical shard and supports
online migration through copy, mutation journal, replay, final validation, and a
bounded quiesced cutover. Payment can remain active during that process for
minutes: a customer may be awaiting checkout, authorized, captured, issuing
tickets, refunding, or in review. Binding a control payment intent to its first
shard would make subsequent commands stale after cutover.

## Decision

Payment intent, saga, provider operation, webhook, and reconciliation state live
primarily in control PostgreSQL and are keyed by globally unique identities.
They record reservation and train-run identities but do not store a shard as
permanent authority. Before every shard mutation, the coordinator resolves the
current reservation directory and train-run assignment and sends the command to
exactly one allowlisted shard with the expected generation.

Control also owns a route-independent ticket-code directory. Issuance writes
each immutable code-to-ticket-ID claim atomically with the ticket locator and
control finalization. Forward migration, permitted direct rollback, and reverse
migration update only the locator's shard and generation; they never rewrite or
duplicate the code claim.

Every begin-payment, issuance, cancellation, and refund-compensation command
validates the selected database's storage identity and local generation fence.
A stale route fails before any receipt, reservation, ticket, inventory, journal,
or outbox mutation. The coordinator may refresh a fence-rejected route once; it
does not fan out writes, probe arbitrary databases, or fall back to a prior
storage kind. This preserves ADR 037 and the failure policy in ADR 045.

Payment-related authoritative shard state moves with the train run. Physical
booking-shard migration version 2 extends the fixed base-copy groups, source
mutation journal, target replay/apply receipts, online validation, final exact
validation, reverse migration, and retained-source reconciliation to include:

- reservation payment state, immutable payment intent/amount/currency snapshot,
  and payment grace deadline;
- payment-command and refund-completion receipts;
- ticket-issuance receipts;
- ticket-order payment and issuance states;
- tickets and opaque ticket codes; and
- the corresponding shard-local outbox expectations and target-write evidence.

There is no cross-database foreign key to the control payment intent. Global UUID
identities and immutable fingerprints preserve correlation. Copy and replay are
idempotent and do not synthesize duplicate business events; source-local event
intent follows ADR 041 and source drain remains required after cutover.

During online copy and catch-up the source remains authoritative and payment
commands continue there under its enabled fence. At final quiesce, new shard
commands receive a bounded retryable response while source is disabled, the
final payment/ticket journal is replayed and validated, target is enabled, and
control assignment switches in the order defined by ADR 043. There may be a
durable zero-writer recovery window, never two authorized writers.

After cutover, a retry resolves the target and replays the migrated command or
issuance receipt. A provider event can be stored in control during quiesce and
processed after routing resumes. Captured or refund-unknown cases retain
inventory; migration does not reinterpret their financial state. A target
cannot be promoted if payment/ticket counts, fingerprints, receipts, masks,
states, outbox expectations, journal coverage, or fence evidence disagree.

A physical-shard outage does not change ownership or select another shard. The
control saga remains pending, uncertain, or manual-review as appropriate, and
provider observations may still be durably collected. Operations that could
create a new charge or refund wait for query and shard safety checks. Healthy
shards continue through their own bounded pools.

Reconciliation consults the current assigned shard and the bounded retained
source evidence required for an active migration or post-cutover drain. It does
not scan an unbounded catalog. It detects discrepancies and invokes normal
fenced commands; it never directly writes inventory or tickets.

## Invariants

- A train run has at most one authorized physical writer; payment uncertainty
  never overrides assignment or a local fence.
- Payment intents survive shard movement without changing provider financial
  identity or immutable amount/currency.
- No migration copy, replay, retry, or event delivery duplicates capture,
  refund, reservation confirmation, ticket issuance, or seat release.
- Source deletion remains forbidden until journal, payment/ticket validation,
  outbox drain, consumer receipts, reverse evidence, and reconciliation gates
  pass.

## Consequences

- Migration has a larger fixed table and invariant set and requires payment-
  state failure tests at awaiting-customer, authorized, captured, issuance, and
  refund stages.
- Provider and control work can progress while a shard is unavailable, but no
  shard-local business result is fabricated.
- Route resolution on each shard command adds coordination work but prevents a
  stale stored shard binding from becoming authority.
- The design remains a fixed, single-region bounded pilot; it does not establish
  automatic failover, multi-region payment processing, or production RPO/RTO.

## Rejected alternatives

- Store the shard ID permanently on the payment intent and reuse it: rejected
  because cutover would direct commands to a disabled source.
- Pause all provider webhook ingestion during migration: rejected because
  authenticated control-plane observations can be stored independently.
- Copy only current reservation/ticket rows: rejected because missing receipts,
  journal, and outbox evidence would make retries duplicate or unverifiable.
- Dual-write payment transitions to source and target: rejected because one-
  sided commits can produce duplicate tickets or conflicting seat authority.
- Promote a target despite payment mismatches: rejected because copied seat
  masks alone do not prove financial and issuance correctness.
