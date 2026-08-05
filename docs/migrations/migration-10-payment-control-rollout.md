# Migration 10 Payment Control-Plane Rollout

## Scope and safety boundary

Migration 10 is the additive control-plane schema for Milestone 6 payment
orchestration. It must create the provider-neutral payment-intent and saga
ledger, stable provider-operation identities and observations, the signed
webhook inbox, reconciliation findings, retry/lease state, and the indexes and
constraints needed for bounded worker claims. It must not store raw card data,
raw customer idempotency keys, webhook secrets, provider credentials, arbitrary
provider URLs, database URLs, or shard bindings on a payment intent.

Applying the migration does not enable payment creation, contact a provider,
capture funds, issue tickets, move a train run, or authorize repair. All M6
feature flags and worker/provider activation remain disabled until every gate
below passes.

## Preconditions

- Record the exact commit and SHA-256 checksums for both migration directions;
  verify the control history is clean at version 9 and `dirty=false`.
- Take a control-database backup and complete a restore rehearsal in a
  disposable environment. Record PostgreSQL version, table sizes, disk/WAL
  headroom, active locks, statement/lock timeout, and connection budget.
- Deploy binaries that understand both the version-9 baseline and the expanded
  M6 enums/schema, while payment routes, workers, webhook acceptance and the
  provider adapter remain disabled.
- Verify runtime roles cannot alter schema, provider configuration, webhook
  keys, saga history or reconciliation findings. Use a separate migration
  identity; never print a DSN, key, signature or provider credential.
- Verify no control schema object, fixture or log column can hold PAN, CVV, PIN,
  magnetic-stripe data, raw provider credentials or an unrestricted request
  body.

Any failed, timed-out, truncated or unavailable precondition is **inconclusive
and blocks rollout**. It is never a pass.

## Disposable rehearsal

Supply `DATABASE_URL` only through the approved secret mechanism, then run:

```powershell
go run ./cmd/migrate -path migrations version
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations up
go run ./cmd/migrate -path migrations version
```

Expect clean version `10`, `dirty=false`, and no change from the repeated `up`.
Rehearse fresh 0-to-10, populated 9-to-10, one-step `down`, and 9-to-10 again.
Inspect every new table, column, type, constraint, index, trigger and privilege.

The rehearsal must prove at least:

- amount and currency are immutable after intent creation; amounts are
  non-negative integer minor units and currency is an allowlisted three-letter
  code;
- one active payment intent/saga exists per reservation and idempotency stores
  only fixed-length hashes/fingerprints;
- provider operation identity is unique and stable per intent/operation while
  attempt observations remain append-only/auditable;
- the same provider/event ID plus the same body hash is a harmless replay, while
  the same ID plus a different hash cannot overwrite accepted evidence;
- captured and refunded accounting satisfies `0 <= refunded <= captured` and
  partial refunds are rejected;
- worker claims are index-backed, bounded, lease-based and compatible with
  short `FOR UPDATE SKIP LOCKED` transactions;
- no payment intent stores a physical shard ID, generation, connection
  reference or DSN;
- a schema-only install leaves all payment, provider-operation, webhook and
  reconciliation tables empty.

## Staged rollout

1. Deploy compatible binaries with M6 disabled. Prove Milestones 1-5 regression
   gates and verify all replicas report the expected minimum/maximum schema
   compatibility.
2. Apply migration 10 once with bounded statement and lock timeouts. Stop on
   timeout, dirty state, unexpected lock growth, disk/WAL pressure or validation
   mismatch.
3. Run read-only post-validation and verify version 10, constraints, indexes,
   privileges, empty work queues, and zero prohibited schema/log sentinels.
4. Apply booking-shard schema version 2 independently to every configured
   physical booking database. Keep M6 disabled if even one enabled shard is
   absent, dirty, unreachable or below version 2.
5. Start the deterministic sandbox only in the explicitly allowed local/test
   profile. Prove production configuration rejects the sandbox and non-HTTPS
   future provider endpoints by default.
6. Enable the webhook receiver in store-only mode, then the payment worker and
   reconciler for synthetic canary traffic. The webhook HTTP request may only
   verify and persist; it may not capture, refund, issue or mutate seats.
7. Enable payment-intent creation last, in a bounded canary. Verify queue age,
   unknown outcomes, manual-review count, duplicate-operation invariants,
   control/shard reconciliation and pgx pool pressure before widening.

## Readiness and stop conditions

Payment readiness is fail-closed unless the control schema is clean at version
10, the configured provider and key ring pass validation, every enabled booking
shard is reachable at clean schema version 2, worker lease settings are bounded,
and no incompatible replica is serving. A provider outage may degrade payment
readiness without making existing non-payment reads authoritative elsewhere.

Stop new payment-intent creation and webhook processing, without discarding
durable inbox/work, on any of these conditions:

- dirty or incompatible control/shard schema;
- provider configuration, TLS, redirect, key-ring or sandbox/profile mismatch;
- duplicate capture/refund/ticket evidence, changed-hash webhook conflict,
  amount/currency mismatch, or refunded amount outside its invariant;
- growing unknown/manual-review queues beyond the approved bound;
- control/shard reconciliation mismatch, stale shard generation, migration lag,
  pool exhaustion or unavailable authoritative shard;
- missing/truncated security, migration or runtime evidence.

## Rollback and recovery

Operational rollback disables payment-intent creation, pauses worker claims,
keeps the webhook inbox durable, waits for in-flight leases to expire or finish,
and reconciles every nonterminal/unknown operation. Do not discard a webhook,
change an operation key, release a seat, or retry a provider mutation merely to
make rollback appear complete. Keep the expanded schema and repair forward
whenever any version-10 durable payment evidence exists.

Schema down is exceptional and allowed only when a reviewed database preflight
proves there are no payment intents, sagas, provider operations/observations,
webhook inbox rows, reconciliation findings, M6 control outbox events, or
booking-shard v2 payment evidence that depends on version 10. The down migration
must fail closed otherwise and must not use `CASCADE` or delete evidence.

If a provider outcome is unknown, query the provider with the stable operation
identity and retain the saga in retry/manual-review state. If control
finalization failed after a shard commit, recover from the route-, generation-,
intent-, operation-, amount- and currency-bound shard receipt. Recovery is not
authorization for a second capture or refund.

This runbook is not production authorization, a live-provider review, a
zero-downtime claim, or evidence that migration 10 has been executed.
