# Booking-Shard Schema Version 2 Payment Rollout

## Scope and safety boundary

Booking-shard version 2 extends every independent physical booking database
with Milestone 6 reservation payment states, payment operation/issuance/refund
receipts, durable ticket-order and ticket transitions, compensation metadata,
shard-local outbox types, and payment-aware mutation capture/replay/validation.
It preserves the existing VARBIT inventory design, local write fence,
generation authority, command receipts and migration protocol.

The shard stores only the minimum provider-neutral proof needed to bind a
payment command to a reservation, amount, currency and result. It must not store
raw card data, raw customer idempotency keys, webhook bodies/secrets, provider
credentials, arbitrary URLs, control-only saga history, or cross-database
foreign keys. Installing version 2 never enables a writer or payment feature.

## Preconditions

- For each shard, record the fixed allowlisted shard identity, exact commit,
  migration checksums, PostgreSQL version, current schema version and
  `dirty=false`. The source must be clean at booking-shard version 1.
- Take and restore-test an independent recovery point. Measure table sizes,
  locks, disk/WAL headroom and the per-shard/global pool budget.
- Confirm control migration 10 is clean, payment remains disabled, and all API,
  worker, migration and operator binaries understand both shard versions 1 and
  2 before any shard is upgraded.
- Confirm the local fence and current control assignment identify at most one
  writer. Do not apply schema with a public/runtime credential; ordinary roles
  cannot disable triggers, alter fences, forge receipts or mutate apply
  evidence.
- Pause physical cutover/reverse/cleanup for the shard while DDL is applied.
  An active source capture may resume only after trigger inventory and schema
  compatibility are revalidated.

An unreachable shard, timeout, dirty history, version disagreement, missing
backup, incomplete trigger inventory or truncated output blocks rollout.

## Apply and validate every shard

Place only one shard URL at a time in `DATABASE_URL` through the secret
mechanism, without printing it:

```powershell
go run ./cmd/migrate -path migrations/booking-shard version
go run ./cmd/migrate -path migrations/booking-shard up
go run ./cmd/migrate -path migrations/booking-shard up
go run ./cmd/migrate -path migrations/booking-shard version
```

Expect version `2`, `dirty=false`, and no change on repeated `up`. Rehearse a
fresh 0-to-2 history, populated 1-to-2, one-step disposable `down`, and 1-to-2
again. Apply to each configured shard independently and record only sanitized
version/checksum/validation results.

Validation must prove:

- reservation states include the reviewed `payment_pending`, `payment_review`
  and `refund_pending` transitions while the hold expirer still claims only
  `held` rows;
- ticket-order states include `payment_pending`, `authorized`, `captured`,
  `issuance_pending`, `issued`, `refund_pending`, `refunded`, `cancelled` and
  `manual_review`; ticket states include `pending`, `active`, `refund_pending`
  and `cancelled`;
- captured proof and stable issuance receipt are unique per reservation/payment
  command, every reservation seat can yield at most one ticket, ticket codes are
  unique, and command replay returns the same receipt;
- full-refund accounting uses integer minor units, rejects partial/over-refund,
  and cannot release inventory until successful refund compensation is durable;
- captured proof, ticket/refund transitions and local outbox append occur in
  one shard-local transaction under the current route generation and enabled
  fence;
- every new table and changed field is in base-copy cursors, mutation-capture
  trigger inventory, bounded journal metadata, target apply, validation,
  cleanup and reverse-migration coverage;
- duplicate apply is idempotent, conflicting fingerprint/order fails closed,
  final journal lag is zero before cutover, and no journal/outbox/receipt
  contains passenger PII, raw keys, tokens, signatures, URLs or credentials.

## Staged enablement and readiness

Upgrade disabled/empty shards first, then retained/non-writing sources, then a
bounded canary writer, and finally the remaining enabled shards. Do not enable
M6 anywhere until **all** enabled/routable shards are clean at version 2; mixed
versions may serve compatible non-payment behavior only if the binary readiness
contract explicitly proves it.

For a canary payment, prove the following sequence and durable checkpoints:

1. a held reservation enters `payment_pending` under a stable command;
2. control-side authorization/capture is observed without holding a shard
   transaction open;
3. the current shard verifies captured proof and atomically creates one order,
   one ticket per reservation seat, a stable receipt and local outbox events;
4. replay returns the same receipt and creates no second ticket/outbox effect;
5. control finalization can recover from the shard receipt after an injected
   crash;
6. cancellation and permanent issuance failure use full void/refund ordering,
   retaining the seat in `payment_review`/`refund_pending` while the provider
   outcome is unknown.

Readiness is fail-closed on schema/version mismatch, dirty state, missing
trigger/constraint/index, fence or assignment disagreement, migration lag,
unknown shard connection reference, pool-budget violation, or failed
control/shard reconciliation. There is no fallback writer and no cross-shard
scan to repair a payment request.

## Interaction with online migration

A train run with nonterminal payment or refund work may migrate only when the
source and target both run version 2 and the M6 capture/replay/validation set is
complete. Payment intents stay in control and never bind to a physical shard;
each shard command resolves the current authoritative route and generation.

Cutover must retain the existing ordered protocol: source fence, final
catch-up, payment/ticket/refund validation, source disable, target enable with a
newer generation, then control assignment switch. Stale workers fail before a
side effect and retry only after resolving the current route. Direct rollback
is forbidden after target-era evidence; use reverse migration with a newer
generation so target-era payment receipts, tickets and refund state are copied
and replayed.

## Rollback and recovery

Operational rollback disables new payment-intent creation, pauses payment
claims for the affected shard, preserves local receipts/outbox/journal, and
reconciles all nonterminal operations. Keep the schema expanded and repair
forward whenever any reservation, receipt, order, ticket, outbox event, journal
row, apply receipt or target-write evidence depends on version 2.

Version-2 `down` is allowed only for an unrouted disposable/retired shard after
a reviewed preflight proves there is no M6 state and no recovery/retention
requirement. It must fail closed on dependent data, must not disable evidence
to pass, and must not use `CASCADE`. A schema rollback never authorizes pointing
the train run at another database or retrying a financial operation.

This runbook does not claim a production rollout, live-provider compatibility,
zero downtime, exactly-once delivery, or completed runtime evidence.
