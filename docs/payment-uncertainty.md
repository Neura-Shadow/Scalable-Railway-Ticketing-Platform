# Payment Uncertainty

## Principle

A provider timeout, connection reset, response loss, worker crash, or ambiguous
webhook does not prove failure. The provider may have committed after the
application lost the response. Milestone 6 treats this as a first-class
`uncertain` operation and fails conservatively: query before retry, preserve
inventory while money may have moved, and make prolonged uncertainty visible.

## Deadlines

Three independent bounded settings control the lifecycle:

- `payment_processing_grace_seconds` bounds ordinary `payment_pending` work
  before reconciliation is required;
- `payment_manual_review_after_seconds` bounds unattended retry before an
  operator-visible case is created; and
- `payment_max_uncertain_seconds` bounds automatic uncertainty processing
  before the intent and saga remain in `manual_review`.

Crossing a deadline triggers durable reconciliation/escalation, never silent
deletion or normal hold expiration. The deadlines are not claims that provider
settlement is complete.

## Outcome classification

| Evidence | Classification | Safe next action |
|---|---|---|
| validation rejected before provider acceptance | permanent failure | stop/compensate as appropriate |
| adapter proves no request was delivered | retryable pre-commit failure | retry same operation identity within bounds |
| timeout/reset after possible delivery | unknown | persist `uncertain`, query status |
| verified provider status `authorized` | known authorization | capture or void using stable operation |
| verified provider status `captured` with exact money | known capture | record once and issue tickets |
| verified status `voided`/`cancelled` without capture | known no capture | shard-local safe cancellation/release |
| verified status `refunded` for full captured amount | known refund | apply shard-local compensation |
| contradictory status or money | inconsistent | manual review; no new side effect |

The same rules apply to capture, void, and refund. A status-query transport
failure does not turn an unknown operation into a known failure.

## Recovery algorithm

1. Persist the original operation, immutable fingerprint, stable provider
   idempotency identity, amount/currency, attempt count, and `uncertain` state.
2. Schedule a separate `query_status` operation with bounded backoff.
3. Query by the established provider payment/operation reference outside all
   database transactions.
4. In a short transaction, compare current intent and operation versions and
   exact money before applying the normalized status.
5. Continue issuance, void, refund, or safe cancellation only from proven
   status. If still unknown, reschedule within bounds or enter manual review.

No retry changes the provider idempotency identity. No worker interprets a
missing webhook, old timestamp, or client redirect as proof of non-payment.

## Inventory policy

The normal hold expirer processes only `held`. `payment_pending` has a grace
deadline; `payment_review` and `refund_pending` are owned by payment
reconciliation. If capture may have occurred, seats remain blocked. If provider
state proves no capture, a void succeeds, or a full refund succeeds, one fenced
shard-local command may release the exact masks.

This policy can retain sellable inventory while a provider is unavailable. It
is deliberately safer than reselling a seat that may already be paid. Metrics,
age buckets, manual-review cases, and operator inspection expose that cost; no
documented timer alone authorizes release.

## Outage behavior

- Provider outage retains durable operations and backs off; it does not create
  a retry storm or make non-payment APIs unavailable.
- Control outage prevents new provider operations. A shard receipt or issued
  ticket remains durable until control recovery.
- Shard outage does not discard captured proof. Issuance retries the same local
  command after routing/recovery.
- Migration causes a stale route to fail before mutation. The worker refreshes
  assignment within the bounded routing policy and reuses the same command.

## Manual review

Review cases contain bounded identifiers, reason category, status snapshots,
age, attempts, and audit metadata, not raw payloads, card data, secrets, or PII.
Operators can inspect provider status and select only safe idempotent actions.
They cannot mark a payment complete without captured proof, activate tickets
without an issuance receipt, or release seats directly.
