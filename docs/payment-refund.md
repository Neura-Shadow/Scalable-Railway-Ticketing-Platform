# Payment Void and Refund

## Void versus refund

A void applies only when provider state proves an authorization exists and
capture has not completed. A refund applies only after exact captured status is
durably known. Both are durable provider operations with stable idempotency
identities, immutable fingerprints, bounded attempts, status reconciliation,
and audit history.

Milestone 6 supports full refund only. It does not implement partial refunds,
partial capture, fees, split tender, installments, FX, tax, disputes,
chargebacks, settlement, or payout accounting.

## Void flow

1. Lock/control-check the intent and prove an authorized, uncaptured state.
2. Create or replay one `void` operation with exact amount/currency.
3. Call the provider outside the database transaction.
4. On success, durably record provider operation ID and verified `voided`
   status; then route the shard-local cancellation command.
5. On timeout or response loss, mark uncertain and query status before retry.
6. Release inventory only after provider state proves there was no capture and
   the local fenced cancellation transaction commits.

If a query instead proves capture, the flow switches to full refund; it never
pretends the void succeeded.

## Refund flow

1. Validate a durable captured operation and calculate full refund from the
   immutable captured amount; customer input cannot supply the amount.
2. Create or replay one `refund` operation with one stable provider idempotency
   identity. Enforce provider refund ID uniqueness.
3. Mark reservation/order/tickets `refund_pending` through a current-shard
   command while retaining seats.
4. Call provider outside any transaction and record the definite result in a
   short control transaction.
5. Query provider after any ambiguous outcome. A retryable pre-commit failure
   reuses the same operation; no new refund identity is invented.
6. After verified full refund, execute shard-local compensation to cancel
   resources and release exact masks once, then finalize control from receipt.

Repeated customer cancellation or refund requests return the same saga and
operation result. They cannot multiply refunded total.

## Financial records

Immutable operations are sufficient to derive:

```text
captured_amount = sum(definite successful capture, at most one)
refunded_amount = sum(definite successful full refund, at most capture)
net_paid_amount = captured_amount - refunded_amount
0 <= refunded_amount <= captured_amount
```

All values are integer minor units and one bounded currency. An adapter response
with different amount/currency, a partial provider result, duplicate provider
operation ID, or a refund greater than capture enters manual review and cannot
advance local cancellation.

## Customer cancellation matrix

| Reservation/payment state | Action |
|---|---|
| `held`, no intent | existing local cancellation and exact release |
| checkout pending/awaiting customer, no authorization | cancel session if supported, prove no capture, then local cancel |
| authorized, not captured | idempotent void, then local cancel |
| capture unknown | `payment_review`; retain seat and query |
| captured or issued | full-refund saga, `refund_pending`, then compensate after proof |
| refund unknown/failed | manual review; retain seat |
| already refunded/compensated | stable replay of existing result |

## Audit and administration

Provider-side admin commands require current operator authorization, explicit
confirmation, bounded output, nonzero failure exits, and dry-run where useful.
They cannot exceed captured amount, bypass idempotency, mark completion without
receipts, or directly release inventory. Audit fields contain bounded actor,
reason, operation, money, state, and timestamps, never card data, raw keys,
secrets, payloads, or passenger PII.
