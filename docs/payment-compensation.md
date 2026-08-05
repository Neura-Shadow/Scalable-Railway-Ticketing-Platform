# Payment Compensation

## Purpose

Compensation closes a cross-boundary failure after money may have moved but the
requested ticket outcome cannot complete safely. It is an explicit payment
saga, not transaction rollback: the original provider capture remains a
durable fact and the compensating full refund is a second idempotent financial
operation.

## When compensation applies

- Ticket issuance has a permanent invariant or data failure after captured
  status is durably known.
- A customer cancels a captured/issued reservation under the Milestone 6 full
  refund policy.
- Reconciliation finds a captured payment without a safely issuable ticket and
  an audited operator selects compensation.

Transient shard/network/fence failures do not immediately compensate. They
retain the seat and replay the same issuance command. A provider outcome that
is merely unknown cannot authorize compensation or seat release.

## State sequence

1. Control changes saga to `compensating`, intent to `refund_pending`, and
   creates or replays one immutable full-refund operation.
2. A fenced shard-local command moves the reservation, ticket order, and any
   active tickets to `refund_pending`; it retains the exact inventory masks and
   commits a receipt/event.
3. The worker calls the provider outside the transaction with the stable refund
   idempotency identity and exact captured amount/currency.
4. A definite success is recorded durably. Timeout/response loss becomes
   `uncertain` and is queried; no blind second refund is created.
5. Only after verified full-refund proof does the worker route the globally
   unique compensation command to the current authoritative shard.
6. One local transaction validates the fence, intent, refund operation,
   amount/currency and receipt, cancels reservation/order/tickets, releases the
   exact seat masks once, writes compensation/refund receipts and bounded
   events, and commits.
7. Control finalizes the intent `refunded` and saga `compensated` from the shard
   receipt. A finalize failure is repairable without repeating refund or local
   cancellation.

## Invariants

- There is one active compensation/refund path per payment intent.
- Refund is full only and equals captured amount in integer minor units.
- `0 <= refunded_amount <= captured_amount`; duplicate success cannot add to
  the total twice.
- Inventory release occurs only in the fenced local compensation transaction,
  never directly from control, provider event, timer, or reconciler.
- Release uses the reservation's exact committed masks and is idempotent.
- An active ticket cannot remain after completed compensation; a fully refunded
  payment cannot coexist unnoticed with an active ticket.
- A changed command fingerprint conflicts. A matching replay returns the same
  receipt and emits no new logical business event.

## Unknown and failed refund

An unknown refund leaves local resources `refund_pending`, retains inventory,
and enters `payment_review`/`manual_review`. The next operation is a provider
status query. A definite retryable pre-commit failure can retry the same stable
identity within bounds. A permanent failure remains visible for operator
action; it is never reported as cancelled/refunded and never releases seats.

## Migration and outage behavior

Refund-pending states, exact masks, commands, receipts, tickets, and outbox
events are copied/journaled/validated in physical migration. Provider operations
stay in control and never bind a shard. After cutover, the same compensation
command executes once on the target; a stale source fence rejects mutation.
Provider, control, or shard outage delays progress but preserves durable proof.

## Reconciliation

Detect-only checks flag captured-without-ticket, refunded-with-active-ticket,
refund total mismatch, missing receipt, wrong command fingerprint, and stale
directory. Safe control finalization may be repaired from a verified shard
receipt. The reconciler cannot refund, issue, cancel, or release inventory
without an explicit audited command path.
