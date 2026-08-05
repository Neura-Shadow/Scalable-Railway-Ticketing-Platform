# ADR 052: Payment Compensation and Refund

- Status: Accepted for Milestone 6 implementation
- Date: 2026-08-05

## Context

There is no transaction that can atomically capture provider funds and issue
physical-shard tickets. Capture can therefore succeed before a shard outage,
validation failure, or irrecoverable ticket-issuance defect. Customer
cancellation can also occur before authorization, after authorization, or after
capture, and each point requires a different safe action.

## Decision

Recovery first tries to complete the intended business outcome. A transient or
ambiguous issuance failure retries or queries the same shard command and receipt
under ADR 051. Compensation begins only when capture is durably proven and
ticket issuance is proven permanently unable to complete safely. A timeout by
itself is not permanent failure.

The post-capture compensation saga performs a full refund and then a shard-local
business reversal:

1. move the control intent and saga to `refund_pending`/`refunding` and create
   one stable full-refund operation for the captured amount and currency;
2. execute the provider refund outside a database transaction using ADR 050;
3. if the result is unknown, query the provider before any retry and retain the
   occupied seat and `refund_pending` state;
4. after a proven full refund, dispatch one globally unique compensation command
   to the reservation's current authoritative shard;
5. in one shard transaction verify fence, reservation, payment, amount,
   currency, refund operation and any ticket state; record a unique refund
   completion/compensation receipt; cancel pending or active tickets and order;
   cancel the reservation and release its exact seat masks; append local outbox
   and migration-journal intent; then commit; and
6. finalize the control intent as `refunded` and saga as `compensated` from the
   verified shard receipt.

The platform supports only full refunds in Milestone 6. Money is represented in
integer minor units. At every transition, `0 <= refunded_minor <=
captured_minor`; a compensated reservation requires `refunded_minor ==
captured_minor`. Currency is immutable. Duplicate provider observations and
duplicate shard commands replay the original result without another refund or
seat release.

If a refund is unknown, retryable, contradicted, or unavailable beyond policy,
the saga enters visible manual review. The seat remains occupied and tickets or
reservation stay `refund_pending`; the system does not release inventory until
the financial result is proven. This conservative behavior can reduce
availability but prevents resale while the first customer may still hold an
unrefunded charge or ticket entitlement.

Customer cancellation maps to the financial boundary:

- before authorization, cancel the pending provider checkout/operation and use
  the idempotent shard cancellation command;
- after authorization and before capture, void the authorization, query any
  unknown outcome, then cancel locally only after absence of capture is proven;
- after capture, use the same full-refund saga, move reservation/tickets to
  `refund_pending`, and release only after proven refund and local compensation;
  and
- after full compensation, repeated cancellation returns the same terminal
  result.

Provider permanent rejection before capture permits cancellation without a
refund once status proves that no money was captured. An observed capture during
void or cancellation wins over the pre-capture assumption and switches to the
refund path. Manual operator actions invoke the same idempotent operations and
shard commands and are audit recorded; they do not directly edit state.

## Invariants

- Unknown capture, void, or refund outcomes never cause automatic seat release.
- One captured payment has at most one logical full-refund operation and one
  shard-local compensation result.
- Seat release and ticket cancellation commit together with the local refund
  completion receipt and shard outbox intent.
- The reconciler cannot directly charge, refund, cancel tickets, or release
  seats; it schedules or reports the ordinary durable paths.
- Compensation is an explicit saga, not rollback of a provider transaction and
  not an exactly-once or atomic cross-system claim.

## Consequences

- Some captured customers may wait for retry or refund rather than receiving an
  unsafe immediate cancellation.
- Manual-review age, refund latency, retry counts, retained-seat counts, and
  mismatches require alerts and operator runbooks.
- Refund success followed by shard outage remains recoverable: the same
  compensation command completes after the shard returns without another
  provider refund.
- Partial refunds, fees, exchange rates, disputes, and chargebacks remain outside
  the milestone.

## Rejected alternatives

- Release the seat as soon as refund is requested: rejected because the refund
  may fail or its outcome may be unknown.
- Generate a new refund operation on retry: rejected because it can refund the
  same capture more than once.
- Cancel locally before a post-authorization void is proven: rejected because a
  concurrent capture could charge a customer after resale.
- Treat ticket-issuance failure as proof capture failed: rejected because the
  provider and shard are independent failure domains.
- Use distributed rollback or XA: rejected because provider, control, and shard
  do not share a transaction and recovery must use durable compensation.
