# Payment Reconciliation

## Policy

Reconciliation is detect-only by default. It compares durable provider,
control-plane, directory, current-shard, reservation, ticket, receipt, and
financial-operation state. The core accepts only replay of an already recorded
command through an explicit safe path. The scheduled reconciler is always
detect-only and has no shard mutation gateway. `payment-admin` constructs the
recorded-command replayer only after operator-role validation, an explicit
confirmation, and a non-dry-run request. It never directly changes seat masks,
mints tickets, blindly charges, blindly refunds, bypasses a shard fence, or
treats an unreachable dependency as proof of absence.

The long-running entry point is `cmd/payment-reconciler`; the operator tool is
`cmd/payment-admin`. The disposable Compose topology creates separate
`payment_reconciler` roles with table SELECT on control and both physical
shards. Control additionally grants INSERT/UPDATE on reconciliation
checkpoints and INSERT-only on manual-review cases, so detection evidence is durable while
payment, reservation, ticket, receipt, and inventory mutation is denied by
PostgreSQL. Expected application methods are `RunOnce(ctx)`,
`InspectPayment(ctx, paymentIntentID)`, `ReconcilePayment(ctx,
paymentIntentID)`, and bounded `ReconcileAll(ctx, options)`.

## Scopes

- `payment-intents`: intent/saga uniqueness, immutable money, progress and age.
- `payment-operations`: capture/void/refund identity, totals, uncertainty and
  provider operation uniqueness.
- `payment-webhooks`: event identity/hash conflicts, unsupported/backlogged
  events, processing attempts and lag.
- `payment-tickets`: routed reservation/order/ticket/issuance/compensation
  consistency.
- `payment-provider`: current normalized provider state versus local state.
- `payment-all`: bounded composition of the above with checkpoints and totals.

All scans require deterministic pagination, explicit batch/time limits,
reclaimable checkpoints, and context cancellation. They do not fan out to an
unbounded shard set or scan shards when the global directory is missing.

## Control checks

- At most one active payment intent exists per reservation, and at most one
  active saga exists per intent.
- Intent amount/currency equals its immutable reservation snapshot.
- `(provider, provider_event_id)` is unique and one event ID has one payload
  hash; changed hashes have a visible conflict case.
- Capture and full-refund operation identities are unique and their immutable
  fingerprints agree with the intent.
- Captured amount is consistent, refunded amount does not exceed capture, and
  the completed saga has a durable captured operation.
- Every `uncertain` operation has a scheduled/active reconciliation case.
- Every `manual_review` state has an operator-visible case and bounded age.
- No provider operation ID is attached to different logical operations.

## Cross-boundary checks

- Captured payment maps to a `payment_pending`, `payment_review`, `confirmed`,
  or refund-flow reservation on the current authoritative shard.
- Completed intent maps to an issued order; the order has one issuance receipt;
  that receipt references the same payment intent, reservation, capture, amount
  and currency.
- An issued order has exactly one active ticket per reservation seat on its one
  current shard. Ticket-code duplicate checks are current-shard-local. A global
  cross-shard duplicate proof requires a future control-plane ticket-code
  directory/index, because the reconciler must not scan unbound shards.
- Refunded payment has `refund_pending`/cancelled tickets according to saga
  progress; no active ticket remains after completed compensation.
- Shard command/issuance/refund fingerprints agree with control intent and
  operations.
- The reservation directory resolves the current assignment. Stored shard or
  generation hints are not treated as authority.
- A captured payment without a ticket, ticket without captured proof,
  cancelled ticket without required refund, or missing receipt is a mismatch.

## Provider checks

Current status queries use the configured provider adapter outside database
transactions and never originate a capture/refund. The normalized result must
agree on provider payment identity, terminal state, captured total, refunded
total, amount, and currency. Missing/unreachable provider state is `unknown`,
not local failure. Contradiction enters manual review.

## Safe repair matrix

| Finding | Default | Explicit safe repair |
|---|---|---|
| shard begin receipt, control still securing | report | finalize control after route/fence/fingerprint validation |
| shard issuance receipt, control not completed | report | finalize intent/saga/projection; no capture or issuance |
| shard compensation receipt, control not compensated | report | finalize refunded/compensated control state |
| shard void-cancellation receipt, control not compensated | report | finalize cancelled/compensated control state |
| uncertain provider operation | query/report | apply a definite normalized result only |
| captured without issuance receipt | report/escalate | enqueue the existing issuance command, never issue directly |
| refunded with active ticket | report/escalate | enqueue existing local compensation command after proof |
| inventory mismatch | report Critical/High as classified | no automatic repair |

These rows define the only admissible repair shape. The current admin runtime
supports the receipt-backed begin, issuance, void-cancellation, and refund
compensation cases plus rescheduling an already persisted provider operation.
Every replay uses the original deterministic identity, acquires a bounded saga
lease before shard I/O, rechecks exact intent/saga/step state, and finalizes
under the same live lease. A mismatch, missing receipt, conflicting locator, or
unsupported manual-review category fails closed and opens/escalates manual
review. No command accepts provider amount, currency, shard, or financial proof
from the operator.

## Results and metrics

Each run records scope, checkpoint, examined/matched/mismatched/unknown counts,
bounded reason categories, repair requested/succeeded/failed counts, duration,
and oldest age. Metrics never label by intent, reservation, ticket, event,
provider payment, user, endpoint, DSN, or connection reference. A clean local
run is evidence only for its recorded fixtures and topology, not settlement or
production correctness.
