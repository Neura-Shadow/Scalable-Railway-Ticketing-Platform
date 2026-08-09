# Payment Saga

## Why a saga

The payment provider, control PostgreSQL, and authoritative physical booking
shard have independent commit and failure domains. None can participate in one
application-visible atomic transaction. Milestone 6 therefore records a
purpose-built payment saga whose steps have durable inputs, stable identities,
idempotent local effects, explicit compensation, and reconciliation. It is not
XA, two-phase commit, a generic transaction coordinator, or a workflow engine.

## Ownership and boundaries

Control owns the intent, saga, provider operations, webhook inbox, uncertainty,
manual-review state, and control events. The current booking shard owns the
reservation state, payment command receipts, ticket order/tickets,
issuance/refund/compensation receipts, exact seat masks, and local events.

The intent stores a train-run identity but no physical shard binding. Each
shard command resolves the current assignment immediately before execution and
is fenced locally. Control-to-shard convergence uses globally unique commands
and committed receipts; provider-to-control convergence uses stable operation
identities, verified events, and status queries.

## Happy-path steps

1. **Create intent:** in one control transaction, enforce owner-scoped request
   idempotency and one active intent/saga for the reservation. Derive immutable
   money from the reservation.
2. **Secure reservation:** route `begin_payment` to the current shard. The local
   transaction fences, locks and validates the reservation, changes `held` to
   `payment_pending`, sets grace, prepares the order, and commits a receipt and
   event. Control finalizes from the receipt.
3. **Create checkout:** claim the durable operation, call the provider outside
   the transaction, then persist the provider payment/session references.
4. **Authorize:** converge a verified webhook or current-status query to
   `authorized`. A browser redirect is ignored as financial proof.
5. **Capture:** execute one stable `capture` operation. A timeout becomes
   uncertain and schedules `query_status`; it never creates a second blind
   capture.
6. **Issue:** after durable exact captured proof, route one issuance command to
   the current shard. One local transaction confirms the reservation, issues
   one ticket per seat, and commits receipts/events.
7. **Finalize:** control verifies the shard receipt and marks operation, intent,
   and saga complete. Replaying finalization cannot recapture or reissue.

## Worker execution model

The payment worker exposes deterministic `RunOnce(ctx)` and handles inbox
processing, saga advancement, provider operations, issuance dispatch, and
compensation. It claims a bounded batch with a short `FOR UPDATE SKIP LOCKED`
transaction, records `lease_owner`/`lease_until`, and commits before provider or
shard I/O. Final state is written in a separate short transaction with an
expected-state predicate.

Expired leases are recoverable. Retry count, maximum attempts, next-attempt
time, base backoff, jitter, and lease duration are bounded by configuration.
Permanent or exhausted failures become `failed` only when no financial
uncertainty exists; otherwise they become `manual_review`. Cancellation stops
cleanly, no background operation outlives its context, and multiple replicas
can claim different rows without duplicating effects.

## Crash-window recovery

| Crash window | Durable evidence | Recovery |
|---|---|---|
| control intent committed before shard command | intent/saga | replay same begin command |
| shard begin committed before control finalize | shard command receipt | finalize control; do not secure again |
| checkout committed before response but no provider ID returned | operation `uncertain` and stable identity | replay the exact checkout request with the same provider idempotency key |
| capture/void/refund committed before response | operation `uncertain`, provider ID and stable identity | query status before any side-effect retry |
| verified webhook stored before processing | inbox row | worker reclaims and applies idempotently |
| capture recorded before issuance | intent/operation captured | replay same issuance command |
| shard issuance committed before control finalize | issuance receipt/order/tickets | finalize control; do not capture or issue again |
| refund committed before response | refund `uncertain` | query provider; do not blind-refund |
| refund recorded before local compensation | refunded control proof | replay same compensation command |

## Cancellation and compensation

Before capture, cancellation proves provider state and idempotently cancels the
checkout or voids an authorization before local release. After capture, it
creates one full-refund operation, moves local resources to `refund_pending`,
retains the seat, and applies local cancellation only after refund proof.

The customer reservation-cancel endpoint routes every reservation with a
payment intent through this saga. The legacy shard-local cancellation predicate
also requires `payment_intent_id IS NULL`, so a racing or stale caller cannot
release paid inventory outside compensation.

An irrecoverable issuance failure uses the same post-capture path. A transient
issuance failure instead retains the seat and retries the original command. An
unknown void/refund/capture outcome is never treated as success or safe failure;
it enters review and is queried.

## Control finalization repair

The reconciliation interface permits only replaying an already recorded
command after validating a current-shard receipt:
load the current assignment, validate generation, command fingerprint, payment
intent, reservation, amount/currency, order, and issued-ticket count, then
complete control in one transaction. The scheduled reconciler remains
detect-only. The operator-only admin path can replay the existing deterministic
shard command after explicit confirmation, a bounded saga lease, and exact
state/receipt validation. It never invents a command, calls a provider for
repair, or mutates inventory directly. A stale route, expired lease,
contradictory receipt, unsupported state, or conflicting locator becomes a
manual-review mismatch rather than an approximate repair.

## Bounded customer behavior

Intent creation and cancellation return stable resources under replay.
Asynchronous work uses `202 Accepted` and bounded states such as
`payment_processing`, `payment_under_review`, `refund_processing`, or
`ticket_issuance_processing`. Provider errors, internal operations, shard
identity, and receipts are not exposed.
