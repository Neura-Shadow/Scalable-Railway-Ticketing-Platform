# Milestone 6: Payment Saga and Durable Ticket Issuance

Status: Design baseline; implementation and acceptance evidence pending

Target: Milestone 6

Last updated: 2026-08-05

## Problem statement

Milestone 5 can retain a reservation and issue synthetic ticket records inside
an authoritative physical shard, but it does not coordinate an external
payment result with control-plane workflow state and shard-local inventory.
A payment provider, the control database, and a booking shard cannot share an
atomic transaction. Timeouts can therefore leave the provider outcome unknown,
webhooks can be duplicated or reordered, and a shard mutation can commit while
control finalization fails.

Milestone 6 must make those uncertainty windows explicit. It adds a
provider-neutral payment boundary, deterministic local sandbox, signed webhook
inbox, durable control-plane payment saga, payment-aware reservation states,
idempotent provider operations, shard-local ticket issuance, full-refund
compensation, and detect-first reconciliation. The milestone is a
single-region synthetic proof. It is not a live gateway integration, PCI
certification, exactly-once external processing, zero-failure settlement, or
production-capacity evidence.

## Product outcome

An authenticated reservation owner can create one durable payment intent for
an active held reservation. The server derives the immutable amount and
currency, secures the reservation on its current authoritative physical shard,
creates a hosted/tokenized sandbox session, establishes authorization, and
captures using stable provider idempotency. Once captured status is durably
known, one shard-local transaction confirms the reservation and issues exactly
one ticket per reserved seat. Retries return stable resources.

If a provider call has an unknown outcome, workers query current provider state
before retrying. If tickets cannot be issued safely after capture, the saga
performs one idempotent full refund and applies shard-local compensation only
after the refund or void is proven. Unresolved financial outcomes retain the
seat conservatively and become visible manual-review cases.

## Actors and journeys

### Customer payment journey

1. The authenticated owner calls
   `POST /api/v1/reservations/:id/payment-intents` with `Idempotency-Key`.
2. The API derives owner, reservation, train run, amount, currency, current
   assignment, and request fingerprint. The request cannot select any of them.
3. Control PostgreSQL creates or replays one payment intent and active saga.
4. A globally unique command transitions the routed shard reservation from
   `held` to `payment_pending`, records a bounded processing-grace deadline,
   prepares the ticket order, receipt, and local outbox event atomically.
5. The API returns a stable intent, normally with `202 Accepted` while work is
   asynchronous. A duplicate key and identical fingerprint returns the same
   intent; a changed fingerprint conflicts.

### Hosted checkout journey

1. A worker claims `create_checkout` in a short control transaction.
2. It calls the configured provider outside all database transactions using
   server-derived values, a stable provider idempotency identity, and bounded
   metadata.
3. It records the provider payment ID and bounded hosted-session reference in a
   short finalize transaction.
4. The browser may perform customer action, but redirects and client success
   flags never authorize capture, completion, or ticket issuance.
5. Only synthetic payment-method tokens are accepted by the sandbox. Card
   numbers, CVV, PIN, magnetic-stripe data, and raw credentials are rejected.

### Authorization and capture journey

Authorization is learned from a verified durable webhook, an explicit provider
status query, or a deterministic sandbox action. Before capture, control state
must show a durable intent, an authorized provider result, a shard receipt for
the secured reservation, and exact amount/currency equality.

Capture has one durable operation and one stable provider idempotency identity.
A definite pre-commit transport failure can retry within bounds. A timeout or
lost response becomes `uncertain`; the next action is `query_status`, never a
blind second capture. Partial capture and provider amount/currency mismatch are
rejected and escalated to manual review.

### Webhook journey

`POST /webhooks/payments/:provider` is outside customer JWT authentication and
uses a provider allowlist, strict content type/body/time limits, timestamped
HMAC-SHA256 verification, accepted key IDs, a replay window, and constant-time
comparison. The handler verifies, hashes, normalizes bounded fields, inserts
the inbox row, commits, and returns. It never issues tickets or calls the
provider inline.

A duplicate `(provider, provider_event_id)` with the same payload hash is a
successful no-op. A changed hash creates a security conflict and requires
review. A validly signed unknown event is stored as ignored/unsupported and
returns success without state mutation. Workers process supported events
idempotently and query provider state when ordering is ambiguous.

### Ticket issuance journey

After capture is durably recorded, one globally unique issuance command is
routed using the current assignment. In one physical-shard transaction it
validates the generation fence, locks the reservation, validates intent,
amount and currency snapshots, acquires unique command and issuance receipts,
confirms the reservation, marks the ticket order issued, creates or activates
one opaque globally unique ticket per reservation seat, and writes local
outbox events. Duplicate commands return the committed resources; mismatched
commands conflict.

After the shard commits, control finalization marks the saga and intent
completed and updates the bounded projection. If that finalization fails, the
shard receipt remains proof: retry or reconciliation completes control without
another capture, ticket, or logical event.

### Cancellation before capture

- A plain held reservation follows the existing cancellation path.
- For checkout or authorization work, cancellation first proves provider
  state, cancels the checkout or voids the authorization idempotently, then
  cancels the reservation and releases its exact masks.
- An unknown void/capture outcome moves to `payment_review`; the seat remains
  occupied until provider status proves release is safe.

### Cancellation after capture and refund journey

Cancellation creates or replays one full-refund saga. A shard-local command
moves the reservation, ticket order, and tickets to `refund_pending` while
retaining inventory. The provider refund uses a stable idempotency identity and
must equal the captured amount. Only after a durable refund success does a
local compensation command cancel the reservation and tickets, release the
exact masks, record receipts, and emit events. Unknown or failed refund remains
visible and never releases seats automatically.

### Ticket-issuance failure journey

- A transient shard or issuance failure retains the seat and replays the same
  issuance command without capturing again.
- An irrecoverable issuance failure moves the saga to compensation, requests a
  full refund, and waits for a proven result before applying local cancellation.
- An unknown refund outcome enters manual review, retains inventory, and is
  reconciled by status query. A captured payment must never disappear without
  an issued ticket, explicit refund, or visible review state.

### Timeout and unknown-outcome journey

Every outbound operation has bounded connect/request/response limits and a
durable operation record. Timeouts after a possible provider commit are
classified as unknown, not failed. The worker schedules a bounded status query
using the provider payment ID and original operation identity. If status stays
unknown beyond configured grace and uncertainty deadlines, the saga becomes
`manual_review`, preserves inventory, alerts operators, and remains visible to
reconciliation. This conservative policy has an explicit inventory cost.

### Reconciliation journey

The default reconciler is detect-only. It inspects stale intents and uncertain
operations; compares provider captured/refunded totals with immutable local
operations; validates shard payment, ticket, receipt, directory and control
state; repairs only proven control finalization or an explicitly selected safe
command; and otherwise creates a manual-review case. It cannot directly change
seat masks, mint tickets, blindly charge, or blindly refund.

### Physical-shard migration during payment

Control payment rows do not bind a physical shard. Every local command resolves
the current assignment at execution and the shard validates the generation
fence. Migration version 2 copies and journals reservation payment fields,
orders, tickets, payment/issuance/refund receipts, local idempotency results,
and outbox state. Replay and cutover validation cover those objects. A stale
source rejects post-cutover commands; captured-but-not-issued and
`refund_pending` reservations resume once on the target. Reverse migration
preserves all payment and ticket history without repeating provider calls.

### Dependency outage behavior

- **Provider outage:** non-payment APIs may remain ready. New payment creation
  returns a bounded unavailable/processing state, while uncertain operations
  remain durable and visible.
- **Control-plane outage:** no new intent, provider operation, or control
  finalization starts. Already committed shard tickets remain authoritative and
  are finalized after recovery.
- **Physical-shard outage:** verified webhooks still enter control PostgreSQL;
  processing for the affected intent retries or escalates. Healthy shards
  continue and no request falls back to another shard.
- **One API/worker crash:** leases expire and another replica replays the same
  durable command or provider operation identity.

## State and data ownership

Control PostgreSQL owns `payment_intents`, `payment_sagas`,
`payment_operations`, `payment_webhook_inbox`, provider-event conflicts,
reconciliation checkpoints, manual-review cases, and control outbox events.
The authoritative booking shard owns reservation payment state, immutable
financial snapshots used for local validation, payment command receipts,
ticket orders, tickets, issuance/refund/compensation receipts, and shard-local
outbox events. There is no cross-database foreign key and no transaction spans
provider, control, and shard.

All cross-boundary identities are application-generated and globally unique.
Money uses integer minor units and a bounded currency code. Invariants include
one active intent/saga per reservation, one capture and full-refund operation
per intent, one order and issuance receipt per reservation, one ticket per
reserved seat, and `0 <= refunded_amount <= captured_amount`.

## APIs and bounded errors

Customer APIs:

- `POST /api/v1/reservations/:id/payment-intents`
- `GET /api/v1/payment-intents/:id`
- `POST /api/v1/payment-intents/:id/cancel`
- `GET /api/v1/ticket-orders`
- `GET /api/v1/ticket-orders/:id`
- `GET /api/v1/tickets/:id`

Webhook API: `POST /webhooks/payments/:provider`.

Public errors are limited to `payment_not_enabled`,
`payment_intent_conflict`, `reservation_not_payable`,
`payment_already_completed`, `payment_provider_unavailable`,
`payment_processing`, `payment_requires_customer_action`, `payment_failed`,
`payment_under_review`, `refund_processing`, `refund_failed`, and
`ticket_issuance_processing`. They never expose raw provider errors, secrets,
signatures, SQL, topology, DSNs, or stack traces.

## Security and privacy requirements

- Amount, currency, ownership, callback references, provider type, endpoint,
  operation IDs, and shard assignment are server-controlled.
- No card number, CVV, PIN, magnetic-stripe data, raw payment credential, raw
  idempotency key, or provider secret enters logs, storage, events, metrics,
  fixtures, or evidence.
- Provider endpoints are configuration-only; production adapters require HTTPS,
  SSRF-safe DNS/IP policy, disabled or tightly controlled redirects, strict
  response limits, and process-specific secrets.
- The sandbox is test/development-only and rejected in production by default.
- Webhook signatures have timestamp, key ID, replay tolerance, key rotation,
  strict body limits, and constant-time comparison.
- Payment metadata excludes passenger PII, database topology, JWTs, DSNs, and
  connection references.
- Owner authorization uses current authenticated identity. Operator actions
  recheck the current database role, produce an audit trail, require bounded
  scope, and require confirmation for provider-side changes.

## Operational requirements

- Payment workers implement deterministic `RunOnce(ctx)`, bounded batches,
  short `FOR UPDATE SKIP LOCKED` claims, recoverable leases, external I/O
  outside transactions, bounded backoff/attempts, graceful cancellation, and
  safe multi-replica execution.
- Health separates liveness from dependency readiness. Webhook availability
  requires control persistence, not every booking shard. Readiness output is
  sanitized.
- Metrics use bounded allowlisted labels only. IDs, user data, signatures,
  endpoints, DSNs, and connection references are forbidden labels.
- Control migration version 10 and booking-shard migration version 2 have
  fresh, repeat, populated-upgrade, dirty-state, and rollback evidence where
  safe. No `AutoMigrate` is used.
- Deterministic clocks, barriers, provider fault hooks, and worker `RunOnce`
  replace arbitrary sleeps in correctness tests.
- Evidence covers duplicate/out-of-order delivery, response loss, unknown
  capture/refund, worker and API crashes, control/shard/provider outages,
  forward/reverse migration, and multi-replica retries.

## Acceptance criteria

Acceptance requires implementation and direct recorded evidence for all of the
following; design text alone does not satisfy them:

- One owner-scoped durable intent with immutable server-derived money and
  idempotent same-request replay/conflict behavior.
- Hosted/tokenized synthetic collection only; no prohibited payment data in
  repository, runtime records, logs, fixtures, or evidence.
- Verified durable webhook inbox with harmless exact duplicates, recorded
  changed-payload conflicts, safe unknown events, and order-independent
  convergence.
- Stable idempotent authorize/capture/void/refund operations; unknown outcomes
  are queried before retry and partial capture/refund are rejected.
- `payment_pending`, `payment_review`, and `refund_pending` prevent unsafe hold
  expiration yet remain visible, bounded, and reconcilable.
- Captured proof leads to one atomic shard-local confirmation/order/issuance,
  one ticket per seat, stable replay, and repairable control finalization.
- Irrecoverable issuance/cancellation after capture performs full-refund
  compensation before exact seat release; refund never exceeds capture.
- Forward and reverse physical migration preserve payment/ticket state and
  receipts, reject stale routes, and do not duplicate provider operations.
- Detect-first reconciliation reports all required financial and cross-boundary
  mismatches and performs no direct seat mutation.
- Unit, integration, concurrency, race, static analysis, vulnerability, secret,
  filesystem/image, Compose, container, load/failure, and prior Milestone 1-5
  regressions pass with recorded limitations.
- Independent code, security, architecture, migration, concurrency, and QA
  review has no unresolved Critical or High finding.
- A non-draft, mergeable, green pull request is open from
  `feat/milestone-6-payment-ticket-issuance`; it is not merged and no tag or
  GitHub Release is created.

## Explicit non-goals

This milestone does not implement direct card/CVV collection, a card vault,
live production credentials, Apple Pay, Google Pay, bank or convenience-store
payment, partial/split/installment payment, partial capture/refund,
cancellation fees, FX, tax or invoice accounting, disputes/chargebacks,
settlement/payout or merchant-of-record accounting, offline ticket verification
or signed boarding credentials, email/SMS/PDF delivery, frontend payment UI,
multi-region payment, active-active workers, Kafka, service mesh, Kubernetes
operators, XA/two-phase commit, a generic workflow engine, microservice split,
or VARBIT inventory redesign.

Capacity observations from a disposable local environment are not production
sizing, PCI compliance, RPO/RTO, or national-scale throughput evidence.
