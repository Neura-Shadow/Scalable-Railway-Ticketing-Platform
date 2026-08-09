# ADR 050: Idempotent Authorize, Capture, Void, and Refund

- Status: Accepted for Milestone 6 implementation
- Date: 2026-08-05

## Context

Financial operations cross a network boundary whose timeout cannot reveal
whether the provider committed. Worker crashes can occur immediately before or
after a provider response, and provider responses and webhooks can race. A
simple retry loop can therefore capture or refund more than once.

## Decision

Represent every checkout, query, authorize, capture, void, and refund as a
durable `PaymentOperation`. The platform generates a globally unique operation
identity and derives a stable provider idempotency identity from immutable
operation inputs. Only its hash is retained for platform idempotency lookup;
raw customer or provider idempotency keys are never logged or stored as reusable
secrets.

An operation request is built exclusively from the durable server snapshot:
payment intent, provider payment identity when known, operation type, integer
minor-unit amount, bounded currency, and stable idempotency identity. A retry of
the same operation uses the same identity and identical parameters. A changed
amount, currency, intent, or operation under that identity is a conflict and
fails closed.

Workers claim bounded batches in a short control transaction, commit, call the
provider outside the transaction with explicit connect/request deadlines and
response limits, then conditionally finalize in a new short transaction. A
provider response is classified as success, retryable failure, permanent
failure, or uncertain. Process cancellation and graceful shutdown release no
false result; lease recovery repeats observation using durable identity.

Authorization establishes only that the provider accepted a bounded hold. It
does not confirm the reservation or activate a ticket. Capture is allowed only
for the established authorization and exact immutable amount/currency. The
sandbox uses one full capture; partial capture is outside Milestone 6.

Void is used only for an authorization proven not captured. A void timeout or
contradictory capture event triggers a status query and may enter manual review;
it does not release the seat. Refund is used only after durable capture proof.
Milestone 6 supports one full refund path, not partial refunds. The invariant is
`0 <= refunded_minor <= captured_minor`, with success requiring equality to the
full captured amount for the compensation flow.

When authorize, capture, void, or refund returns an unknown outcome, the
operation becomes `uncertain`. Before retrying the money operation, a
`query_status` operation asks the provider for the current payment and operation
facts. If the query proves the first request absent and the provider's
idempotency contract permits it, the worker may retry with the same stable
identity. If it proves success, the platform records that success without
reissuing. If it remains contradictory or unavailable beyond policy, the saga
enters manual review.

Webhook, query, and synchronous observations all finalize through the same
conditional transition logic. The first consistent observation wins the
monotonic transition; later equivalent observations are replays. A changed
response fingerprint, different amount/currency, second provider operation ID,
or impossible terminal sequence becomes a conflict and cannot cause another
financial call.

For the deterministic sandbox, provider payment state and the hashed stable-key
result map survive a process restart. A response-loss or timeout-after-commit
test may restart the sandbox before observation; status query and same-key
replay must return the single installed capture/refund result. Webhook delivery
itself remains at least once: normalized undelivered events survive restart and
may be delivered again, while old signatures are regenerated. Webhook presence
is still never the only recovery mechanism.

## Invariants

- One logical financial operation has one stable provider idempotency identity
  and at most one durable successful result.
- Capture never exceeds the authorized immutable amount, and refund never
  exceeds captured amount.
- A timeout, lease expiry, HTTP 5xx, or worker crash is not proof of provider
  failure and never authorizes a blind money-operation retry.
- Ticket issuance requires captured proof; seat release after capture requires
  proven full refund and shard-local compensation.
- No operation holds a database transaction during network I/O.

## Consequences

- Correctness depends on provider query and idempotency semantics, which every
  future adapter must validate rather than assume.
- Uncertain operations can retain inventory and require visible manual review.
- Operation receipts, fingerprints, and retry metrics provide auditable recovery
  evidence without storing raw payment credentials.
- Partial capture/refund and multi-currency conversion remain explicit non-goals.

## Rejected alternatives

- Generate a new provider key on every retry: rejected because the provider may
  accept both calls as distinct financial operations.
- Mark a timeout failed and compensate immediately: rejected because capture or
  refund may already have succeeded.
- Use webhook event ID as operation identity: rejected because several events
  can describe one operation and an event may never arrive.
- Allow client-supplied amount or currency: rejected because it breaks the
  immutable server-priced reservation contract.
- Wrap the provider call and database update in XA or two-phase commit: rejected
  because the provider does not participate in the database transaction and the
  approach is outside the bounded architecture.
