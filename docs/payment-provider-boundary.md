# Payment Provider Boundary

## Scope

Milestone 6 exposes a provider-neutral interface backed only by a deterministic
local sandbox. The interface isolates provider transport and status semantics
from payment orchestration; it does not claim that real providers are
interchangeable without adapter-specific review.

```go
type Provider interface {
	CreateCheckout(context.Context, CreateCheckoutRequest) (CheckoutResult, error)
	GetPaymentStatus(context.Context, string) (PaymentStatusResult, error)
	Authorize(context.Context, AuthorizeRequest) (OperationResult, error)
	Capture(context.Context, CaptureRequest) (OperationResult, error)
	Void(context.Context, VoidRequest) (OperationResult, error)
	Refund(context.Context, RefundRequest) (OperationResult, error)
	VerifyWebhook(context.Context, WebhookHeaders, []byte) (VerifiedEvent, error)
}
```

Concrete types may differ, but every request and result follows the invariants
below. The application never branches on an unbounded provider response.

## Normalized contract

Normalized payment states are `created`, `requires_customer_action`,
`authorized`, `captured`, `voided`, `refunded`, `failed`, `cancelled`, and
`unknown`. An adapter must reject an unmappable, contradictory, oversized, or
amount/currency-mismatched response rather than inventing a success.

Requests include only:

- application-generated `payment_intent_id`;
- a durable stable provider idempotency identity;
- server-derived integer `amount_minor` and bounded uppercase currency;
- bounded non-PII metadata; and
- server-configured return/webhook references.

They exclude passenger PII unless a future adapter proves it is contractually
required and separately approved. They always exclude raw customer
idempotency keys, card data, raw payment credentials, JWTs, DSNs, physical
shard IDs, connection references, and internal topology.

## Error classification

Adapters classify errors into a bounded set:

| Category | Outcome | Orchestrator action |
|---|---|---|
| `transport_retryable` | provider definitely did not accept the request | bounded retry with the same operation identity |
| `timeout_unknown` | provider may have committed | mark `uncertain`; query status before any retry |
| `validation_permanent` | request rejected deterministically | fail or compensate; no retry |
| `authentication` | adapter credentials/config invalid | stop, alert, no retry storm |
| `provider_unavailable` | bounded outage | back off and retain durable work |
| `rate_limited` | provider asks for delay | honor bounded delay, keep same identity |
| `conflict` | identity or state conflict | query and reconcile; never overwrite |
| `inconsistent_response` | malformed or contradictory result | manual review and status query |

HTTP status alone does not prove whether an operation committed. A context
deadline, connection reset, or response-body failure after request delivery is
an unknown outcome unless the adapter can prove otherwise.

## Idempotency and transaction boundary

Each provider side effect has one immutable control-plane `payment_operation`
and one stable provider idempotency identity derived from that globally unique
operation. The stored value is a hash/fingerprint; raw caller keys are not
persisted. Amount, currency, operation type, and intent cannot change on replay.

Workers claim work in a short control transaction, commit the claim, perform
network I/O outside any database transaction, and finalize in a second short
transaction. No transaction spans provider, control PostgreSQL, or a physical
booking shard. A lost finalize write is repaired from the same operation and a
provider status query, never by inventing another charge/refund identity.

## Configuration and outbound security

Supported `payment_provider_type` values are exactly `disabled` and `sandbox`.
The endpoint, credentials, webhook keyring, accepted key IDs, connect/request
timeouts, response/body limits, and replay tolerance come only from server
configuration. HTTP callers cannot influence an endpoint, redirect, DNS name,
provider type, callback target, or credential.

Production mode rejects `sandbox` by default. Any explicit disposable override
must be visibly named and must not appear in production manifests. A future
production adapter must enforce HTTPS, an SSRF-safe DNS/IP policy, disabled or
tightly allowlisted redirects, bounded connect/TLS/response-header/whole-call
timeouts, strict response limits, and process-specific secrets. Public health,
logs, metrics, and errors never expose provider endpoints or credentials.

## Customer-facing data

Only a bounded hosted-session reference and normalized state may be returned.
Provider raw errors, request IDs, operation IDs, payment secrets, response
bodies, signatures, and topology remain internal. A hosted-session reference
is not payment proof; only verified webhook/status evidence advances the saga.
The disposable sandbox keeps its synthetic token inside the provider boundary:
the hosted action endpoint returns only `processing`, then emits the same signed
webhook/current-status evidence used by every other authorization path.

## Disposable sandbox restart contract

The sandbox stores synthetic provider payments, counters, operation results,
and SHA-256 identities of stable idempotency keys in one bounded, versioned,
atomic snapshot on its project-scoped named volume. It never persists raw
idempotency keys, webhook signatures, card data, provider credentials, JWTs, or
passenger PII. Invalid or unavailable state fails startup; a later durable-save
failure makes `/readyz` fail closed and rejects new mutations.

Undelivered synthetic webhook facts persist as normalized bounded events, not
old signed envelopes. On restart they are re-signed with the configured active
key and may be delivered more than once. Workers must still resolve any
uncertain capture, void, or refund with the durable provider object/status and
the original operation identity before replay. This proves a disposable
recovery contract only; it does not model production provider storage,
replication, retention, or settlement.

## Future adapter gate

A real provider adapter remains future work. It needs an explicit status and
error map, idempotency retention analysis, authorization-expiry policy,
webhook/key-rotation implementation, settlement reconciliation, compliance and
privacy assessment, secret/network design, incident runbook, and independent
security acceptance. No fake production provider name is introduced here.
