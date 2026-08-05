# Milestone 6 Payment Orchestration Research

Status: Design research; no production payment provider selected

Last updated: 2026-08-05

## Research boundary

This research uses official specifications and provider documentation to
identify portable payment-orchestration properties. Provider-specific behavior
is evidence for the boundary, not a decision to integrate that provider. The
milestone implements only a deterministic local sandbox with synthetic payment
tokens and a provider-neutral interface. A future production adapter requires
separate contractual, operational, security, compliance, and deployment work.

## Official-source findings

### Hosted or tokenized collection

PCI SSC explains that outsourced e-commerce payment eligibility depends on all
payment-page elements originating from the compliant third party, and separately
describes merchants that outsource payment processing and do not electronically
store, process, or transmit cardholder data. This supports a boundary where the
railway application receives only a bounded hosted-session reference or
synthetic token, not card data.

Sources:

- [PCI SSC FAQ 1438: embedded payment forms and SAQ A](https://www.pcisecuritystandards.org/faqs/1438/)
- [PCI SSC FAQ 1439: eligibility for SAQ A](https://www.pcisecuritystandards.org/faqs/1439/)

This architecture alone is not a PCI assessment or certification claim.

### Sensitive authentication data

PCI SSC states that card verification codes cannot be retained after
authorization, including in encrypted form. Milestone 6 chooses the stronger
application boundary: card numbers, CVV, PIN, magnetic-stripe data, and raw
payment credentials never enter the API, sandbox, database, logs, metrics,
events, fixtures, or evidence.

Source: [PCI SSC FAQ 1280: storage of card verification codes](https://www.pcisecuritystandards.org/faqs/1280/)

### Intent lifecycle and server authority

Stripe's official Payment Intents guidance recommends one intent per order or
customer session, reusing it when checkout resumes, using idempotency keys, and
monitoring webhooks after the client leaves. Its status-verification guidance
warns against fulfilling from the client and places fulfillment on the server
after status confirmation. These are portable reasons to make the durable
server-side intent authoritative and client redirects non-authoritative.

Sources:

- [Stripe Payment Intents lifecycle](https://docs.stripe.com/payments/payment-intents)
- [Stripe server-side status and fulfillment](https://docs.stripe.com/payments/payment-intents/verifying-status)

### Authorization, capture, void, and refund

Official authorization/capture guidance models a hold followed by a later
capture and notes that uncaptured authorizations expire. Official refund
guidance distinguishes cancelling an uncaptured payment from refunding a
captured payment and constrains total refunds to the original charge. The
portable design is therefore: void/cancel an authorization before capture; use
one full refund after capture; record integer minor-unit totals and enforce
`0 <= refunded_amount <= captured_amount`.

Sources:

- [Stripe manual authorization and capture](https://docs.stripe.com/payments/place-a-hold-on-a-payment-method)
- [Stripe refunds](https://docs.stripe.com/refunds?dashboard-or-api=api)

### Idempotent provider operations and retries

Stripe documents that an idempotency key returns the first operation result,
including an HTTP 500, and rejects parameter reuse that differs from the first
request. Adyen documents safe retries with the same idempotency key after a
timeout, recommends random unique keys, and distinguishes transient conflicts.
The provider-neutral consequence is a durable operation row, stable derived
provider idempotency identity, immutable request fingerprint, and no blind new
operation after an ambiguous result.

Sources:

- [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests)
- [Adyen API idempotency](https://docs.adyen.com/development-resources/api-idempotency)

Idempotency reduces duplicate effects; it does not make an external call part
of a database transaction or justify an exactly-once claim.

### Webhook authenticity, replay, duplicates, and ordering

Stripe's webhook documentation states that event ordering is not guaranteed,
duplicate events can occur, signature verification must use the raw body, and
timestamp tolerance mitigates replay. It recommends asynchronous processing
and quick success responses. Portable controls are timestamped signatures with
key rotation, constant-time comparison, strict body limits, a durable unique
event inbox, payload-hash conflict detection, and provider status query when an
event would regress state.

Source: [Stripe webhook delivery and signatures](https://docs.stripe.com/webhooks)

Go's standard library provides `hmac.Equal` for constant-time MAC comparison.
Source: [Go `crypto/hmac`](https://pkg.go.dev/crypto/hmac).

### Provider HTTP client safety

Go's HTTP library exposes whole-client timeouts, request contexts,
`CheckRedirect`, and `MaxBytesReader`. A production adapter must also enforce a
configured HTTPS-only endpoint, SSRF-safe DNS/IP resolution, bounded connect
and response-header deadlines, strict response-size limits, and no caller
control over endpoint or redirect target.

Source: [Go `net/http`](https://pkg.go.dev/net/http).

### Audit trails and reconciliation

PCI DSS requirement 10 frames audit logs as necessary for anomaly detection,
forensics, and accountability, including administrative actions. For this
milestone the financial audit trail is immutable bounded operation metadata:
intent, operation type, amount/currency, state changes, provider references,
idempotency hashes, response fingerprints, timestamps, actor category, and
manual-review decisions. It excludes card data, secrets, raw webhook bodies,
raw idempotency keys, and passenger PII.

Source: [PCI DSS v4.0 SAQ D for Merchants, Requirement 10](https://www.pcisecuritystandards.org/documents/PCI-DSS-v4-0-SAQ-D-Merchant.pdf)

## Strategy comparison

| Strategy | Uncertainty and correctness | Operational cost | Decision |
|---|---|---|---|
| Capture first, issue ticket, refund on issue failure | Simple happy path, but creates a charged-without-ticket window and needs full compensation | Moderate; refund path is mandatory | Accept only as the post-authorization segment of the preferred saga, with durable captured proof and explicit compensation |
| Authorize, secure reservation, capture, then issue | Separates customer action, confirms inventory protection before capture, and allows pre-capture void; still has capture/issuance uncertainty | Moderate; requires intent, operations, webhooks, worker, receipts, and reconciliation | Preferred for the sandbox |
| Issue ticket before capture | Can expose an active ticket without payment and requires revocation on payment failure | High integrity and fraud risk | Reject |
| Distributed transaction between provider and databases | External providers do not enlist in local PostgreSQL atomic commit; blocking and recovery semantics are unsuitable | Very high and misleading | Reject XA/2PC |
| Webhook-only completion | Durable async delivery is valuable, but delivery can be delayed, duplicated, missing, or out of order | Requires reconciliation anyway | Reject as the only completion signal |
| Synchronous-response-only completion | Response loss after provider commit leaves an unknown outcome; browser callbacks are not authoritative | Appears simple but cannot resolve ambiguity safely | Reject as the only completion signal |

## Selected provider-neutral model

1. Create one durable payment intent and saga from an owner-scoped reservation,
   with server-derived immutable amount and currency.
2. Route a globally unique command to the current physical shard and transition
   the reservation to `payment_pending` with a local receipt and outbox event.
3. Create a hosted/tokenized checkout or authorization session using a stable
   provider idempotency identity.
4. Accept only verified durable webhook evidence or an explicit current-status
   query as provider evidence; client redirects never advance payment state.
5. Capture through one durable idempotent operation outside database
   transactions.
6. If the result is ambiguous, query status before retry; durably record exact
   captured amount/currency when known.
7. Route one issuance command to the current authoritative shard. Confirm the
   reservation and issue exactly one ticket per seat in one local transaction.
8. Finalize the control saga from the shard receipt. A finalization crash does
   not repeat capture or issuance.
9. If issuance is irrecoverable, perform one idempotent full refund, then apply
   local compensation and exact seat release. Unknown refund stays under
   review with inventory retained.

## Rejected assumptions

- Raw card data, CVV, PIN, magnetic-stripe data, and raw provider credentials
  are never stored or accepted.
- Client success callbacks cannot issue tickets or mark payment complete.
- A synchronous response or webhook alone is insufficient; both converge
  through durable state and provider query.
- No transaction spans the provider, control PostgreSQL, and a booking shard.
- Retries never invent a new provider operation identity after an unknown
  outcome.
- Seats are not released while capture or refund outcome may have committed.
- No production provider is selected and no production credentials are needed.

## Future production adapter gate

A real adapter must define provider-specific status mapping, authorization
expiry, capture/refund semantics, webhook algorithms and key rotation,
idempotency retention, retry/rate-limit behavior, settlement reconciliation,
regional endpoint and data handling, credential lifecycle, network egress,
contracted availability, incident response, and compliance scope. It must pass
security review and deployment-specific acceptance before `sandbox` can be
replaced. This milestone supplies only the interface and synthetic evidence.
