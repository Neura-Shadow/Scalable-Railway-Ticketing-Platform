# ADR 056: Stripe as the Milestone 7 Production-Oriented Provider

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

ADR 055 deferred a real provider until hosted collection, status recovery,
idempotency, webhook rotation, partial refund, settlement, privacy, and
operational semantics were researched. Milestone 7 needs one concrete adapter
without turning provider choice into runtime routing or live-payment evidence.

## Decision

Implement Stripe Checkout Sessions with PaymentIntents using manual capture,
initially restricted to card payments. Pin `stripe-go/v86` v86.2.0 and Stripe
API/webhook version `2026-07-29.dahlia`. Use Refunds for whole-ticket partial
refunds and Balance Transactions/Payouts for normalized settlement evidence.

The deterministic sandbox remains the mandatory CI and fault adapter. Stripe
test mode is optional, protected-secret gated, and truthfully skipped when the
credential is absent. No live credential, charge, refund, or customer card data
appears in repository evidence.

Persist provider object identities as soon as observed. A Stripe idempotency
key may be pruned after at least 24 hours and is never treated as long-lived
payment identity. Unknown old operations without a queryable provider object
enter manual review. There is no automatic provider switch or new key.

## Invariants

- Provider-specific types and states do not enter saga, booking, ledger, or
  public HTTP state.
- Every dispatched mutating timeout, disconnect, truncated response, and 5xx
  is outcome-unknown and is queried before replay.
- Checkout is provider hosted and the platform accepts no PAN or sensitive
  authentication data.
- Stripe availability or test mode never substitutes for the deterministic
  conformance and failure suite.

## Consequences

- The adapter owns protocol version, authentication, error mapping, webhook
  verification, pagination, and response normalization.
- Card/manual-capture eligibility must be proven in provider test mode before
  any deployment enablement.
- Commercial, jurisdictional, acquirer, privacy, accessibility, and compliance
  acceptance remain deployment work.

## Rejected alternatives

- Adyen for Milestone 7: rejected because Hosted Checkout requires a newer
  Checkout API than the tagged Go SDK documents, status recovery is more
  webhook-centric, and settlement adds a report-file pipeline.
- Configuration-driven generic HTTP provider: rejected because provider
  semantics leak into arbitrary mappings and weaken locality.
- Automatic provider failover: rejected because an unknown first-provider
  result can create duplicate financial effects.

