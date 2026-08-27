# Production-Oriented Payment Provider Adapter

## Decision and scope

Milestone 7 selects Stripe Checkout Sessions backed by PaymentIntents with
manual capture, initially card-only, plus Refunds, Balance Transactions, and
Payouts. The adapter pins stripe-go v86.2.0 and Stripe API version
`2026-07-29.dahlia`. The local sandbox remains the deterministic provider used
by standard CI. This adapter is production-oriented code, not evidence of a
live production deployment or PCI certification.

## Boundary

The application sends only server-derived integer minor units, currency,
merchant references, stable operation identities, and bounded non-sensitive
metadata. Checkout is fully provider-hosted. No endpoint, DTO, log, trace,
queue, database column, fixture, or evidence artifact may accept or contain a
PAN, CVC, track data, PIN, or customer payment credential.

Provider endpoints, account identity, API version, API key, webhook keyring,
timeouts, and response limits come only from process configuration. Customer
input cannot select a provider, endpoint, account, API version, return URL, or
credential. Redirects are disabled for server-to-server calls. Production
requires an explicit HTTPS origin and an outbound network policy; a fixed URL
alone does not replace DNS and egress controls.

## Recovery contract

Every provider mutation has one durable local operation and one stable
idempotency identity. A timeout, response loss, or HTTP 5xx after dispatch is
uncertain, never evidence that the mutation was not applied. Recovery retrieves
the stored provider object identity and evaluates current status before any
retry. Stripe may prune idempotency keys after at least 24 hours, so replay is
not a substitute for a persisted PaymentIntent, Charge, or Refund identity.
An old uncertain operation without a retrievable identity moves to manual
review rather than inventing a second financial effect.

One shared financial-observation evaluator validates amount, currency,
captured total, and refunded total for synchronous responses, queries,
webhooks, and reconciliation. Contradictory evidence cannot issue tickets,
release seats, or post ledger entries.

## Configuration and secrets

The selected type is `stripe`; `disabled` remains the default and `sandbox`
remains rejected in production unless the explicitly named disposable-test
override is set. Provider API credentials belong only to processes that call
the provider. Webhook verification secrets belong only to ingress. Settlement
read credentials are independently scoped. Key values are never returned by
health, metrics, errors, or evidence.

The payment worker uses a separately provisioned mutation credential. The
reconciler and settlement worker require Stripe restricted keys (`rk_test_` in
non-production and `rk_live_` in production) with only the account,
PaymentIntent-status, Balance Transaction, Payout, and Refund-read permissions
their configured process actually calls. Test and live key modes are rejected
outside their matching application environment.

Credential rotation is operator-controlled and has no automatic fallback:

1. Provision a replacement key with the same minimal process-specific
   permissions, without revoking the current key.
2. Inject it into the passive region first, restart only its credential
   consumers, and verify startup readiness plus a bounded read-only provider
   status/evidence query. This does not authorize passive writes.
3. Inject and restart the active-region consumers one bounded group at a time;
   verify readiness, stable provider identities, and zero new manual-review or
   authentication failures after every group.
4. Retain the old key only for the documented overlap window. Record sanitized
   key-version identifiers and readiness results, never key material.
5. Revoke the old key at Stripe, then repeat active and passive read-only
   verification. A failure stops the sequence; rollback restores the prior
   deployment secret only while that key remains valid. The system must never
   silently try an older key after revocation.

## Live test mode

Live Stripe test mode is optional, manual, protected-environment-only, and
never part of standard CI. It uses synthetic low-value test objects, verifies
that the account is in test mode, sanitizes all output, and refunds test
captures where practical. If the protected secrets are absent, the workflow
reports `not_run`; it never falls back to production credentials.
