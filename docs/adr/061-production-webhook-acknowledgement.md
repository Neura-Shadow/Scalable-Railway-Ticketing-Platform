# ADR 061: Production Webhook Acknowledgement and Key Rotation

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

Provider webhooks are public, duplicated, reordered, delayed, and can arrive
during outages or key rotation. Stripe does not provide a trusted key ID in the
signature header, while the Milestone 6 sandbox protocol does.

## Decision

Keep webhook ingress as a deep verify-and-store module. It reads one bounded raw
body, invokes the configured provider verifier, validates environment/account
and event type, normalizes bounded fields, hashes the payload, and commits an
immutable inbox row or conflict evidence before returning 2xx. Persistence
failure returns 5xx. An exact duplicate and a committed same-ID/changed-hash
conflict are acknowledged after their durable evidence exists; neither repeats
business work.

The Stripe verifier tries a bounded accepted secret set and requires exactly one
valid match. Local key metadata includes activation, retirement, and grace.
Rotation is add, accept, verify across replicas/regions, retire, then remove.
Every Stripe event must carry an explicit live/test mode matching the runtime.
Connect events must match the configured account when Stripe supplies the
top-level account; platform events without that optional field inherit the
account bound to the configured endpoint secret. The normalized account and
mode are retained as immutable inbox evidence.
Stripe's signed timestamp uses a nonzero five-minute tolerance and synchronized
time. Provider retries can carry new timestamps, so durable dedupe and monotonic
state remain mandatory.

## Invariants

- Business work, provider queries, capture, refund, ticket, and seat mutation do
  not occur in the HTTP transaction.
- Raw bodies, signatures, and secrets are not logged or stored by default.
- Signature success does not bypass identity, amount, currency, totals,
  ordering, operation, or state validation.
- Webhook key consumers are separate from outbound provider credentials.

## Consequences

- A missing webhook is recovered by a provider query, including stale
  `awaiting_customer`; webhook presence is never the only progress mechanism.
- Provider-specific freshness and rotation semantics remain in each verifier.
- Network source allowlists are defense in depth and not authentication.

## Rejected alternatives

- Return success before persistence: rejected because provider retry evidence
  can be lost.
- Trust a caller-provided key ID for Stripe: rejected because Stripe does not
  supply that trust fact.
- Apply one global event-age rule to every provider: rejected because providers
  have different signed-freshness and retry semantics.
