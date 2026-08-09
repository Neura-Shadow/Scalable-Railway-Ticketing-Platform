# ADR 046: Payment Provider Boundary

- Status: Accepted for Milestone 6 implementation
- Date: 2026-08-05

## Context

Milestone 6 adds customer payment to the physical-shard booking model. A payment
provider, control PostgreSQL, and the authoritative booking shard are three
independent failure domains. A provider response can be lost after the provider
accepted an operation, a webhook can be delayed or duplicated, and either
database can commit while the next participant is unavailable. Treating those
systems as one transaction would obscure partial success and would contradict
the control/shard boundary in ADR 037.

The platform must not receive or retain a card number, CVV, PIN, magnetic-stripe
data, or equivalent raw payment credential. Milestone 6 also needs deterministic
failure evidence without sending money or depending on a live provider account.

## Decision

Payment coordination is a provider-neutral saga in the existing modular
monolith. The control database owns payment intents, saga progress, provider
operations, the verified webhook inbox, provider-event conflicts,
reconciliation checkpoints, and manual-review cases. These are global
coordination facts and must remain addressable while a reservation moves between
physical shards.

The authoritative physical booking shard owns reservation payment transitions,
ticket-order and ticket lifecycle, payment-command receipts, ticket-issuance
receipts, refund-completion receipts, and the related shard-local outbox intent.
Those facts commit with the seat and ticket mutation they justify. They are not
replaced by a control-plane projection.

No transaction spans the provider, control PostgreSQL, and a booking shard. The
control and shard portions use the durable command/receipt approach established
by ADR 038. Provider calls occur outside database transactions. Stable command,
operation, and receipt identities make each local transition repeatable; a
reconciler converges independently committed facts. XA, prepared transactions,
and a generic distributed-transaction or workflow coordinator remain excluded.

The provider adapter exposes bounded operations for hosted checkout creation,
status query, authorization, capture, void, and full refund. Requests contain
only server-derived integer minor units, a bounded currency, opaque platform and
provider identifiers, a stable provider idempotency identity, and hosted or
synthetic token references. Requests never contain passenger PII unless a future
deployment explicitly justifies and protects a minimum field set, and they never
contain raw card data.

The customer completes payment through a provider-hosted or provider-tokenized
surface. A browser redirect or success callback is only a customer-experience
signal. It cannot authorize capture, confirm a reservation, issue a ticket, or
release a seat. Authoritative progress requires a durable provider response, a
verified webhook inbox event, or an explicit provider status query, followed by
the applicable control and shard commits.

The Milestone 6 provider is a deterministic local sandbox. It implements the
same contract, signed webhook behavior, idempotency semantics, timeout windows,
and fault hooks needed to prove recovery. It does not simulate handling real
card data and must be rejected by production-mode configuration. A real
production provider adapter is future deployment-specific work under ADR 055.

Synthetic provider payment objects, financial outcomes, monotonic counters, and
hashed idempotency identities persist in a bounded, versioned atomic snapshot
on a disposable named volume. Corrupt, oversized, unavailable, or semantically
invalid state fails startup or readiness; a mutation is not reported as applied
until its snapshot is installed. Undelivered synthetic webhook facts persist in
normalized form and are re-signed with the active sandbox key after restart;
old raw signatures are not retained. Delivery remains at least once, and the
control saga still converges through durable operation identity and current
provider-status query without issuing a second side effect.

Authorization followed by capture is the preferred sandbox flow. It permits the
platform to secure the reservation before capture, void an unused authorization,
and demonstrate a distinct refund compensation path after capture. It does not
create an assertion that every future provider or payment method has identical
authorization windows or semantics.

## Invariants

- Amount and currency are derived from the authoritative reservation, become
  immutable when payment begins, and are revalidated at every money transition.
- One reservation has at most one active payment intent and saga.
- A provider timeout is an unknown outcome, not proof of failure; status is
  queried before any operation that could duplicate a charge or refund.
- Captured money does not by itself create a ticket. Ticket issuance requires a
  durable captured proof and a separate shard-local atomic command.
- Redis, browser callbacks, webhooks not yet durably stored, and control read
  projections are never payment or ticket authority.

## Consequences

- Provider, control, and shard failures become explicit saga states that can be
  inspected, retried, reconciled, or escalated without pretending atomic commit.
- Control state may lag an authoritative shard receipt, and a shard may
  conservatively retain inventory while a provider result is uncertain.
- Provider-neutral domain states and error categories require adapter mapping
  rather than leaking provider-specific strings through the platform.
- The sandbox supplies reproducible correctness evidence but is neither a live
  payment integration nor production-readiness or PCI-compliance evidence.
- Bounded snapshot storage is suitable only for disposable acceptance fixtures;
  capacity, backup, replication, and retention belong to a future provider.

## Rejected alternatives

- Accept raw card details in the platform: rejected because hosted/tokenized
  collection keeps sensitive authentication and account data outside the
  application boundary.
- Treat the browser callback as payment proof: rejected because it can be
  forged, replayed, omitted, or delivered before durable provider settlement.
- Complete only from webhooks: rejected because webhooks can be delayed,
  duplicated, reordered, or unavailable and therefore require status queries
  and reconciliation.
- Issue before capture: rejected because a ticket could become active without
  durable payment and would require a more dangerous unpaid-ticket recovery.
- Capture immediately with no authorization phase: rejected for the sandbox
  because it removes the safer pre-capture void path and weakens failure tests.
