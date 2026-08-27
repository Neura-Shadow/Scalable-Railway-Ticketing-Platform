# ADR 057: Typed Provider Capability and Conformance Contract

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

The Milestone 6 provider interface carries sandbox assumptions, including a
synthetic authorization token and caller-supplied webhook key ID. Widening it
with every settlement and refund operation would make all callers understand
capabilities they do not use and would duplicate financial validation.

## Decision

Expose one finite typed provider profile and consumer-owned narrow interfaces.
The profile covers hosted checkout, hosted or direct authorization, capture,
void, full refund, partial refund, status query, settlement transactions,
payouts, webhook signing, and key rotation. Missing required capability fails
startup or disables the corresponding worker/route safely.

Keep payment operations, settlement pages, and webhook verification as
separate consumer interfaces. Stripe and sandbox are concrete adapters. A
provider conformance harness exercises the same public interfaces with
pre-commit timeout, post-commit response loss, 429, 5xx, malformed/contradictory
response, duplicate identity, pagination restart, and key rotation.

One pure financial-observation evaluator is shared by synchronous responses,
verified webhooks, uncertain recovery, and reconciliation. It validates exact
identity, amount, currency, cumulative captured/refunded totals, and monotonic
state before any ticket, seat, ledger, or compensation effect.

## Invariants

- Capability names, error classifications, states, and metric labels are fixed
  enums, never arbitrary maps or provider strings.
- Unsupported optional behavior returns a bounded unsupported result.
- The conformance harness is a test adapter, not a second production interface.
- Provider adapters cannot write control, booking, ledger, settlement, or
  migration storage.

## Consequences

- Protocol drift is local to one provider adapter.
- Common financial contradictions are fixed once for every observation path.
- Processes receive only credentials and interfaces they need.

## Rejected alternatives

- Add every optional method to one broad client: rejected as a shallow
  interface with unsupported implementations.
- Configure routes, status maps, and webhook rules in one generic client:
  rejected because provider behavior becomes unsafe runtime data.

