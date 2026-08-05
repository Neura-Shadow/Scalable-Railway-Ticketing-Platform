# ADR 055: Future Production Provider Adapter

- Status: Accepted as a Milestone 6 boundary; implementation deferred
- Date: 2026-08-05

## Context

Milestone 6 must prove provider-neutral payment saga and durable ticket issuance
against a deterministic local sandbox. Connecting a live provider requires
commercial account decisions, payment-method and regional rules, provider-
specific authentication, hosted-checkout integration, webhook delivery and key
rotation, authorization windows, idempotency guarantees, operational limits,
compliance review, and deployment secrets. Those facts cannot be inferred from
the sandbox and are intentionally outside this milestone.

## Decision

The only Milestone 6 payment implementation is the deterministic sandbox. A
real production provider adapter remains deployment-specific future work and is
disabled unless an explicit production configuration, security review, and
provider conformance evidence exist. No live charge, refund, credential, account,
or customer payment UI is introduced by this ADR.

A future adapter must implement the provider-neutral contract for hosted
checkout creation, status query, authorization, capture, void, and full refund.
It maps provider-specific states and bounded error codes into the domain states
from ADR 047 without leaking arbitrary strings through APIs, schemas, metrics,
or business logic. It must preserve the operation semantics in ADR 050: stable
idempotency identity and identical retry parameters, query-before-retry after an
unknown result, monotonic observations, exact amount/currency validation, and no
partial capture or refund unless a later ADR extends the domain.

Production enablement requires evidence for at least:

- provider-hosted or tokenized collection that keeps raw card and sensitive
  authentication data outside the platform;
- provider idempotency retention, replay, parameter-conflict, query, timeout,
  authorization-expiry, capture, void, and refund semantics;
- webhook signature bytes, timestamp tolerance, event uniqueness, ordering,
  retry policy, key identification and rotation;
- secret provisioning and rotation, least-privilege account scopes, environment
  separation, and auditable administrative access;
- allowlisted HTTPS egress, redirect behavior, DNS/network controls, timeouts,
  request/response limits, rate limits, backoff, and circuit behavior;
- hosted-session expiry and the exact bounded client response contract;
- provider reconciliation, settlement/financial reporting boundaries, incident
  response, support ownership, and manual-review runbooks; and
- legal, privacy, compliance, accessibility, and provider certification work
  applicable to the selected deployment.

The adapter must pass the same deterministic contract suite as the sandbox plus
provider-approved test-environment scenarios for duplicate and reordered
webhooks, lost responses, status query, worker crash recovery, multi-replica
claims, expired authorization, capture/void races, full-refund ambiguity, and
key rotation. Test-environment evidence is not represented as live production
capacity, availability, settlement, or compliance evidence.

The control and shard architecture does not change for a provider. Payment
state remains in control, reservation and ticket authority remains shard-local,
and no transaction spans provider and either database. A provider adapter cannot
write reservation, inventory, order, ticket, migration, or outbox tables. It
returns normalized observations to the existing saga and reconciliation paths.

Provider selection is startup allowlisted, not customer controlled. Introducing
multiple providers, provider routing, automatic failover, smart retries across
providers, partial payment, multi-currency conversion, disputes, chargebacks,
or settlement ledger accounting requires separate requirements and ADRs. A
timeout must never fail over a money operation to another provider because the
first provider may already have committed.

Multi-region read failover and disaster recovery is the recommended Milestone 7
direction, but it does not authorize multi-region active-active payment writes.
Regional payment execution, residency, failover, provider routing, and global
reconciliation require separate single-writer and recovery decisions.

## Invariants

- Enabling a production adapter cannot weaken the no-raw-card-data boundary,
  webhook authentication, idempotency, query-before-retry, immutable money,
  shard fencing, ticket issuance, or compensation invariants.
- Sandbox mode is rejected by production configuration, and production secrets
  are rejected by sandbox fixtures and test output.
- A provider-specific success callback, webhook, dashboard, or HTTP response is
  not by itself authoritative ticket evidence.
- No vendor selection or future adapter can introduce XA, hidden cross-database
  writes, or a generic distributed transaction/workflow coordinator.

## Consequences

- Milestone 6 remains reproducible without network accounts or real financial
  side effects.
- Production adoption has an explicit conformance and security gate rather than
  an assumption that a sandbox-compatible HTTP client is sufficient.
- Provider-specific behavior stays at the adapter boundary while domain recovery
  and shard invariants remain common.
- A future project must supply deployment-specific evidence before claiming a
  live integration or production readiness.

## Rejected alternatives

- Connect a live provider during Milestone 6: rejected because credentials,
  commercial configuration, compliance scope, and production operations are not
  part of the accepted work.
- Treat the sandbox as a production provider: rejected because deterministic
  fault injection does not implement real authorization, settlement, fraud, or
  provider controls.
- Put provider-specific states in core schemas: rejected because it couples the
  saga and migrations to one vendor and weakens bounded transition validation.
- Automatically fail over timed-out payments to another provider: rejected
  because unknown first-provider outcomes can create duplicate charges or
  refunds.
- Expand Milestone 6 into multi-region active-active payments: rejected because
  regional ownership and disaster recovery require their own evidence and ADRs.
