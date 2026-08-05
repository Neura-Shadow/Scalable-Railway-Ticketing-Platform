# ADR 049: Webhook Inbox and Provider Reconciliation

- Status: Accepted for Milestone 6 implementation
- Date: 2026-08-05

## Context

Payment webhooks can be duplicated, delayed, reordered, replayed, delivered
concurrently to several replicas, or omitted during an outage. A valid
signature authenticates a bounded request but does not prove that the event is
new, ordered, internally consistent, or sufficient to advance platform state.
Synchronous webhook processing would also couple provider retries to database,
shard, and ticket latency.

## Decision

The webhook HTTP endpoint verifies and stores; it does not capture, issue,
refund, release inventory, or run a saga inline. It performs these bounded steps:

1. apply route and content-type checks and read a strictly size-limited body;
2. verify provider, key identifier, timestamp tolerance, and HMAC-SHA256 over
   the specified signed bytes using constant-time comparison and a rotating
   allowlisted key ring;
3. derive the payload hash and parse only bounded normalized fields;
4. insert the event into the control-plane inbox; and
5. commit before returning the documented success response.

Inbox uniqueness is `(provider, provider_event_id)`. A duplicate with the same
payload hash is a harmless replay and returns the same receipt behavior. The
same identity with a different hash creates a provider-event conflict and
security alert; it never overwrites the first record or advances payment.
Unsigned, stale, malformed, oversized, or unknown-provider requests fail closed
without revealing secrets or internal topology.

The inbox stores provider, event ID and type, provider payment ID, payload hash,
provider event time, receive time, bounded state, attempts, next-attempt time,
and bounded error category. It does not retain a raw secret or a full webhook
payload by default. Logs and metrics contain no body, hosted token, idempotency
key, or high-cardinality payment/event identity.

A validly signed but unknown event type is acknowledged and durably marked
ignored. It cannot block provider delivery or mutate payment. A known event is
claimed by a worker with a short lease and `SKIP LOCKED`; processing and state
finalization are conditional and replay-safe across replicas.

Event order is not trusted. An older authorization event cannot regress a
captured or refunded fact. A terminal-looking event does not bypass amount,
currency, provider-payment identity, operation, and current-state validation.
If an event arrives before its intent mapping, conflicts with existing facts, or
could represent a later financial state, the worker schedules a provider status
query or manual review rather than guessing.

Provider queries are the canonical recovery mechanism for missing, reordered,
or ambiguous observations. A query uses only configured provider endpoints and
the known provider payment/operation identity. Query results pass the same
state-machine and amount/currency checks as synchronous responses and webhooks.
An unknown money-operation outcome must be queried before retrying that
operation.

The payment reconciler scans bounded indexed control states and checkpoints.
It compares platform intent, operation, webhook, and shard-receipt observations;
schedules status queries or idempotent saga work; and reports mismatches. Its
default mode is detect-only. It cannot directly alter seats or tickets and
cannot blindly charge, void, or refund. Any repair uses the ordinary provider
operation and shard command paths with their receipts and fences.

## Invariants

- No webhook side effect occurs before the authenticated inbox row commits.
- Signature validity never bypasses event uniqueness, ordering, domain-state,
  amount, currency, or ownership checks.
- Duplicate and out-of-order delivery converge to the same financial and ticket
  state without duplicate capture, refund, or issuance.
- Reconciliation checkpoints are durable and bounded; an unavailable provider
  or shard produces retry/manual-review state, not fabricated success.

## Consequences

- Webhook latency stays independent of provider calls and shard ticket work.
- Delayed customer progress is possible while asynchronous workers and queries
  establish the authoritative state.
- Key rotation, timestamp tolerance, body limits, conflict alerting, inbox lag,
  and ignored-event counts become explicit operational surfaces.
- Reconciliation repairs observations through existing commands; it does not
  become a second mutation authority.

## Rejected alternatives

- Process the full payment saga in the HTTP handler: rejected because provider
  retries would span independent failure domains and long-running operations.
- Deduplicate only in memory or Redis: rejected because restart or eviction
  would permit duplicate processing.
- Trust provider event creation time as total order: rejected because delivery
  and provider state transitions can race and clocks are not transaction order.
- Blindly accept all signed event types: rejected because authentic but unknown
  content is not an authorized domain transition.
- Let reconciliation write seats or tickets directly: rejected because it
  bypasses shard fences, command receipts, and local atomicity.
