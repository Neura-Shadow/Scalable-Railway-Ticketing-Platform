# ADR 066: Payment Saga Recovery after Regional Failure

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

Asynchronous failover may lose the latest control or shard observation while a
provider effect remains committed. Webhooks can be delayed or routed during the
switch. Replaying provider or shard mutations before reconciling durable facts
can duplicate money, tickets, refunds, or seat release.

## Decision

After database promotion and epoch installation, start applications in recovery
mode with customer/worker writes disabled. Reconcile regional authority,
payment intents/sagas, operations, webhook inbox, provider status, action
attempts, shard receipts, ticket issuance, full/partial refunds, ledger,
settlement, locators, outboxes, migration journals, and backup/replication
evidence.

Provider status is queried outside transactions using persisted provider object
identity. Every observation passes the shared financial evaluator. Unknown or
contradictory results enter manual review. A lost authorization webhook is
recovered from stale `awaiting_customer` by a stable status query and the normal
convergence path.

Repairs are limited to proven control finalization or replay of the existing
provider operation/shard command with its stable identity and receipt. Each
shard action has an independent lease/backoff/attempt budget; provider-query
history cannot authorize ticket compensation. Current route, regional epoch,
and train-run generation are re-resolved before every shard mutation.

## Invariants

- No provider mutation is blindly retried because a database failed over.
- No ticket/seat/ledger state is invented to fill an asynchronous replication
  gap.
- Webhook ingress may store authenticated evidence before customer writes, but
  business processing waits for recovery authority.
- Customer and worker writes enable only after a nontruncated bounded clean
  recovery result or explicit visible manual-review policy.

## Consequences

- Some reservations remain occupied/manual-review while provider or shard
  truth is unavailable.
- Observed RPO can be zero in a drill but the architecture still reports a
  nonzero-capable asynchronous RPO.
- Recovery evidence must include response-loss/provider-restart, failover during
  payment/refund, and receipt-backed convergence.

## Rejected alternatives

- Replay all pending provider operations after promotion: rejected because the
  provider may already have committed.
- Treat webhook delivery as complete authority: rejected because it can be
  missing, duplicated, reordered, or contradictory.
- Let reconciliation edit seats/tickets directly: rejected because it bypasses
  regional/generation fences and local receipts.
