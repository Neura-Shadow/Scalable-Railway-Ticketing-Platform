# Payment Recovery After Regional Failover

Provider financial truth may advance while local workers or databases are
unavailable. After promotion, recovery uses the same durable intent, operation,
saga action, webhook inbox, shard command, receipt, and ledger identities; it
never creates replacement charges, refunds, tickets, or seat releases.

Stale `awaiting_customer`, authorized, capture/refund uncertain, ticket-issue,
and shard-finalize states are candidates independent of a previous checkpoint.
The reconciler queries current provider state within a bounded per-item budget,
runs the shared financial-observation evaluator, and either schedules the
recorded next operation/action or creates visible manual review. A lost
authorization webhook can therefore converge from provider status.

Provider mutation attempts and shard-action attempts have separate durable
budgets. An exhausted status-query history cannot turn the first transient
ticket-issuance failure into a refund. Ambiguous shard outcomes are resolved by
their immutable receipts before compensation.

Webhooks remain at-least-once and can arrive during the ingress switch. Durable
event identity/conflict evidence and current status make duplicate or out-of-
order delivery safe. Redis loss affects waiting/admission performance only and
cannot erase payment, ticket, refund, ledger, or recovery state.
