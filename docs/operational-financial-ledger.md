# Operational Financial Ledger

## Boundary

The Milestone 7 ledger is an immutable operational double-entry subledger used
to reconcile payment, ticket, refund, settlement, and payout facts. It is not a
general ledger and is not evidence of GAAP, IFRS, tax, statutory-accounting, or
regulatory compliance.

Accounts are a bounded set: `customer_funds_pending`, `ticket_sales`,
`provider_receivable`, `provider_refund_receivable`, `provider_fee_expense`,
`settlement_cash`, and `reconciliation_suspense`. Money is signed-safe integer
minor units with one uppercase currency per transaction; floating point is
forbidden.

## Posting rules

Every committed ledger transaction has a globally unique identity, an
immutable source identity, at least two non-negative postings, and equal debit
and credit totals. Replaying the same identity and fingerprint returns the
existing result. Reusing an identity with changed source, currency, account, or
amount conflicts. Committed rows cannot be updated or deleted by runtime roles.

Corrections are new reversal transactions. A reversal references one existing
unreversed transaction and can be applied once. Historical postings are never
rewritten. No posting contains passenger PII, raw provider reports, webhook
bodies, signatures, credentials, or topology details.

## Operational mapping

- capture: debit `provider_receivable`, credit `customer_funds_pending`;
- ticket issuance: debit `customer_funds_pending`, credit `ticket_sales`;
- issued-ticket refund: debit `ticket_sales`, credit
  `provider_refund_receivable`;
- pre-issuance refund: debit `customer_funds_pending`, credit
  `provider_refund_receivable`;
- settlement: debit `settlement_cash` and any `provider_fee_expense`, credit
  `provider_receivable`;
- refund settlement: debit `provider_refund_receivable`, credit the applicable
  provider settlement clearing/cash account.

Posting is transaction-scoped with the authoritative control-plane fact. A
ledger append never performs provider or shard I/O. Reconciliation records a
mismatch or suspense entry without mutating prior history.
