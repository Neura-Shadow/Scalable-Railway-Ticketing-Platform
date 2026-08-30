# ADR 058: Immutable Operational Financial Ledger

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

Payment operations prove provider execution, but they cannot express balanced
customer funds, ticket sales, refund receivables, provider fees, settlements,
payouts, or corrections. Deriving totals independently in payment, refund,
settlement, and admin code would create several financial truths.

## Decision

Add one control-owned append-only operational ledger. A transaction has one
bounded event identity, correlation, currency, typed purpose, and at least two
positive minor-unit postings. Accounts are an allowlist covering pending
customer funds, ticket sales, provider receivable, provider refund receivable,
provider fee expense, settlement cash/clearing, and reconciliation suspense.

The ledger validates checked arithmetic, one currency, allowed debit/credit
accounts, and equal totals in both Go and PostgreSQL. Event identity is unique.
Exact replay returns the original transaction; changed replay conflicts.
Committed rows have no normal update/delete path. A correction appends one
balanced reversal linked to the original and cannot reverse it twice.

Capture/refund operation evidence and its posting commit in the same control
transaction. Verified shard issuance finalization and its ticket-sale posting
commit in the same control transaction. Provider and shard I/O remain outside
that transaction.

## Invariants

- Every committed ledger transaction is balanced in one currency.
- No caller supplies an arbitrary account, sign, exchange rate, or free-form
  posting purpose.
- Provider execution rows remain provider evidence; they are not rewritten as
  ledger history.
- Reconciliation detects and records mismatch; it never edits ledger history.

## Consequences

- Partial refund and settlement share one operational financial truth.
- Evidence-backed M6 facts may be deterministically backfilled; ambiguous facts
  remain visible readiness mismatches rather than inferred postings.
- This is an operational ledger, not GAAP/IFRS/tax or merchant-of-record
  accounting.

## Rejected alternatives

- Treat payment-operation rows as the ledger: rejected because they cannot
  balance fees, settlement, payout, or reversal.
- Mutable balances without postings: rejected because provenance and replay
  conflicts are lost.
- Direct repair updates: rejected in favor of append-only reversal.

