# ADR 059: Settlement and Payout Evidence Reconciliation

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

Capture and refund success do not prove fee, availability, payout, or bank
arrival. A settlement importer must tolerate delayed/reordered pages and restart
without making provider reports or the reconciler another payment authority.

## Decision

Use Stripe Balance Transactions and Payouts through a narrow read-only source
interface. A bounded importer claims one due provider account in a short
transaction, reads one page outside transactions, normalizes immutable records,
commits records/hash conflicts and the page cursor together, and then stops or
continues within explicit limits.

Stripe Balance Transactions are the selected provider's canonical settlement
line evidence and Stripe Payouts are its canonical batch/payout evidence. We do
not duplicate those facts into the generic settlement-line or payout-line
tables. Those generic shapes remain capability surfaces for a later provider
that exposes separate settlement resources.

The account claim is a durable expiring lease. Page commits compare its random
token while holding the checkpoint row lock, so a stalled replica cannot
commit after takeover. After a successful import pass, the same worker starts
one bounded period-scoped detect-only reconciliation. Import commits remain
durable if reconciliation fails; the lease is released as immediately due so a
later pass can retry detection without replaying financial mutations.

Normalized evidence records provider identity, related payment/refund identity
when available, gross, fee, net, currency, creation/availability times, payout
identity/state, payload fingerprint, and import time. Raw reports are not stored
by default.

A separate detector compares provider evidence, payment operations, ledger
transactions, and settlement/payout totals for a bounded payment, period,
settlement, or payout scope. It appends immutable runs and mismatches. It has no
repair interface. Operator review appends actor-bound audit evidence; financial
correction uses the normal command or ledger reversal.

## Invariants

- Duplicate same-fingerprint records are replay; changed fingerprints are
  durable conflicts and never overwrite the first evidence.
- Page records/conflicts and cursor advancement are atomic.
- Importer and detector cannot mutate bookings, tickets, seats, provider money,
  or ledger history.
- A provider outage or missing correlation is unknown/mismatch, not absence.

## Consequences

- Automatic Stripe payouts are preferred because transaction-to-payout
  correlation is available.
- Delayed provider evidence can keep a mismatch open without blocking booking
  correctness.
- Settlement correctness and bank arrival require provider/account evidence;
  synthetic fixtures cannot establish live settlement.

## Rejected alternatives

- Extend the payment reconciler with settlement repair: rejected because its
  interface already owns different receipt-backed repair semantics.
- Store raw reports by default: rejected pending encryption, retention, and
  data-classification review.
