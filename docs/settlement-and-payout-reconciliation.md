# Settlement and Payout Reconciliation

## Import boundary

The settlement worker reads normalized Stripe Balance Transactions and Payouts
through optional provider capability interfaces. It performs bounded external
I/O outside database transactions, persists one page in a short transaction,
and advances a durable opaque cursor only after the page commits. A restart
replays safely. Raw provider reports are not stored by default.

For the selected Stripe contract, Balance Transactions are the settlement-line
source and Payouts are the settlement-batch/payout source; Stripe does not
expose a separate generic `settlements` collection in this adapter contract.
The generic settlement-batch, settlement-line, and payout-line tables remain
available for providers whose APIs expose those shapes directly. Stripe rows
stay in their canonical balance-transaction and payout tables so the same
provider fact is not duplicated under synthetic identities. Reconciliation
uses `provider_payout_id` links on balance transactions as payout lines.

Each configured provider account is protected by a durable bounded lease. A
replica claims due work in a short regional-authority transaction, performs all
provider I/O after that transaction closes, and must present the unguessable
lease token when committing a page. An expired worker cannot commit after a
second replica takes over. Successful import runs a bounded period-scoped,
detect-only reconciliation; an importer or detector failure releases the lease
for retry without rolling back pages that already committed.

Provider transaction, settlement, and payout identities are unique within the
configured provider account. Repeated identical payload hashes are no-ops;
reuse of an identity with a changed normalized hash creates conflict evidence.
Gross, fee, and net values are integer minor units and must satisfy the
provider-specific normalized equation.

## Detection

Reconciliation compares local capture/refund operations, provider balance
transactions, settlement batches, payouts, and ledger postings. It detects
missing local or provider effects, amount/currency/fee mismatches, duplicate or
conflicting identities, unsettled operations beyond a bounded age, payout
total mismatch, ledger imbalance, and event conflicts.

The default is detect-only. A run writes its checkpoint, mismatch records, and
manual-review evidence, but never charges, refunds, issues or cancels a ticket,
changes seat inventory, rewrites a ledger transaction, or marks a provider
fact as true without evidence.

## Administration

The private settlement admin can inspect a transaction, batch, payout, payment,
or period; export a sanitized report; and mark an investigated mismatch as
reviewed. It cannot perform a provider mutation or direct booking mutation.
Every bounded administrative action records actor, reason, expected state,
result, and timestamp without secrets or raw report content.
