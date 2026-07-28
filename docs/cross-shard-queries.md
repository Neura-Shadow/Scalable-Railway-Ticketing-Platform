# Cross-Shard Queries

## Customer-path rule

Customer hot paths do not enumerate booking shards. A correct resource route
must come from a train-run assignment or global locator. Missing or corrupt
control state fails safely and enters reconciliation; it is not repaired by
probing storage targets.

## Locator-based reads

The public schema maintains:

- a reservation locator keyed by reservation ID;
- a ticket-order locator keyed by order ID and indexed for exact owner paging;
  and
- a ticket locator keyed by ticket ID.

Locators carry domain identity, owner, train run, fixed shard ID, assignment
generation, and bounded timestamps. The ticket-order locator additionally
carries the exact owner-list fields `created_at`, `status`,
`total_amount_minor`, and `currency`.

Creation or lifecycle update of a resource and its locator commits atomically
with local state. The locator owner is an early rejection aid, not final
authorization. The routed authoritative row must still match the authenticated
owner before protected data or mutation is allowed.

A stale locator triggers one authoritative assignment refresh. Customer errors
do not reveal whether the locator, generation, shard, or schema was stale.

## Owner-scoped ticket-order listing

The exact global order and count come from the public locator index, ordered by
descending creation time with a deterministic order-ID tie-break. Processing is
bounded as follows:

1. query one configured page and total from the global index;
2. group only that page's resource IDs by fixed route;
3. fetch optional authoritative details with bounded concurrency and per-route
   and global deadlines; and
4. return complete data or an explicit safe failure, never silently incomplete
   customer data.

Taking independent pages from every shard and merging them is prohibited
because it cannot preserve the existing stable global pagination contract.

## Operator and worker fanout

Only operational workflows may fan out over the fixed enabled storage set.
They must have:

- allowlisted enumeration (`legacy`, `shard-0`, `shard-1` only);
- deterministic serial traversal in stable order, with effective concurrency
  `1` and no parallel-fanout setting;
- per-shard and global deadlines;
- bounded page size, memory, and output;
- context cancellation that stops new dispatch;
- an opaque cursor when continuation is necessary; and
- explicit `complete`, `partial`, or `unavailable` status.

One failed logical shard cannot starve healthy work. A partial result includes
only bounded reason/status categories and never a schema name, DSN, SQL error,
resource identifier, or PII. Partial operator data must not be used as if it
were a complete correctness proof.

## Worker behavior

- Hold expiration enumerates bounded authoritative shard batches, then
  revalidates assignment/fence per mutation. A retained source row cannot
  expire after cutover.
- The central outbox worker does not fan out; it leases from
  `public.outbox_events`.
- Admission remains shard-neutral until token use enters the booking path.
- Read-model reload uses event identity/locator and current authority, never a
  blind scan.
- Reconciliation is detect-only and reports explicit completeness.

## Administration examples

`cmd/shard-admin` and its focused tests are present in the work-in-progress
branch. Final controlled runtime, CI, independent review, and release
acceptance remain pending. The bounded interface is:

```text
shard-admin list-shards
shard-admin list-assignments
shard-admin inspect-train-run --train-run-id <train-run-id>
shard-admin reconcile --train-run-id <train-run-id>
shard-admin inspect-health
```

Mutation commands are private CLI operations, not public HTTP endpoints. See
[shard reconciliation](shard-reconciliation.md) and
[ADR 032](adr/032-reservation-locator-and-cross-shard-reads.md).
