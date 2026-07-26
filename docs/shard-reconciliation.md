# Shard Reconciliation

## Contract

Milestone 4 reconciliation is detect-only by default. It reads bounded scopes,
emits sanitized summaries, and exits non-zero on mismatch, timeout,
unavailability, truncation, or invalid configuration. It never changes
assignment, fences, locators, inventory, lifecycle state, migration state, or
source retention automatically.

The `cmd/shard-admin` CLI and shard-aware scopes in `cmd/reconcile` are present
in the working implementation. Final controlled runtime evidence, CI, review,
and release acceptance remain **pending**, not assumed.

## Required invariants

### Catalog and single-writer ownership

- every assignment references one fixed enabled catalog entry;
- every assignment generation is positive and matches the active fence;
- stable serving state has exactly one write-enabled fence;
- permitted migration phases may have zero writers but never two;
- source and target generations match the durable migration plan; and
- retained source is disabled after cutover.

### Locators and ownership

- every reservation, ticket order, and ticket has exactly one global locator;
- locator train run, fixed shard ID, generation, and owner match authoritative
  local state;
- the resource exists only in its allowed current/retained migration scope;
- no ordinary lookup needs a shard scan; and
- ticket-order list metadata matches status, amount, currency, and creation
  time on the authoritative order.

### Booking state

- each storage's inventory mask equals the union of held/confirmed reservation
  masks for the same train run and seat;
- no reservation/resource identity is authoritatively duplicated across
  storages;
- active global quota claims match active holds and passenger membership;
- ticket orders/tickets reference valid confirmed/cancelled lifecycle state;
- local idempotency completion points to an existing resource and agrees with
  the global key claim and expiry; and
- each committed booking transition has required central outbox intent with
  valid bounded provenance.

### Migration copy

- source/target row identities, counts, lifecycle states, and exact masks match;
- local idempotency fingerprint, result reference, and expiry match;
- global quota/key claims and locators cover copied authority;
- copy cursor/counts describe committed bounded batches;
- latest validation is complete and untruncated; and
- target-write evidence agrees with rollback eligibility.

## Scope and shard-awareness matrix

| Command | Current authority/coverage | Milestone 4 use |
|---|---|---|
| `reconcile seat-inventory --train-run-id ID` | Standalone legacy-oriented booking-store invariant | Not sufficient alone as schema-shard proof |
| `reconcile reservation-quotas` | Standalone legacy-oriented reservation derivation and global claims | Not sufficient alone as schema-shard proof |
| `reconcile admission-state` | PostgreSQL policy and Redis generation/token state | Shard-neutral final scope |
| `reconcile read-model --train-run-id ID` | Global disposable projection for one run | Shard-neutral final scope |
| `reconcile cache-versions --train-run-id ID` | Global cache-version state for one run | Shard-neutral final scope |
| `reconcile shard-assignments` | Fixed catalog, assignments, and all local fences | Shard-aware final scope |
| `reconcile shard-locators` | Global locators against all fixed storages | Shard-aware final scope |
| `reconcile shard-migration --migration-id ID` | Source, target, validation, central references, and write evidence | Shard-aware final scope |
| `shard-admin reconcile --train-run-id ID` | Bounded locator/resource reconciliation for one train run | Operator wrapper; not every scope |

No single command is a complete Milestone 4 verdict. The bounded runner combines
the shard-aware scopes, operator wrapper, admission/read-model/cache scopes,
migration validation, and bounded integrity SQL.

The current operator traversal is deterministic and serial over the three fixed
storages, with effective concurrency `1`. Pages, rows, timeout, output, and
completeness remain bounded.

The legacy-oriented seat-inventory and reservation-quota commands remain useful
for legacy mode. Do not cite them as schema-shard-wide evidence without a
shard-aware routed implementation.

Use a canonical bounded identifier, a configured row/page cap, query timeout,
and cancellation. Cross-shard aggregation must return `complete`, `partial`,
or `unavailable`; a partial result is not a clean reconciliation.

## When to run

Run the applicable scopes:

- before planning a migration;
- after quiescence and copy, as part of validation;
- immediately before cutover;
- immediately after cutover;
- before direct rollback or reverse migration;
- before completing a rollback window;
- before source cleanup;
- after concurrency, outage, recovery, and load tests; and
- on a bounded operational schedule for active/retained assignments.

## Incident handling

On a mismatch:

1. stop or drain new writes for the affected train run according to the
   incident policy;
2. preserve sanitized migration, assignment, fence, locator, and database
   evidence;
3. do not flip assignment, delete source rows, or clear Redis as a repair;
4. classify completeness and the violated invariant;
5. design and review an explicit repair with rollback; and
6. rerun every dependent invariant before serving resumes.

Outputs must not contain passenger PII, raw reservation/ticket rows,
idempotency material, credentials, DSNs, SQL, schema names, or local machine
paths.

See [migration validation](shard-migration.md) and
[failure policy](shard-failure-policy.md).
