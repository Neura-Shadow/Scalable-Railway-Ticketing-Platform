# Milestone 4 Data Dependency Map

Status: Architecture gate candidate; implementation must not change the table boundary until this map passes independent review

Last updated: 2026-07-23

## Scope and evidence baseline

Milestone 4 adds reversible train-run routing and schema-isolated logical booking shards inside the existing single PostgreSQL database. It does not add physical databases, distributed transactions, dual writes, regional writers, or a new network module.

This map is grounded in migrations 1 through 7 and the current PostgreSQL transactions in `internal/booking/postgres/reservation_store.go`. The current create, confirm, cancel, and expiration paths commit booking state, durable idempotency, and outbox intent together. Any sharding design that splits that atomic transaction is rejected.

The proposed fixed storage topology is:

- `public`: global/control data and the compatible `legacy` booking shard;
- `booking_shard_0`: opt-in logical booking shard `shard-0`; and
- `booking_shard_1`: opt-in logical booking shard `shard-1`.

Schema identifiers are compile-time allowlisted. Customer input, JWT claims, Redis, catalog rows, and arbitrary environment strings can select neither a schema nor a database identifier.

## Table classification

| Relation or state | Classification | Milestone 4 authority | Routed by | Notes and future physical-shard constraint |
|---|---|---|---|---|
| `users` | account/identity | `public` | user ID | Global FK target; physical extraction needs replicated identity or removal of cross-database FK |
| `refresh_tokens` | account/identity | `public` | user ID | Not train-run-scoped |
| `passengers` | account/identity | `public` | owner and passenger ID | Read/locked by create; remains global and PII-bearing |
| `stations` | railway reference | `public` | station code/ID | Global catalog |
| `routes` | railway reference | `public` | route ID | Global catalog |
| `route_stops` | railway reference | `public` | route and stop index | Global journey definition |
| `trains` | railway reference | `public` | train ID | Global catalog |
| `coaches` | railway reference | `public` | train ID | Global FK target for seats |
| `seats` | railway reference | `public` | train/coach/seat ID | Global FK target for shard inventory and reservation seats |
| `train_runs` | global catalog | `public` | train-run ID | Shard key source and global lifecycle authority; not copied |
| `fares` | railway reference | `public` | route or train-run ID | Mixed global/train-run lookup, but pricing authority remains global |
| `hot_train_policies` | global catalog/control | `public` | train-run ID | Admission control only; Redis does not choose booking storage |
| `booking_shards` | global catalog/control | `public` | shard ID | Fixed rows `legacy`, `shard-0`, `shard-1`; validates enabled/write state |
| `train_run_shard_assignments` | global catalog/control | `public` | train-run ID | One durable assignment and positive monotonic generation per run |
| `train_run_shard_migrations` | global catalog/control | `public` | train-run or migration ID | One active migration per run; bounded phase/cursor/error metadata |
| `train_run_generation_writes` | global catalog/control | `public` | train-run and generation | Atomic first-success evidence; absence is required for direct post-cutover rollback |
| `booking_idempotency_key_claims` | global integrity guard | `public` | user, operation, key hash | Inserted only after route/fence selection and in the same transaction as the local record; preserves ADR 005 uniqueness but stores no completion result and cannot route or replay a command; shares the local expiry and may be atomically reacquired only after that expiry |
| `reservation_quota_claims` | unsupported cross-shard made explicit | `public` | owner and reservation ID | Preserves user-wide hold/passenger limits under one advisory-lock protocol; must be replaced or coordinated before physical extraction |
| `seat_inventory` | train-run authoritative | assigned shard schema | train-run ID | Segment masks and version remain PostgreSQL authority |
| `reservations` | train-run authoritative | assigned shard schema | train-run ID or reservation locator | Parent for reservation lifecycle |
| `reservation_seats` | train-run authoritative | assigned shard schema | reservation locator | Colocated with reservation/inventory; retains global passenger and seat FKs in this PoC |
| `ticket_orders` | train-run authoritative | assigned shard schema | ticket-order locator | Created only by confirmation |
| `tickets` | train-run authoritative | assigned shard schema | ticket locator or containing order | Colocated with order and reservation seat |
| `idempotency_records` | booking idempotency | assigned shard schema | train-run route or reservation locator | Acquired only after routing and fence validation; copied with its train run |
| `outbox_events` | outbox/event relay | centrally indexed in `public` | global relay with bounded train-run/shard provenance | Booking and offering intent stays in one same-database table and commits with its source transaction; it is validated, not copied, during migration and remains an explicit physical-extraction limitation |
| `reservation_shard_locators` | global catalog/control | `public` | reservation ID | Inserted in the reservation transaction; ordinary reservation access never scans shards |
| `ticket_order_shard_locators` | global catalog/control | `public` | ticket-order ID or owner page | Inserted with order and maintains owner, status, total, currency, and created time; exact global sort/total comes from this index before fetching tickets for only the bounded page routes |
| `ticket_shard_locators` | global catalog/control | `public` | ticket ID | Inserted with ticket and references its reservation/order route; supports event/read routing without shard scans |
| `train_run_write_fences` | train-run authoritative control | every booking shard schema | train-run ID | Assignment generation and write-enabled predicate are checked in every mutation transaction |
| shard copy/reconciliation progress | train-run derived/control | `public` migration row plus per-shard observations | migration ID | Detect-only evidence; never booking authority |
| `train_run_journey_read_model` | global derived/read model | `public` | search keys | Disposable Milestone 3 projection; never copied to booking shards |
| `read_model_event_receipts` | global derived/read model | `public` | consumer/event ID | At-least-once deduplication remains global |
| `read_model_event_progress` | global derived/read model | `public` | stream/consumer | Operational progress, not booking authority |
| `read_model_projection_state` | global derived/read model | `public` | projection name | Global readiness/fallback metadata |
| Redis waiting-room/admission state | ephemeral admission | Redis | train-run request identity | Token binds train run, not shard; never overrides catalog or fence |
| Redis query caches and namespace versions | train-run/global derived | Redis | normalized cache key | Availability envelopes include assignment generation; disposable and non-authoritative |

## Atomic transaction groups

### Reservation create

Current evidence: durable idempotency, hot-policy recheck, quota, `train_runs`, passengers, fares, inventory masks, reservation rows, and outbox commit in one transaction.

Milestone 4 routed group:

1. resolve the train-run assignment outside the transaction;
2. begin a transaction with an allowlisted transaction-local shard schema;
3. lock and verify `public.train_run_shard_assignments`, `public.booking_shards`, and the local `train_run_write_fences` row;
4. insert or validate the minimal global `booking_idempotency_key_claims` row, then acquire shard-local `idempotency_records`;
5. lock the global user quota seam and check/update `public.reservation_quota_claims`;
6. read global train-run, fare, policy, and passenger references;
7. allocate shard-local inventory and insert reservation and reservation seats;
8. insert `public.reservation_shard_locators` with the route generation;
9. append the centrally indexed `public.outbox_events` booking intent with bounded provenance;
10. record successful generation-write evidence only for a non-replay mutation; and
11. commit everything together.

Any assignment/fence/migration rejection rolls back idempotency ownership, quota claims, locator, outbox, and booking rows.

The global key claim and local idempotency record use the same database-derived
expiry. A conflicting claim is reusable only when both the claim expiry has
passed and the routed transaction can atomically replace it together with the
new local record. Bounded cleanup never deletes a claim before its local retry
window; migration copies the local record without changing the global expiry.

### Reservation confirm

1. resolve the global reservation locator;
2. begin the locator's current routed transaction;
3. lock/verify assignment and fence, refreshing once on a stale locator/route;
4. validate the global idempotency key claim and acquire shard-local confirm idempotency;
5. lock and owner-check the shard-local reservation, then close its global active-hold quota claim;
6. insert shard-local ticket order/tickets and global ticket-order/ticket locators with exact global list metadata;
7. append central ticket and reservation outbox events;
8. record successful generation-write evidence; and
9. commit atomically.

### Reservation cancel

1. resolve the reservation locator and routed transaction;
2. verify assignment/fence before idempotency ownership;
3. owner-lock the reservation, release local inventory masks, cancel local order/tickets, and update the global quota claim;
4. update global ticket-order locator status where applicable, append central outbox intent and generation-write evidence; and
5. commit atomically.

### Hold expiration

The hold-expirer enumerates a bounded allowlisted shard workset. Each claimed reservation uses its locator and local transaction to verify the current generation/fence before changing status, releasing inventory, updating the global quota claim, appending outbox intent, and committing. A stale retained source row cannot expire after cutover.

### Reconciliation

Reconciliation is detect-only. A train-run scope resolves exactly one current route and may compare the retained source and migration target only when an explicit migration ID authorizes that comparison. Global assignment, fence, locator, quota-claim, and outbox checks use bounded joins and allowlisted shard worksets. Production reconciliation never repairs automatically.

## Cross-seam foreign keys and dependencies

| Local relation | Dependency | PoC enforcement | Physical-shard consequence |
|---|---|---|---|
| `seat_inventory` | `public.train_runs`, `public.seats` | Cross-schema FK | Cannot survive separate databases |
| `reservations` | `public.users`, `public.train_runs` | Cross-schema FK | Identity/train-run validation needs a new model |
| `reservation_seats` | local reservation/inventory; `public.passengers`, `public.seats` | Local composite FKs plus cross-schema FKs | Passenger/seat reference validation must be replicated or application-enforced |
| `ticket_orders` | local reservations; `public.users` | Local plus cross-schema FK | User reference must be redesigned |
| `tickets` | local orders and reservation seats | Local FKs | Portable when all booking rows move together |
| local `idempotency_records` | `public.users`; logical reservation resource | Cross-schema user FK; logical resource check | User reference and move protocol require redesign |
| global idempotency key claims | local `idempotency_records` | Same-database atomic insert; no completion result | Explicit blocker requiring a new uniqueness protocol before physical extraction |
| central `public.outbox_events` | local booking aggregate and train-run provenance | No aggregate FK; atomic same-database insert | Must move or gain a physical-shard relay protocol before extraction |
| global locators | assignment plus local resource existence | Assignment FK and transaction/reconciliation checks | Cross-database atomic locator updates require a new protocol |
| global quota claims | local reservation lifecycle | Same-database atomic write and global advisory lock | Explicit blocker to physical extraction |

Existing trigger functions contain unqualified table names. Migration 8 must install schema-fixed trigger implementations or prove that the allowlisted transaction-local `search_path` is applied for every local mutation and reset after commit/rollback. Tests must reject identifier injection and cross-transaction schema leakage.

Migration 8 must also install database guards on retained `public` booking tables. The guards derive the row's train run and reject mutations when its assignment is no longer `legacy` or its legacy fence is disabled. This makes stale binaries and any repository path that bypasses the routed transaction fail closed after cutover. `schema_poc` rollout additionally requires an explicit minimum-writer-version/drain gate so no known pre-M4 writer remains active.

## Entry-point routing map

| Entry point | Initial identifier | Resolution | Fanout rule |
|---|---|---|---|
| create reservation | train-run ID | current assignment | exactly one shard; refresh once on stale fence |
| reservation get/confirm/cancel | reservation ID | global reservation locator | no shard scan |
| ticket-order get | ticket-order ID | global ticket-order locator | no shard scan |
| owner ticket-order list | owner ID plus bounded page | page global locator index, group only page routes | bounded by page size, not shard count |
| availability | train-run ID | current assignment | exactly one authoritative shard |
| search | normalized journey query | global projection/source | never enumerate booking shards; bounded result availability grouping only |
| operator inventory initialization | train-run ID | current assignment | exactly one fenced shard |
| waiting-room join/status | train-run/request or queue token | global policy plus Redis | shard-neutral; route only at booking use |
| seat reconciliation | train-run ID | current assignment | exactly one current shard unless explicit migration comparison |
| migration/admin inspection | train-run or migration ID | catalog plus explicit source/target | at most the recorded pair |
| cross-shard admin summary | operator command | allowlisted enabled workset | bounded concurrency, per-shard/global deadlines, explicit partial result |

## Worker routing map

| Process | Routed work | Global work | Failure isolation |
|---|---|---|---|
| API | create/read/confirm/cancel and availability | auth, catalog, search projection, admission policy | one failed logical shard returns bounded error without exposing topology |
| hold-expirer | bounded batches per enabled authoritative shard | none beyond catalog/quota/locators | continue healthy shards; stale source fails fence |
| outbox-worker | none; booking events carry central bounded route provenance | the central booking/offering outbox in `public` | existing bounded lease/finalize protocol continues without migration copy or source/target claim races |
| admission-worker | none | policy catalog and Redis queue | never selects or caches shard ownership |
| read-model-worker | routed authoritative aggregate reload for booking events | global receipt/projection/cache invalidation | event provenance/locators avoid shard scans |
| reconcile | routed train-run and bounded shard summaries | account/admission/read-model/cache checks | explicit complete/partial/unavailable result |
| shard-admin | recorded source/target or bounded workset | catalog/migration control | dry-run/confirmation/cancellation and no automatic cleanup |

## Routing and fencing seam

The deepest Milestone 4 module owns this interface as one operation rather than exposing schema construction to callers:

`resolve -> begin routed transaction -> set allowlisted local schema -> lock assignment -> verify shard/generation/state -> lock local fence -> run mutation -> record locator/outbox/write evidence -> commit`

The HTTP module knows no schema name. Application modules pass train-run or opaque resource IDs. PostgreSQL adapters receive only validated shard handles. Route cache entries are bounded and disposable; database locks and fencing make stale entries safe.

Stable serving states require exactly one write-enabled fence. The bounded quiesced copy/cutover interval may have zero writable fences. More than one writable fence is always invalid.

## Migration dependency order

The source is authoritative until cutover and becomes immutable before copy. No source/target dual write exists.

1. disable new creates during drain while existing lifecycle operations finish;
2. acquire the assignment lock and disable the source fence, waiting on transaction-held fence/assignment locks rather than sleeping;
3. copy in deterministic resumable phases: inventory, reservations, reservation seats, orders, tickets, idempotency, local fence/reconciliation observations; central quota, key-claim, locator, and outbox rows are not copied;
4. validate row identities/counts, states, masks, quota claims, idempotency resources/key claims, order/ticket links, central outbox provenance, and locator expectations;
5. preflight a documented locator row cap, required indexes, and cutover statement timeout, then atomically install target generation/fence, update reservation/order/ticket locators, switch assignment, preserve disabled source, rotate availability namespace intent, and enter the rollback window;
6. allow target writes only after the cutover commit; and
7. retain source read-only until a separately confirmed cleanup after the rollback window.

Pre-cutover rollback re-enables the source without a routing change. Post-cutover direct rollback takes the assignment and both fences under exclusive locks that conflict with mutation fence locks, then checks for target-generation write evidence in that same transaction. It is allowed only when no evidence exists and always uses a newer generation. Otherwise a complete reverse migration is mandatory.

## Unsupported cross-shard operations

- A customer request may not select a shard or schema.
- Ordinary reservation, ticket, availability, or idempotency lookup may not probe every shard.
- A booking transaction may not span two authoritative booking schemas.
- Source and target may not both accept writes.
- Redis may not override assignments, fences, locators, idempotency, quota, or outbox state.
- Cross-shard foreign keys or atomicity are not claimed to work across future databases.
- Global idempotency-key uniqueness, user-wide quota, locator atomicity, and the central outbox are explicit blockers for physical database extraction.
- Cleanup may not run automatically or before revalidation and rollback-window expiry.

## Architecture review checklist

- [x] Every existing booking table and transaction is classified.
- [x] Global/reference, authoritative, derived, idempotency, outbox, and unsupported cross-shard state are distinguished.
- [x] Create, confirm, cancel, expire, and reconciliation atomic groups are mapped.
- [x] Cross-schema FKs and future physical-shard limits are explicit.
- [x] Train-run, reservation, ticket-order, ticket, worker, and global read entry points have a bounded routing rule.
- [x] Legacy compatibility uses explicit generation-1 legacy assignments bootstrapped for populated v7 train runs; new runs receive a legacy assignment transactionally unless explicitly assigned otherwise.
- [x] The migration interval permits zero writers but never two.
- [x] Independent architecture review confirms no remaining Critical or High finding and implementation may begin.
