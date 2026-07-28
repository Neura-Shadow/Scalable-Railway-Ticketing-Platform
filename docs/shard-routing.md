# Shard Routing

## Contract

Routing maps a domain identifier to an opaque fixed shard handle and a positive
assignment generation. It is not write authority. Every mutation must recheck
the public assignment and the selected storage's fence inside the mutation
transaction.

The only Milestone 4 shard IDs are `legacy`, `shard-0`, and `shard-1`. Their
schema mapping is compiled into the PostgreSQL adapter. Application services
and HTTP handlers never receive schema names or construct schema-qualified SQL.

## Entry-point map

| Operation | Initial identifier | Resolution | Maximum booking-storage work |
|---|---|---|---:|
| Create reservation | Train-run ID | Current assignment | 1 |
| Availability | Train-run ID | Current assignment | 1 |
| Get/confirm/cancel reservation | Reservation ID | Reservation locator, then assignment | 1 |
| Get ticket order | Ticket-order ID | Ticket-order locator, then assignment | 1 |
| Get ticket | Ticket ID | Ticket locator, then assignment | 1 |
| Owner ticket-order list | Owner and page | Global locator page, grouped by page routes | Bounded page routes |
| Hold expiration | Bounded authoritative workset | Assignment and locator recheck per item | At most the configured subset of 3 fixed storages |
| Journey search | Search terms | Global projection/source | 0 |
| Migration inspection | Train run or migration ID | Recorded source and target | At most 2 |

A missing, malformed, or stale locator fails safely or enters detect-only
reconciliation. It never triggers a scan through all booking schemas.

## Route cache

The optional in-process cache is keyed by train-run ID and stores only a fixed
shard ID and observed generation. It has a configured TTL and maximum entry
count. Cache eviction or staleness affects latency, not correctness.

On `shard_assignment_stale`:

1. discard the cached route;
2. refresh from PostgreSQL once;
3. retry the whole routed operation at most once; and
4. stop with a bounded retryable error if authority changes again.

There is no legacy fallback, hash fallback, random selection, blind probe, or
retry against every storage.

## Safe schema selection

The adapter applies one of these constant transaction-local paths:

| Logical storage | Transaction-local `search_path` |
|---|---|
| `legacy` | `pg_catalog, public, pg_temp` |
| `shard-0` | `pg_catalog, booking_shard_0, public, pg_temp` |
| `shard-1` | `pg_catalog, booking_shard_1, public, pg_temp` |

`pg_catalog` is first and `pg_temp` is explicit and last so a temporary object
cannot shadow a catalog, shard-local, or public relation. The required
properties are:

- no HTTP, JWT, Redis, arbitrary environment, or unvalidated catalog value is
  interpolated into an SQL identifier;
- unknown shard IDs fail before SQL execution;
- commit, rollback, timeout, and cancellation return a pooled connection to its
  prior behavior;
- concurrent transactions cannot observe another transaction's schema; and
- triggers/functions resolve explicitly to intended local or `public` objects.

Session-wide mutable `search_path` and caller-managed reset are prohibited.

## Configuration

| Environment variable | Default | Purpose |
|---|---:|---|
| `BOOKING_SHARD_MODE` | `legacy` | `legacy` or explicit `schema_poc` routing mode |
| `BOOKING_SHARD_IDS` | `legacy` | Comma-separated fixed logical IDs; schema mode defaults to all three |
| `BOOKING_SHARD_SCHEMA_POC_PRODUCTION_ENABLED` | `false` | Explicit production acknowledgement for the PoC |
| `BOOKING_ROUTE_CACHE_ENABLED` | `true` | Enables the bounded hint cache |
| `BOOKING_ROUTE_CACHE_TTL_SECONDS` | `30` | Cache TTL |
| `BOOKING_ROUTE_CACHE_MAX_ENTRIES` | `1000` | Process-local entry bound |
| `BOOKING_SHARD_QUERY_TIMEOUT` | `2s` | Routed query bound |

Production schema mode is default-deny and requires explicit acknowledgement,
a valid fixed topology, Migration 8, and a reviewed old-writer drain. Config
validation rejects unknown or duplicate IDs and rejects schema identifiers in
place of logical IDs. Full configuration must not be logged.

Worker traversal is a serial, fail-isolated pass over the configured subset of
the three compiled-in storages; it has no independent fanout setting. Private
reconciliation traversal is also serial, with effective concurrency `1`.
Operator commands bound each invocation with `--timeout`. Migration copy uses
the bounded `--batch-size` flag, and `plan-migration` persists the bounded
retained-source duration supplied by `--rollback-window`.

These are validated `shard-admin` command inputs, not application runtime
environment settings.

## Public errors and telemetry

Internal failures use bounded categories such as
`shard_assignment_stale`, `shard_write_fenced`, `shard_unavailable`, and
`train_run_migrating`. Customer responses collapse topology-sensitive details
to a safe retryable error such as `service_temporarily_rebalancing`.

Logs, errors, health responses, and metrics must not expose schemas,
generations, migration IDs, DSNs, SQL, resource IDs, or customer data. Metric
`shard_id` values, where used, come only from the fixed allowlist.

See [single-writer fencing](single-writer-fencing.md),
[cross-shard queries](cross-shard-queries.md), and
[ADR 028](adr/028-shard-catalog-and-routing.md).
