# Journey Read-model Design

Milestone 3 adds a disposable PostgreSQL projection for public journey search.
PostgreSQL source tables remain authoritative for stations, routes, trains,
fares, schedules, and all booking state. The projection can lag, be dropped,
or be rebuilt; it is never consulted by reservation commands to allocate a
seat.

## Shape and identity

`train_run_journey_read_model` contains one row for each ordered pair of stops
and each active fare class on a train run. Its identity is:

```text
(train_run_id, origin_stop_index, destination_stop_index, seat_class)
```

Rows denormalize public station, train, service-date, departure/arrival, fare,
currency, and run-status observations. They do not contain passenger data,
seat segment masks, reservation state, or an authoritative availability
count. The search index begins with normalized origin, destination,
service-date, seat class, status, and departure time.

The projection uses the same journey-time anchor as the source search and
availability paths. This preserves overnight schedules and routes whose first
stop has a non-zero offset.

## Consistency boundary

Source mutations append minimized outbox events in their PostgreSQL
transaction. The outbox worker publishes committed events to
`railway:outbox:v1`. A `railway-read-model` consumer reloads current source
state, replaces a complete train-run projection, and records a durable receipt.
The event payload is a change notification, not a current-state document.

Receipt insertion and projection replacement share one PostgreSQL transaction.
A duplicate `(consumer_name, event_id)` is a successful no-op. An old event
delivered after a newer one still reloads the same current source state, so it
cannot overwrite rows with stale payload fields.

Cache invalidation follows the PostgreSQL commit. Redis is not part of a
distributed transaction. If rotation fails, the stream entry remains pending
for bounded retry; short TTLs remain the final staleness bound.

## Read path

1. Validate and normalize the request.
2. Resolve the current cache generation by exact key.
3. Return a valid current-generation cache entry when present.
4. On miss, coalesce identical fills within the API process.
5. Search the complete PostgreSQL projection.
6. If it is unavailable or has no usable result, run the normalized source
   query and record a projection fallback.
7. Best-effort fill Redis and return the complete PostgreSQL result.

A Redis error never permits a booking bypass. A projection error never returns
a known partial row set. Elevated lag is an alert condition, not by itself an
API readiness failure.

## Atomic replacement

`RebuildTrainRun` opens one transaction, obtains current source state, derives
all rows, deletes the old train-run set, inserts the new set, optionally records
the event receipt, and commits. Readers using PostgreSQL's statement snapshot
observe the old committed set or the new committed set, never the intermediate
delete. A missing source train run produces an empty derived set and removes
orphaned projection rows.

Cancelled runs remain representable for reconciliation but are filtered from
public search. Repeated rebuilds produce the same logical rows; only observation
timestamps are expected to change.

## Operations

`read-model-admin` provides bounded `rebuild-train-run`, `rebuild-all`,
`reconcile`, and `inspect-lag` commands. Rebuild commands are dry-run unless
`--apply` is explicit. `rebuild-all` uses a stable service-date/ID cursor and a
maximum batch of 100. `cmd/reconcile read-model` is detect-only.

The worker exposes private `/livez`, `/readyz`, and `/metrics`. Readiness checks
its process-owned PostgreSQL, Redis, configuration, and clean migration version
7. It receives neither JWT nor admission-token secrets.

## Deliberate exclusions

There is no search microservice, OpenSearch cluster, Kafka transport,
PostgreSQL/Redis distributed transaction, regional cache replication, train-run
shard, active-active writer, or payment implementation. The deployment remains
a single-region modular monolith with separate worker executables.
