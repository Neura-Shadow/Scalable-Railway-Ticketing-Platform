# Projection Event Processing

The read-model worker consumes the existing Redis Stream with consumer group
`railway-read-model`. Events are at-least-once and contain only the bounded
fields `event_id`, `event_type`, `aggregate_type`, and `aggregate_id` at the
consumer boundary.

## Processing algorithm

For every `RunOnce` pass the worker:

1. claims idle pending entries before reading new entries;
2. validates the allowlisted event/aggregate pair and UUIDs;
3. resolves at most 100 affected train runs from current PostgreSQL state;
4. opens the projection transaction;
5. inserts the durable consumer receipt;
6. treats a receipt conflict as duplicate success;
7. otherwise rebuilds each affected complete train-run set from current state;
8. commits receipt and projection together;
9. rotates the bounded station, search, or per-run availability generations;
10. acknowledges the stream entry only after required work succeeds.

Pending entries use Redis delivery counts for a maximum-attempt decision. A
poison or repeatedly failing event is appended to the bounded DLQ before its
source entry is acknowledged. DLQ records contain only safe envelope metadata
and a bounded reason. Raw payloads, cache values, credentials, and customer
identifiers are not copied.

## Ordering and idempotency

The durable receipt key is `(consumer_name, event_id)` where the logical
consumer name is stable even though each process receives a unique Redis
consumer instance name. This makes restarts and multiple replicas converge on
one PostgreSQL application of an event.

Handlers do not patch projection rows from event payloads. They use the event
only to identify an impact scope, then query current authoritative source rows.
Consequently duplicate and out-of-order deliveries converge on the latest
committed source state.

Invalidation is intentionally retried for duplicate events as well. A crash
after the PostgreSQL transaction but before Redis rotation/ACK therefore does
not strand the old generation: receipt conflict suppresses another projection
write, but the deterministic rotation mapping runs again.

## Impact map

| Aggregate | Projection scope |
|---|---|
| station | train runs whose route contains the station |
| route | train runs on the route |
| train | train runs using the train |
| coach or seat | train runs using the owning train |
| fare | the fare's train run, or bounded route runs |
| train_run | the named train run |
| reservation or ticket | availability for the named train run |

An event whose impact exceeds the 100-run bound fails visibly instead of doing
unbounded work in one pass. Operators then use the resumable rebuild command.

## Lifecycle and failure behavior

The process is disabled by default. Disabled mode performs no initial pass but
still serves private health until shutdown. Enabled mode runs one immediate
bounded pass and then ticks at the configured interval. Every pass has a
deadline. SIGINT/SIGTERM cancels the root context, stops new passes, and closes
the health server within the shutdown timeout.

Redis interruption prevents stream consumption and worker readiness; it does
not alter source tables or booking correctness. PostgreSQL failure likewise
prevents projection progress. Backlog, pending age, retries, DLQ, rebuild
duration, and projection lag are operational signals; none grants authority to
serve cached availability as a booking decision.
