# Projection Event Processing

The read-model worker consumes the existing Redis Stream with consumer group
`railway-read-model`. Events are at-least-once and contain only the bounded
fields `event_id`, `event_type`, `aggregate_type`, and `aggregate_id` at the
consumer boundary.

## Processing algorithm

For every `RunOnce` pass the worker:

1. claims idle pending entries before reading new entries;
2. validates the allowlisted event/aggregate pair and UUIDs;
3. creates or resumes a durable PostgreSQL progress row;
4. rotates global cache generations before projection work and checkpoints that
   phase only after Redis succeeds;
5. resolves at most 100 affected train runs after the durable UUID cursor;
6. locks run projection writers in deterministic UUID order and reloads current
   source state under `READ COMMITTED`;
7. commits each complete train-run page and its cursor in one transaction;
8. atomically appends a safe-field continuation and acknowledges the old entry
   when another page remains;
9. inserts the final receipt and removes progress only after every required
   projection and invalidation phase succeeds;
10. acknowledges the completed stream entry.

Pending entries use Redis delivery counts for a maximum-attempt decision. A
poison or repeatedly failing event is appended to the bounded DLQ before its
source entry is acknowledged in the same Redis script. DLQ records contain only safe envelope metadata
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

The final receipt now means every invalidation checkpoint completed. A crash
after Redis rotation but before the PostgreSQL checkpoint safely repeats the
rotation; a completed duplicate performs no projection or invalidation work.

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

An event whose impact exceeds 100 runs is processed as keyset pages of at most
100. The progress row survives process termination, and projection search
explicitly falls back to the authoritative source while any projection page is
in progress. A 251-run event therefore advances as 100/100/51 without one
unbounded PostgreSQL transaction.

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

The source stream does not use blind `MAXLEN` trimming: Redis can trim entries
that are still referenced by a consumer-group PEL. Operators must capacity-plan
and alert on stream/backlog growth, then apply only a reviewed retention policy
that proves every consumer group has advanced beyond the removal floor.

Runtime controls are process-owned and bounded:

- `READ_MODEL_WORKER_ENABLED`, `READ_MODEL_WORKER_BATCH_SIZE`, and
  `READ_MODEL_WORKER_INTERVAL_MILLISECONDS`;
- `READ_MODEL_CONSUMER_GROUP` plus a unique `READ_MODEL_CONSUMER_NAME` per
  replica (an empty local value generates a unique process name);
- `READ_MODEL_MAX_ATTEMPTS` and `READ_MODEL_CLAIM_MIN_IDLE_SECONDS`, where
  claim-min-idle must be greater than `WORKER_PASS_TIMEOUT`.

If a dependency failure reaches the bounded retry limit, the safe envelope is
placed in the DLQ while durable progress keeps projection reads on source
fallback. After fixing the dependency, an operator previews and then redrives
that exact event:

```text
read-model-admin resume-event --event-id <uuid>
read-model-admin resume-event --event-id <uuid> --apply
```

The apply form requires secret-managed `DATABASE_URL`, `REDIS_ADDR` (or
`REDIS_ADDRESS`), and optional `REDIS_PASSWORD`. It reconstructs only the four
safe stream fields from PostgreSQL progress or, if failure preceded progress,
the exact published or dead-lettered outbox row. It does not copy payloads or
clear the safety gate. The normal worker removes progress only after
convergence. A pre-publication outbox dead letter is recovered with the same
exact-ID preview/apply command using the outbox event ID.

After complete Redis stream/PEL loss, preview and replay every missing receipt
in bounded stable-cursor pages:

```text
read-model-admin replay-outbox --batch-size 100
read-model-admin replay-outbox --batch-size 100 --apply
```

Already receipted events are excluded. Duplicate enqueues remain safe, and a
published projection event with neither progress nor receipt keeps projection
search on the authoritative source until recovery completes.
