# ADR 041: Physical Booking Shards Own Transactional Outboxes

- Status: Accepted for the Milestone 5 bounded pilot
- Date: 2026-07-29

## Context

ADR 033 keeps booking event intent in a central outbox because Milestone 4
stores control and logical-shard rows in one PostgreSQL database. A physical
booking shard cannot commit a booking mutation and a control-database outbox
row atomically. Publishing directly from a booking transaction would replace
that lost atomicity with a network call and still leave one-sided failure.

Milestone 5 must preserve durable event intent when the control database, one
booking shard, the relay, Redis, or a consumer is unavailable. It must also
keep global read models correct without claiming exactly-once delivery.

## Decision

Each physical booking PostgreSQL instance owns an `outbox_events` relation.
Every booking mutation writes its event intent in the same shard-local
transaction as inventory, reservation lifecycle, idempotency execution,
booking-command receipt, and target-write evidence. Control-plane mutations
continue to use a separate control-plane outbox in control PostgreSQL. No
transaction writes both outboxes.

### Event identity and envelope

The application generates a random UUID event ID before the local transaction
and uses it as the immutable event identity across shard storage, relay retry,
the global read model, and consumer receipts. Event IDs are globally unique by
contract; they are never derived from a shard-local sequence. Each local table
has a primary key on the event ID, and every global consumer has a unique
receipt on the same ID. An observed identity collision with a different event
fingerprint fails closed and requires reconciliation; it is not overwritten.

The versioned event envelope contains only bounded fields required by
consumers: event ID, allowlisted event type and schema version, aggregate type
and ID, train-run ID, observed assignment generation, database timestamp, and
a size-limited payload. It contains no DSN, connection reference, credential,
passenger name, email, identity document, admission token, raw idempotency key,
or arbitrary topology label. High-cardinality identities are not metric
labels.

### Relay and delivery

The event-relay module enumerates only the fixed, configured physical-shard
allowlist. It uses one bounded pool per shard, a per-shard batch limit,
transaction and statement timeouts, stable fair rotation, and a global
concurrency cap. A relay adapter claims rows with a bounded lease and
`SKIP LOCKED`, publishes outside the claim transaction, and conditionally
finalizes only the lease it owns. A crashed relay or timed-out publish leaves
the row retryable.

Delivery is at least once. Duplicate publish and duplicate consumer delivery
are expected. Consumers commit a receipt keyed by event ID with the derived
read-model mutation, so replay is idempotent. The system does not claim
exactly-once distributed processing. Redis Streams may transport events, but
Redis is neither the durable event authority nor the shard-discovery source.

Failure of one shard does not stop relay work for healthy shards. The relay
reports `complete`, `partial`, or `unavailable` with bounded shard and reason
labels. It never substitutes another shard, scans an unbounded catalog, or
logs connection details.

### Migration behavior

Source booking mutations continue to create source-local outbox rows while an
online base copy and journal catch-up run. Migration apply does not synthesize
a second business event for a copied row or replayed journal entry: the
original source transaction already owns that intent. Target apply receipts
deduplicate migration work separately from event receipts.

After cutover, new target mutations create target-local events. The relay keeps
the retained source in its bounded drain set until every committed source
event is published or explicitly reconciled. Source outbox history is not
blindly copied, relabelled, or deleted at cutover. Source cleanup is prohibited
until the drain, consumer-receipt, and reconciliation gates in ADR 044 pass.

## Consequences

- Booking state and event intent remain atomic inside the physical shard that
  owns the booking transaction.
- A shard commit can be relayed even when control-plane finalization is
  delayed; the command reconciler and read-model consumer remain idempotent.
- The relay requires bounded multi-database enumeration, per-shard pools,
  fairness, lag metrics, lease recovery, and partial-result semantics.
- Global UUID identity plus consumer receipts prevents ordinary replay from
  duplicating read-model effects, but does not create exactly-once delivery.
- Retaining and draining source outboxes increases storage and operational
  work during migration.
- The decision proves only a fixed, single-region pilot topology. It is not
  evidence of multi-region delivery, production capacity, or an RPO/RTO SLA.

## Rejected alternatives

- Keep one control-database booking outbox: rejected because it cannot commit
  atomically with a physical-shard mutation.
- Publish synchronously from the booking transaction: rejected because a
  network publish and PostgreSQL commit can succeed on only one side.
- Copy source events and republish them from the target: rejected because it
  creates two publication authorities for one business mutation.
- Use a per-shard sequence as global event identity: rejected because values
  collide across independent databases.
- Treat Redis as the durable outbox or discovery catalog: rejected because it
  can fail independently and cannot authorize PostgreSQL ownership.
- Claim exactly-once delivery: rejected because relay and consumer crashes
  require replay across independent durability domains.
