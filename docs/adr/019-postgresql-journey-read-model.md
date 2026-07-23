# ADR 019: PostgreSQL Journey Read Model

- Status: Accepted
- Date: 2026-07-22

## Context

Customer train search currently joins train runs, trains, routes, route stops,
stations, and fares for every request. The application then executes one
authoritative availability query per returned train run. These reads are
correct but repeat relational work on the same PostgreSQL primary that owns
booking transactions.

Milestone 3 needs an independently optimizable search read path without a new
network service, a new database authority, or a claim that reads are strongly
consistent. Current search semantics are relational and bounded by origin,
destination, service date, seat class, pagination, status, and allowlisted sort.

## Decision

Create `train_run_journey_read_model` in PostgreSQL as a disposable projection.
One row represents one train run, ordered origin/destination pair, and active
seat class fare. It includes train, route, station, schedule, status, fare,
currency, `source_updated_at`, and `rebuilt_at` observations.

The unique identity is `(train_run_id, from_stop_index, to_stop_index,
seat_class)`. Checks require `from_stop_index < to_stop_index`, non-empty station
codes, non-negative fare, a normalized three-letter currency, a known train-run
status, and departure before arrival. Combined indexes support origin code,
destination code, service date, seat class, status, departure, and fare sorts.

The projection stores no seat inventory mask and no authoritative availability
count. Availability is computed from `seat_inventory` and cached separately as
a hint. Reservation creation continues to read and mutate authoritative source
tables only.

Public search reads this projection first. A missing, unavailable, or detected
inconsistent projection uses the existing normalized PostgreSQL source query
and records a bounded fallback/repair signal. Cancelled rows are retained for
diagnosis and reconciliation but excluded from customer search. A permanently
removed train run has no projection rows.

The Query supporting context remains inside the modular monolith. No separate
search service is created.

## Consequences

- Repeated search avoids reconstructing the full normalized join when the
  projection is current.
- Projection lag may temporarily delay schedule/fare visibility, but source
  fallback remains available and booking correctness is unchanged.
- The projection consumes additional PostgreSQL disk and index maintenance.
- Initial backfill must be bounded, measured, resumable, and explicit about
  locks and disk growth.
- PostgreSQL remains both source authority and current read-model host, so this
  does not prove unlimited or national-scale read capacity.

## Rejected alternatives

- Elasticsearch or OpenSearch: rejected because current relational filters do
  not justify another distributed data system and operational failure domain.
- Redis-only search documents: rejected because Redis eviction/loss is not a
  durable rebuild checkpoint and source fallback remains relational.
- Continue only normalized joins: rejected because it does not meet the
  independent read-scaling and measurable repeated-query reduction objective.
- Put availability masks/counts in the projection as authority: rejected
  because booking occupancy changes independently and remains PostgreSQL seat
  inventory state.
