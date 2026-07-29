# Shard-Local Booking Snapshots

## Why snapshots are local

A booking transaction cannot synchronously query the control database and
still claim a shard-local atomic boundary. Each physical booking database
therefore stores the immutable/versioned booking facts needed to validate a
command: train-run status and segment count, route/policy versions, seat and
coach catalog, fare intervals, class, currency, and source version.

Booking-shard schema version 1 provides `train_run_booking_snapshots`,
`booking_seat_catalog`, and `booking_fare_snapshots`. Inventory and reservation
rows repeat the generation/segment facts needed for database constraints.

## Installation and use

Snapshot installation is idempotent by stable identity and monotonic source
version. The normal writer locks the train-run fence and booking snapshot in
the same local transaction before inventory mutation. It fails closed when the
snapshot is absent, inactive, cancelled, non-bookable, incompatible with the
route generation, or stale relative to the command contract.

Control projection publication must not announce a fare, cancellation, seat,
or policy change as usable until the owning shard has installed compatible
local state. Migration copies snapshot dependencies before mutable booking
rows and validates identities, versions, segment bounds, classes, and
relationships at the target.

Snapshots deliberately duplicate non-PII reference data. They do not make the
booking shard authoritative for global offering management. See
[ADR 040](adr/040-shard-local-booking-reference-snapshots.md).
