-- Migration 5 post-validation. Run on clean schema version 5.
-- Every incompatible_rows result must be zero and every expected object true.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '5min';
SET LOCAL lock_timeout = '2s';
SET LOCAL idle_in_transaction_session_timeout = '6min';

SELECT version,
       dirty,
       version = 5 AND NOT dirty AS migration_5_is_clean
FROM schema_migrations;

SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'reservation_seats'
      AND column_name = 'train_run_id'
      AND is_nullable = 'NO'
) AS reservation_seats_train_run_id_is_not_null;

WITH expected(table_name, constraint_name, constraint_type) AS (
    VALUES
        ('reservations', 'reservations_id_train_run_segment_count_key', 'u'::"char"),
        ('reservation_seats', 'reservation_seats_reservation_run_segment_fkey', 'f'::"char"),
        ('reservation_seats', 'reservation_seats_inventory_fkey', 'f'::"char")
)
SELECT expected.table_name,
       expected.constraint_name,
       constraint_entry.oid IS NOT NULL AS is_present,
       COALESCE(constraint_entry.contype = expected.constraint_type, false) AS has_expected_type,
       COALESCE(constraint_entry.convalidated, false) AS is_validated,
       CASE
           WHEN expected.constraint_type = 'u'::"char"
               THEN COALESCE(index_entry.indisvalid AND index_entry.indisready, false)
           ELSE true
       END AS backing_index_is_ready
FROM expected
LEFT JOIN pg_class AS relation
  ON relation.oid = to_regclass(format('%I.%I', current_schema(), expected.table_name))
LEFT JOIN pg_constraint AS constraint_entry
  ON constraint_entry.conrelid = relation.oid
 AND constraint_entry.conname = expected.constraint_name
LEFT JOIN pg_index AS index_entry
  ON index_entry.indexrelid = constraint_entry.conindid
ORDER BY expected.table_name, expected.constraint_name;

SELECT NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_entry
    JOIN pg_class AS relation ON relation.oid = constraint_entry.conrelid
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = current_schema()
      AND relation.relname = 'reservation_seats'
      AND constraint_entry.conname = 'reservation_seats_reservation_id_segment_count_fkey'
) AS legacy_reservation_seat_foreign_key_is_absent;

WITH expected(table_name, trigger_name) AS (
    VALUES
        ('seat_inventory', 'seat_inventory_validate_class'),
        ('reservation_seats', 'reservation_seats_populate_train_run_id'),
        ('reservation_seats', 'reservation_seats_validate'),
        ('route_stops', 'route_stops_validate_sequence'),
        ('routes', 'routes_validate_minimum_stops'),
        ('route_stops', 'route_stops_validate_minimum')
)
SELECT expected.table_name,
       expected.trigger_name,
       trigger_entry.oid IS NOT NULL AS is_present,
       COALESCE(trigger_entry.tgenabled IN ('O', 'A'), false) AS is_enabled
FROM expected
LEFT JOIN pg_class AS relation
  ON relation.oid = to_regclass(format('%I.%I', current_schema(), expected.table_name))
LEFT JOIN pg_trigger AS trigger_entry
  ON trigger_entry.tgrelid = relation.oid
 AND trigger_entry.tgname = expected.trigger_name
ORDER BY expected.table_name, expected.trigger_name;

SELECT 'reservation_seat_train_run_id_is_null' AS check_name,
       count(*) AS incompatible_rows
FROM reservation_seats AS reservation_seat
WHERE reservation_seat.train_run_id IS NULL
UNION ALL
SELECT 'reservation_seat_train_run_disagrees_with_reservation', count(*)
FROM reservation_seats AS reservation_seat
JOIN reservations AS reservation
  ON reservation.id = reservation_seat.reservation_id
WHERE reservation_seat.train_run_id IS DISTINCT FROM reservation.train_run_id
   OR reservation_seat.segment_count IS DISTINCT FROM reservation.segment_count
UNION ALL
SELECT 'reservation_seat_missing_inventory_for_run', count(*)
FROM reservation_seats AS reservation_seat
LEFT JOIN seat_inventory AS inventory
  ON inventory.train_run_id = reservation_seat.train_run_id
 AND inventory.seat_id = reservation_seat.seat_id
WHERE inventory.seat_id IS NULL
UNION ALL
SELECT 'inventory_wrong_train_or_class', count(*)
FROM seat_inventory AS inventory
JOIN train_runs AS train_run
  ON train_run.id = inventory.train_run_id
JOIN seats AS seat
  ON seat.id = inventory.seat_id
JOIN coaches AS coach
  ON coach.id = seat.coach_id
WHERE coach.train_id IS DISTINCT FROM train_run.train_id
   OR coach.seat_class IS DISTINCT FROM inventory.seat_class
UNION ALL
SELECT 'non_contiguous_route_stop_rows', count(*)
FROM (
    SELECT route_stop.route_id,
           route_stop.stop_index,
           row_number() OVER (
               PARTITION BY route_stop.route_id
               ORDER BY route_stop.stop_index
           ) - 1 AS expected_index
    FROM route_stops AS route_stop
) AS ordered_stops
WHERE ordered_stops.stop_index <> ordered_stops.expected_index
UNION ALL
SELECT 'decreasing_route_offset_rows', count(*)
FROM (
    SELECT route_stop.route_id,
           route_stop.arrival_offset_minutes,
           lag(route_stop.departure_offset_minutes) OVER (
               PARTITION BY route_stop.route_id
               ORDER BY route_stop.stop_index
           ) AS previous_departure
    FROM route_stops AS route_stop
) AS ordered_offsets
WHERE ordered_offsets.previous_departure IS NOT NULL
  AND ordered_offsets.arrival_offset_minutes < ordered_offsets.previous_departure
UNION ALL
SELECT 'routes_with_fewer_than_two_stops', count(*)
FROM routes AS route
WHERE (SELECT count(*) FROM route_stops AS route_stop WHERE route_stop.route_id = route.id) < 2
ORDER BY check_name;

COMMIT;
