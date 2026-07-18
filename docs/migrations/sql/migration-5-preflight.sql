-- Migration 5 preflight. Run on clean schema version 4.
-- Every incompatible_rows result must be zero. A timeout is not a pass.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '5min';
SET LOCAL lock_timeout = '2s';
SET LOCAL idle_in_transaction_session_timeout = '6min';

SELECT current_database() AS database_name,
       current_schema() AS schema_name,
       current_setting('server_version') AS server_version,
       current_setting('transaction_read_only') AS transaction_read_only;

SELECT version,
       dirty,
       version = 4 AND NOT dirty AS ready_for_migration_5
FROM schema_migrations;

WITH targets(table_name) AS (
    VALUES
        ('reservations'),
        ('reservation_seats'),
        ('seat_inventory'),
        ('routes'),
        ('route_stops')
)
SELECT targets.table_name,
       GREATEST(COALESCE(classes.reltuples, 0), 0)::bigint AS planner_estimated_rows,
       COALESCE(stats.n_live_tup, 0)::bigint AS statistics_estimated_live_rows,
       pg_size_pretty(pg_table_size(classes.oid)) AS heap_size,
       pg_size_pretty(pg_indexes_size(classes.oid)) AS indexes_size,
       pg_size_pretty(pg_total_relation_size(classes.oid)) AS total_size,
       stats.last_analyze,
       stats.last_autoanalyze
FROM targets
LEFT JOIN pg_class AS classes
  ON classes.oid = to_regclass(format('%I.%I', current_schema(), targets.table_name))
LEFT JOIN pg_stat_user_tables AS stats
  ON stats.schemaname = current_schema()
 AND stats.relname = targets.table_name
ORDER BY targets.table_name;

SELECT activity.pid,
       COALESCE(NULLIF(activity.application_name, ''), '(unset)') AS application_name,
       activity.state,
       activity.wait_event_type,
       activity.wait_event,
       clock_timestamp() - activity.xact_start AS transaction_age,
       left(md5(COALESCE(activity.query, '')), 12) AS query_fingerprint
FROM pg_stat_activity AS activity
WHERE activity.datname = current_database()
  AND activity.pid <> pg_backend_pid()
  AND activity.xact_start IS NOT NULL
ORDER BY activity.xact_start;

SELECT locks.relation::regclass::text AS relation_name,
       locks.mode,
       locks.granted,
       count(*) AS lock_count
FROM pg_locks AS locks
WHERE locks.database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND locks.pid <> pg_backend_pid()
  AND locks.relation IN (
      to_regclass('reservations'),
      to_regclass('reservation_seats'),
      to_regclass('seat_inventory'),
      to_regclass('routes'),
      to_regclass('route_stops')
  )
GROUP BY locks.relation, locks.mode, locks.granted
ORDER BY relation_name, locks.mode, locks.granted;

SELECT 'reservation_seat_train_run_backfill_would_be_null' AS check_name,
       count(*) AS incompatible_rows
FROM reservation_seats AS reservation_seat
LEFT JOIN reservations AS reservation
  ON reservation.id = reservation_seat.reservation_id
WHERE reservation.train_run_id IS NULL
UNION ALL
SELECT 'reservation_seat_segment_mismatch', count(*)
FROM reservation_seats AS reservation_seat
JOIN reservations AS reservation
  ON reservation.id = reservation_seat.reservation_id
WHERE reservation.segment_count IS DISTINCT FROM reservation_seat.segment_count
UNION ALL
SELECT 'reservation_seat_missing_inventory_for_run', count(*)
FROM reservation_seats AS reservation_seat
JOIN reservations AS reservation
  ON reservation.id = reservation_seat.reservation_id
LEFT JOIN seat_inventory AS inventory
  ON inventory.train_run_id = reservation.train_run_id
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
SELECT 'duplicate_reservation_run_segment_key', count(*)
FROM (
    SELECT reservation.id, reservation.train_run_id, reservation.segment_count
    FROM reservations AS reservation
    GROUP BY reservation.id, reservation.train_run_id, reservation.segment_count
    HAVING count(*) > 1
) AS duplicates
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

WITH expected(table_name, trigger_name) AS (
    VALUES
        ('seat_inventory', 'seat_inventory_validate_class'),
        ('reservation_seats', 'reservation_seats_validate'),
        ('route_stops', 'route_stops_validate_sequence')
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

COMMIT;
