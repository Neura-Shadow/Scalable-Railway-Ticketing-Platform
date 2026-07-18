-- Read-only checks before application rollback or exceptional migration-5 down.
-- Run only while the database is still on clean schema version 5.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '5min';
SET LOCAL lock_timeout = '2s';
SET LOCAL idle_in_transaction_session_timeout = '6min';

SELECT version,
       dirty,
       version = 5 AND NOT dirty AS rollback_source_is_clean
FROM schema_migrations;

SELECT EXISTS (
    SELECT 1
    FROM pg_trigger AS trigger_entry
    JOIN pg_class AS relation ON relation.oid = trigger_entry.tgrelid
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = current_schema()
      AND relation.relname = 'reservation_seats'
      AND trigger_entry.tgname = 'reservation_seats_populate_train_run_id'
      AND trigger_entry.tgenabled IN ('O', 'A')
) AS version_4_writer_compatibility_trigger_is_enabled;

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
SELECT 'missing_or_unvalidated_migration_5_constraints', count(*)
FROM (
    VALUES
        ('reservations', 'reservations_id_train_run_segment_count_key'),
        ('reservation_seats', 'reservation_seats_reservation_run_segment_fkey'),
        ('reservation_seats', 'reservation_seats_inventory_fkey')
) AS expected(table_name, constraint_name)
LEFT JOIN pg_class AS relation
  ON relation.oid = to_regclass(format('%I.%I', current_schema(), expected.table_name))
LEFT JOIN pg_constraint AS constraint_entry
  ON constraint_entry.conrelid = relation.oid
 AND constraint_entry.conname = expected.constraint_name
WHERE constraint_entry.oid IS NULL
   OR NOT constraint_entry.convalidated
ORDER BY check_name;

SELECT COALESCE(NULLIF(activity.application_name, ''), '(unset)') AS application_name,
       activity.state,
       count(*) AS session_count,
       max(clock_timestamp() - activity.xact_start) AS oldest_transaction_age
FROM pg_stat_activity AS activity
WHERE activity.datname = current_database()
  AND activity.pid <> pg_backend_pid()
GROUP BY COALESCE(NULLIF(activity.application_name, ''), '(unset)'), activity.state
ORDER BY application_name, activity.state;

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

SELECT 'operator_attestation_required' AS check_name,
       'SQL cannot prove that every version-5 process is drained or that restart automation is disabled.' AS required_attestation;

COMMIT;
