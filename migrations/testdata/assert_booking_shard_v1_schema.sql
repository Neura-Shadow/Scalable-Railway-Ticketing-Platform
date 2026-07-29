DO $assert$
DECLARE
    current_version bigint;
    current_dirty boolean;
BEGIN
    SELECT version, dirty
    INTO current_version, current_dirty
    FROM public.schema_migrations
    LIMIT 1;

    IF current_version <> 1 OR current_dirty THEN
        RAISE EXCEPTION 'booking shard migration must be clean at version 1';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN (
              'train_run_booking_snapshots',
              'booking_seat_catalog',
              'booking_fare_snapshots',
              'seat_inventory',
              'reservations',
              'reservation_seats',
              'ticket_orders',
              'tickets',
              'idempotency_records',
              'booking_command_receipts',
              'outbox_events',
              'train_run_write_fences',
              'train_run_target_write_evidence',
              'migration_capture_state',
              'train_run_mutation_journal',
              'migration_apply_receipts'
          )) <> 16 THEN
        RAISE EXCEPTION 'booking shard version 1 tables are incomplete';
    END IF;

    IF (SELECT count(DISTINCT event_object_table)
        FROM information_schema.triggers
        WHERE trigger_schema = 'public'
          AND trigger_name LIKE '%_capture_mutation') <> 10 THEN
        RAISE EXCEPTION 'mutation capture trigger coverage is incomplete';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_schema = 'public'
          AND constraint_type = 'FOREIGN KEY'
          AND constraint_name ILIKE '%user%'
    ) THEN
        RAISE EXCEPTION 'booking shard must not assume a control user foreign key';
    END IF;
END;
$assert$;
