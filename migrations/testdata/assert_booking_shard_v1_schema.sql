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
              'migration_outbox_staging',
              'train_run_write_fences',
              'train_run_target_write_evidence',
              'migration_capture_state',
              'train_run_mutation_journal',
              'migration_apply_receipts'
          )) <> 17 THEN
        RAISE EXCEPTION 'booking shard version 1 tables are incomplete';
    END IF;

    IF (SELECT count(DISTINCT event_object_table)
        FROM information_schema.triggers
        WHERE trigger_schema = 'public'
          AND trigger_name LIKE '%_capture_mutation') <> 10 THEN
        RAISE EXCEPTION 'mutation capture trigger coverage is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'booking_fare_snapshots'
          AND indexname = 'booking_fare_snapshots_active_version_unique_idx'
          AND indexdef LIKE 'CREATE UNIQUE INDEX%'
          AND indexdef LIKE '%WHERE active%'
    ) THEN
        RAISE EXCEPTION 'active fare version uniqueness is missing';
    END IF;

    IF (SELECT count(*)
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname LIKE '%_migration_cursor_idx'
          AND indexdef LIKE '%(train_run_id, assignment_generation, id)%') <> 11 THEN
        RAISE EXCEPTION 'base-copy keyset cursor indexes are incomplete';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'train_run_target_write_evidence'
          AND column_name IN (
              'baseline_initialized',
              'baseline_reservation_count',
              'baseline_command_receipt_count',
              'baseline_outbox_count'
          )) <> 4 THEN
        RAISE EXCEPTION 'target-write rollback baselines are incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'migration_outbox_staging'
          AND column_name = 'created_at'
          AND data_type = 'timestamp with time zone'
          AND is_nullable = 'NO'
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'migration_outbox_staging'
          AND indexname = 'migration_outbox_staging_created_at_idx'
          AND indexdef LIKE '%(created_at, migration_id)%'
    ) THEN
        RAISE EXCEPTION 'bounded migration outbox staging observability is incomplete';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.referential_constraints
        WHERE constraint_schema = 'public'
          AND update_rule <> 'CASCADE'
    ) THEN
        RAISE EXCEPTION 'booking authority generation foreign keys must cascade monotonically';
    END IF;

    IF NOT EXISTS (
		SELECT 1
		FROM pg_constraint AS constraint_row
		JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
		JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
		WHERE schema_row.nspname = 'public'
		  AND table_row.relname = 'booking_command_receipts'
		  AND constraint_row.contype = 'c'
		  AND pg_get_constraintdef(constraint_row.oid) LIKE '%seat.enable%'
		  AND pg_get_constraintdef(constraint_row.oid) LIKE '%booking_policy.bump%'
	) THEN
		RAISE EXCEPTION 'operator booking command receipt types are incomplete';
	END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'booking_command_receipts'
          AND column_name = 'result_source_version'
          AND data_type = 'bigint'
          AND is_nullable = 'YES'
    ) THEN
        RAISE EXCEPTION 'operator command receipt result version is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'booking_command_receipts'
          AND column_name = 'result_booking_policy_version'
          AND data_type = 'bigint'
          AND is_nullable = 'YES'
    ) THEN
        RAISE EXCEPTION 'operator policy receipt result version is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
        WHERE schema_row.nspname = 'public'
          AND table_row.relname = 'booking_command_receipts'
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%booking_policy.bump%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%result_source_version IS NOT NULL%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%result_booking_policy_version IS NOT NULL%'
    ) THEN
        RAISE EXCEPTION 'operator policy receipt does not require both result versions';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc AS function_row
        JOIN pg_namespace AS schema_row
          ON schema_row.oid = function_row.pronamespace
        WHERE schema_row.nspname = 'public'
          AND function_row.proname = 'booking_shard_capture_mutation'
          AND pg_get_functiondef(function_row.oid)
              LIKE '%result_source_version%'
          AND pg_get_functiondef(function_row.oid)
              LIKE '%result_booking_policy_version%'
    ) THEN
        RAISE EXCEPTION 'operator receipt mutation capture omits result versions';
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
