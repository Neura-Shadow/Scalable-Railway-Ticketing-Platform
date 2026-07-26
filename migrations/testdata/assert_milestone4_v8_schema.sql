DO $assert$
DECLARE
    schema_version integer;
    schema_dirty boolean;
BEGIN
    SELECT version, dirty INTO schema_version, schema_dirty
    FROM public.schema_migrations
    LIMIT 1;
    IF schema_version <> 8 OR schema_dirty THEN
        RAISE EXCEPTION 'expected clean schema version 8, got version % dirty %',
            schema_version, schema_dirty;
    END IF;

    IF (SELECT count(*) FROM public.booking_shards) <> 3
       OR NOT EXISTS (SELECT 1 FROM public.booking_shards WHERE shard_id = 'legacy')
       OR NOT EXISTS (SELECT 1 FROM public.booking_shards WHERE shard_id = 'shard-0')
       OR NOT EXISTS (SELECT 1 FROM public.booking_shards WHERE shard_id = 'shard-1') THEN
        RAISE EXCEPTION 'fixed booking shard catalog is incomplete';
    END IF;

    IF to_regnamespace('booking_shard_0') IS NULL
       OR to_regnamespace('booking_shard_1') IS NULL THEN
        RAISE EXCEPTION 'fixed booking shard schemas are missing';
    END IF;

    IF to_regclass('public.train_run_journey_read_model') IS NULL
       OR to_regclass('public.read_model_event_receipts') IS NULL
       OR to_regclass('public.train_run_journey_read_model_search_idx') IS NULL
       OR to_regclass('public.train_run_journey_read_model_fare_search_idx') IS NULL THEN
        RAISE EXCEPTION 'inherited read-model schema is incomplete';
    END IF;

    IF to_regclass('public.train_run_shard_assignments') IS NULL
       OR to_regclass('public.train_run_shard_migrations') IS NULL
       OR to_regclass('public.train_run_generation_writes') IS NULL
       OR to_regclass('public.train_run_write_fences') IS NULL
       OR to_regclass('public.booking_idempotency_key_claims') IS NULL
       OR to_regclass('public.reservation_quota_claims') IS NULL
       OR to_regclass('public.reservation_shard_locators') IS NULL
       OR to_regclass('public.ticket_order_shard_locators') IS NULL
       OR to_regclass('public.ticket_shard_locators') IS NULL THEN
        RAISE EXCEPTION 'milestone-4 public control or locator table is missing';
    END IF;

    IF to_regclass('booking_shard_0.seat_inventory') IS NULL
       OR to_regclass('booking_shard_0.reservations') IS NULL
       OR to_regclass('booking_shard_0.reservation_seats') IS NULL
       OR to_regclass('booking_shard_0.ticket_orders') IS NULL
       OR to_regclass('booking_shard_0.tickets') IS NULL
       OR to_regclass('booking_shard_0.idempotency_records') IS NULL
       OR to_regclass('booking_shard_0.train_run_write_fences') IS NULL
       OR to_regclass('booking_shard_1.seat_inventory') IS NULL
       OR to_regclass('booking_shard_1.reservations') IS NULL
       OR to_regclass('booking_shard_1.reservation_seats') IS NULL
       OR to_regclass('booking_shard_1.ticket_orders') IS NULL
       OR to_regclass('booking_shard_1.tickets') IS NULL
       OR to_regclass('booking_shard_1.idempotency_records') IS NULL
       OR to_regclass('booking_shard_1.train_run_write_fences') IS NULL THEN
        RAISE EXCEPTION 'schema-local booking table set is incomplete';
    END IF;

    IF (SELECT count(*)
        FROM pg_constraint AS constraint_record
        JOIN pg_class AS relation_record
          ON relation_record.oid = constraint_record.conrelid
        JOIN pg_namespace AS namespace_record
          ON namespace_record.oid = relation_record.relnamespace
        WHERE namespace_record.nspname = 'booking_shard_0'
          AND constraint_record.contype = 'f') <> 15
       OR (SELECT count(*)
           FROM pg_constraint AS constraint_record
           JOIN pg_class AS relation_record
             ON relation_record.oid = constraint_record.conrelid
           JOIN pg_namespace AS namespace_record
             ON namespace_record.oid = relation_record.relnamespace
           WHERE namespace_record.nspname = 'booking_shard_1'
             AND constraint_record.contype = 'f') <> 15 THEN
        RAISE EXCEPTION 'schema-local booking foreign-key set is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'idempotency_records'
          AND column_name = 'train_run_id'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'outbox_events'
          AND column_name = 'assignment_generation'
    ) THEN
        RAISE EXCEPTION 'legacy idempotency or central outbox expansion is missing';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'train_run_shard_migrations'
          AND column_name IN (
              'copy_checkpoint', 'copied_rows', 'copy_complete',
              'rollback_window_seconds', 'rollback_generation',
              'last_validation'
          )) <> 6 THEN
        RAISE EXCEPTION 'durable migration replay fields are incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.booking_idempotency_key_claims'::regclass
          AND conname = 'booking_idempotency_key_claims_train_run_compatibility_check'
    ) THEN
        RAISE EXCEPTION 'version-7 unresolved-key compatibility constraint is missing';
    END IF;

    IF to_regclass('public.reservation_shard_locators_train_run_idx') IS NULL
       OR to_regclass('public.ticket_order_shard_locators_train_run_idx') IS NULL
       OR to_regclass('public.ticket_order_shard_locators_owner_created_idx') IS NULL
       OR to_regclass('public.ticket_shard_locators_train_run_idx') IS NULL THEN
        RAISE EXCEPTION 'bounded locator indexes are missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgrelid = 'public.train_runs'::regclass
          AND tgname = 'train_runs_bootstrap_shard_assignment'
          AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgrelid = 'public.reservations'::regclass
          AND tgname = 'legacy_booking_write_guard'
          AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'assignment bootstrap or retained-legacy guard is missing';
    END IF;
END
$assert$;
