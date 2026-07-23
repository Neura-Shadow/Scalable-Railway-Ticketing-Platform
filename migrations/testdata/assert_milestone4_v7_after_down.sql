DO $assert$
DECLARE
    schema_version integer;
    schema_dirty boolean;
BEGIN
    SELECT version, dirty INTO schema_version, schema_dirty
    FROM public.schema_migrations
    LIMIT 1;
    IF schema_version <> 7 OR schema_dirty THEN
        RAISE EXCEPTION 'expected clean schema version 7 after down, got version % dirty %',
            schema_version, schema_dirty;
    END IF;

    IF to_regnamespace('booking_shard_0') IS NOT NULL
       OR to_regnamespace('booking_shard_1') IS NOT NULL
       OR to_regclass('public.booking_shards') IS NOT NULL
       OR to_regclass('public.train_run_shard_assignments') IS NOT NULL
       OR to_regclass('public.reservation_shard_locators') IS NOT NULL THEN
        RAISE EXCEPTION 'milestone-4 topology survived one-step down';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'idempotency_records'
          AND column_name = 'train_run_id'
    ) OR EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'outbox_events'
          AND column_name IN ('train_run_id', 'shard_id', 'assignment_generation')
    ) THEN
        RAISE EXCEPTION 'version-8 expansion columns survived one-step down';
    END IF;

    IF (SELECT count(*) FROM public.reservations
        WHERE train_run_id = '66666666-6666-4666-8666-666666666666') <> 2
       OR NOT EXISTS (
           SELECT 1 FROM public.tickets
           WHERE id = 'd0000000-0000-4000-8000-000000000003'
       )
       OR (SELECT count(*) FROM public.idempotency_records
           WHERE id IN (
               'e0000000-0000-4000-8000-000000000001',
               'e0000000-0000-4000-8000-000000000002',
               'e0000000-0000-4000-8000-000000000003'
           )) <> 3
       OR NOT EXISTS (
           SELECT 1 FROM public.outbox_events
           WHERE id = 'f0000000-0000-4000-8000-000000000001'
       ) THEN
        RAISE EXCEPTION 'version-7 booking data was lost during one-step down';
    END IF;
END
$assert$;
