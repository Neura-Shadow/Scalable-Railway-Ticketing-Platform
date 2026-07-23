DO $assert$
DECLARE
    schema_version integer;
    schema_dirty boolean;
BEGIN
    SELECT version, dirty INTO schema_version, schema_dirty
    FROM schema_migrations
    LIMIT 1;
    IF schema_version <> 6 OR schema_dirty THEN
        RAISE EXCEPTION 'expected clean schema version 6 after down, got version % dirty %', schema_version, schema_dirty;
    END IF;
    IF to_regclass('public.train_run_journey_read_model') IS NOT NULL OR
       to_regclass('public.read_model_event_receipts') IS NOT NULL THEN
        RAISE EXCEPTION 'version-7 read-model tables survived down migration';
    END IF;
    IF (SELECT count(*) FROM outbox_events WHERE id = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc') <> 0 THEN
        RAISE EXCEPTION 'version-7-only outbox event survived down migration';
    END IF;
END
$assert$;
