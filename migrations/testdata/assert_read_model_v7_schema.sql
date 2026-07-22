DO $assert$
DECLARE
    schema_version integer;
    schema_dirty boolean;
BEGIN
    SELECT version, dirty INTO schema_version, schema_dirty
    FROM schema_migrations
    LIMIT 1;
    IF schema_version <> 7 OR schema_dirty THEN
        RAISE EXCEPTION 'expected clean schema version 7, got version % dirty %', schema_version, schema_dirty;
    END IF;
    IF to_regclass('public.train_run_journey_read_model') IS NULL THEN
        RAISE EXCEPTION 'train_run_journey_read_model is missing';
    END IF;
    IF to_regclass('public.read_model_event_receipts') IS NULL THEN
        RAISE EXCEPTION 'read_model_event_receipts is missing';
    END IF;
    IF to_regclass('public.train_run_journey_read_model_search_idx') IS NULL OR
       to_regclass('public.train_run_journey_read_model_fare_search_idx') IS NULL THEN
        RAISE EXCEPTION 'read-model search indexes are missing';
    END IF;
END
$assert$;
