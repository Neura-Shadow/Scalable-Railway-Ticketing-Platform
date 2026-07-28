DO $assert$
DECLARE
    schema_version integer;
    schema_dirty boolean;
BEGIN
    SELECT version, dirty INTO schema_version, schema_dirty
    FROM public.schema_migrations
    LIMIT 1;
    IF schema_version <> 8 OR schema_dirty THEN
        RAISE EXCEPTION 'failed raw down did not preserve clean version 8, got version % dirty %',
            schema_version, schema_dirty;
    END IF;

    IF to_regnamespace('booking_shard_0') IS NULL
       OR to_regnamespace('booking_shard_1') IS NULL
       OR to_regclass('booking_shard_0.seat_inventory') IS NULL
       OR to_regclass('booking_shard_1.seat_inventory') IS NULL
       OR to_regclass('public.booking_shards') IS NULL
       OR to_regclass('public.train_run_shard_assignments') IS NULL THEN
        RAISE EXCEPTION 'failed raw down did not roll back the Milestone 4 topology';
    END IF;
END
$assert$;
