DO $assert$
DECLARE
    current_version bigint;
    current_dirty boolean;
BEGIN
    SELECT version, dirty
    INTO current_version, current_dirty
    FROM public.schema_migrations
    LIMIT 1;

    IF current_version <> 9 OR current_dirty THEN
        RAISE EXCEPTION 'control migration must be clean at version 9';
    END IF;

    IF (SELECT count(*) FROM public.booking_shards) <> 5
       OR (SELECT count(*) FROM public.booking_shards
           WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
             AND storage_kind = 'postgres'
             AND connection_ref = shard_id
             AND protocol_version = 1
             AND schema_version = 1
             AND NOT enabled
             AND NOT write_enabled) <> 2 THEN
        RAISE EXCEPTION 'fixed physical shard catalog is invalid';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN (
              'booking_commands',
              'booking_quota_leases',
              'reservation_directory',
              'physical_shard_migrations',
              'physical_shard_migration_checkpoints',
              'physical_shard_target_write_observations',
              'physical_shard_reconciliation_runs'
          )) <> 7 THEN
        RAISE EXCEPTION 'control-plane version 9 tables are incomplete';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.booking_shards
        WHERE connection_ref LIKE '%://%'
           OR connection_ref LIKE '%@%'
    ) THEN
        RAISE EXCEPTION 'catalog contains endpoint-like connection reference';
    END IF;
END;
$assert$;
