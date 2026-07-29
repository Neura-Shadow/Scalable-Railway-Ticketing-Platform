DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.reservation_directory AS directory
        JOIN public.reservation_shard_locators AS locator
          ON locator.reservation_id = directory.reservation_id
        WHERE directory.train_run_id = locator.train_run_id
          AND directory.owner_user_id = locator.owner_user_id
          AND directory.last_known_shard_id = locator.shard_id
          AND directory.last_known_generation = locator.assignment_generation
          AND directory.state = 'active'
    ) THEN
        RAISE EXCEPTION 'version 8 reservation locator was not backfilled';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.reservation_shard_locators AS locator
        LEFT JOIN public.reservation_directory AS directory
          ON directory.reservation_id = locator.reservation_id
        WHERE directory.reservation_id IS NULL
    ) THEN
        RAISE EXCEPTION 'version 8 reservation locator backfill is incomplete';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.booking_commands
    ) THEN
        RAISE EXCEPTION 'upgrade must not fabricate booking commands';
    END IF;
END;
$assert$;
