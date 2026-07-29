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

-- Version 9 must preserve the version-8 logical migration zero-writer state.
-- Exercise the invariant directly while the deferred constraint triggers keep
-- the surrounding fixture unchanged at transaction end.
DO $assert_logical_migration$
DECLARE
    selected_train_run_id uuid;
    selected_generation bigint;
    selected_migration_id uuid := '41900000-0000-4000-8000-000000000901';
BEGIN
    SELECT assignment.train_run_id, assignment.assignment_generation
    INTO STRICT selected_train_run_id, selected_generation
    FROM public.train_run_shard_assignments AS assignment
    WHERE assignment.shard_id = 'legacy'
      AND assignment.assignment_state = 'stable'
      AND assignment.active_migration_id IS NULL
      AND assignment.active_physical_migration_id IS NULL
    ORDER BY assignment.train_run_id
    LIMIT 1;

    INSERT INTO public.train_run_shard_migrations (
        id, train_run_id, source_shard_id, target_shard_id,
        source_generation, target_generation, state
    ) VALUES (
        selected_migration_id, selected_train_run_id, 'legacy', 'shard-0',
        selected_generation, selected_generation + 1, 'copying'
    );

    UPDATE public.train_run_write_fences
    SET write_enabled = false
    WHERE train_run_id = selected_train_run_id
      AND assignment_generation = selected_generation;

    UPDATE public.train_run_shard_assignments
    SET assignment_state = 'migrating',
        active_migration_id = selected_migration_id
    WHERE train_run_id = selected_train_run_id;

    PERFORM public.assert_train_run_fence_invariant(selected_train_run_id);

    UPDATE public.train_run_shard_assignments
    SET assignment_state = 'stable',
        active_migration_id = NULL
    WHERE train_run_id = selected_train_run_id;

    UPDATE public.train_run_write_fences
    SET write_enabled = true
    WHERE train_run_id = selected_train_run_id
      AND assignment_generation = selected_generation;

    DELETE FROM public.train_run_shard_migrations
    WHERE id = selected_migration_id;
END;
$assert_logical_migration$;
