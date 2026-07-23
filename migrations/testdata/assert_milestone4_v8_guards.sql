BEGIN;

INSERT INTO public.train_runs (
    id, train_id, route_id, service_date, scheduled_departure_at,
    status, segment_count
) VALUES (
    'a4000000-0000-4000-8000-000000000001',
    '55555555-5555-4555-8555-555555555555',
    '44444444-4444-4444-8444-444444444444',
    '2099-01-02', '2099-01-02 08:00:00+00', 'scheduled', 1
);

DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.train_run_shard_assignments AS assignment
        JOIN public.train_run_write_fences AS fence
          ON fence.train_run_id = assignment.train_run_id
        WHERE assignment.train_run_id = 'a4000000-0000-4000-8000-000000000001'
          AND assignment.shard_id = 'legacy'
          AND assignment.assignment_generation = 1
          AND fence.assignment_generation = 1
          AND fence.write_enabled
    ) THEN
        RAISE EXCEPTION 'new train run did not receive atomic legacy ownership';
    END IF;

    BEGIN
        UPDATE public.train_run_shard_assignments
        SET assignment_generation = 0
        WHERE train_run_id = 'a4000000-0000-4000-8000-000000000001';
        RAISE EXCEPTION 'assignment generation decrease unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO booking_shard_1.train_run_write_fences (
            train_run_id, assignment_generation, write_enabled
        ) VALUES (
            'a4000000-0000-4000-8000-000000000001', 1, true
        );
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION 'dual booking writers unexpectedly passed deferred validation';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END
$assert$;

INSERT INTO public.train_run_shard_migrations (
    id, train_run_id, source_shard_id, target_shard_id,
    source_generation, target_generation, state
) VALUES (
    'a4000000-0000-4000-8000-000000000002',
    'a4000000-0000-4000-8000-000000000001',
    'legacy', 'shard-0', 1, 2, 'planned'
);

DO $assert$
BEGIN
    BEGIN
        UPDATE public.train_run_shard_migrations
        SET state = 'copying'
        WHERE id = 'a4000000-0000-4000-8000-000000000002';
        RAISE EXCEPTION 'invalid migration state transition unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END
$assert$;

UPDATE public.train_run_write_fences
SET write_enabled = false
WHERE train_run_id = '66666666-6666-4666-8666-666666666666';

INSERT INTO booking_shard_0.train_run_write_fences (
    train_run_id, assignment_generation, write_enabled
) VALUES (
    '66666666-6666-4666-8666-666666666666', 2, true
);

UPDATE public.train_run_shard_assignments
SET shard_id = 'shard-0',
    assignment_generation = 2,
    assignment_state = 'stable',
    availability_generation = 2
WHERE train_run_id = '66666666-6666-4666-8666-666666666666';

DO $assert$
BEGIN
    BEGIN
        UPDATE public.reservations
        SET updated_at = clock_timestamp()
        WHERE id = '77777777-7777-4777-8777-777777777777';
        RAISE EXCEPTION 'retained legacy booking write unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END
$assert$;

ROLLBACK;
