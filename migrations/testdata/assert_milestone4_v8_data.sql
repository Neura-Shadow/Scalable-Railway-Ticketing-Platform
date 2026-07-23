DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.train_run_shard_assignments
        WHERE train_run_id = '66666666-6666-4666-8666-666666666666'
          AND shard_id = 'legacy'
          AND assignment_generation = 1
          AND assignment_state = 'stable'
          AND availability_generation = 1
    ) THEN
        RAISE EXCEPTION 'fixture train-run legacy assignment was not bootstrapped';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.train_run_write_fences
        WHERE train_run_id = '66666666-6666-4666-8666-666666666666'
          AND assignment_generation = 1
          AND write_enabled
    ) THEN
        RAISE EXCEPTION 'fixture legacy write fence was not enabled';
    END IF;

    IF (SELECT count(*) FROM public.reservation_shard_locators
        WHERE train_run_id = '66666666-6666-4666-8666-666666666666') <> 2
       OR (SELECT count(*) FROM public.ticket_order_shard_locators
        WHERE train_run_id = '66666666-6666-4666-8666-666666666666') <> 1
       OR (SELECT count(*) FROM public.ticket_shard_locators
        WHERE train_run_id = '66666666-6666-4666-8666-666666666666') <> 1 THEN
        RAISE EXCEPTION 'legacy locator bootstrap is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.reservation_quota_claims
        WHERE reservation_id = '77777777-7777-4777-8777-777777777777'
          AND passenger_count = 1 AND active AND closed_at IS NULL
    ) OR NOT EXISTS (
        SELECT 1
        FROM public.reservation_quota_claims
        WHERE reservation_id = 'd0000000-0000-4000-8000-000000000001'
          AND passenger_count = 1 AND NOT active AND closed_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'legacy quota claims do not match reservation lifecycle';
    END IF;

    IF (SELECT count(*) FROM public.idempotency_records
        WHERE id IN (
            'e0000000-0000-4000-8000-000000000001',
            'e0000000-0000-4000-8000-000000000002'
        )
          AND train_run_id = '66666666-6666-4666-8666-666666666666') <> 2
       OR (SELECT count(*) FROM public.booking_idempotency_key_claims
        WHERE local_record_id IN (
            'e0000000-0000-4000-8000-000000000001',
            'e0000000-0000-4000-8000-000000000002'
        )
          AND train_run_id = '66666666-6666-4666-8666-666666666666'
          AND shard_id = 'legacy'
          AND assignment_generation = 1) <> 2 THEN
        RAISE EXCEPTION 'legacy idempotency completion or key-claim bootstrap is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.idempotency_records AS record
        JOIN public.booking_idempotency_key_claims AS claim
          ON claim.local_record_id = record.id
        WHERE record.id = 'e0000000-0000-4000-8000-000000000003'
          AND record.status = 'in_progress'
          AND record.train_run_id IS NULL
          AND claim.train_run_id IS NULL
          AND claim.shard_id = 'legacy'
          AND claim.assignment_generation = 1
    ) THEN
        RAISE EXCEPTION 'unresolved version-7 idempotency conflict was not preserved';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.outbox_events
        WHERE id = '88888888-8888-4888-8888-888888888888'
          AND train_run_id = '66666666-6666-4666-8666-666666666666'
          AND shard_id = 'legacy'
          AND assignment_generation = 1
    ) OR NOT EXISTS (
        SELECT 1
        FROM public.outbox_events
        WHERE id = 'f0000000-0000-4000-8000-000000000001'
          AND train_run_id = '66666666-6666-4666-8666-666666666666'
          AND shard_id = 'legacy'
          AND assignment_generation = 1
    ) THEN
        RAISE EXCEPTION 'central outbox provenance was not backfilled';
    END IF;

    IF EXISTS (SELECT 1 FROM booking_shard_0.reservations)
       OR EXISTS (SELECT 1 FROM booking_shard_1.reservations) THEN
        RAISE EXCEPTION 'migration 8 moved legacy booking rows without an operator migration';
    END IF;

    PERFORM public.assert_train_run_fence_invariant(
        '66666666-6666-4666-8666-666666666666'
    );
END
$assert$;
