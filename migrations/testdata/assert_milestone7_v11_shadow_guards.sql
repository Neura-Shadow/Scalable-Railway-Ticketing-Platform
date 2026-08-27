BEGIN;

SELECT set_config('railway.deployment_region', 'region-a', true),
       set_config('railway.deployment_role', 'active', true),
       set_config('railway.region_epoch', '1', true),
       set_config('railway.regional_writes_enabled', 'true', true);

INSERT INTO public.physical_shard_migrations (
    migration_id, train_run_id, source_shard_id, target_shard_id,
    source_generation, target_generation, state
) VALUES (
    '79000000-0000-4000-8000-000000000001',
    '66666666-6666-4666-8666-666666666666',
    'legacy', 'physical-shard-0', 1, 2, 'capture_enabled'
);

UPDATE public.train_run_shard_assignments
   SET assignment_state = 'migrating',
       active_physical_migration_id =
           '79000000-0000-4000-8000-000000000001'
 WHERE train_run_id = '66666666-6666-4666-8666-666666666666';

INSERT INTO public.physical_source_migration_capture_state (
    train_run_id, migration_id, source_shard_id, source_generation,
    capture_enabled, next_sequence, enabled_at
) VALUES (
    '66666666-6666-4666-8666-666666666666',
    '79000000-0000-4000-8000-000000000001',
    'legacy', 1, true, 0, clock_timestamp()
);

INSERT INTO public.ticket_refund_compensation_receipts (
    id, command_id, refund_request_id, refund_operation_id,
    payment_intent_id, reservation_id, ticket_order_id, train_run_id,
    request_fingerprint, provider_proof_hash, amount_minor, currency,
    selected_ticket_count, released_seat_count,
    resulting_active_ticket_count, resulting_order_state, committed_at
) VALUES (
    '79000000-0000-4000-8000-000000000010',
    '79000000-0000-4000-8000-000000000011',
    '79000000-0000-4000-8000-000000000012',
    '79000000-0000-4000-8000-000000000013',
    '79000000-0000-4000-8000-000000000014',
    'd0000000-0000-4000-8000-000000000001',
    'd0000000-0000-4000-8000-000000000002',
    '66666666-6666-4666-8666-666666666666',
    decode(repeat('71', 32), 'hex'), decode(repeat('72', 32), 'hex'),
    15000, 'TWD', 1, 1, 0, 'refunded', clock_timestamp()
);

INSERT INTO public.selected_ticket_refund_receipts (
    id, compensation_receipt_id, refund_request_id, ticket_id,
    reservation_seat_id, train_run_id, fare_amount_minor, currency,
    segment_mask_hash, released_at
) VALUES (
    '79000000-0000-4000-8000-000000000015',
    '79000000-0000-4000-8000-000000000010',
    '79000000-0000-4000-8000-000000000012',
    'd0000000-0000-4000-8000-000000000003',
    'c0000000-0000-4000-8000-000000000002',
    '66666666-6666-4666-8666-666666666666',
    15000, 'TWD', decode(repeat('73', 32), 'hex'), clock_timestamp()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM public.physical_source_ticket_refund_compensation_receipt_rows
         WHERE source_shard_id = 'legacy'
           AND id = '79000000-0000-4000-8000-000000000010'
    ) OR NOT EXISTS (
        SELECT 1
          FROM public.physical_source_selected_ticket_refund_receipt_rows
         WHERE source_shard_id = 'legacy'
           AND id = '79000000-0000-4000-8000-000000000015'
    ) OR (
        SELECT count(*)
          FROM public.physical_source_train_run_mutation_journal
         WHERE migration_id = '79000000-0000-4000-8000-000000000001'
           AND table_name IN (
               'ticket_refund_compensation_receipts',
               'selected_ticket_refund_receipts'
           )
           AND operation = 'INSERT'
    ) <> 2 THEN
        RAISE EXCEPTION 'control v11 shadow view or capture contract failed';
    END IF;
END;
$$;

DELETE FROM public.selected_ticket_refund_receipts
 WHERE id = '79000000-0000-4000-8000-000000000015';
DELETE FROM public.ticket_refund_compensation_receipts
 WHERE id = '79000000-0000-4000-8000-000000000010';
DELETE FROM public.physical_source_migration_capture_state
 WHERE migration_id = '79000000-0000-4000-8000-000000000001';
DELETE FROM public.physical_source_train_run_mutation_journal
 WHERE migration_id = '79000000-0000-4000-8000-000000000001';

UPDATE public.train_run_shard_assignments
   SET assignment_state = 'stable', active_physical_migration_id = NULL
 WHERE train_run_id = '66666666-6666-4666-8666-666666666666';
DELETE FROM public.physical_shard_migrations
 WHERE migration_id = '79000000-0000-4000-8000-000000000001';

UPDATE public.train_run_write_fences
   SET write_enabled = false
 WHERE train_run_id = '66666666-6666-4666-8666-666666666666';

INSERT INTO public.physical_shard_migrations (
    migration_id, train_run_id, source_shard_id, target_shard_id,
    source_generation, target_generation, reverse_migration, state
) VALUES (
    '79000000-0000-4000-8000-000000000020',
    '66666666-6666-4666-8666-666666666666',
    'physical-shard-0', 'legacy', 2, 3, true, 'preparing_target'
);
UPDATE public.train_run_shard_assignments
   SET shard_id = 'physical-shard-0', assignment_generation = 2,
       assignment_state = 'migrating',
       active_physical_migration_id =
           '79000000-0000-4000-8000-000000000020'
 WHERE train_run_id = '66666666-6666-4666-8666-666666666666';

DO $$
BEGIN
    BEGIN
        INSERT INTO public.physical_control_target_apply_authorizations (
            migration_id, train_run_id, target_shard_id,
            target_generation, transaction_id
        ) VALUES (
            '79000000-0000-4000-8000-000000000020',
            '66666666-6666-4666-8666-666666666666',
            'legacy', 999, txid_current()
        );
        RAISE EXCEPTION 'wrong target generation authorization was stored';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
    BEGIN
        INSERT INTO public.ticket_refund_compensation_receipts (
            id, command_id, refund_request_id, refund_operation_id,
            payment_intent_id, reservation_id, ticket_order_id, train_run_id,
            request_fingerprint, provider_proof_hash, amount_minor, currency,
            selected_ticket_count, released_seat_count,
            resulting_active_ticket_count, resulting_order_state, committed_at
        ) VALUES (
            '79000000-0000-4000-8000-000000000030',
            '79000000-0000-4000-8000-000000000031',
            '79000000-0000-4000-8000-000000000032',
            '79000000-0000-4000-8000-000000000033',
            '79000000-0000-4000-8000-000000000034',
            'd0000000-0000-4000-8000-000000000001',
            'd0000000-0000-4000-8000-000000000002',
            '66666666-6666-4666-8666-666666666666',
            decode(repeat('74', 32), 'hex'),
            decode(repeat('75', 32), 'hex'),
            15000, 'TWD', 1, 1, 0, 'refunded', clock_timestamp()
        );
        RAISE EXCEPTION 'wrong target generation authorized shadow write';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;

    BEGIN
        INSERT INTO public.physical_control_target_apply_authorizations (
            migration_id, train_run_id, target_shard_id,
            target_generation, transaction_id
        ) VALUES (
            '79000000-0000-4000-8000-000000000020',
            '66666666-6666-4666-8666-666666666666',
            'legacy', 3, txid_current()
        );
        SET CONSTRAINTS physical_control_target_authorization_release IMMEDIATE;
        RAISE EXCEPTION 'unreleased physical target authorization was accepted';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END;
$$;
INSERT INTO public.physical_control_target_apply_authorizations (
    migration_id, train_run_id, target_shard_id,
    target_generation, transaction_id
) VALUES (
    '79000000-0000-4000-8000-000000000020',
    '66666666-6666-4666-8666-666666666666',
    'legacy', 3, txid_current()
);

INSERT INTO public.ticket_refund_compensation_receipts (
    id, command_id, refund_request_id, refund_operation_id,
    payment_intent_id, reservation_id, ticket_order_id, train_run_id,
    request_fingerprint, provider_proof_hash, amount_minor, currency,
    selected_ticket_count, released_seat_count,
    resulting_active_ticket_count, resulting_order_state, committed_at
) VALUES (
    '79000000-0000-4000-8000-000000000030',
    '79000000-0000-4000-8000-000000000031',
    '79000000-0000-4000-8000-000000000032',
    '79000000-0000-4000-8000-000000000033',
    '79000000-0000-4000-8000-000000000034',
    'd0000000-0000-4000-8000-000000000001',
    'd0000000-0000-4000-8000-000000000002',
    '66666666-6666-4666-8666-666666666666',
    decode(repeat('74', 32), 'hex'), decode(repeat('75', 32), 'hex'),
    15000, 'TWD', 1, 1, 0, 'refunded', clock_timestamp()
);
DELETE FROM public.ticket_refund_compensation_receipts
 WHERE id = '79000000-0000-4000-8000-000000000030';
DELETE FROM public.physical_control_target_apply_authorizations
 WHERE migration_id = '79000000-0000-4000-8000-000000000020';

ROLLBACK;
