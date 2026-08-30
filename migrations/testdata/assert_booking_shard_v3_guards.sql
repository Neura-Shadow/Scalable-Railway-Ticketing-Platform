BEGIN;

DO $$
BEGIN
    BEGIN
        UPDATE public.reservations
           SET updated_at = clock_timestamp()
         WHERE id = '61000000-0000-0000-0000-000000000010';
        RAISE EXCEPTION 'write without regional context unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;

END;
$$;

SELECT set_config('railway.deployment_region', 'region-a', true),
       set_config('railway.deployment_role', 'active', true),
       set_config('railway.region_epoch', '1', true),
       set_config('railway.regional_writes_enabled', 'true', true);

DO $$
BEGIN
    BEGIN
        TRUNCATE public.dr_reconciliation_checkpoints;
        RAISE EXCEPTION 'booking-shard table truncate unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$$;

DO $$
BEGIN
    BEGIN
        INSERT INTO public.dr_reconciliation_checkpoints (
            checkpoint_id, scope, region, epoch, rows_examined,
            mismatch_count, truncated, state, evidence_hash,
            started_at, completed_at
        ) VALUES (
            '74000000-0000-4000-8000-000000000020',
            'steady_state', 'region-b', 1, 1,
            0, false, 'passed', decode(repeat('34', 32), 'hex'),
            clock_timestamp(), clock_timestamp()
        );
        RAISE EXCEPTION 'mismatched DR checkpoint was accepted';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END;
$$;

INSERT INTO public.dr_reconciliation_checkpoints (
    checkpoint_id, scope, region, epoch, rows_examined,
    mismatch_count, truncated, state, evidence_hash,
    started_at, completed_at
) VALUES (
    '74000000-0000-4000-8000-000000000021',
    'steady_state', 'region-a', 1, 1,
    0, false, 'passed', decode(repeat('35', 32), 'hex'),
    clock_timestamp(), clock_timestamp()
);

DO $$
BEGIN
    BEGIN
        UPDATE public.dr_reconciliation_checkpoints
           SET rows_examined = 2
         WHERE checkpoint_id =
               '74000000-0000-4000-8000-000000000021';
        RAISE EXCEPTION 'append-only DR checkpoint was updated';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$$;

DO $$
BEGIN
    BEGIN
        PERFORM set_config('railway.deployment_role', 'passive', true);
        UPDATE public.reservations
           SET updated_at = clock_timestamp()
         WHERE id = '61000000-0000-0000-0000-000000000010';
        RAISE EXCEPTION 'passive-region write unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
    BEGIN
        PERFORM set_config('railway.region_epoch', '2', true);
        UPDATE public.reservations
           SET updated_at = clock_timestamp()
         WHERE id = '61000000-0000-0000-0000-000000000010';
        RAISE EXCEPTION 'stale-authority epoch write unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END;
$$;

INSERT INTO public.migration_capture_state (
    train_run_id, migration_id, source_generation,
    capture_enabled, next_sequence, enabled_at
) VALUES (
    '61000000-0000-0000-0000-000000000002',
    '75000000-0000-4000-8000-000000000001',
    1, true, 0, clock_timestamp()
);

INSERT INTO public.ticket_refund_compensation_receipts (
    id, command_id, refund_request_id, refund_operation_id,
    payment_intent_id, reservation_id, ticket_order_id,
    train_run_id, assignment_generation,
    request_fingerprint, provider_proof_hash,
    amount_minor, currency, selected_ticket_count, released_seat_count,
    resulting_active_ticket_count, resulting_order_state, committed_at
) VALUES (
    '74000000-0000-4000-8000-000000000001',
    '74000000-0000-4000-8000-000000000002',
    '74000000-0000-4000-8000-000000000003',
    '74000000-0000-4000-8000-000000000004',
    '74000000-0000-4000-8000-000000000005',
    '61000000-0000-0000-0000-000000000010',
    '61000000-0000-0000-0000-000000000014',
    '61000000-0000-0000-0000-000000000002', 1,
    decode(repeat('31', 32), 'hex'),
    decode(repeat('32', 32), 'hex'),
    12500, 'TWD', 1, 1, 0, 'refunded', clock_timestamp()
);

INSERT INTO public.selected_ticket_refund_receipts (
    id, compensation_receipt_id, refund_request_id, ticket_id,
    reservation_seat_id, train_run_id, assignment_generation,
    fare_amount_minor, currency, segment_mask_hash, released_at
) VALUES (
    '74000000-0000-4000-8000-000000000006',
    '74000000-0000-4000-8000-000000000001',
    '74000000-0000-4000-8000-000000000003',
    '61000000-0000-0000-0000-000000000015',
    '61000000-0000-0000-0000-000000000012',
    '61000000-0000-0000-0000-000000000002', 1,
    12500, 'TWD', decode(repeat('33', 32), 'hex'), clock_timestamp()
);

DO $$
BEGIN
    IF (
        SELECT count(*)
          FROM public.train_run_mutation_journal
         WHERE migration_id = '75000000-0000-4000-8000-000000000001'
           AND train_run_id = '61000000-0000-0000-0000-000000000002'
           AND source_generation = 1
           AND table_name IN (
               'ticket_refund_compensation_receipts',
               'selected_ticket_refund_receipts'
           )
           AND operation = 'INSERT'
    ) <> 2 OR (
        SELECT next_sequence
          FROM public.migration_capture_state
         WHERE train_run_id = '61000000-0000-0000-0000-000000000002'
    ) <> 2 THEN
        RAISE EXCEPTION 'v3 receipt mutations were not captured exactly once';
    END IF;

    BEGIN
        UPDATE public.ticket_refund_compensation_receipts
           SET amount_minor = amount_minor + 1
         WHERE id = '74000000-0000-4000-8000-000000000001';
        RAISE EXCEPTION 'immutable receipt update unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        DELETE FROM public.selected_ticket_refund_receipts
         WHERE id = '74000000-0000-4000-8000-000000000006';
        RAISE EXCEPTION 'unauthorized receipt delete unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$$;

UPDATE public.migration_capture_state
   SET capture_enabled = false, disabled_at = clock_timestamp()
 WHERE train_run_id = '61000000-0000-0000-0000-000000000002';

DO $$
BEGIN
    BEGIN
        INSERT INTO public.migration_evidence_mutation_authorizations (
            transaction_id, migration_id, train_run_id,
            assignment_generation, table_name
        ) VALUES (
            txid_current() + 1,
            '75000000-0000-4000-8000-000000000001',
            '61000000-0000-0000-0000-000000000002', 1,
            'selected_ticket_refund_receipts'
        );
        RAISE EXCEPTION 'wrong-transaction authorization was stored';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;

    BEGIN
        INSERT INTO public.migration_evidence_mutation_authorizations (
            transaction_id, migration_id, train_run_id,
            assignment_generation, table_name
        ) VALUES (
            txid_current(),
            '75000000-0000-4000-8000-000000000001',
            '61000000-0000-0000-0000-000000000002', 1,
            'selected_ticket_refund_receipts'
        );
        SET CONSTRAINTS migration_evidence_authorization_release IMMEDIATE;
        RAISE EXCEPTION 'unreleased migration evidence authorization was accepted';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END;
$$;

INSERT INTO public.migration_evidence_mutation_authorizations (
    transaction_id, migration_id, train_run_id,
    assignment_generation, table_name
)
SELECT txid_current(),
       '75000000-0000-4000-8000-000000000001'::uuid,
       '61000000-0000-0000-0000-000000000002'::uuid,
       1, required.table_name
FROM (VALUES
    ('ticket_refund_compensation_receipts'),
    ('selected_ticket_refund_receipts')
) AS required(table_name);

DELETE FROM public.selected_ticket_refund_receipts
 WHERE id = '74000000-0000-4000-8000-000000000006';
DELETE FROM public.ticket_refund_compensation_receipts
 WHERE id = '74000000-0000-4000-8000-000000000001';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.selected_ticket_refund_receipts
         WHERE id = '74000000-0000-4000-8000-000000000006'
    ) OR EXISTS (
        SELECT 1 FROM public.ticket_refund_compensation_receipts
         WHERE id = '74000000-0000-4000-8000-000000000001'
    ) THEN
        RAISE EXCEPTION 'exact transaction authorization did not permit cleanup';
    END IF;
END;
$$;

DELETE FROM public.migration_evidence_mutation_authorizations
 WHERE transaction_id = txid_current();

ROLLBACK;
