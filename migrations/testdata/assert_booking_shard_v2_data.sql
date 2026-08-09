BEGIN;

DO $assert_v1_preserved$
BEGIN
    IF (SELECT count(*)
        FROM public.reservations
        WHERE id = '61000000-0000-0000-0000-000000000010'
          AND status = 'confirmed'
          AND total_amount_minor = 12500
          AND currency = 'TWD'
          AND payment_intent_id IS NULL
          AND payment_amount_minor IS NULL
          AND payment_currency IS NULL
          AND payment_grace_expires_at IS NULL) <> 1 THEN
        RAISE EXCEPTION 'version-1 reservation was not preserved';
    END IF;

    IF (SELECT count(*)
        FROM public.ticket_orders
        WHERE id = '61000000-0000-0000-0000-000000000014'
          AND status = 'confirmed'
          AND payment_intent_id IS NULL
          AND authorized_amount_minor = 0
          AND captured_amount_minor = 0
          AND refunded_amount_minor = 0) <> 1 THEN
        RAISE EXCEPTION 'version-1 ticket order was not preserved';
    END IF;

    IF (SELECT count(*)
        FROM public.tickets
        WHERE id = '61000000-0000-0000-0000-000000000015'
          AND status = 'active'
          AND ticket_code = 'TKT.61000000/legacy?00000015') <> 1 THEN
        RAISE EXCEPTION 'version-1 ticket was not preserved';
    END IF;
END;
$assert_v1_preserved$;

DO $assert_required_payment_snapshot$
BEGIN
    BEGIN
        INSERT INTO public.reservations (
            id, user_id, train_run_id, assignment_generation, segment_count,
            from_stop_index, to_stop_index, seat_class, status, expires_at,
            total_amount_minor, currency, payment_intent_id
        ) VALUES (
            '62000000-0000-0000-0000-000000000030',
            '62000000-0000-0000-0000-000000000031',
            '61000000-0000-0000-0000-000000000002', 1, 2,
            0, 2, 'standard', 'payment_pending',
            clock_timestamp() + interval '1 hour', 12500, 'TWD',
            '62000000-0000-0000-0000-000000000032'
        );
        RAISE EXCEPTION 'incomplete reservation payment snapshot was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$assert_required_payment_snapshot$;

INSERT INTO public.migration_capture_state (
    train_run_id, migration_id, source_generation, capture_enabled, enabled_at
) VALUES (
    '61000000-0000-0000-0000-000000000002',
    '62000000-0000-0000-0000-000000000001', 1, true,
    clock_timestamp()
);

INSERT INTO public.reservations (
    id, user_id, train_run_id, assignment_generation, segment_count,
    from_stop_index, to_stop_index, seat_class, status, expires_at,
    total_amount_minor, currency
) VALUES (
    '62000000-0000-0000-0000-000000000002',
    '62000000-0000-0000-0000-000000000003',
    '61000000-0000-0000-0000-000000000002', 1, 2,
    0, 2, 'standard', 'held', clock_timestamp() + interval '1 hour',
    12500, 'TWD'
);

UPDATE public.reservations
   SET status = 'payment_pending',
       payment_intent_id = '62000000-0000-0000-0000-000000000004',
       payment_amount_minor = 12500,
       payment_currency = 'TWD',
       payment_grace_expires_at = clock_timestamp() + interval '15 minutes'
 WHERE id = '62000000-0000-0000-0000-000000000002';

INSERT INTO public.ticket_orders (
    id, reservation_id, user_id, train_run_id, assignment_generation,
    status, total_amount_minor, currency, payment_intent_id, payment_currency
) VALUES (
    '62000000-0000-0000-0000-000000000005',
    '62000000-0000-0000-0000-000000000002',
    '62000000-0000-0000-0000-000000000003',
    '61000000-0000-0000-0000-000000000002', 1,
    'payment_pending', 12500, 'TWD',
    '62000000-0000-0000-0000-000000000004', 'TWD'
);

INSERT INTO public.payment_command_receipts (
    id, command_id, payment_intent_id, reservation_id, train_run_id,
    assignment_generation, operation, request_fingerprint, amount_minor,
    currency, status, result_resource_id, result_status, committed_at
) VALUES (
    '62000000-0000-0000-0000-000000000006',
    '62000000-0000-0000-0000-000000000007',
    '62000000-0000-0000-0000-000000000004',
    '62000000-0000-0000-0000-000000000002',
    '61000000-0000-0000-0000-000000000002', 1,
    'reservation.payment_begin', decode(repeat('11', 32), 'hex'),
    12500, 'TWD', 'succeeded',
    '62000000-0000-0000-0000-000000000002',
    'payment_pending', clock_timestamp()
);

DO $assert_duplicate_begin$
BEGIN
    BEGIN
        INSERT INTO public.payment_command_receipts (
            id, command_id, payment_intent_id, reservation_id, train_run_id,
            assignment_generation, operation, request_fingerprint,
            amount_minor, currency
        ) VALUES (
            '62000000-0000-0000-0000-000000000008',
            '62000000-0000-0000-0000-000000000009',
            '62000000-0000-0000-0000-000000000004',
            '62000000-0000-0000-0000-000000000002',
            '61000000-0000-0000-0000-000000000002', 1,
            'reservation.payment_begin', decode(repeat('12', 32), 'hex'),
            12500, 'TWD'
        );
        RAISE EXCEPTION 'duplicate payment begin was accepted';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;
END;
$assert_duplicate_begin$;

DO $assert_immutable_money$
BEGIN
    BEGIN
        UPDATE public.reservations
           SET payment_amount_minor = 12000,
               total_amount_minor = 12000
         WHERE id = '62000000-0000-0000-0000-000000000002';
        RAISE EXCEPTION 'reservation payment money was mutable';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$assert_immutable_money$;

UPDATE public.ticket_orders
   SET status = 'payment_authorized', authorized_amount_minor = 12500
 WHERE id = '62000000-0000-0000-0000-000000000005';

UPDATE public.ticket_orders
   SET status = 'payment_captured', captured_amount_minor = 12500
 WHERE id = '62000000-0000-0000-0000-000000000005';

UPDATE public.ticket_orders
   SET status = 'issuance_pending'
 WHERE id = '62000000-0000-0000-0000-000000000005';

INSERT INTO public.ticket_issuance_receipts (
    id, issuance_id, payment_intent_id, reservation_id,
    payment_operation_id, ticket_order_id, train_run_id,
    assignment_generation, capture_proof_hash, amount_minor, currency,
    issued_ticket_count
) VALUES (
    '62000000-0000-0000-0000-000000000010',
    '62000000-0000-0000-0000-000000000011',
    '62000000-0000-0000-0000-000000000004',
    '62000000-0000-0000-0000-000000000002',
    '62000000-0000-0000-0000-000000000012',
    '62000000-0000-0000-0000-000000000005',
    '61000000-0000-0000-0000-000000000002', 1,
    decode(repeat('13', 32), 'hex'), 12500, 'TWD', 1
);

DO $assert_duplicate_issuance$
BEGIN
    BEGIN
        INSERT INTO public.ticket_issuance_receipts (
            id, issuance_id, payment_intent_id, reservation_id,
            payment_operation_id, ticket_order_id, train_run_id,
            assignment_generation, capture_proof_hash, amount_minor,
            currency, issued_ticket_count
        ) VALUES (
            '62000000-0000-0000-0000-000000000013',
            '62000000-0000-0000-0000-000000000014',
            '62000000-0000-0000-0000-000000000004',
            '62000000-0000-0000-0000-000000000002',
            '62000000-0000-0000-0000-000000000015',
            '62000000-0000-0000-0000-000000000005',
            '61000000-0000-0000-0000-000000000002', 1,
            decode(repeat('14', 32), 'hex'), 12500, 'TWD', 1
        );
        RAISE EXCEPTION 'duplicate issuance receipt was accepted';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;
END;
$assert_duplicate_issuance$;

UPDATE public.reservations
   SET status = 'refund_pending'
 WHERE id = '62000000-0000-0000-0000-000000000002';

UPDATE public.ticket_orders
   SET status = 'refund_pending'
 WHERE id = '62000000-0000-0000-0000-000000000005';

INSERT INTO public.payment_refund_receipts (
    id, refund_operation_id, payment_intent_id, reservation_id,
    ticket_order_id, train_run_id, assignment_generation, refund_proof_hash,
    captured_amount_minor, refunded_amount_minor, currency, refunded_at
) VALUES (
    '62000000-0000-0000-0000-000000000016',
    '62000000-0000-0000-0000-000000000017',
    '62000000-0000-0000-0000-000000000004',
    '62000000-0000-0000-0000-000000000002',
    '62000000-0000-0000-0000-000000000005',
    '61000000-0000-0000-0000-000000000002', 1,
    decode(repeat('15', 32), 'hex'), 12500, 12500, 'TWD',
    clock_timestamp()
);

DO $assert_full_refund_only$
BEGIN
    BEGIN
        INSERT INTO public.payment_refund_receipts (
            id, refund_operation_id, payment_intent_id, reservation_id,
            ticket_order_id, train_run_id, assignment_generation,
            refund_proof_hash, captured_amount_minor, refunded_amount_minor,
            currency, refunded_at
        ) VALUES (
            '62000000-0000-0000-0000-000000000018',
            '62000000-0000-0000-0000-000000000019',
            '62000000-0000-0000-0000-000000000020',
            '62000000-0000-0000-0000-000000000002',
            '62000000-0000-0000-0000-000000000005',
            '61000000-0000-0000-0000-000000000002', 1,
            decode(repeat('16', 32), 'hex'), 12500, 10000, 'TWD',
            clock_timestamp()
        );
        RAISE EXCEPTION 'partial refund receipt was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$assert_full_refund_only$;

INSERT INTO public.payment_compensation_receipts (
    id, compensation_id, payment_intent_id, reservation_id, ticket_order_id,
    refund_receipt_id, train_run_id, assignment_generation,
    released_seat_count, cancelled_ticket_count
) VALUES (
    '62000000-0000-0000-0000-000000000021',
    '62000000-0000-0000-0000-000000000022',
    '62000000-0000-0000-0000-000000000004',
    '62000000-0000-0000-0000-000000000002',
    '62000000-0000-0000-0000-000000000005',
    '62000000-0000-0000-0000-000000000016',
    '61000000-0000-0000-0000-000000000002', 1, 1, 0
);

UPDATE public.ticket_orders
   SET status = 'refunded', refunded_amount_minor = 12500
 WHERE id = '62000000-0000-0000-0000-000000000005';

UPDATE public.reservations
   SET status = 'cancelled'
 WHERE id = '62000000-0000-0000-0000-000000000002';

DO $assert_opaque_ticket_code$
BEGIN
    BEGIN
        UPDATE public.tickets
           SET ticket_code = 'not an opaque code'
         WHERE id = '61000000-0000-0000-0000-000000000015';
        RAISE EXCEPTION 'non-opaque ticket code was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$assert_opaque_ticket_code$;

DO $assert_journal_coverage$
BEGIN
    IF (SELECT count(DISTINCT table_name)
        FROM public.train_run_mutation_journal
        WHERE migration_id = '62000000-0000-0000-0000-000000000001'
          AND table_name IN (
              'payment_command_receipts',
              'ticket_issuance_receipts',
              'payment_refund_receipts',
              'payment_compensation_receipts'
          )) <> 4 THEN
        RAISE EXCEPTION 'payment receipt mutations were not fully journaled';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.train_run_mutation_journal
        WHERE migration_id = '62000000-0000-0000-0000-000000000001'
          AND metadata ?| ARRAY[
              'user_id', 'passenger_id', 'key_hash', 'request_fingerprint',
              'capture_proof_hash', 'refund_proof_hash', 'token', 'secret',
              'password', 'payload'
          ]
    ) THEN
        RAISE EXCEPTION 'payment journal metadata contains a sensitive field';
    END IF;
END;
$assert_journal_coverage$;

ROLLBACK;
