DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM public.reservations
         WHERE id = '61000000-0000-0000-0000-000000000010'::uuid
           AND status = 'confirmed'
           AND total_amount_minor = 12500
           AND currency = 'TWD'
    ) OR NOT EXISTS (
        SELECT 1
          FROM public.ticket_orders
         WHERE id = '61000000-0000-0000-0000-000000000014'::uuid
           AND status = 'confirmed'
           AND captured_amount_minor = 0
           AND refunded_amount_minor = 0
    ) OR NOT EXISTS (
        SELECT 1
          FROM public.tickets
         WHERE id = '61000000-0000-0000-0000-000000000015'::uuid
           AND status = 'active'
           AND ticket_code = 'TKT.61000000/legacy?00000015'
    ) THEN
        RAISE EXCEPTION 'populated booking-shard v1-to-v3 data was not preserved with financial evidence';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.train_run_booking_snapshots
    ) OR EXISTS (
        SELECT 1 FROM public.train_run_booking_snapshots
         WHERE isfinite(scheduled_departure_at)
    ) THEN
        RAISE EXCEPTION 'populated snapshots lost exact departure rematerialization state';
    END IF;

    IF (
        SELECT count(*)
          FROM public.ticket_refund_compensation_receipts
         WHERE id = '78000000-0000-4000-8000-000000000001'::uuid
           AND command_id = '78000000-0000-4000-8000-000000000002'::uuid
           AND refund_request_id = '78000000-0000-4000-8000-000000000003'::uuid
           AND refund_operation_id = '78000000-0000-4000-8000-000000000004'::uuid
           AND payment_intent_id = '78000000-0000-4000-8000-000000000005'::uuid
           AND reservation_id = '61000000-0000-0000-0000-000000000010'::uuid
           AND ticket_order_id = '61000000-0000-0000-0000-000000000014'::uuid
           AND train_run_id = '61000000-0000-0000-0000-000000000002'::uuid
           AND assignment_generation = 1
           AND request_fingerprint = decode(repeat('61', 32), 'hex')
           AND provider_proof_hash = decode(repeat('62', 32), 'hex')
           AND amount_minor = 12500
           AND currency = 'TWD'
           AND selected_ticket_count = 1
           AND released_seat_count = 1
           AND resulting_active_ticket_count = 0
           AND resulting_order_state = 'refunded'
           AND committed_at IS NOT NULL
    ) <> 1 THEN
        RAISE EXCEPTION 'booking-shard v3 compensation evidence was not preserved';
    END IF;

    IF (
        SELECT count(*)
          FROM public.selected_ticket_refund_receipts
         WHERE id = '78000000-0000-4000-8000-000000000006'::uuid
           AND compensation_receipt_id = '78000000-0000-4000-8000-000000000001'::uuid
           AND refund_request_id = '78000000-0000-4000-8000-000000000003'::uuid
           AND ticket_id = '61000000-0000-0000-0000-000000000015'::uuid
           AND reservation_seat_id = '61000000-0000-0000-0000-000000000012'::uuid
           AND train_run_id = '61000000-0000-0000-0000-000000000002'::uuid
           AND assignment_generation = 1
           AND fare_amount_minor = 12500
           AND currency = 'TWD'
           AND segment_mask_hash = decode(repeat('63', 32), 'hex')
           AND released_at IS NOT NULL
    ) <> 1 THEN
        RAISE EXCEPTION 'booking-shard v3 selected-ticket evidence was not preserved';
    END IF;
END;
$$;
