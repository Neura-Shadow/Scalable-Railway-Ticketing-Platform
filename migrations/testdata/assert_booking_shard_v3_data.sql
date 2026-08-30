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
        RAISE EXCEPTION 'populated booking-shard v1-to-v3 data was not preserved';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.train_run_booking_snapshots
    ) OR EXISTS (
        SELECT 1 FROM public.train_run_booking_snapshots
         WHERE isfinite(scheduled_departure_at)
    ) THEN
        RAISE EXCEPTION 'populated snapshots were not marked for exact departure rematerialization';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM public.ticket_refund_compensation_receipts
    ) OR EXISTS (
        SELECT 1
          FROM public.selected_ticket_refund_receipts
    ) THEN
        RAISE EXCEPTION 'upgrade invented partial-refund evidence';
    END IF;
END;
$$;
