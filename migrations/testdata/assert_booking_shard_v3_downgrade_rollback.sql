DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM public.ticket_refund_compensation_receipts
         WHERE id = '78000000-0000-4000-8000-000000000001'::uuid
    ) OR NOT EXISTS (
        SELECT 1
          FROM public.selected_ticket_refund_receipts
         WHERE id = '78000000-0000-4000-8000-000000000006'::uuid
    ) OR NOT EXISTS (
        SELECT 1
          FROM public.schema_migrations
         WHERE version = 3 AND NOT dirty
    ) THEN
        RAISE EXCEPTION 'booking-shard v3 failed downgrade lost receipt evidence';
    END IF;
END;
$$;
