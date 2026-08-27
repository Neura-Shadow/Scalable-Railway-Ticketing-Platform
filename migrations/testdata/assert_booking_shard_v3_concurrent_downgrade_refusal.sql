DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.schema_migrations
         WHERE version = 2 AND dirty
    ) OR to_regclass('public.ticket_refund_compensation_receipts') IS NULL
       OR NOT EXISTS (
            SELECT 1 FROM public.ticket_refund_compensation_receipts
             WHERE id = '78000000-0000-4000-8000-000000000001'::uuid
       ) THEN
        RAISE EXCEPTION 'concurrent shard evidence did not leave intact v3 schema and honest dirty v2 marker';
    END IF;
END;
$$;
