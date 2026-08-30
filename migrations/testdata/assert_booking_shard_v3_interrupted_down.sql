DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.schema_migrations
         WHERE version = 2 AND dirty
    ) OR to_regclass('public.dr_reconciliation_checkpoints') IS NOT NULL
       OR to_regclass('public.ticket_refund_compensation_receipts') IS NOT NULL
       OR to_regclass('public.payment_refund_receipts') IS NULL THEN
        RAISE EXCEPTION 'interrupted booking-shard v3 down did not retain an honest dirty v2 physical state';
    END IF;
END;
$$;
