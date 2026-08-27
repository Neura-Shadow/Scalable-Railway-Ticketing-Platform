DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.schema_migrations
         WHERE version = 10 AND dirty
    ) OR to_regclass('public.financial_ledger_transactions') IS NOT NULL
       OR to_regclass('public.payment_provider_capabilities') IS NOT NULL
       OR to_regclass('public.payment_operations') IS NULL THEN
        RAISE EXCEPTION 'interrupted control v11 down did not retain an honest dirty v10 physical state';
    END IF;
END;
$$;
