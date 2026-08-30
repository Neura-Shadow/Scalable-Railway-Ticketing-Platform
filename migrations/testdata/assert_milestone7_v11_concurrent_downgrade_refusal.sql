DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.schema_migrations
         WHERE version = 10 AND dirty
    ) OR to_regclass('public.financial_ledger_transactions') IS NULL
       OR NOT EXISTS (
            SELECT 1 FROM public.financial_ledger_transactions
             WHERE event_id = 'synthetic:migration-downgrade-guard'
       ) THEN
        RAISE EXCEPTION 'concurrent control evidence did not leave intact v11 schema and honest dirty v10 marker';
    END IF;
END;
$$;
