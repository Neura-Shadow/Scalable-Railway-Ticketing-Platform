DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM public.financial_ledger_transactions
         WHERE transaction_id =
               '77000000-0000-4000-8000-000000000001'::uuid
           AND event_id = 'synthetic:migration-downgrade-guard'
    ) OR (
        SELECT count(*)
          FROM public.financial_ledger_postings
         WHERE transaction_id =
               '77000000-0000-4000-8000-000000000001'::uuid
    ) <> 2 OR NOT EXISTS (
        SELECT 1
          FROM public.schema_migrations
         WHERE version = 11 AND NOT dirty
    ) THEN
        RAISE EXCEPTION 'control v11 failed downgrade did not preserve evidence';
    END IF;
END;
$$;
