BEGIN;

SELECT set_config('railway.deployment_region', 'region-a', true),
       set_config('railway.deployment_role', 'active', true),
       set_config('railway.region_epoch', '1', true),
       set_config('railway.regional_writes_enabled', 'true', true);

INSERT INTO public.financial_ledger_transactions (
    transaction_id, event_id, correlation, purpose,
    currency, fingerprint, created_at
) VALUES (
    '77000000-0000-4000-8000-000000000001',
    'synthetic:migration-downgrade-guard',
    'synthetic-migration-downgrade-guard',
    'capture', 'TWD', decode(repeat('51', 32), 'hex'), clock_timestamp()
);

INSERT INTO public.financial_ledger_postings (
    transaction_id, posting_index, account_code, side,
    amount_minor, currency
) VALUES
    ('77000000-0000-4000-8000-000000000001', 0,
     'provider_receivable', 'debit', 12500, 'TWD'),
    ('77000000-0000-4000-8000-000000000001', 1,
     'customer_funds_pending', 'credit', 12500, 'TWD');

COMMIT;
