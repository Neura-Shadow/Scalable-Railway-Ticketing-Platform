DO $assert_m6_control$
DECLARE
    current_version bigint;
    current_dirty boolean;
BEGIN
    SELECT version, dirty
      INTO current_version, current_dirty
      FROM public.schema_migrations
     LIMIT 1;

    IF current_version <> 10 OR current_dirty THEN
        RAISE EXCEPTION 'control migration must be clean at version 10';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN (
              'payment_intents',
              'payment_sagas',
              'payment_operations',
              'payment_webhook_inbox',
              'payment_provider_event_conflicts',
              'payment_reconciliation_checkpoints',
              'payment_manual_review_cases'
          )) <> 7 THEN
        RAISE EXCEPTION 'control-plane version 10 payment tables are incomplete';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN (
              'payment_intents', 'payment_sagas', 'payment_operations',
              'payment_webhook_inbox', 'payment_provider_event_conflicts',
              'payment_reconciliation_checkpoints',
              'payment_manual_review_cases'
          )
          AND column_name IN (
              'raw_idempotency_key', 'raw_provider_secret', 'provider_secret',
              'webhook_secret', 'raw_payload', 'payload', 'request_body',
              'response_body', 'card_number', 'pan', 'cvv', 'cvc', 'pin',
              'magstripe', 'track_data', 'payment_credential'
          )
    ) THEN
        RAISE EXCEPTION 'payment control schema contains a forbidden sensitive-data column';
    END IF;

    IF (SELECT count(*)
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname IN (
              'payment_intents_one_active_reservation_idx',
              'payment_sagas_one_active_intent_idx',
              'payment_sagas_one_active_reservation_idx',
              'payment_operations_one_financial_type_idx',
              'payment_reconciliation_one_active_global_idx',
              'payment_reconciliation_one_active_intent_idx',
              'payment_manual_review_one_active_reason_idx'
          )
          AND indexdef LIKE '%UNIQUE INDEX%') <> 7 THEN
        RAISE EXCEPTION 'payment active-resource uniqueness indexes are incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
        WHERE schema_row.nspname = 'public'
          AND table_row.relname = 'payment_operations'
          AND constraint_row.conname = 'payment_operations_intent_amount_fkey'
          AND constraint_row.contype = 'f'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%amount_minor%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%currency%'
    ) THEN
        RAISE EXCEPTION 'provider operations are not bound to intent amount and currency';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc AS function_row
        JOIN pg_namespace AS schema_row
          ON schema_row.oid = function_row.pronamespace
        WHERE schema_row.nspname = 'public'
          AND function_row.proname = 'guard_payment_financial_settlement'
          AND pg_get_functiondef(function_row.oid) LIKE '%FOR UPDATE%'
          AND pg_get_functiondef(function_row.oid) LIKE '%operation_type = ''capture''%'
          AND pg_get_functiondef(function_row.oid) LIKE '%state = ''succeeded''%'
    ) THEN
        RAISE EXCEPTION 'full-refund accounting guard is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
        WHERE schema_row.nspname = 'public'
          AND table_row.relname = 'payment_webhook_inbox'
          AND constraint_row.contype = 'u'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%provider, provider_event_id%'
    ) THEN
        RAISE EXCEPTION 'provider webhook event identity is not unique';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
        WHERE schema_row.nspname = 'public'
          AND table_row.relname = 'payment_provider_event_conflicts'
          AND constraint_row.conname =
              'payment_provider_event_conflicts_distinct_hash_check'
    ) THEN
        RAISE EXCEPTION 'provider event conflict digest guard is missing';
    END IF;
END;
$assert_m6_control$;

-- LIKE copies the exact CHECK contracts without foreign keys or unique indexes,
-- allowing focused invalid-value probes without retaining financial fixtures.
CREATE TEMP TABLE payment_intent_constraint_probe
    (LIKE public.payment_intents INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

DO $assert_m6_intent_constraints$
BEGIN
    BEGIN
        INSERT INTO payment_intent_constraint_probe (
            payment_intent_id, reservation_id, train_run_id, owner_user_id,
            provider, amount_minor, currency, state, idempotency_key_hash,
            request_fingerprint
        ) VALUES (
            '10000000-0000-0000-0000-000000000001',
            '20000000-0000-0000-0000-000000000001',
            '30000000-0000-0000-0000-000000000001',
            '40000000-0000-0000-0000-000000000001',
            'sandbox', -1, 'TWD', 'created',
            decode(repeat('01', 32), 'hex'), decode(repeat('02', 32), 'hex')
        );
        RAISE EXCEPTION 'negative payment amount was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO payment_intent_constraint_probe (
            payment_intent_id, reservation_id, train_run_id, owner_user_id,
            provider, amount_minor, currency, state, idempotency_key_hash,
            request_fingerprint
        ) VALUES (
            '10000000-0000-0000-0000-000000000002',
            '20000000-0000-0000-0000-000000000002',
            '30000000-0000-0000-0000-000000000002',
            '40000000-0000-0000-0000-000000000002',
            'sandbox', 100, 'twd', 'created',
            decode(repeat('03', 32), 'hex'), decode(repeat('04', 32), 'hex')
        );
        RAISE EXCEPTION 'invalid payment currency was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO payment_intent_constraint_probe (
            payment_intent_id, reservation_id, train_run_id, owner_user_id,
            provider, amount_minor, currency, state, idempotency_key_hash,
            request_fingerprint
        ) VALUES (
            '10000000-0000-0000-0000-000000000003',
            '20000000-0000-0000-0000-000000000003',
            '30000000-0000-0000-0000-000000000003',
            '40000000-0000-0000-0000-000000000003',
            'sandbox', 100, 'TWD', 'provider_said_ok',
            decode(repeat('05', 32), 'hex'), decode(repeat('06', 32), 'hex')
        );
        RAISE EXCEPTION 'unbounded payment intent state was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO payment_intent_constraint_probe (
            payment_intent_id, reservation_id, train_run_id, owner_user_id,
            provider, amount_minor, currency, state, idempotency_key_hash,
            request_fingerprint
        ) VALUES (
            '10000000-0000-0000-0000-000000000004',
            '20000000-0000-0000-0000-000000000004',
            '30000000-0000-0000-0000-000000000004',
            '40000000-0000-0000-0000-000000000004',
            'sandbox', 100, 'TWD', 'created',
            decode(repeat('07', 31), 'hex'), decode(repeat('08', 32), 'hex')
        );
        RAISE EXCEPTION 'non-SHA-256 payment idempotency digest was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$assert_m6_intent_constraints$;

CREATE TEMP TABLE payment_operation_constraint_probe
    (LIKE public.payment_operations INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

DO $assert_m6_operation_constraints$
BEGIN
    BEGIN
        INSERT INTO payment_operation_constraint_probe (
            operation_id, payment_intent_id, provider, operation_type,
            provider_idempotency_key_hash, amount_minor, currency, state
        ) VALUES (
            '50000000-0000-0000-0000-000000000001',
            '10000000-0000-0000-0000-000000000001',
            'sandbox', 'partial_refund', decode(repeat('09', 32), 'hex'),
            100, 'TWD', 'pending'
        );
        RAISE EXCEPTION 'unsupported partial-refund operation was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO payment_operation_constraint_probe (
            operation_id, payment_intent_id, provider, operation_type,
            provider_idempotency_key_hash, amount_minor, currency, state
        ) VALUES (
            '50000000-0000-0000-0000-000000000002',
            '10000000-0000-0000-0000-000000000001',
            'sandbox', 'capture', decode(repeat('0a', 32), 'hex'),
            100, 'TWD', 'provider_maybe_succeeded'
        );
        RAISE EXCEPTION 'unbounded provider operation state was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$assert_m6_operation_constraints$;
