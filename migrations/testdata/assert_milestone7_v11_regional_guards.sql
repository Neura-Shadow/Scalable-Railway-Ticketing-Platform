BEGIN;

DO $$
BEGIN
    BEGIN
        INSERT INTO public.payment_provider_capabilities (
            provider, provider_account_id, api_version,
            hosted_checkout, authorize, capture, void_payment,
            full_refund, partial_refund, payment_status_query,
            settlement_transactions, payout_reports, webhook_signatures,
            webhook_key_rotation, profile_hash
        ) VALUES (
            'sandbox', 'synthetic', 'v1',
            true, true, true, true, true, true, true,
            true, true, true, true, decode(repeat('41', 32), 'hex')
        );
        RAISE EXCEPTION 'control write without regional context succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END;
$$;

SELECT set_config('railway.deployment_region', 'region-a', true),
       set_config('railway.deployment_role', 'active', true),
       set_config('railway.region_epoch', '1', true),
       set_config('railway.regional_writes_enabled', 'true', true);

INSERT INTO public.payment_provider_capabilities (
    provider, provider_account_id, api_version,
    hosted_checkout, authorize, capture, void_payment,
    full_refund, partial_refund, payment_status_query,
    settlement_transactions, payout_reports, webhook_signatures,
    webhook_key_rotation, profile_hash
) VALUES (
    'sandbox', 'synthetic', 'v1',
    true, true, true, true, true, true, true,
    true, true, true, true, decode(repeat('41', 32), 'hex')
);

DO $$
BEGIN
    BEGIN
        INSERT INTO public.payment_webhook_inbox (
            inbox_id, provider, provider_event_id, provider_account_id,
            event_type, payload_hash, verified_key_id, event_created_at,
            signature_verified_at, received_at, body_size_bytes
        ) VALUES (
            '76000000-0000-4000-8000-000000000020', 'stripe',
            'evt_binding_pair_invalid', 'acct_contract', 'payment.captured',
            decode(repeat('42', 32), 'hex'), 'current', clock_timestamp(),
            clock_timestamp(), clock_timestamp(), 128
        );
        RAISE EXCEPTION 'partial webhook provider binding was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$$;

INSERT INTO public.payment_webhook_inbox (
    inbox_id, provider, provider_event_id, provider_account_id,
    provider_environment, event_type, payload_hash, verified_key_id,
    event_created_at, signature_verified_at, received_at, body_size_bytes
) VALUES (
    '76000000-0000-4000-8000-000000000021', 'stripe',
    'evt_binding_immutable', 'acct_contract', 'test', 'payment.captured',
    decode(repeat('43', 32), 'hex'), 'current', clock_timestamp(),
    clock_timestamp(), clock_timestamp(), 128
);

DO $$
BEGIN
    BEGIN
        UPDATE public.payment_webhook_inbox
           SET provider_environment = 'live'
         WHERE provider = 'stripe'
           AND provider_event_id = 'evt_binding_immutable';
        RAISE EXCEPTION 'webhook provider binding was mutable';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$$;

DO $$
BEGIN
    BEGIN
        TRUNCATE public.payment_provider_capabilities;
        RAISE EXCEPTION 'control table truncate unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
    IF NOT EXISTS (
        SELECT 1 FROM public.payment_provider_capabilities
         WHERE provider = 'sandbox' AND provider_account_id = 'synthetic'
    ) THEN
        RAISE EXCEPTION 'control truncate guard did not preserve evidence';
    END IF;
END;
$$;

DO $$
BEGIN
    BEGIN
        PERFORM set_config('railway.deployment_role', 'passive', true);
        UPDATE public.payment_provider_capabilities
           SET api_version = 'v2'
         WHERE provider = 'sandbox'
           AND provider_account_id = 'synthetic';
        RAISE EXCEPTION 'passive control write unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
    BEGIN
        PERFORM set_config('railway.region_epoch', '2', true);
        UPDATE public.payment_provider_capabilities
           SET api_version = 'v2'
         WHERE provider = 'sandbox'
           AND provider_account_id = 'synthetic';
        RAISE EXCEPTION 'stale-authority control write unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
    BEGIN
        UPDATE public.regional_write_authority
           SET updated_at = clock_timestamp()
         WHERE singleton;
        RAISE EXCEPTION 'active role changed regional authority';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END;
$$;

SELECT set_config('railway.deployment_role', 'recovery', true),
       set_config('railway.regional_writes_enabled', 'false', true);

UPDATE public.regional_write_authority
   SET state = 'fenced', writes_enabled = false
 WHERE singleton;

DO $$
BEGIN
    BEGIN
        INSERT INTO public.regional_failover_operations (
            operation_id, operation_kind, source_region, target_region,
            source_epoch, incident_id, operator_id, reason_category,
            stage, checkpoint, declared_at
        ) VALUES (
            '76000000-0000-4000-8000-000000000010',
            'failover', 'region-b', 'region-a', 1,
            '76000000-0000-4000-8000-000000000011',
            'synthetic-operator', 'regional_test', 'planned', '{}'::jsonb,
            clock_timestamp()
        );
        RAISE EXCEPTION 'unbound failover operation was accepted';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END;
$$;

INSERT INTO public.regional_failover_operations (
    operation_id, operation_kind, source_region, target_region,
    source_epoch, incident_id, operator_id, reason_category,
    stage, checkpoint, declared_at
) VALUES (
    '76000000-0000-4000-8000-000000000001',
    'failover', 'region-a', 'region-b', 1,
    '76000000-0000-4000-8000-000000000002',
    'synthetic-operator', 'regional_test', 'planned', '{}'::jsonb,
    clock_timestamp()
);

CREATE TEMP TABLE refund_operation_guard_probe (
    refund_operation_id uuid, refund_request_id uuid, provider text,
    provider_payment_id text, provider_idempotency_key_hash bytea,
    amount_minor bigint, currency text, state text, provider_refund_id text,
    response_fingerprint bytea, captured_total_minor bigint,
    refunded_total_minor bigint, completed_at timestamptz
);
CREATE TRIGGER refund_operation_guard_probe_trigger BEFORE UPDATE
ON refund_operation_guard_probe FOR EACH ROW
EXECUTE FUNCTION public.guard_ticket_refund_operation_state();
INSERT INTO refund_operation_guard_probe VALUES (
    gen_random_uuid(),gen_random_uuid(),'sandbox','pi_probe',decode(repeat('11',32),'hex'),
    100,'TWD','processing',NULL,NULL,NULL,NULL,NULL
);
DO $$ BEGIN
    BEGIN
        UPDATE refund_operation_guard_probe SET state='succeeded';
        RAISE EXCEPTION 'refund succeeded without provider proof';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END $$;
UPDATE refund_operation_guard_probe SET state='succeeded',provider_refund_id='re_probe',
 response_fingerprint=decode(repeat('22',32),'hex'),captured_total_minor=100,
 refunded_total_minor=100,completed_at=clock_timestamp();
DO $$ BEGIN
    BEGIN
        UPDATE refund_operation_guard_probe SET response_fingerprint=decode(repeat('33',32),'hex');
        RAISE EXCEPTION 'terminal refund provider proof was mutable';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END $$;

CREATE TEMP TABLE refund_request_guard_probe (state text, marker integer);
CREATE TRIGGER refund_request_guard_probe_trigger BEFORE UPDATE
ON refund_request_guard_probe FOR EACH ROW
EXECUTE FUNCTION public.guard_ticket_refund_request_state();
INSERT INTO refund_request_guard_probe VALUES ('created',1);
DO $$ BEGIN
    BEGIN
        UPDATE refund_request_guard_probe SET state='failed';
        RAISE EXCEPTION 'illegal refund request transition succeeded';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END $$;
UPDATE refund_request_guard_probe SET state='refund_pending';
UPDATE refund_request_guard_probe SET state='failed';
DO $$ BEGIN
    BEGIN
        UPDATE refund_request_guard_probe SET marker=2;
        RAISE EXCEPTION 'terminal refund request evidence was mutable';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END $$;

CREATE TEMP TABLE refund_prepare_guard_probe (
    id uuid,command_id uuid,refund_request_id uuid,refund_operation_id uuid,
    payment_intent_id uuid,reservation_id uuid,ticket_order_id uuid,train_run_id uuid,
    assignment_generation bigint,request_fingerprint bytea,amount_minor bigint,currency text,
    ticket_ids uuid[],prior_order_state text,prior_reservation_state text,state text,
    requested_at timestamptz,eligibility_cutoff_at timestamptz,prepared_at timestamptz,resolved_at timestamptz
);
CREATE TRIGGER refund_prepare_guard_probe_trigger BEFORE UPDATE
ON refund_prepare_guard_probe FOR EACH ROW
EXECUTE FUNCTION public.guard_ticket_refund_prepare_receipt_transition();
INSERT INTO refund_prepare_guard_probe VALUES (
    gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),
    gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),1,decode(repeat('44',32),'hex'),100,'TWD',
    ARRAY[gen_random_uuid()],'issued','confirmed','prepared',clock_timestamp()-interval '1 minute',
    clock_timestamp()+interval '1 hour',clock_timestamp(),NULL
);
UPDATE refund_prepare_guard_probe SET state='released',resolved_at=clock_timestamp();
DO $$ BEGIN
    BEGIN
        UPDATE refund_prepare_guard_probe SET state='applied';
        RAISE EXCEPTION 'resolved refund prepare evidence was mutable';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END $$;

DO $$
BEGIN
    BEGIN
        DELETE FROM public.regional_failover_operations
         WHERE operation_id = '76000000-0000-4000-8000-000000000001';
        RAISE EXCEPTION 'regional failover journal was deleted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$$;

ROLLBACK;
