DO $$
DECLARE
    relation_name text;
    actual_columns text[];
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'payment_saga_actions',
        'payment_provider_capabilities',
        'financial_ledger_accounts',
        'financial_ledger_transactions',
        'financial_ledger_postings',
        'financial_ledger_reversals',
        'provider_balance_transactions',
        'provider_settlement_batches',
        'provider_settlement_lines',
        'provider_payouts',
        'provider_payout_lines',
        'provider_settlement_import_checkpoints',
        'provider_settlement_import_conflicts',
        'settlement_reconciliation_runs',
        'settlement_reconciliation_mismatches',
        'settlement_reconciliation_reviews',
        'ticket_refund_requests',
        'ticket_refund_request_items',
        'ticket_refund_sagas',
        'ticket_refund_operations',
        'ticket_refund_prepare_bindings',
        'ticket_refund_manual_reviews',
        'ticket_refund_prepare_receipts',
        'ticket_refund_compensation_receipts',
        'selected_ticket_refund_receipts',
        'payment_webhook_key_versions',
        'payment_webhook_key_version_archive',
        'payment_webhook_key_rotation_audit',
        'regional_write_authority',
        'regional_failover_operations',
        'backup_artifacts',
        'backup_operations',
        'backup_verifications',
        'restore_validations',
        'backup_expiration_operations'
    ] LOOP
        IF to_regclass('public.' || relation_name) IS NULL THEN
            RAISE EXCEPTION 'control v11 relation % is missing', relation_name;
        END IF;
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema='public' AND table_name='payment_webhook_key_versions'
           AND column_name='material_proof' AND data_type='bytea'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_indexes
         WHERE schemaname='public'
           AND tablename='payment_webhook_key_versions'
           AND indexname='payment_webhook_key_versions_material_identity_idx'
           AND indexdef LIKE '%provider_account_id, material_proof%'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_indexes
         WHERE schemaname='public'
           AND tablename='payment_webhook_key_version_archive'
           AND indexname='payment_webhook_key_version_archive_material_identity_idx'
           AND indexdef LIKE '%provider_account_id, material_proof%'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid='public.payment_webhook_key_version_archive'::regclass
           AND tgname='payment_webhook_key_version_archive_guard_immutable'
           AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid='public.payment_webhook_key_versions'::regclass
           AND tgname='payment_webhook_key_versions_guard'
           AND pg_get_triggerdef(oid) LIKE '%INSERT%'
           AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid='public.payment_webhook_key_rotation_audit'::regclass
           AND tgname='payment_webhook_key_rotation_audit_guard_immutable'
           AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'webhook key proof or immutable rotation audit is missing';
    END IF;

    IF (
        SELECT array_agg(column_name::text ORDER BY ordinal_position)
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'provider_settlement_import_checkpoints'
    ) IS DISTINCT FROM ARRAY[
        'provider', 'provider_account_id', 'cursor', 'next_attempt_at',
        'lease_owner', 'lease_token', 'lease_until', 'updated_at'
    ]::text[] OR NOT EXISTS (
        SELECT 1
          FROM pg_indexes
         WHERE schemaname = 'public'
           AND tablename = 'provider_settlement_import_checkpoints'
           AND indexname = 'provider_settlement_import_due_idx'
    ) THEN
        RAISE EXCEPTION 'control v11 settlement import lease shape is invalid';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'payment_webhook_inbox'
           AND column_name = 'provider_account_id'
           AND is_nullable = 'YES'
    ) OR NOT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'payment_webhook_inbox'
           AND column_name = 'provider_environment'
           AND is_nullable = 'YES'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'public.payment_webhook_inbox'::regclass
           AND conname = 'payment_webhook_inbox_provider_binding_check'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid = 'public.payment_webhook_inbox'::regclass
           AND tgname = 'payment_webhook_inbox_provider_binding_guard'
           AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'control v11 webhook provider binding is missing';
    END IF;

    SELECT array_agg(column_name::text ORDER BY ordinal_position)
      INTO actual_columns
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name =
           'physical_source_ticket_refund_prepare_receipt_rows';
    IF actual_columns IS DISTINCT FROM ARRAY[
        'source_shard_id', 'id', 'command_id', 'refund_request_id',
        'refund_operation_id', 'payment_intent_id', 'reservation_id',
        'ticket_order_id', 'train_run_id', 'assignment_generation', 'request_fingerprint',
        'amount_minor', 'currency', 'ticket_ids', 'prior_order_state',
        'prior_reservation_state', 'state', 'requested_at',
        'eligibility_cutoff_at', 'prepared_at', 'resolved_at'
    ]::text[] THEN
        RAISE EXCEPTION 'control v11 prepare shadow view shape is invalid';
    END IF;

    SELECT array_agg(column_name::text ORDER BY ordinal_position)
      INTO actual_columns
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name =
           'physical_source_ticket_refund_compensation_receipt_rows';
    IF actual_columns IS DISTINCT FROM ARRAY[
        'source_shard_id', 'id', 'command_id', 'refund_request_id',
        'refund_operation_id', 'payment_intent_id', 'reservation_id',
        'ticket_order_id', 'train_run_id', 'request_fingerprint',
        'provider_proof_hash', 'amount_minor', 'currency',
        'selected_ticket_count', 'released_seat_count',
        'resulting_active_ticket_count', 'resulting_order_state',
        'committed_at'
    ]::text[] THEN
        RAISE EXCEPTION 'control v11 compensation shadow view shape is invalid';
    END IF;

    SELECT array_agg(column_name::text ORDER BY ordinal_position)
      INTO actual_columns
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'physical_source_selected_ticket_refund_receipt_rows';
    IF actual_columns IS DISTINCT FROM ARRAY[
        'source_shard_id', 'id', 'compensation_receipt_id',
        'refund_request_id', 'ticket_id', 'reservation_seat_id',
        'train_run_id', 'fare_amount_minor', 'currency',
        'segment_mask_hash', 'released_at'
    ]::text[] THEN
        RAISE EXCEPTION 'control v11 selected-ticket shadow view shape is invalid';
    END IF;

    FOREACH relation_name IN ARRAY ARRAY[
        'booking_shard_0.ticket_refund_prepare_receipts',
        'booking_shard_0.ticket_refund_compensation_receipts',
        'booking_shard_0.selected_ticket_refund_receipts',
        'booking_shard_1.ticket_refund_prepare_receipts',
        'booking_shard_1.ticket_refund_compensation_receipts',
        'booking_shard_1.selected_ticket_refund_receipts',
        'public.physical_source_ticket_refund_prepare_receipt_rows',
        'public.physical_source_ticket_refund_compensation_receipt_rows',
        'public.physical_source_selected_ticket_refund_receipt_rows'
    ] LOOP
        IF to_regclass(relation_name) IS NULL THEN
            RAISE EXCEPTION 'control v11 compatibility relation % is missing',
                relation_name;
        END IF;
    END LOOP;

    IF (SELECT count(*) FROM public.financial_ledger_accounts) <> 7
       OR NOT EXISTS (
            SELECT 1 FROM public.financial_ledger_accounts
             WHERE account_code = 'reconciliation_suspense'
       ) THEN
        RAISE EXCEPTION 'control v11 bounded ledger accounts are invalid';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.regional_write_authority
         WHERE singleton = true
           AND region = 'region-a'
           AND epoch = 1
           AND state = 'active'
           AND writes_enabled = true
    ) THEN
        RAISE EXCEPTION 'control v11 regional authority seed is invalid';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid = 'public.financial_ledger_transactions'::regclass
           AND tgname = 'financial_ledger_transactions_guard_immutable'
           AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid = 'public.financial_ledger_postings'::regclass
           AND tgname = 'financial_ledger_postings_guard_immutable'
           AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid = 'public.regional_write_authority'::regclass
           AND tgname = 'regional_write_authority_guard_transition'
           AND NOT tgisinternal
    ) OR (
        SELECT count(*)
          FROM pg_trigger AS trigger_row
          JOIN pg_class AS table_row ON table_row.oid = trigger_row.tgrelid
          JOIN pg_namespace AS schema_row
            ON schema_row.oid = table_row.relnamespace
         WHERE schema_row.nspname IN (
                   'public', 'booking_shard_0', 'booking_shard_1'
               )
           AND table_row.relname IN (
                   'ticket_refund_prepare_receipts',
                   'ticket_refund_compensation_receipts',
                   'selected_ticket_refund_receipts'
               )
           AND trigger_row.tgname IN (
                   'physical_target_write_guard', 'physical_source_capture'
               )
           AND NOT trigger_row.tgisinternal
    ) <> 18 OR NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid='public.ticket_refund_requests'::regclass
           AND tgname='ticket_refund_requests_guard_state' AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid='public.ticket_refund_operations'::regclass
           AND tgname='ticket_refund_operations_guard_state' AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'control v11 immutable, authority, or migration guards are missing';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class AS table_row
          JOIN pg_catalog.pg_namespace AS schema_row
            ON schema_row.oid = table_row.relnamespace
         WHERE schema_row.nspname IN (
                   'public', 'booking_shard_0', 'booking_shard_1'
               )
           AND table_row.relkind IN ('r', 'p')
           AND NOT (
               schema_row.nspname = 'public'
               AND table_row.relname = 'schema_migrations'
           )
           AND NOT EXISTS (
               SELECT 1
                 FROM pg_catalog.pg_trigger AS trigger_row
                WHERE trigger_row.tgrelid = table_row.oid
                  AND trigger_row.tgname = 'regional_write_context_guard'
                  AND NOT trigger_row.tgisinternal
           )
    ) OR to_regprocedure(
        'public.guard_regional_application_write()'
    ) IS NULL OR to_regprocedure(
        'public.lock_regional_write_authority()'
    ) IS NULL OR to_regprocedure(
        'public.guard_regional_operational_write()'
    ) IS NULL OR to_regprocedure(
        'public.guard_regional_authority_command()'
    ) IS NULL THEN
        RAISE EXCEPTION 'control v11 regional DML coverage is incomplete';
    END IF;

    IF NOT (
        SELECT function_row.prosecdef
          FROM pg_catalog.pg_proc AS function_row
         WHERE function_row.oid =
               'public.lock_regional_write_authority()'::regprocedure
    ) OR EXISTS (
        SELECT 1
          FROM pg_catalog.pg_proc AS function_row
          CROSS JOIN LATERAL pg_catalog.aclexplode(
              COALESCE(
                  function_row.proacl,
                  pg_catalog.acldefault('f', function_row.proowner)
              )
          ) AS privilege_row
         WHERE function_row.oid =
               'public.lock_regional_write_authority()'::regprocedure
           AND privilege_row.grantee = 0
           AND privilege_row.privilege_type = 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'regional authority lock function privilege boundary is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger
         WHERE tgrelid =
               'public.physical_control_target_apply_authorizations'::regclass
           AND tgname =
               'physical_control_target_apply_authorizations_guard'
           AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger
         WHERE tgrelid =
               'public.physical_control_target_apply_authorizations'::regclass
           AND tgname = 'physical_control_target_authorization_release'
           AND tgdeferrable
           AND tginitdeferred
    ) THEN
        RAISE EXCEPTION 'control v11 target authorization guard is missing';
    END IF;

    IF position(
        'migration.target_generation = apply_auth.target_generation'
        IN pg_get_functiondef(
            'public.guard_control_booking_receipt_write()'::regprocedure
        )
    ) = 0 OR position(
        'migration.train_run_id = apply_auth.train_run_id'
        IN pg_get_functiondef(
            'public.guard_control_booking_receipt_write()'::regprocedure
        )
    ) = 0 OR position(
        'assignment.active_physical_migration_id = migration.migration_id'
        IN pg_get_functiondef(
            'public.guard_control_booking_receipt_write()'::regprocedure
        )
    ) = 0 THEN
        RAISE EXCEPTION 'control v11 reverse authorization is not exact';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'public.financial_ledger_postings'::regclass
           AND conname = 'financial_ledger_postings_transaction_index_key'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'public.ticket_refund_request_items'::regclass
           AND conname = 'ticket_refund_request_items_request_ticket_key'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_indexes
         WHERE schemaname = 'public'
           AND tablename = 'ticket_refund_request_items'
           AND indexname = 'ticket_refund_request_items_active_ticket_idx'
           AND indexdef LIKE '%UNIQUE INDEX%'
           AND indexdef LIKE '%(ticket_id)%'
           AND indexdef LIKE '%WHERE (state <> ''failed''%'
    ) OR EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'public.ticket_refund_request_items'::regclass
           AND contype = 'u'
           AND conkey = ARRAY[
               (
                   SELECT attnum
                     FROM pg_attribute
                    WHERE attrelid = 'public.ticket_refund_request_items'::regclass
                      AND attname = 'ticket_id'
               )
           ]::smallint[]
    ) THEN
        RAISE EXCEPTION 'control v11 duplicate financial/refund identity guard is missing';
    END IF;

    IF to_regprocedure(
        'public.guard_control_ticket_refund_evidence_mutation()'
    ) IS NULL OR (
        SELECT count(*)
          FROM pg_trigger AS trigger_row
          JOIN pg_class AS table_row ON table_row.oid = trigger_row.tgrelid
          JOIN pg_namespace AS schema_row
            ON schema_row.oid = table_row.relnamespace
         WHERE schema_row.nspname IN (
                   'public', 'booking_shard_0', 'booking_shard_1'
               )
           AND table_row.relname IN (
                   'ticket_refund_prepare_receipts',
                   'ticket_refund_compensation_receipts',
                   'selected_ticket_refund_receipts'
               )
           AND trigger_row.tgname IN (
                   'ticket_refund_prepare_receipts_guard_evidence',
                   'ticket_refund_compensation_receipts_guard_evidence',
                   'selected_ticket_refund_receipts_guard_evidence'
               )
           AND NOT trigger_row.tgisinternal
    ) <> 9 THEN
        RAISE EXCEPTION 'control v11 refund evidence immutability guard is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema='public' AND table_name='ticket_refund_sagas'
           AND column_name='prepare_attempts'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema='public' AND table_name='ticket_refund_requests'
           AND column_name='eligibility_cutoff_at'
    ) THEN
        RAISE EXCEPTION 'control v11 independent prepare budget or cutoff binding is missing';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name IN (
               'payment_provider_capabilities',
               'provider_balance_transactions',
               'provider_settlement_batches',
               'provider_settlement_lines',
               'provider_payouts',
               'provider_payout_lines',
               'payment_webhook_key_versions',
               'payment_webhook_key_version_archive',
               'payment_webhook_key_rotation_audit',
               'backup_artifacts',
               'backup_operations',
               'backup_verifications',
               'restore_validations',
               'backup_expiration_operations'
           )
           AND lower(column_name) ~ '(secret|password|credential|raw_body|card|pan|cvv|cvc|api_key|dsn|encryption_key|backup_path)'
    ) THEN
        RAISE EXCEPTION 'control v11 exposes a forbidden sensitive column';
    END IF;

    IF (
        SELECT count(*)
          FROM public.booking_shards
         WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
           AND schema_version = 3
    ) <> 2 THEN
        RAISE EXCEPTION 'control v11 physical shard catalog is not schema v3';
    END IF;
END;
$$;
