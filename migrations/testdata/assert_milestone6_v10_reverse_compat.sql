DO $assert_m6_reverse_compat$
DECLARE
    schema_name text;
    expected_column text;
BEGIN
    FOREACH schema_name IN ARRAY ARRAY[
        'public', 'booking_shard_0', 'booking_shard_1'
    ] LOOP
        FOREACH expected_column IN ARRAY ARRAY[
            'payment_intent_id', 'payment_amount_minor',
            'payment_currency', 'payment_grace_expires_at'
        ] LOOP
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns AS column_row
                WHERE table_schema=schema_name
                  AND table_name='reservations'
                  AND column_row.column_name=expected_column
            ) THEN
                RAISE EXCEPTION '%.reservations is missing %', schema_name, expected_column;
            END IF;
        END LOOP;

        FOREACH expected_column IN ARRAY ARRAY[
            'payment_intent_id', 'payment_currency',
            'authorized_amount_minor', 'captured_amount_minor',
            'refunded_amount_minor'
        ] LOOP
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns AS column_row
                WHERE table_schema=schema_name
                  AND table_name='ticket_orders'
                  AND column_row.column_name=expected_column
            ) THEN
                RAISE EXCEPTION '%.ticket_orders is missing %', schema_name, expected_column;
            END IF;
        END LOOP;
    END LOOP;

    IF (
        SELECT count(*) FROM information_schema.tables
        WHERE table_schema IN ('public', 'booking_shard_0', 'booking_shard_1')
          AND table_name IN (
              'booking_command_receipts', 'payment_command_receipts',
              'ticket_issuance_receipts', 'payment_refund_receipts',
              'payment_compensation_receipts'
          )
    ) <> 15 THEN
        RAISE EXCEPTION 'fixed control layouts do not expose all 15 receipt relations';
    END IF;

    IF (
        SELECT count(*) FROM information_schema.views
        WHERE table_schema='public'
          AND table_name IN (
              'physical_source_booking_command_receipt_rows',
              'physical_source_payment_command_receipt_rows',
              'physical_source_ticket_issuance_receipt_rows',
              'physical_source_payment_refund_receipt_rows',
              'physical_source_payment_compensation_receipt_rows'
          )
    ) <> 5 THEN
        RAISE EXCEPTION 'receipt source views are incomplete';
    END IF;

    IF (
        SELECT count(*)
        FROM pg_trigger AS trigger_row
        JOIN pg_class AS table_row ON table_row.oid=trigger_row.tgrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid=table_row.relnamespace
        WHERE NOT trigger_row.tgisinternal
          AND schema_row.nspname IN ('public', 'booking_shard_0', 'booking_shard_1')
          AND table_row.relname IN (
              'booking_command_receipts', 'payment_command_receipts',
              'ticket_issuance_receipts', 'payment_refund_receipts',
              'payment_compensation_receipts'
          )
          AND trigger_row.tgname IN ('physical_target_write_guard', 'physical_source_capture')
    ) <> 30 THEN
        RAISE EXCEPTION 'receipt write guards or mutation capture triggers are incomplete';
    END IF;

    IF (
        SELECT count(*)
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid=constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid=table_row.relnamespace
        WHERE schema_row.nspname IN ('public', 'booking_shard_0', 'booking_shard_1')
          AND table_row.relname='ticket_orders'
          AND constraint_row.conname='ticket_orders_payment_state_check'
          AND constraint_row.contype='c'
    ) <> 3 THEN
        RAISE EXCEPTION 'ticket-order payment state constraints are incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid=constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid=table_row.relnamespace
        WHERE schema_row.nspname='public'
          AND table_row.relname='physical_source_train_run_mutation_journal'
          AND constraint_row.conname='physical_source_train_run_mutation_journal_table_name_check'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%payment_compensation_receipts%'
    ) THEN
        RAISE EXCEPTION 'control-source mutation journal does not allow schema-v2 receipts';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema IN ('public', 'booking_shard_0', 'booking_shard_1')
          AND table_name IN (
              'booking_command_receipts', 'payment_command_receipts',
              'ticket_issuance_receipts', 'payment_refund_receipts',
              'payment_compensation_receipts'
          )
          AND column_name IN (
              'raw_provider_secret', 'provider_secret', 'webhook_secret',
              'raw_payload', 'request_body', 'response_body', 'card_number',
              'pan', 'cvv', 'cvc', 'pin', 'payment_credential'
          )
    ) THEN
        RAISE EXCEPTION 'reverse compatibility relations expose sensitive payment material';
    END IF;
END;
$assert_m6_reverse_compat$;
