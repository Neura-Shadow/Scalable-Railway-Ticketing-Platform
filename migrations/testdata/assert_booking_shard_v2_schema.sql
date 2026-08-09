DO $assert$
DECLARE
    current_version bigint;
    current_dirty boolean;
BEGIN
    SELECT version, dirty
      INTO current_version, current_dirty
      FROM public.schema_migrations
     LIMIT 1;

    IF current_version <> 2 OR current_dirty THEN
        RAISE EXCEPTION 'booking shard migration must be clean at version 2';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN (
              'payment_command_receipts',
              'ticket_issuance_receipts',
              'payment_refund_receipts',
              'payment_compensation_receipts'
          )) <> 4 THEN
        RAISE EXCEPTION 'booking shard payment receipt tables are incomplete';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'reservations'
          AND column_name IN (
              'payment_intent_id',
              'payment_amount_minor',
              'payment_currency',
              'payment_grace_expires_at'
          )) <> 4 THEN
        RAISE EXCEPTION 'reservation payment snapshot is incomplete';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'ticket_orders'
          AND column_name IN (
              'payment_intent_id',
              'payment_currency',
              'authorized_amount_minor',
              'captured_amount_minor',
              'refunded_amount_minor'
          )) <> 5 THEN
        RAISE EXCEPTION 'ticket-order payment snapshot is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        JOIN pg_class AS t ON t.oid = c.conrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = 'reservations'
          AND c.conname = 'reservations_status_check'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_pending%'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_review%'
          AND pg_get_constraintdef(c.oid) LIKE '%refund_pending%'
    ) THEN
        RAISE EXCEPTION 'reservation payment states are incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        JOIN pg_class AS t ON t.oid = c.conrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = 'ticket_orders'
          AND c.conname = 'ticket_orders_status_check'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_pending%'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_authorized%'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_captured%'
          AND pg_get_constraintdef(c.oid) LIKE '%issuance_pending%'
          AND pg_get_constraintdef(c.oid) LIKE '%issued%'
          AND pg_get_constraintdef(c.oid) LIKE '%refund_pending%'
          AND pg_get_constraintdef(c.oid) LIKE '%refunded%'
          AND pg_get_constraintdef(c.oid) LIKE '%manual_review%'
    ) THEN
        RAISE EXCEPTION 'ticket-order payment states are incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        JOIN pg_class AS t ON t.oid = c.conrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = 'tickets'
          AND c.conname = 'tickets_status_check'
          AND pg_get_constraintdef(c.oid) LIKE '%pending%'
          AND pg_get_constraintdef(c.oid) LIKE '%active%'
          AND pg_get_constraintdef(c.oid) LIKE '%refund_pending%'
          AND pg_get_constraintdef(c.oid) LIKE '%cancelled%'
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        JOIN pg_class AS t ON t.oid = c.conrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = 'tickets'
          AND c.conname = 'tickets_opaque_code_check'
          AND pg_get_constraintdef(c.oid) LIKE '%length(ticket_code)%16%64%'
    ) THEN
        RAISE EXCEPTION 'ticket state or opaque-code constraint is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger AS trigger_row
        JOIN pg_class AS table_row ON table_row.oid=trigger_row.tgrelid
        WHERE NOT trigger_row.tgisinternal
          AND table_row.relname='tickets'
          AND trigger_row.tgname='tickets_guard_identity'
    ) THEN
        RAISE EXCEPTION 'ticket identity immutability trigger is missing';
    END IF;

    IF (SELECT count(DISTINCT event_object_table)
        FROM information_schema.triggers
        WHERE trigger_schema = 'public'
          AND trigger_name LIKE '%_capture_mutation') <> 14 THEN
        RAISE EXCEPTION 'version-2 mutation capture trigger coverage is incomplete';
    END IF;

    IF (SELECT count(*)
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname LIKE '%_migration_cursor_idx'
          AND indexdef LIKE '%(train_run_id, assignment_generation, id)%'
    ) <> 15 THEN
        RAISE EXCEPTION 'version-2 base-copy cursor coverage is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        JOIN pg_class AS t ON t.oid = c.conrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = 'train_run_mutation_journal'
          AND c.conname = 'train_run_mutation_journal_table_name_check'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_command_receipts%'
          AND pg_get_constraintdef(c.oid) LIKE '%ticket_issuance_receipts%'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_refund_receipts%'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_compensation_receipts%'
    ) THEN
        RAISE EXCEPTION 'payment receipt journal allowlist is incomplete';
    END IF;

    IF (SELECT count(*)
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname IN (
              'reservations_payment_intent_unique_idx',
              'ticket_orders_payment_intent_unique_idx',
              'payment_command_receipts_begin_unique_idx',
              'payment_command_receipts_capture_unique_idx'
          )
          AND indexdef LIKE 'CREATE UNIQUE INDEX%') <> 4 THEN
        RAISE EXCEPTION 'payment idempotency uniqueness is incomplete';
    END IF;

    IF (SELECT count(DISTINCT t.relname)
        FROM pg_constraint AS c
        JOIN pg_class AS t ON t.oid = c.conrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname IN (
              'payment_command_receipts',
              'ticket_issuance_receipts',
              'payment_refund_receipts'
          )
          AND c.contype = 'f'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_intent_id%'
          AND pg_get_constraintdef(c.oid) LIKE '%amount_minor%'
          AND pg_get_constraintdef(c.oid) LIKE '%currency%'
    ) <> 3 THEN
        RAISE EXCEPTION 'receipt payment-authority foreign keys are incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        JOIN pg_class AS t ON t.oid = c.conrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = 'payment_compensation_receipts'
          AND c.contype = 'f'
          AND pg_get_constraintdef(c.oid) LIKE '%refund_receipt_id%'
          AND pg_get_constraintdef(c.oid) LIKE '%payment_intent_id%'
          AND pg_get_constraintdef(c.oid) LIKE '%reservation_id%'
          AND pg_get_constraintdef(c.oid) LIKE '%ticket_order_id%'
    ) THEN
        RAISE EXCEPTION 'compensation receipt is not bound to refund authority';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN (
              'reservations', 'ticket_orders', 'tickets',
              'payment_command_receipts', 'ticket_issuance_receipts',
              'payment_refund_receipts', 'payment_compensation_receipts'
          )
          AND column_name ~* (
              '(^|_)(card|pan|cvv|cvc|pin|track|magstripe|password|secret|token)'
          )
    ) THEN
        RAISE EXCEPTION 'payment shard schema contains a prohibited sensitive field';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_schema = 'public'
          AND constraint_type = 'FOREIGN KEY'
          AND constraint_name ILIKE '%payment_intent%'
    ) THEN
        RAISE EXCEPTION 'booking shard assumes a cross-database payment-intent FK';
    END IF;
END;
$assert$;
