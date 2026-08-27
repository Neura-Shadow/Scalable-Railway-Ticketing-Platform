DO $$
DECLARE
    relation_name text;
    constraint_text text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'regional_write_authority',
        'ticket_refund_prepare_receipts',
        'ticket_refund_compensation_receipts',
        'selected_ticket_refund_receipts',
        'migration_evidence_mutation_authorizations',
        'dr_reconciliation_checkpoints'
    ] LOOP
        IF to_regclass('public.' || relation_name) IS NULL THEN
            RAISE EXCEPTION 'booking-shard v3 relation % is missing', relation_name;
        END IF;
    END LOOP;

    IF NOT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'train_run_booking_snapshots'
           AND column_name = 'scheduled_departure_at'
           AND is_nullable = 'NO'
           AND column_default = '''-infinity''::timestamp with time zone'
    ) THEN
        RAISE EXCEPTION 'scheduled departure rematerialization marker is not fail-closed';
    END IF;

    SELECT pg_get_constraintdef(oid)
      INTO constraint_text
      FROM pg_constraint
     WHERE conrelid = 'public.reservations'::regclass
       AND conname = 'reservations_status_check';
    IF constraint_text IS NULL
       OR position('partially_refund_pending' IN constraint_text) = 0
       OR position('partially_refunded' IN constraint_text) = 0 THEN
        RAISE EXCEPTION 'reservation partial-refund states are missing';
    END IF;

    SELECT pg_get_constraintdef(oid)
      INTO constraint_text
      FROM pg_constraint
     WHERE conrelid = 'public.ticket_orders'::regclass
       AND conname = 'ticket_orders_status_check';
    IF constraint_text IS NULL
       OR position('partial_refund_pending' IN constraint_text) = 0
       OR position('partially_refunded' IN constraint_text) = 0 THEN
        RAISE EXCEPTION 'ticket-order partial-refund states are missing';
    END IF;

    SELECT pg_get_constraintdef(oid)
      INTO constraint_text
      FROM pg_constraint
     WHERE conrelid = 'public.tickets'::regclass
       AND conname = 'tickets_status_check';
    IF constraint_text IS NULL
       OR position('refund_pending' IN constraint_text) = 0
       OR position('refunded' IN constraint_text) = 0 THEN
        RAISE EXCEPTION 'ticket partial-refund states are missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM public.regional_write_authority
         WHERE singleton = true
           AND region = 'region-a'
           AND epoch = 1
           AND state = 'active'
           AND writes_enabled = true
    ) THEN
        RAISE EXCEPTION 'booking-shard v3 regional authority seed is invalid';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_trigger
         WHERE tgrelid = 'public.regional_write_authority'::regclass
           AND tgname = 'regional_write_authority_guard_transition'
           AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1
          FROM pg_trigger
         WHERE tgrelid = 'public.ticket_refund_prepare_receipts'::regclass
           AND tgname = 'ticket_refund_prepare_receipts_guard_transition'
           AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1
          FROM pg_trigger
         WHERE tgrelid = 'public.ticket_refund_compensation_receipts'::regclass
           AND tgname = 'ticket_refund_compensation_receipts_guard_immutable'
           AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1
          FROM pg_trigger
         WHERE tgrelid = 'public.selected_ticket_refund_receipts'::regclass
           AND tgname = 'selected_ticket_refund_receipts_guard_immutable'
           AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'booking-shard v3 immutable guards are missing';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class AS table_row
          JOIN pg_catalog.pg_namespace AS schema_row
            ON schema_row.oid = table_row.relnamespace
         WHERE schema_row.nspname = 'public'
           AND table_row.relkind IN ('r', 'p')
           AND table_row.relname <> 'schema_migrations'
           AND NOT EXISTS (
               SELECT 1
                 FROM pg_catalog.pg_trigger AS trigger_row
                WHERE trigger_row.tgrelid = table_row.oid
                  AND trigger_row.tgname = 'regional_write_context_guard'
                  AND NOT trigger_row.tgisinternal
           )
    ) OR to_regprocedure(
        'public.booking_shard_guard_regional_application_write()'
    ) IS NULL OR to_regprocedure(
        'public.booking_shard_guard_regional_operational_write()'
    ) IS NULL OR to_regprocedure(
        'public.booking_shard_guard_regional_authority_command()'
    ) IS NULL THEN
        RAISE EXCEPTION 'booking-shard v3 regional DML coverage is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger
         WHERE tgrelid =
               'public.migration_evidence_mutation_authorizations'::regclass
           AND tgname = 'migration_evidence_mutation_authorizations_guard'
           AND NOT tgisinternal
    ) OR NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger
         WHERE tgrelid =
               'public.migration_evidence_mutation_authorizations'::regclass
           AND tgname = 'migration_evidence_authorization_release'
           AND tgdeferrable
           AND tginitdeferred
    ) THEN
        RAISE EXCEPTION 'booking-shard v3 authorization guard is missing';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name IN (
               'ticket_refund_prepare_receipts',
               'ticket_refund_compensation_receipts',
               'selected_ticket_refund_receipts',
               'dr_reconciliation_checkpoints'
           )
           AND lower(column_name) ~ '(secret|token|password|credential|raw_body|card|pan|cvv|cvc|dsn)'
    ) THEN
        RAISE EXCEPTION 'booking-shard v3 exposes a forbidden sensitive column';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'public.selected_ticket_refund_receipts'::regclass
           AND conname = 'selected_ticket_refund_receipts_ticket_unique'
    ) THEN
        RAISE EXCEPTION 'selected ticket can be released more than once';
    END IF;
END;
$$;
