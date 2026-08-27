-- golang-migrate records the target version as dirty before executing a down
-- migration. Evaluate every refusal predicate while the complete v11 schema is
-- still present. A refusal may restore 11/clean only from the exact 10/dirty
-- marker written by the runner. A permitted downgrade deliberately keeps
-- 10/dirty until golang-migrate postmarks it clean after this file succeeds.
BEGIN;

CREATE TEMP TABLE IF NOT EXISTS pg_temp.control_v11_down_preflight (
    refused boolean NOT NULL
) ON COMMIT PRESERVE ROWS;
TRUNCATE pg_temp.control_v11_down_preflight;

INSERT INTO pg_temp.control_v11_down_preflight (refused)
SELECT
    EXISTS (SELECT 1 FROM public.payment_saga_actions)
    OR EXISTS (SELECT 1 FROM public.payment_provider_capabilities)
    OR EXISTS (SELECT 1 FROM public.financial_ledger_transactions)
    OR EXISTS (SELECT 1 FROM public.financial_ledger_postings)
    OR EXISTS (SELECT 1 FROM public.financial_ledger_reversals)
    OR EXISTS (SELECT 1 FROM public.provider_balance_transactions)
    OR EXISTS (SELECT 1 FROM public.provider_settlement_batches)
    OR EXISTS (SELECT 1 FROM public.provider_settlement_lines)
    OR EXISTS (SELECT 1 FROM public.provider_payouts)
    OR EXISTS (SELECT 1 FROM public.provider_payout_lines)
    OR EXISTS (SELECT 1 FROM public.provider_settlement_import_checkpoints)
    OR EXISTS (SELECT 1 FROM public.provider_settlement_import_conflicts)
    OR EXISTS (SELECT 1 FROM public.settlement_reconciliation_runs)
    OR EXISTS (SELECT 1 FROM public.settlement_reconciliation_reviews)
    OR EXISTS (SELECT 1 FROM public.ticket_refund_requests)
    OR EXISTS (SELECT 1 FROM public.ticket_refund_request_items)
    OR EXISTS (SELECT 1 FROM public.ticket_refund_sagas)
    OR EXISTS (SELECT 1 FROM public.ticket_refund_operations)
    OR EXISTS (SELECT 1 FROM public.ticket_refund_prepare_bindings)
    OR EXISTS (SELECT 1 FROM public.ticket_refund_manual_reviews)
    OR EXISTS (SELECT 1 FROM public.ticket_refund_prepare_receipts)
    OR EXISTS (SELECT 1 FROM booking_shard_0.ticket_refund_prepare_receipts)
    OR EXISTS (SELECT 1 FROM booking_shard_1.ticket_refund_prepare_receipts)
    OR EXISTS (SELECT 1 FROM public.ticket_refund_compensation_receipts)
    OR EXISTS (SELECT 1 FROM booking_shard_0.ticket_refund_compensation_receipts)
    OR EXISTS (SELECT 1 FROM booking_shard_1.ticket_refund_compensation_receipts)
    OR EXISTS (SELECT 1 FROM public.selected_ticket_refund_receipts)
    OR EXISTS (SELECT 1 FROM booking_shard_0.selected_ticket_refund_receipts)
    OR EXISTS (SELECT 1 FROM booking_shard_1.selected_ticket_refund_receipts)
    OR EXISTS (SELECT 1 FROM public.payment_webhook_key_versions)
    OR EXISTS (SELECT 1 FROM public.payment_webhook_key_version_archive)
    OR EXISTS (SELECT 1 FROM public.payment_webhook_key_rotation_audit)
    OR EXISTS (SELECT 1 FROM public.regional_failover_operations)
    OR EXISTS (SELECT 1 FROM public.backup_artifacts)
    OR EXISTS (SELECT 1 FROM public.backup_operations)
    OR EXISTS (SELECT 1 FROM public.backup_verifications)
    OR EXISTS (SELECT 1 FROM public.restore_validations)
    OR EXISTS (SELECT 1 FROM public.backup_expiration_operations)
    OR EXISTS (
        SELECT 1 FROM public.reservations
         WHERE status IN ('partially_refund_pending', 'partially_refunded')
        UNION ALL
        SELECT 1 FROM booking_shard_0.reservations
         WHERE status IN ('partially_refund_pending', 'partially_refunded')
        UNION ALL
        SELECT 1 FROM booking_shard_1.reservations
         WHERE status IN ('partially_refund_pending', 'partially_refunded')
        UNION ALL
        SELECT 1 FROM public.ticket_orders
         WHERE status IN ('partial_refund_pending', 'partially_refunded')
            OR (refunded_amount_minor > 0
                AND refunded_amount_minor < captured_amount_minor)
        UNION ALL
        SELECT 1 FROM booking_shard_0.ticket_orders
         WHERE status IN ('partial_refund_pending', 'partially_refunded')
            OR (refunded_amount_minor > 0
                AND refunded_amount_minor < captured_amount_minor)
        UNION ALL
        SELECT 1 FROM booking_shard_1.ticket_orders
         WHERE status IN ('partial_refund_pending', 'partially_refunded')
            OR (refunded_amount_minor > 0
                AND refunded_amount_minor < captured_amount_minor)
        UNION ALL SELECT 1 FROM public.tickets WHERE status = 'refunded'
        UNION ALL SELECT 1 FROM booking_shard_0.tickets WHERE status = 'refunded'
        UNION ALL SELECT 1 FROM booking_shard_1.tickets WHERE status = 'refunded'
    )
    OR EXISTS (
        SELECT 1 FROM public.regional_write_authority
         WHERE singleton IS DISTINCT FROM true
            OR region <> 'region-a'
            OR epoch <> 1
            OR state <> 'active'
            OR writes_enabled IS DISTINCT FROM true
    );

COMMIT;

BEGIN;
SELECT version, dirty
  FROM public.schema_migrations
 FOR UPDATE;
UPDATE public.schema_migrations
   SET version = 11,
       dirty = false
 WHERE version = 10
   AND dirty
   AND (SELECT refused FROM pg_temp.control_v11_down_preflight);
COMMIT;

DO $control_v11_preflight_refusal$
BEGIN
    IF (SELECT refused FROM pg_temp.control_v11_down_preflight) THEN
        RAISE EXCEPTION 'cannot remove control v11 while Milestone 7 evidence or incompatible state is retained'
            USING ERRCODE = '55000';
    END IF;
END;
$control_v11_preflight_refusal$;

-- Direct execution has no runner premark. Never leave a successfully removed
-- v11 schema advertised as clean v11; manual recovery must adjudicate dirty v10.
BEGIN;
SELECT version, dirty
  FROM public.schema_migrations
 FOR UPDATE;
UPDATE public.schema_migrations
   SET version = 10,
       dirty = true
 WHERE version = 11
   AND NOT dirty;
COMMIT;

BEGIN;

-- Serialize the repeated refusal check with every relation whose rows make a
-- downgrade unsafe. Without these locks, a writer can commit evidence after
-- the guard SELECT but before DROP TABLE obtains its lock.
LOCK TABLE
    public.payment_saga_actions,
    public.payment_provider_capabilities,
    public.financial_ledger_transactions,
    public.financial_ledger_postings,
    public.financial_ledger_reversals,
    public.provider_balance_transactions,
    public.provider_settlement_batches,
    public.provider_settlement_lines,
    public.provider_payouts,
    public.provider_payout_lines,
    public.provider_settlement_import_checkpoints,
    public.provider_settlement_import_conflicts,
    public.settlement_reconciliation_runs,
    public.settlement_reconciliation_reviews,
    public.ticket_refund_requests,
    public.ticket_refund_request_items,
    public.ticket_refund_sagas,
    public.ticket_refund_operations,
    public.ticket_refund_prepare_bindings,
    public.ticket_refund_manual_reviews,
    public.ticket_refund_prepare_receipts,
    booking_shard_0.ticket_refund_prepare_receipts,
    booking_shard_1.ticket_refund_prepare_receipts,
    public.ticket_refund_compensation_receipts,
    booking_shard_0.ticket_refund_compensation_receipts,
    booking_shard_1.ticket_refund_compensation_receipts,
    public.selected_ticket_refund_receipts,
    booking_shard_0.selected_ticket_refund_receipts,
    booking_shard_1.selected_ticket_refund_receipts,
    public.payment_webhook_key_rotation_audit,
    public.payment_webhook_key_version_archive,
    public.payment_webhook_key_versions,
    public.regional_failover_operations,
    public.backup_artifacts,
    public.backup_operations,
    public.backup_verifications,
    public.restore_validations,
    public.backup_expiration_operations,
    public.reservations,
    booking_shard_0.reservations,
    booking_shard_1.reservations,
    public.ticket_orders,
    booking_shard_0.ticket_orders,
    booking_shard_1.ticket_orders,
    public.tickets,
    booking_shard_0.tickets,
    booking_shard_1.tickets,
    public.regional_write_authority
IN ACCESS EXCLUSIVE MODE;

DO $payment_saga_action_downgrade_guard$
BEGIN
    IF EXISTS (SELECT 1 FROM public.payment_saga_actions) THEN
        RAISE EXCEPTION 'cannot remove durable payment saga action evidence'
            USING ERRCODE = '23514';
    END IF;
END;
$payment_saga_action_downgrade_guard$;

DO $payment_ops_dr_downgrade_guard$
BEGIN
    IF EXISTS (SELECT 1 FROM public.payment_provider_capabilities)
       OR EXISTS (SELECT 1 FROM public.financial_ledger_transactions)
       OR EXISTS (SELECT 1 FROM public.financial_ledger_postings)
       OR EXISTS (SELECT 1 FROM public.financial_ledger_reversals)
       OR EXISTS (SELECT 1 FROM public.provider_balance_transactions)
       OR EXISTS (SELECT 1 FROM public.provider_settlement_batches)
       OR EXISTS (SELECT 1 FROM public.provider_settlement_lines)
       OR EXISTS (SELECT 1 FROM public.provider_payouts)
       OR EXISTS (SELECT 1 FROM public.provider_payout_lines)
       OR EXISTS (SELECT 1 FROM public.provider_settlement_import_checkpoints)
       OR EXISTS (SELECT 1 FROM public.provider_settlement_import_conflicts)
       OR EXISTS (SELECT 1 FROM public.settlement_reconciliation_runs)
       OR EXISTS (SELECT 1 FROM public.settlement_reconciliation_reviews)
       OR EXISTS (SELECT 1 FROM public.ticket_refund_requests)
       OR EXISTS (SELECT 1 FROM public.ticket_refund_request_items)
       OR EXISTS (SELECT 1 FROM public.ticket_refund_sagas)
       OR EXISTS (SELECT 1 FROM public.ticket_refund_operations)
       OR EXISTS (SELECT 1 FROM public.ticket_refund_prepare_bindings)
       OR EXISTS (SELECT 1 FROM public.ticket_refund_manual_reviews)
       OR EXISTS (SELECT 1 FROM public.ticket_refund_prepare_receipts)
       OR EXISTS (SELECT 1 FROM booking_shard_0.ticket_refund_prepare_receipts)
       OR EXISTS (SELECT 1 FROM booking_shard_1.ticket_refund_prepare_receipts)
       OR EXISTS (SELECT 1 FROM public.ticket_refund_compensation_receipts)
       OR EXISTS (SELECT 1 FROM booking_shard_0.ticket_refund_compensation_receipts)
       OR EXISTS (SELECT 1 FROM booking_shard_1.ticket_refund_compensation_receipts)
       OR EXISTS (SELECT 1 FROM public.selected_ticket_refund_receipts)
       OR EXISTS (SELECT 1 FROM booking_shard_0.selected_ticket_refund_receipts)
       OR EXISTS (SELECT 1 FROM booking_shard_1.selected_ticket_refund_receipts)
       OR EXISTS (SELECT 1 FROM public.payment_webhook_key_rotation_audit)
       OR EXISTS (SELECT 1 FROM public.payment_webhook_key_version_archive)
       OR EXISTS (SELECT 1 FROM public.payment_webhook_key_versions)
       OR EXISTS (SELECT 1 FROM public.regional_failover_operations)
       OR EXISTS (SELECT 1 FROM public.backup_artifacts)
       OR EXISTS (SELECT 1 FROM public.backup_operations)
       OR EXISTS (SELECT 1 FROM public.backup_verifications)
       OR EXISTS (SELECT 1 FROM public.restore_validations)
       OR EXISTS (SELECT 1 FROM public.backup_expiration_operations) THEN
        RAISE EXCEPTION 'cannot remove control v11 while Milestone 7 evidence is retained'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.reservations
         WHERE status IN ('partially_refund_pending', 'partially_refunded')
        UNION ALL
        SELECT 1 FROM booking_shard_0.reservations
         WHERE status IN ('partially_refund_pending', 'partially_refunded')
        UNION ALL
        SELECT 1 FROM booking_shard_1.reservations
         WHERE status IN ('partially_refund_pending', 'partially_refunded')
        UNION ALL
        SELECT 1 FROM public.ticket_orders
         WHERE status IN ('partial_refund_pending', 'partially_refunded')
            OR (refunded_amount_minor > 0
                AND refunded_amount_minor < captured_amount_minor)
        UNION ALL
        SELECT 1 FROM booking_shard_0.ticket_orders
         WHERE status IN ('partial_refund_pending', 'partially_refunded')
            OR (refunded_amount_minor > 0
                AND refunded_amount_minor < captured_amount_minor)
        UNION ALL
        SELECT 1 FROM booking_shard_1.ticket_orders
         WHERE status IN ('partial_refund_pending', 'partially_refunded')
            OR (refunded_amount_minor > 0
                AND refunded_amount_minor < captured_amount_minor)
        UNION ALL SELECT 1 FROM public.tickets WHERE status = 'refunded'
        UNION ALL SELECT 1 FROM booking_shard_0.tickets WHERE status = 'refunded'
        UNION ALL SELECT 1 FROM booking_shard_1.tickets WHERE status = 'refunded'
    ) THEN
        RAISE EXCEPTION 'cannot remove control v11 while partial-refund booking state is retained'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.regional_write_authority
         WHERE singleton IS DISTINCT FROM true
            OR region <> 'region-a'
            OR epoch <> 1
            OR state <> 'active'
            OR writes_enabled IS DISTINCT FROM true
    ) THEN
        RAISE EXCEPTION 'cannot remove control v11 after regional authority changed'
            USING ERRCODE = '55000';
    END IF;
END;
$payment_ops_dr_downgrade_guard$;

DROP TRIGGER payment_saga_actions_set_updated_at ON public.payment_saga_actions;
DROP TRIGGER payment_saga_actions_guard ON public.payment_saga_actions;
DROP FUNCTION public.guard_payment_saga_action_row();
DROP TABLE public.payment_saga_actions;

-- The migration runner updates schema_migrations outside this SQL transaction,
-- so that relation was never guarded. Remove every v11 regional DML guard only
-- after all downgrade refusal checks have passed and before reverting catalog
-- data or compatibility tables.
DO $remove_regional_write_context_guards$
DECLARE
    guarded_relation record;
BEGIN
    FOR guarded_relation IN
        SELECT schema_row.nspname AS schema_name,
               table_row.relname AS table_name
          FROM pg_catalog.pg_class AS table_row
          JOIN pg_catalog.pg_namespace AS schema_row
            ON schema_row.oid = table_row.relnamespace
         WHERE schema_row.nspname IN (
                   'public', 'booking_shard_0', 'booking_shard_1'
               )
           AND table_row.relkind IN ('r', 'p')
         ORDER BY schema_row.nspname, table_row.relname
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS regional_write_context_guard ON %I.%I',
            guarded_relation.schema_name, guarded_relation.table_name
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS regional_truncate_guard ON %I.%I',
            guarded_relation.schema_name, guarded_relation.table_name
        );
    END LOOP;
END;
$remove_regional_write_context_guards$;

DROP FUNCTION public.guard_regional_authority_command();
DROP FUNCTION public.guard_regional_operational_write();
DROP FUNCTION public.guard_regional_application_write();
DROP FUNCTION public.reject_regional_truncate();

DROP TRIGGER physical_control_target_authorization_release
    ON public.physical_control_target_apply_authorizations;
DROP FUNCTION public.require_physical_target_authorization_release();
DROP TRIGGER physical_control_target_apply_authorizations_guard
    ON public.physical_control_target_apply_authorizations;
DROP FUNCTION public.guard_physical_control_target_apply_authorization();

DO $booking_shard_v2_catalog$
DECLARE
    changed integer;
BEGIN
    UPDATE public.booking_shards
       SET schema_version = 2,
           updated_at = clock_timestamp()
     WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
       AND storage_kind = 'postgres'
       AND schema_version = 3;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 2 THEN
        RAISE EXCEPTION 'expected exactly two physical shard catalog rows at schema 3'
            USING ERRCODE = '23514';
    END IF;
END;
$booking_shard_v2_catalog$;

DROP VIEW public.physical_source_selected_ticket_refund_receipt_rows;
DROP VIEW public.physical_source_ticket_refund_compensation_receipt_rows;
DROP VIEW public.physical_source_ticket_refund_prepare_receipt_rows;

DROP TABLE booking_shard_0.selected_ticket_refund_receipts;
DROP TABLE booking_shard_1.selected_ticket_refund_receipts;
DROP TABLE public.selected_ticket_refund_receipts;
DROP TABLE booking_shard_0.ticket_refund_compensation_receipts;
DROP TABLE booking_shard_1.ticket_refund_compensation_receipts;
DROP TABLE public.ticket_refund_compensation_receipts;
DROP TABLE booking_shard_0.ticket_refund_prepare_receipts;
DROP TABLE booking_shard_1.ticket_refund_prepare_receipts;
DROP TABLE public.ticket_refund_prepare_receipts;
DROP FUNCTION public.guard_control_ticket_refund_evidence_mutation();
DROP FUNCTION public.guard_ticket_refund_prepare_receipt_transition();

DROP TRIGGER tickets_guard_refund_transition ON public.tickets;
DROP TRIGGER tickets_guard_refund_transition ON booking_shard_0.tickets;
DROP TRIGGER tickets_guard_refund_transition ON booking_shard_1.tickets;
DROP FUNCTION public.guard_control_ticket_refund_transition();

ALTER TABLE public.tickets
    DROP CONSTRAINT tickets_status_check,
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN ('pending', 'active', 'refund_pending', 'cancelled')
    );
ALTER TABLE booking_shard_0.tickets
    DROP CONSTRAINT tickets_status_check,
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN ('pending', 'active', 'refund_pending', 'cancelled')
    );
ALTER TABLE booking_shard_1.tickets
    DROP CONSTRAINT tickets_status_check,
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN ('pending', 'active', 'refund_pending', 'cancelled')
    );

ALTER TABLE public.ticket_orders
    DROP CONSTRAINT ticket_orders_status_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    ADD CONSTRAINT ticket_orders_status_check CHECK (
        status IN (
            'confirmed', 'payment_pending', 'payment_authorized',
            'payment_captured', 'issuance_pending', 'issued',
            'refund_pending', 'refunded', 'cancelled', 'manual_review'
        )
    ),
    ADD CONSTRAINT ticket_orders_payment_amounts_check CHECK (
        authorized_amount_minor IN (0, total_amount_minor)
        AND captured_amount_minor IN (0, authorized_amount_minor)
        AND refunded_amount_minor IN (0, captured_amount_minor)
        AND refunded_amount_minor >= 0
        AND refunded_amount_minor <= captured_amount_minor
        AND captured_amount_minor <= authorized_amount_minor
        AND authorized_amount_minor <= total_amount_minor
    ),
    ADD CONSTRAINT ticket_orders_payment_state_check CHECK (
        (status <> 'payment_authorized'
         OR authorized_amount_minor = total_amount_minor)
        AND (status NOT IN (
            'payment_captured', 'issuance_pending', 'issued',
            'refund_pending', 'refunded'
        ) OR captured_amount_minor = total_amount_minor)
        AND (status <> 'refunded'
             OR refunded_amount_minor = captured_amount_minor)
        AND (status <> 'cancelled' OR captured_amount_minor = 0
             OR refunded_amount_minor = captured_amount_minor)
    );

ALTER TABLE booking_shard_0.ticket_orders
    DROP CONSTRAINT ticket_orders_status_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    ADD CONSTRAINT ticket_orders_status_check CHECK (
        status IN (
            'confirmed', 'payment_pending', 'payment_authorized',
            'payment_captured', 'issuance_pending', 'issued',
            'refund_pending', 'refunded', 'cancelled', 'manual_review'
        )
    ),
    ADD CONSTRAINT ticket_orders_payment_amounts_check CHECK (
        authorized_amount_minor IN (0, total_amount_minor)
        AND captured_amount_minor IN (0, authorized_amount_minor)
        AND refunded_amount_minor IN (0, captured_amount_minor)
        AND refunded_amount_minor >= 0
        AND refunded_amount_minor <= captured_amount_minor
        AND captured_amount_minor <= authorized_amount_minor
        AND authorized_amount_minor <= total_amount_minor
    ),
    ADD CONSTRAINT ticket_orders_payment_state_check CHECK (
        (status <> 'payment_authorized'
         OR authorized_amount_minor = total_amount_minor)
        AND (status NOT IN (
            'payment_captured', 'issuance_pending', 'issued',
            'refund_pending', 'refunded'
        ) OR captured_amount_minor = total_amount_minor)
        AND (status <> 'refunded'
             OR refunded_amount_minor = captured_amount_minor)
        AND (status <> 'cancelled' OR captured_amount_minor = 0
             OR refunded_amount_minor = captured_amount_minor)
    );

ALTER TABLE booking_shard_1.ticket_orders
    DROP CONSTRAINT ticket_orders_status_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    ADD CONSTRAINT ticket_orders_status_check CHECK (
        status IN (
            'confirmed', 'payment_pending', 'payment_authorized',
            'payment_captured', 'issuance_pending', 'issued',
            'refund_pending', 'refunded', 'cancelled', 'manual_review'
        )
    ),
    ADD CONSTRAINT ticket_orders_payment_amounts_check CHECK (
        authorized_amount_minor IN (0, total_amount_minor)
        AND captured_amount_minor IN (0, authorized_amount_minor)
        AND refunded_amount_minor IN (0, captured_amount_minor)
        AND refunded_amount_minor >= 0
        AND refunded_amount_minor <= captured_amount_minor
        AND captured_amount_minor <= authorized_amount_minor
        AND authorized_amount_minor <= total_amount_minor
    ),
    ADD CONSTRAINT ticket_orders_payment_state_check CHECK (
        (status <> 'payment_authorized'
         OR authorized_amount_minor = total_amount_minor)
        AND (status NOT IN (
            'payment_captured', 'issuance_pending', 'issued',
            'refund_pending', 'refunded'
        ) OR captured_amount_minor = total_amount_minor)
        AND (status <> 'refunded'
             OR refunded_amount_minor = captured_amount_minor)
        AND (status <> 'cancelled' OR captured_amount_minor = 0
             OR refunded_amount_minor = captured_amount_minor)
    );

ALTER TABLE public.reservations
    DROP CONSTRAINT reservations_status_check,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    ADD CONSTRAINT reservations_status_check CHECK (
        status IN (
            'held', 'payment_pending', 'payment_review', 'confirmed',
            'refund_pending', 'expired', 'cancelled'
        )
    ),
    ADD CONSTRAINT reservations_payment_snapshot_check CHECK (
        (payment_intent_id IS NULL
         AND payment_amount_minor IS NULL
         AND payment_currency IS NULL
         AND payment_grace_expires_at IS NULL
         AND status NOT IN ('payment_pending', 'payment_review', 'refund_pending'))
        OR
        (payment_intent_id IS NOT NULL
         AND status IN (
             'payment_pending', 'payment_review', 'confirmed',
             'refund_pending', 'cancelled'
         )
         AND payment_amount_minor = total_amount_minor
         AND payment_amount_minor >= 0
         AND payment_currency = currency
         AND payment_currency ~ '^[A-Z]{3}$'
         AND payment_grace_expires_at > created_at)
    );

ALTER TABLE booking_shard_0.reservations
    DROP CONSTRAINT reservations_status_check,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    ADD CONSTRAINT reservations_status_check CHECK (
        status IN (
            'held', 'payment_pending', 'payment_review', 'confirmed',
            'refund_pending', 'expired', 'cancelled'
        )
    ),
    ADD CONSTRAINT reservations_payment_snapshot_check CHECK (
        (payment_intent_id IS NULL
         AND payment_amount_minor IS NULL
         AND payment_currency IS NULL
         AND payment_grace_expires_at IS NULL
         AND status NOT IN ('payment_pending', 'payment_review', 'refund_pending'))
        OR
        (payment_intent_id IS NOT NULL
         AND status IN (
             'payment_pending', 'payment_review', 'confirmed',
             'refund_pending', 'cancelled'
         )
         AND payment_amount_minor = total_amount_minor
         AND payment_amount_minor >= 0
         AND payment_currency = currency
         AND payment_currency ~ '^[A-Z]{3}$'
         AND payment_grace_expires_at > created_at)
    );

ALTER TABLE booking_shard_1.reservations
    DROP CONSTRAINT reservations_status_check,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    ADD CONSTRAINT reservations_status_check CHECK (
        status IN (
            'held', 'payment_pending', 'payment_review', 'confirmed',
            'refund_pending', 'expired', 'cancelled'
        )
    ),
    ADD CONSTRAINT reservations_payment_snapshot_check CHECK (
        (payment_intent_id IS NULL
         AND payment_amount_minor IS NULL
         AND payment_currency IS NULL
         AND payment_grace_expires_at IS NULL
         AND status NOT IN ('payment_pending', 'payment_review', 'refund_pending'))
        OR
        (payment_intent_id IS NOT NULL
         AND status IN (
             'payment_pending', 'payment_review', 'confirmed',
             'refund_pending', 'cancelled'
         )
         AND payment_amount_minor = total_amount_minor
         AND payment_amount_minor >= 0
         AND payment_currency = currency
         AND payment_currency ~ '^[A-Z]{3}$'
         AND payment_grace_expires_at > created_at)
    );

ALTER TABLE public.physical_source_train_run_mutation_journal
    DROP CONSTRAINT physical_source_train_run_mutation_journal_table_name_check,
    ADD CONSTRAINT physical_source_train_run_mutation_journal_table_name_check
    CHECK (table_name IN (
        'train_run_booking_snapshots', 'booking_seat_catalog',
        'booking_fare_snapshots', 'seat_inventory', 'reservations',
        'reservation_seats', 'ticket_orders', 'tickets',
        'idempotency_records', 'booking_command_receipts',
        'payment_command_receipts', 'ticket_issuance_receipts',
        'payment_refund_receipts', 'payment_compensation_receipts',
        'outbox_events'
    ));

CREATE OR REPLACE FUNCTION public.append_physical_source_mutation(
    selected_train_run_id uuid,
    selected_source_shard_id text,
    target_table_name text,
    mutation_operation text,
    target_entity_id uuid,
    bounded_primary_key jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $m6_append_physical_source_mutation$
DECLARE
    capture_migration_id uuid;
    capture_generation bigint;
    allocated_sequence bigint;
BEGIN
    IF selected_train_run_id IS NULL
       OR selected_source_shard_id NOT IN ('legacy', 'shard-0', 'shard-1')
       OR target_table_name NOT IN (
           'train_run_booking_snapshots', 'booking_seat_catalog',
           'booking_fare_snapshots', 'seat_inventory', 'reservations',
           'reservation_seats', 'ticket_orders', 'tickets',
           'idempotency_records', 'booking_command_receipts',
           'payment_command_receipts', 'ticket_issuance_receipts',
           'payment_refund_receipts', 'payment_compensation_receipts',
           'outbox_events'
       )
       OR mutation_operation NOT IN ('INSERT', 'UPDATE', 'DELETE')
       OR target_entity_id IS NULL
       OR jsonb_typeof(bounded_primary_key) <> 'object'
       OR octet_length(bounded_primary_key::text) > 512 THEN
        RAISE EXCEPTION 'invalid physical source mutation capture input'
            USING ERRCODE = '22023';
    END IF;

    UPDATE public.physical_source_migration_capture_state AS capture
    SET next_sequence = capture.next_sequence + 1
    FROM public.train_run_shard_assignments AS assignment
    WHERE capture.train_run_id = selected_train_run_id
      AND capture.source_shard_id = selected_source_shard_id
      AND capture.capture_enabled
      AND assignment.train_run_id = capture.train_run_id
      AND assignment.shard_id = capture.source_shard_id
      AND assignment.assignment_generation = capture.source_generation
      AND assignment.assignment_state IN ('stable', 'draining', 'migrating')
    RETURNING capture.migration_id, capture.source_generation,
              capture.next_sequence
    INTO capture_migration_id, capture_generation, allocated_sequence;

    IF capture_migration_id IS NULL THEN
        RETURN;
    END IF;

    INSERT INTO public.physical_source_train_run_mutation_journal (
        migration_id, train_run_id, source_shard_id, source_generation,
        mutation_sequence, table_name, operation, entity_id,
        primary_key, metadata
    ) VALUES (
        capture_migration_id, selected_train_run_id,
        selected_source_shard_id, capture_generation, allocated_sequence,
        target_table_name, mutation_operation, target_entity_id,
        bounded_primary_key,
        jsonb_build_object('source_shard_id', selected_source_shard_id)
    );
END;
$m6_append_physical_source_mutation$;

CREATE OR REPLACE FUNCTION public.capture_physical_source_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $capture_physical_source_receipt_mutation$
DECLARE
    source_shard_id text;
    affected_train_run_id uuid;
    affected_id uuid;
BEGIN
    source_shard_id := CASE TG_TABLE_SCHEMA
        WHEN 'public' THEN 'legacy'
        WHEN 'booking_shard_0' THEN 'shard-0'
        WHEN 'booking_shard_1' THEN 'shard-1'
        ELSE NULL
    END;
    IF source_shard_id IS NULL OR TG_TABLE_NAME NOT IN (
        'booking_command_receipts', 'payment_command_receipts',
        'ticket_issuance_receipts', 'payment_refund_receipts',
        'payment_compensation_receipts'
    ) THEN
        RAISE EXCEPTION 'unapproved physical source receipt relation'
            USING ERRCODE = '22023';
    END IF;
    affected_train_run_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.train_run_id ELSE NEW.train_run_id
    END;
    affected_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END;
    PERFORM public.append_physical_source_mutation(
        affected_train_run_id, source_shard_id, TG_TABLE_NAME, TG_OP,
        affected_id, jsonb_build_object('source_id', affected_id)
    );
    RETURN COALESCE(NEW, OLD);
END;
$capture_physical_source_receipt_mutation$;

CREATE OR REPLACE FUNCTION public.guard_control_booking_receipt_write()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $guard_control_booking_receipt_write$
DECLARE
    selected_train_run_id uuid;
    selected_shard_id text;
    assignment_generation bigint;
    assignment_state text;
    catalog_enabled boolean;
    catalog_write_enabled boolean;
    fence_generation bigint;
    fence_write_enabled boolean;
BEGIN
    selected_train_run_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.train_run_id ELSE NEW.train_run_id
    END;
    selected_shard_id := CASE TG_TABLE_SCHEMA
        WHEN 'public' THEN 'legacy'
        WHEN 'booking_shard_0' THEN 'shard-0'
        WHEN 'booking_shard_1' THEN 'shard-1'
        ELSE NULL
    END;
    IF selected_shard_id IS NULL THEN
        RAISE EXCEPTION 'unapproved booking receipt schema'
            USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.physical_control_target_apply_authorizations AS apply_auth
          JOIN public.physical_shard_migrations AS migration
            ON migration.migration_id = apply_auth.migration_id
         WHERE apply_auth.transaction_id = txid_current()
           AND apply_auth.train_run_id = selected_train_run_id
           AND apply_auth.target_shard_id = selected_shard_id
           AND migration.reverse_migration
           AND migration.source_shard_id IN (
               'physical-shard-0', 'physical-shard-1'
           )
           AND migration.target_shard_id = selected_shard_id
           AND migration.state IN (
               'preparing_target', 'capture_enabled', 'base_copying',
               'catching_up', 'validating_online', 'draining',
               'source_fenced', 'final_catchup', 'final_validating'
           )
    ) THEN
        RETURN COALESCE(NEW, OLD);
    END IF;
    SELECT assignment.assignment_generation, assignment.assignment_state,
           shard.enabled, shard.write_enabled
      INTO assignment_generation, assignment_state,
           catalog_enabled, catalog_write_enabled
      FROM public.train_run_shard_assignments AS assignment
      JOIN public.booking_shards AS shard
        ON shard.shard_id = assignment.shard_id
     WHERE assignment.train_run_id = selected_train_run_id
       AND assignment.shard_id = selected_shard_id
     FOR UPDATE OF assignment;
    IF selected_shard_id = 'legacy' THEN
        SELECT fence.assignment_generation, fence.write_enabled
          INTO fence_generation, fence_write_enabled
          FROM public.train_run_write_fences AS fence
         WHERE fence.train_run_id = selected_train_run_id FOR UPDATE;
    ELSIF selected_shard_id = 'shard-0' THEN
        SELECT fence.assignment_generation, fence.write_enabled
          INTO fence_generation, fence_write_enabled
          FROM booking_shard_0.train_run_write_fences AS fence
         WHERE fence.train_run_id = selected_train_run_id FOR UPDATE;
    ELSE
        SELECT fence.assignment_generation, fence.write_enabled
          INTO fence_generation, fence_write_enabled
          FROM booking_shard_1.train_run_write_fences AS fence
         WHERE fence.train_run_id = selected_train_run_id FOR UPDATE;
    END IF;
    IF assignment_generation IS NULL OR assignment_state <> 'stable'
       OR NOT catalog_enabled OR NOT catalog_write_enabled
       OR fence_generation IS DISTINCT FROM assignment_generation
       OR fence_write_enabled IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'booking receipt write is fenced'
            USING ERRCODE = '55000';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$guard_control_booking_receipt_write$;

DROP TRIGGER backup_verifications_guard_immutable
    ON public.backup_verifications;
DROP TRIGGER backup_expiration_operations_guard_transition
    ON public.backup_expiration_operations;
DROP FUNCTION public.guard_backup_expiration_transition();
DROP TRIGGER restore_validations_guard_transition
    ON public.restore_validations;
DROP FUNCTION public.guard_restore_validation_transition();
DROP TRIGGER backup_operations_guard_transition
    ON public.backup_operations;
DROP FUNCTION public.guard_backup_operation_transition();
DROP TRIGGER backup_artifacts_guard_transition
    ON public.backup_artifacts;
DROP FUNCTION public.guard_backup_artifact_transition();
DROP TABLE public.backup_expiration_operations;
DROP TABLE public.restore_validations;
DROP TABLE public.backup_verifications;
DROP TABLE public.backup_operations;
DROP TABLE public.backup_artifacts;

DROP TRIGGER regional_failover_operations_guard
    ON public.regional_failover_operations;
DROP FUNCTION public.guard_regional_failover_operation();
DROP TABLE public.regional_failover_operations;

DROP TRIGGER regional_write_authority_guard_transition
    ON public.regional_write_authority;
DROP FUNCTION public.lock_regional_write_authority();
DROP FUNCTION public.guard_regional_write_authority();
DROP TABLE public.regional_write_authority;

DROP TABLE public.payment_webhook_key_rotation_audit;
DROP TRIGGER payment_webhook_key_versions_guard
    ON public.payment_webhook_key_versions;
DROP FUNCTION public.guard_payment_webhook_key_version();
DROP TABLE public.payment_webhook_key_version_archive;
DROP TABLE public.payment_webhook_key_versions;

DROP TRIGGER payment_webhook_inbox_provider_binding_guard
    ON public.payment_webhook_inbox;
DROP FUNCTION public.guard_payment_webhook_provider_binding();
ALTER TABLE public.payment_webhook_inbox
    DROP COLUMN provider_environment,
    DROP COLUMN provider_account_id;

DROP TRIGGER IF EXISTS payment_operations_guard_refund_lane
    ON public.payment_operations;
DROP TRIGGER IF EXISTS ticket_refund_requests_guard_refund_lane
    ON public.ticket_refund_requests;
DROP FUNCTION IF EXISTS public.guard_refund_lane_exclusive();
DROP FUNCTION public.guard_ticket_refund_request_state() CASCADE;
DROP FUNCTION public.guard_ticket_refund_operation_state() CASCADE;
DROP FUNCTION public.guard_ticket_refund_identity() CASCADE;
DROP TABLE public.ticket_refund_manual_reviews;
DROP TABLE public.ticket_refund_prepare_bindings;
DROP FUNCTION public.guard_ticket_refund_prepare_binding_immutable();
DROP TABLE public.ticket_refund_operations;
DROP TABLE public.ticket_refund_sagas;
DROP TABLE public.ticket_refund_request_items;
DROP TABLE public.ticket_refund_requests;

DROP TABLE public.settlement_reconciliation_reviews;
DROP TABLE public.settlement_reconciliation_mismatches;
DROP TABLE public.settlement_reconciliation_runs;
DROP TABLE public.provider_settlement_import_conflicts;
DROP TABLE public.provider_settlement_import_checkpoints;
DROP TABLE public.provider_payout_lines;
DROP TABLE public.provider_payouts;
DROP TABLE public.provider_settlement_lines;
DROP TABLE public.provider_settlement_batches;
DROP TABLE public.provider_balance_transactions;

DROP TRIGGER financial_ledger_postings_check_balance
    ON public.financial_ledger_postings;
DROP TRIGGER financial_ledger_transactions_check_balance
    ON public.financial_ledger_transactions;
DROP FUNCTION public.check_financial_ledger_balance();
DROP TRIGGER financial_ledger_accounts_guard_immutable
    ON public.financial_ledger_accounts;
DROP TRIGGER financial_ledger_reversals_guard_immutable
    ON public.financial_ledger_reversals;
DROP TRIGGER financial_ledger_postings_guard_immutable
    ON public.financial_ledger_postings;
DROP TRIGGER financial_ledger_transactions_guard_immutable
    ON public.financial_ledger_transactions;
DROP FUNCTION public.guard_financial_ledger_immutable();
DROP TABLE public.financial_ledger_reversals;
DROP TABLE public.financial_ledger_postings;
DROP TABLE public.financial_ledger_transactions;
DROP TABLE public.financial_ledger_accounts;

DROP FUNCTION public.guard_m7_evidence_immutable() CASCADE;

DROP TABLE public.payment_provider_capabilities;

COMMIT;
