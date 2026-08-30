-- Preflight every refusal predicate while the full v3 schema remains intact.
-- Restore 3/clean only from the exact 2/dirty runner marker when refusal is
-- proven. A permitted downgrade remains 2/dirty until the runner postmarks it.
BEGIN;

CREATE TEMP TABLE IF NOT EXISTS pg_temp.booking_shard_v3_down_preflight (
    refused boolean NOT NULL
) ON COMMIT PRESERVE ROWS;
TRUNCATE pg_temp.booking_shard_v3_down_preflight;

INSERT INTO pg_temp.booking_shard_v3_down_preflight (refused)
SELECT
    EXISTS (SELECT 1 FROM public.ticket_refund_prepare_receipts)
    OR EXISTS (SELECT 1 FROM public.ticket_refund_compensation_receipts)
    OR EXISTS (SELECT 1 FROM public.selected_ticket_refund_receipts)
    OR EXISTS (SELECT 1 FROM public.migration_evidence_mutation_authorizations)
    OR EXISTS (SELECT 1 FROM public.dr_reconciliation_checkpoints)
    OR EXISTS (SELECT 1 FROM public.tickets WHERE status = 'refunded')
    OR EXISTS (
        SELECT 1 FROM public.ticket_orders
         WHERE status IN ('partial_refund_pending', 'partially_refunded')
            OR (refunded_amount_minor > 0
                AND refunded_amount_minor < captured_amount_minor)
    )
    OR EXISTS (
        SELECT 1 FROM public.reservations
         WHERE status IN ('partially_refund_pending', 'partially_refunded')
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
   SET version = 3,
       dirty = false
 WHERE version = 2
   AND dirty
   AND (SELECT refused FROM pg_temp.booking_shard_v3_down_preflight);
COMMIT;

DO $booking_shard_v3_preflight_refusal$
BEGIN
    IF (SELECT refused FROM pg_temp.booking_shard_v3_down_preflight) THEN
        RAISE EXCEPTION 'cannot remove booking-shard v3 while DR, ticket-refund evidence, or incompatible state is retained'
            USING ERRCODE = '55000';
    END IF;
END;
$booking_shard_v3_preflight_refusal$;

-- Direct execution has no runner premark. Preserve an honest dirty target
-- marker if the SQL commits before any migration runner can postmark it clean.
BEGIN;
SELECT version, dirty
  FROM public.schema_migrations
 FOR UPDATE;
UPDATE public.schema_migrations
   SET version = 2,
       dirty = true
 WHERE version = 3
   AND NOT dirty;
COMMIT;

BEGIN;

LOCK TABLE
    public.ticket_refund_prepare_receipts,
    public.ticket_refund_compensation_receipts,
    public.selected_ticket_refund_receipts,
    public.migration_evidence_mutation_authorizations,
    public.dr_reconciliation_checkpoints,
    public.tickets,
    public.ticket_orders,
    public.reservations,
    public.regional_write_authority
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.ticket_refund_prepare_receipts)
       OR EXISTS (SELECT 1 FROM public.ticket_refund_compensation_receipts)
       OR EXISTS (SELECT 1 FROM public.selected_ticket_refund_receipts)
       OR EXISTS (
           SELECT 1 FROM public.migration_evidence_mutation_authorizations
       )
       OR EXISTS (SELECT 1 FROM public.dr_reconciliation_checkpoints) THEN
        RAISE EXCEPTION 'cannot remove booking-shard v3 while DR or ticket-refund evidence is retained'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.tickets WHERE status = 'refunded'
    ) OR EXISTS (
        SELECT 1 FROM public.ticket_orders
         WHERE status IN ('partial_refund_pending', 'partially_refunded')
            OR (refunded_amount_minor > 0 AND refunded_amount_minor < captured_amount_minor)
    ) OR EXISTS (
        SELECT 1 FROM public.reservations
         WHERE status IN ('partially_refund_pending', 'partially_refunded')
    ) THEN
        RAISE EXCEPTION 'cannot remove booking-shard v3 while partial-refund state is retained'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.regional_write_authority
         WHERE singleton IS DISTINCT FROM true
            OR region <> 'region-a'
            OR epoch <> 1
            OR state <> 'active'
            OR writes_enabled IS DISTINCT FROM true
    ) THEN
        RAISE EXCEPTION 'cannot remove booking-shard v3 after regional authority changed'
            USING ERRCODE = '55000';
    END IF;
END;
$$;

DO $remove_booking_shard_regional_write_context_guards$
DECLARE
    guarded_relation record;
BEGIN
    FOR guarded_relation IN
        SELECT table_row.relname AS table_name
          FROM pg_catalog.pg_class AS table_row
          JOIN pg_catalog.pg_namespace AS schema_row
            ON schema_row.oid = table_row.relnamespace
         WHERE schema_row.nspname = 'public'
           AND table_row.relkind IN ('r', 'p')
         ORDER BY table_row.relname
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS regional_write_context_guard ON public.%I',
            guarded_relation.table_name
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS regional_truncate_guard ON public.%I',
            guarded_relation.table_name
        );
    END LOOP;
END;
$remove_booking_shard_regional_write_context_guards$;

DROP FUNCTION booking_shard_guard_regional_authority_command();
DROP FUNCTION booking_shard_guard_regional_operational_write();
DROP FUNCTION booking_shard_guard_regional_application_write();
DROP FUNCTION booking_shard_reject_regional_truncate();

DROP TRIGGER selected_ticket_refund_receipts_capture_mutation
    ON selected_ticket_refund_receipts;
DROP TRIGGER ticket_refund_prepare_receipts_capture_mutation
    ON ticket_refund_prepare_receipts;
DROP TRIGGER ticket_refund_compensation_receipts_capture_mutation
    ON ticket_refund_compensation_receipts;
DROP FUNCTION booking_shard_capture_ticket_refund_mutation();

ALTER TABLE train_run_mutation_journal
    DROP CONSTRAINT train_run_mutation_journal_table_name_check;

ALTER TABLE train_run_mutation_journal
    ADD CONSTRAINT train_run_mutation_journal_table_name_check CHECK (
        table_name IN (
            'train_run_booking_snapshots',
            'booking_seat_catalog',
            'booking_fare_snapshots',
            'seat_inventory',
            'reservations',
            'reservation_seats',
            'ticket_orders',
            'tickets',
            'idempotency_records',
            'booking_command_receipts',
            'payment_command_receipts',
            'ticket_issuance_receipts',
            'payment_refund_receipts',
            'payment_compensation_receipts'
        )
    );

DROP TRIGGER dr_reconciliation_checkpoints_guard_immutable
    ON dr_reconciliation_checkpoints;
DROP TRIGGER selected_ticket_refund_receipts_guard_immutable
    ON selected_ticket_refund_receipts;
DROP TRIGGER ticket_refund_compensation_receipts_guard_immutable
    ON ticket_refund_compensation_receipts;
DROP TRIGGER ticket_refund_prepare_receipts_guard_transition
    ON ticket_refund_prepare_receipts;

DROP TABLE dr_reconciliation_checkpoints;
DROP TABLE selected_ticket_refund_receipts;
DROP TABLE ticket_refund_compensation_receipts;
DROP TABLE ticket_refund_prepare_receipts;
DROP FUNCTION guard_ticket_refund_prepare_receipt_transition();
DROP FUNCTION booking_shard_reject_evidence_mutation();
DROP TRIGGER migration_evidence_authorization_release
    ON migration_evidence_mutation_authorizations;
DROP FUNCTION booking_shard_require_evidence_authorization_release();
DROP TRIGGER migration_evidence_mutation_authorizations_guard
    ON migration_evidence_mutation_authorizations;
DROP FUNCTION booking_shard_guard_evidence_mutation_authorization();
DROP TABLE migration_evidence_mutation_authorizations;

DROP TRIGGER regional_write_authority_guard_transition
    ON regional_write_authority;
DROP FUNCTION public.lock_regional_write_authority();
DROP FUNCTION booking_shard_guard_regional_write_authority();
DROP TABLE regional_write_authority;

DROP TRIGGER tickets_guard_refund_transition ON tickets;
DROP FUNCTION booking_shard_guard_ticket_refund_transition();

ALTER TABLE train_run_booking_snapshots
    DROP COLUMN scheduled_departure_at;

ALTER TABLE tickets
    DROP CONSTRAINT tickets_status_check;

ALTER TABLE tickets
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN ('pending', 'active', 'refund_pending', 'cancelled')
    );

DROP INDEX ticket_orders_payment_state_idx;

ALTER TABLE ticket_orders
    DROP CONSTRAINT ticket_orders_status_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_state_check;

ALTER TABLE ticket_orders
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
        AND 0 <= refunded_amount_minor
        AND refunded_amount_minor <= captured_amount_minor
        AND captured_amount_minor <= authorized_amount_minor
        AND authorized_amount_minor <= total_amount_minor
    ),
    ADD CONSTRAINT ticket_orders_payment_state_check CHECK (
        (
            status <> 'payment_authorized'
            OR authorized_amount_minor = total_amount_minor
        )
        AND (
            status NOT IN (
                'payment_captured', 'issuance_pending', 'issued',
                'refund_pending', 'refunded'
            )
            OR captured_amount_minor = total_amount_minor
        )
        AND (
            status <> 'refunded'
            OR refunded_amount_minor = captured_amount_minor
        )
        AND (
            status <> 'cancelled'
            OR captured_amount_minor = 0
            OR refunded_amount_minor = captured_amount_minor
        )
    );

CREATE INDEX ticket_orders_payment_state_idx
    ON ticket_orders (
        status, train_run_id, assignment_generation, updated_at, id
    )
    WHERE status IN (
        'payment_pending', 'payment_authorized', 'payment_captured',
        'issuance_pending', 'refund_pending', 'manual_review'
    );

DROP INDEX reservations_payment_work_idx;

ALTER TABLE reservations
    DROP CONSTRAINT reservations_status_check,
    DROP CONSTRAINT reservations_payment_snapshot_check;

ALTER TABLE reservations
    ADD CONSTRAINT reservations_status_check CHECK (
        status IN (
            'held', 'payment_pending', 'payment_review', 'confirmed',
            'refund_pending', 'expired', 'cancelled'
        )
    ),
    ADD CONSTRAINT reservations_payment_snapshot_check CHECK (
        (
            payment_intent_id IS NULL
            AND payment_amount_minor IS NULL
            AND payment_currency IS NULL
            AND payment_grace_expires_at IS NULL
            AND status NOT IN (
                'payment_pending', 'payment_review', 'refund_pending'
            )
        )
        OR
        (
            payment_intent_id IS NOT NULL
            AND status IN (
                'payment_pending', 'payment_review', 'confirmed',
                'refund_pending', 'cancelled'
            )
            AND payment_amount_minor IS NOT NULL
            AND payment_amount_minor = total_amount_minor
            AND payment_amount_minor >= 0
            AND payment_currency IS NOT NULL
            AND payment_currency = currency
            AND payment_currency ~ '^[A-Z]{3}$'
            AND payment_grace_expires_at IS NOT NULL
            AND payment_grace_expires_at > created_at
        )
    );

CREATE INDEX reservations_payment_work_idx
    ON reservations (
        status, payment_grace_expires_at, train_run_id,
        assignment_generation, id
    )
    WHERE status IN ('payment_pending', 'payment_review', 'refund_pending');

COMMIT;
