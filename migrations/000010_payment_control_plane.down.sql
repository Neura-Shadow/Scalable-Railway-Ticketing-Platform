BEGIN;

SELECT pg_advisory_xact_lock(804230010);

-- Version 9 cannot represent any payment orchestration, provider operation,
-- signed webhook, reconciliation, or manual-review evidence. Refuse the
-- destructive downgrade when even a nonterminal row exists: an apparently
-- pending/uncertain row can still correspond to an external financial effect.
DO $m6_down_preflight$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.booking_shards
        WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
          AND (
              storage_kind <> 'postgres'
              OR schema_version <> 2
              OR enabled
              OR write_enabled
              OR state <> 'disabled'
          )
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while a physical shard is enabled or has an unexpected schema contract'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_intents) THEN
        RAISE EXCEPTION 'cannot downgrade while payment intent evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_sagas) THEN
        RAISE EXCEPTION 'cannot downgrade while payment saga evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_operations) THEN
        RAISE EXCEPTION 'cannot downgrade while provider operation evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_webhook_inbox) THEN
        RAISE EXCEPTION 'cannot downgrade while verified webhook evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_provider_event_conflicts) THEN
        RAISE EXCEPTION 'cannot downgrade while provider event conflict evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_reconciliation_checkpoints) THEN
        RAISE EXCEPTION 'cannot downgrade while payment reconciliation evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_manual_review_cases) THEN
        RAISE EXCEPTION 'cannot downgrade while payment manual-review evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.booking_command_receipts
        UNION ALL SELECT 1 FROM booking_shard_0.booking_command_receipts
        UNION ALL SELECT 1 FROM booking_shard_1.booking_command_receipts
        UNION ALL SELECT 1 FROM public.payment_command_receipts
        UNION ALL SELECT 1 FROM booking_shard_0.payment_command_receipts
        UNION ALL SELECT 1 FROM booking_shard_1.payment_command_receipts
        UNION ALL SELECT 1 FROM public.ticket_issuance_receipts
        UNION ALL SELECT 1 FROM booking_shard_0.ticket_issuance_receipts
        UNION ALL SELECT 1 FROM booking_shard_1.ticket_issuance_receipts
        UNION ALL SELECT 1 FROM public.payment_refund_receipts
        UNION ALL SELECT 1 FROM booking_shard_0.payment_refund_receipts
        UNION ALL SELECT 1 FROM booking_shard_1.payment_refund_receipts
        UNION ALL SELECT 1 FROM public.payment_compensation_receipts
        UNION ALL SELECT 1 FROM booking_shard_0.payment_compensation_receipts
        UNION ALL SELECT 1 FROM booking_shard_1.payment_compensation_receipts
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while control-local booking receipt evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.reservations WHERE payment_intent_id IS NOT NULL
        UNION ALL SELECT 1 FROM booking_shard_0.reservations WHERE payment_intent_id IS NOT NULL
        UNION ALL SELECT 1 FROM booking_shard_1.reservations WHERE payment_intent_id IS NOT NULL
        UNION ALL SELECT 1 FROM public.ticket_orders WHERE payment_intent_id IS NOT NULL
        UNION ALL SELECT 1 FROM booking_shard_0.ticket_orders WHERE payment_intent_id IS NOT NULL
        UNION ALL SELECT 1 FROM booking_shard_1.ticket_orders WHERE payment_intent_id IS NOT NULL
        UNION ALL SELECT 1 FROM public.tickets WHERE status IN ('pending','refund_pending')
        UNION ALL SELECT 1 FROM booking_shard_0.tickets WHERE status IN ('pending','refund_pending')
        UNION ALL SELECT 1 FROM booking_shard_1.tickets WHERE status IN ('pending','refund_pending')
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while control-local payment booking state is retained'
            USING ERRCODE = '55000';
    END IF;
END
$m6_down_preflight$;

DROP TABLE public.payment_manual_review_cases;
DROP TABLE public.payment_reconciliation_checkpoints;
DROP TABLE public.payment_provider_event_conflicts;
DROP TABLE public.payment_webhook_inbox;
DROP TABLE public.payment_operations;
DROP TABLE public.payment_sagas;
DROP TABLE public.payment_intents;

DROP FUNCTION public.guard_payment_manual_review_case_row();
DROP FUNCTION public.guard_payment_reconciliation_checkpoint_row();
DROP FUNCTION public.guard_payment_provider_event_conflict_row();
DROP FUNCTION public.guard_payment_webhook_inbox_row();
DROP FUNCTION public.guard_payment_financial_settlement();
DROP FUNCTION public.guard_payment_operation_row();
DROP FUNCTION public.guard_payment_saga_row();
DROP FUNCTION public.guard_payment_intent_row();

DROP VIEW public.physical_source_payment_compensation_receipt_rows;
DROP VIEW public.physical_source_payment_refund_receipt_rows;
DROP VIEW public.physical_source_ticket_issuance_receipt_rows;
DROP VIEW public.physical_source_payment_command_receipt_rows;
DROP VIEW public.physical_source_booking_command_receipt_rows;
DROP VIEW public.physical_source_ticket_rows;
DROP VIEW public.physical_source_ticket_order_rows;
DROP VIEW public.physical_source_reservation_rows;

DROP TABLE booking_shard_0.payment_compensation_receipts;
DROP TABLE booking_shard_1.payment_compensation_receipts;
DROP TABLE public.payment_compensation_receipts;
DROP TABLE booking_shard_0.payment_refund_receipts;
DROP TABLE booking_shard_1.payment_refund_receipts;
DROP TABLE public.payment_refund_receipts;
DROP TABLE booking_shard_0.ticket_issuance_receipts;
DROP TABLE booking_shard_1.ticket_issuance_receipts;
DROP TABLE public.ticket_issuance_receipts;
DROP TABLE booking_shard_0.payment_command_receipts;
DROP TABLE booking_shard_1.payment_command_receipts;
DROP TABLE public.payment_command_receipts;
DROP TABLE booking_shard_0.booking_command_receipts;
DROP TABLE booking_shard_1.booking_command_receipts;
DROP TABLE public.booking_command_receipts;

DROP FUNCTION public.guard_control_booking_receipt_write();
DROP FUNCTION public.capture_physical_source_receipt_mutation();

DROP TRIGGER reservations_guard_payment_snapshot ON public.reservations;
DROP TRIGGER ticket_orders_guard_payment_snapshot ON public.ticket_orders;
DROP TRIGGER reservations_guard_payment_snapshot ON booking_shard_0.reservations;
DROP TRIGGER ticket_orders_guard_payment_snapshot ON booking_shard_0.ticket_orders;
DROP TRIGGER reservations_guard_payment_snapshot ON booking_shard_1.reservations;
DROP TRIGGER ticket_orders_guard_payment_snapshot ON booking_shard_1.ticket_orders;
DROP FUNCTION public.guard_control_booking_payment_snapshot();

DROP INDEX public.reservations_payment_intent_unique_idx;
DROP INDEX booking_shard_0.reservations_payment_intent_unique_idx;
DROP INDEX booking_shard_1.reservations_payment_intent_unique_idx;
DROP INDEX public.ticket_orders_payment_intent_unique_idx;
DROP INDEX booking_shard_0.ticket_orders_payment_intent_unique_idx;
DROP INDEX booking_shard_1.ticket_orders_payment_intent_unique_idx;

ALTER TABLE public.ticket_orders
    DROP CONSTRAINT ticket_orders_payment_authority_unique,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_snapshot_check,
    DROP CONSTRAINT ticket_orders_status_check,
    DROP COLUMN refunded_amount_minor,
    DROP COLUMN captured_amount_minor,
    DROP COLUMN authorized_amount_minor,
    DROP COLUMN payment_currency,
    DROP COLUMN payment_intent_id,
    ADD CONSTRAINT ticket_orders_status_check CHECK (status IN ('confirmed','cancelled'));
ALTER TABLE booking_shard_0.ticket_orders
    DROP CONSTRAINT ticket_orders_payment_authority_unique,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_snapshot_check,
    DROP CONSTRAINT ticket_orders_status_check,
    DROP COLUMN refunded_amount_minor,
    DROP COLUMN captured_amount_minor,
    DROP COLUMN authorized_amount_minor,
    DROP COLUMN payment_currency,
    DROP COLUMN payment_intent_id,
    ADD CONSTRAINT ticket_orders_status_check CHECK (status IN ('confirmed','cancelled'));
ALTER TABLE booking_shard_1.ticket_orders
    DROP CONSTRAINT ticket_orders_payment_authority_unique,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_snapshot_check,
    DROP CONSTRAINT ticket_orders_status_check,
    DROP COLUMN refunded_amount_minor,
    DROP COLUMN captured_amount_minor,
    DROP COLUMN authorized_amount_minor,
    DROP COLUMN payment_currency,
    DROP COLUMN payment_intent_id,
    ADD CONSTRAINT ticket_orders_status_check CHECK (status IN ('confirmed','cancelled'));

ALTER TABLE public.reservations
    DROP CONSTRAINT reservations_payment_authority_unique,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    DROP CONSTRAINT reservations_status_check,
    DROP COLUMN payment_grace_expires_at,
    DROP COLUMN payment_currency,
    DROP COLUMN payment_amount_minor,
    DROP COLUMN payment_intent_id,
    ADD CONSTRAINT reservations_status_check CHECK (status IN ('held','confirmed','expired','cancelled'));
ALTER TABLE booking_shard_0.reservations
    DROP CONSTRAINT reservations_payment_authority_unique,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    DROP CONSTRAINT reservations_status_check,
    DROP COLUMN payment_grace_expires_at,
    DROP COLUMN payment_currency,
    DROP COLUMN payment_amount_minor,
    DROP COLUMN payment_intent_id,
    ADD CONSTRAINT reservations_status_check CHECK (status IN ('held','confirmed','expired','cancelled'));
ALTER TABLE booking_shard_1.reservations
    DROP CONSTRAINT reservations_payment_authority_unique,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    DROP CONSTRAINT reservations_status_check,
    DROP COLUMN payment_grace_expires_at,
    DROP COLUMN payment_currency,
    DROP COLUMN payment_amount_minor,
    DROP COLUMN payment_intent_id,
    ADD CONSTRAINT reservations_status_check CHECK (status IN ('held','confirmed','expired','cancelled'));

ALTER TABLE public.tickets
    DROP CONSTRAINT tickets_opaque_code_check,
    DROP CONSTRAINT tickets_status_check,
    ADD CONSTRAINT tickets_status_check CHECK (status IN ('active','cancelled'));
ALTER TABLE booking_shard_0.tickets
    DROP CONSTRAINT tickets_opaque_code_check,
    DROP CONSTRAINT tickets_status_check,
    ADD CONSTRAINT tickets_status_check CHECK (status IN ('active','cancelled'));
ALTER TABLE booking_shard_1.tickets
    DROP CONSTRAINT tickets_opaque_code_check,
    DROP CONSTRAINT tickets_status_check,
    ADD CONSTRAINT tickets_status_check CHECK (status IN ('active','cancelled'));

CREATE VIEW public.physical_source_reservation_rows AS
SELECT 'legacy'::text AS source_shard_id, reservation.* FROM public.reservations AS reservation
UNION ALL SELECT 'shard-0'::text, reservation.* FROM booking_shard_0.reservations AS reservation
UNION ALL SELECT 'shard-1'::text, reservation.* FROM booking_shard_1.reservations AS reservation;
CREATE VIEW public.physical_source_ticket_order_rows AS
SELECT 'legacy'::text AS source_shard_id, orders.*, reservation.train_run_id
FROM public.ticket_orders AS orders JOIN public.reservations AS reservation ON reservation.id=orders.reservation_id
UNION ALL SELECT 'shard-0'::text, orders.*, reservation.train_run_id
FROM booking_shard_0.ticket_orders AS orders JOIN booking_shard_0.reservations AS reservation ON reservation.id=orders.reservation_id
UNION ALL SELECT 'shard-1'::text, orders.*, reservation.train_run_id
FROM booking_shard_1.ticket_orders AS orders JOIN booking_shard_1.reservations AS reservation ON reservation.id=orders.reservation_id;
CREATE VIEW public.physical_source_ticket_rows AS
SELECT 'legacy'::text AS source_shard_id, ticket.*, reservation.train_run_id
FROM public.tickets AS ticket JOIN public.ticket_orders AS orders ON orders.id=ticket.ticket_order_id
JOIN public.reservations AS reservation ON reservation.id=orders.reservation_id
UNION ALL SELECT 'shard-0'::text, ticket.*, reservation.train_run_id
FROM booking_shard_0.tickets AS ticket JOIN booking_shard_0.ticket_orders AS orders ON orders.id=ticket.ticket_order_id
JOIN booking_shard_0.reservations AS reservation ON reservation.id=orders.reservation_id
UNION ALL SELECT 'shard-1'::text, ticket.*, reservation.train_run_id
FROM booking_shard_1.tickets AS ticket JOIN booking_shard_1.ticket_orders AS orders ON orders.id=ticket.ticket_order_id
JOIN booking_shard_1.reservations AS reservation ON reservation.id=orders.reservation_id;

ALTER TABLE public.physical_source_train_run_mutation_journal
    DROP CONSTRAINT physical_source_train_run_mutation_journal_table_name_check,
    ADD CONSTRAINT physical_source_train_run_mutation_journal_table_name_check CHECK (
        table_name IN (
            'train_run_booking_snapshots','booking_seat_catalog',
            'booking_fare_snapshots','seat_inventory','reservations',
            'reservation_seats','ticket_orders','tickets',
            'idempotency_records','outbox_events'
        )
    );

ALTER TABLE public.outbox_events
    DROP CONSTRAINT outbox_events_event_pair_check,
    DROP CONSTRAINT outbox_events_aggregate_type_check,
    DROP CONSTRAINT outbox_events_event_type_check,
    ADD CONSTRAINT outbox_events_aggregate_type_check CHECK (
        aggregate_type IN (
            'reservation', 'ticket', 'train_run', 'hot_train_policy',
            'station', 'route', 'train', 'coach', 'seat', 'fare',
            'booking_command', 'physical_shard_migration'
        )
    ),
    ADD CONSTRAINT outbox_events_event_type_check CHECK (
        event_type IN (
            'reservation.held', 'reservation.confirmed',
            'reservation.expired', 'reservation.cancelled', 'ticket.created',
            'trainrun.created', 'trainrun.updated', 'trainrun.cancelled',
            'hot_train_policy.created', 'hot_train_policy.updated',
            'hot_train_policy.disabled', 'station.created', 'station.updated',
            'station.disabled', 'route.created', 'route.updated',
            'route.disabled', 'train.updated', 'coach.updated', 'seat.updated',
            'fare.created', 'fare.updated', 'fare.disabled',
            'booking_command.finalized', 'booking_command.repaired',
            'booking_command.failed', 'physical_shard_migration.cutover',
            'physical_shard_migration.rolled_back',
            'physical_shard_migration.reverse_cutover',
            'physical_shard_migration.completed'
        )
    ),
    ADD CONSTRAINT outbox_events_event_pair_check CHECK (
        (aggregate_type='reservation' AND event_type IN (
            'reservation.held','reservation.confirmed',
            'reservation.expired','reservation.cancelled'
        ))
        OR (aggregate_type='ticket' AND event_type='ticket.created')
        OR (aggregate_type='train_run' AND event_type IN (
            'trainrun.created','trainrun.updated','trainrun.cancelled'
        ))
        OR (aggregate_type='hot_train_policy' AND event_type IN (
            'hot_train_policy.created','hot_train_policy.updated',
            'hot_train_policy.disabled'
        ))
        OR (aggregate_type='station' AND event_type IN (
            'station.created','station.updated','station.disabled'
        ))
        OR (aggregate_type='route' AND event_type IN (
            'route.created','route.updated','route.disabled'
        ))
        OR (aggregate_type='train' AND event_type='train.updated')
        OR (aggregate_type='coach' AND event_type='coach.updated')
        OR (aggregate_type='seat' AND event_type='seat.updated')
        OR (aggregate_type='fare' AND event_type IN (
            'fare.created','fare.updated','fare.disabled'
        ))
        OR (aggregate_type='booking_command' AND event_type IN (
            'booking_command.finalized','booking_command.repaired',
            'booking_command.failed'
        ))
        OR (aggregate_type='physical_shard_migration' AND event_type IN (
            'physical_shard_migration.cutover',
            'physical_shard_migration.rolled_back',
            'physical_shard_migration.reverse_cutover',
            'physical_shard_migration.completed'
        ))
    );

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
AS $m6_restore_append_physical_source_mutation$
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
           'idempotency_records', 'outbox_events'
       )
       OR mutation_operation NOT IN ('INSERT', 'UPDATE', 'DELETE')
       OR target_entity_id IS NULL
       OR jsonb_typeof(bounded_primary_key) <> 'object'
       OR octet_length(bounded_primary_key::text) > 512 THEN
        RAISE EXCEPTION 'invalid physical source mutation capture input'
            USING ERRCODE = '22023';
    END IF;
    UPDATE public.physical_source_migration_capture_state AS capture
    SET next_sequence=capture.next_sequence+1
    FROM public.train_run_shard_assignments AS assignment
    WHERE capture.train_run_id=selected_train_run_id
      AND capture.source_shard_id=selected_source_shard_id
      AND capture.capture_enabled
      AND assignment.train_run_id=capture.train_run_id
      AND assignment.shard_id=capture.source_shard_id
      AND assignment.assignment_generation=capture.source_generation
      AND assignment.assignment_state IN ('stable','draining','migrating')
    RETURNING capture.migration_id,capture.source_generation,capture.next_sequence
    INTO capture_migration_id,capture_generation,allocated_sequence;
    IF capture_migration_id IS NULL THEN RETURN; END IF;
    INSERT INTO public.physical_source_train_run_mutation_journal (
        migration_id,train_run_id,source_shard_id,source_generation,
        mutation_sequence,table_name,operation,entity_id,primary_key,metadata
    ) VALUES (
        capture_migration_id,selected_train_run_id,selected_source_shard_id,
        capture_generation,allocated_sequence,target_table_name,
        mutation_operation,target_entity_id,bounded_primary_key,
        jsonb_build_object('source_shard_id',selected_source_shard_id)
    );
END;
$m6_restore_append_physical_source_mutation$;

UPDATE public.booking_shards
SET schema_version = 1
WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
  AND storage_kind = 'postgres'
  AND schema_version = 2;

COMMIT;
