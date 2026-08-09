BEGIN;

-- Version 2 is evidence-bearing. Schema rollback is permitted only before any
-- payment state, receipt, payment-aware outbox event or mutation-journal row
-- exists. Operators must reconcile and retain real M6 evidence, then repair
-- forward; this migration never destroys it to make rollback succeed.
DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM reservations
        WHERE payment_intent_id IS NOT NULL
           OR status IN (
                'payment_pending', 'payment_review', 'refund_pending'
           )
    ) THEN
        RAISE EXCEPTION 'cannot roll back booking shard v2 with payment reservations';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM ticket_orders
        WHERE payment_intent_id IS NOT NULL
           OR status IN (
                'payment_pending', 'payment_authorized',
                'payment_captured', 'issuance_pending', 'issued',
                'refund_pending', 'refunded', 'manual_review'
           )
    ) THEN
        RAISE EXCEPTION 'cannot roll back booking shard v2 with payment ticket orders';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM tickets
        WHERE status IN ('pending', 'refund_pending')
    ) THEN
        RAISE EXCEPTION 'cannot roll back booking shard v2 with payment tickets';
    END IF;

    IF EXISTS (SELECT 1 FROM payment_command_receipts)
       OR EXISTS (SELECT 1 FROM ticket_issuance_receipts)
       OR EXISTS (SELECT 1 FROM payment_refund_receipts)
       OR EXISTS (SELECT 1 FROM payment_compensation_receipts) THEN
        RAISE EXCEPTION 'cannot roll back booking shard v2 with payment receipts';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM outbox_events
        WHERE aggregate_type IN ('payment', 'ticket_order')
           OR event_type IN (
                'reservation.payment_pending',
                'reservation.refund_pending',
                'ticket_order.issued',
                'ticket.issued',
                'payment.compensation_applied'
           )
    ) THEN
        RAISE EXCEPTION 'cannot roll back booking shard v2 with payment outbox evidence';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM train_run_mutation_journal
        WHERE table_name IN (
                'payment_command_receipts',
                'ticket_issuance_receipts',
                'payment_refund_receipts',
                'payment_compensation_receipts'
            )
           OR metadata ->> 'status' IN (
                'payment_pending', 'payment_review', 'refund_pending',
                'payment_authorized', 'payment_captured',
                'issuance_pending', 'issued', 'refunded', 'manual_review'
           )
    ) THEN
        RAISE EXCEPTION 'cannot roll back booking shard v2 with payment journal evidence';
    END IF;
END;
$guard$;

DROP TRIGGER payment_compensation_receipts_capture_mutation
    ON payment_compensation_receipts;
DROP TRIGGER payment_refund_receipts_capture_mutation
    ON payment_refund_receipts;
DROP TRIGGER ticket_issuance_receipts_capture_mutation
    ON ticket_issuance_receipts;
DROP TRIGGER payment_command_receipts_capture_mutation
    ON payment_command_receipts;

DROP TRIGGER payment_command_receipts_set_updated_at
    ON payment_command_receipts;

DROP TABLE payment_compensation_receipts;
DROP TABLE payment_refund_receipts;
DROP TABLE ticket_issuance_receipts;
DROP TABLE payment_command_receipts;

DROP FUNCTION booking_shard_capture_payment_mutation();

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
            'booking_command_receipts'
        )
    );

ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_aggregate_type_check;

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_aggregate_type_check CHECK (
        aggregate_type IN (
            'reservation', 'ticket', 'train_run', 'booking_command'
        )
    );

ALTER TABLE tickets
    DROP CONSTRAINT tickets_opaque_code_check,
    DROP CONSTRAINT tickets_status_check;

ALTER TABLE tickets
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN ('active', 'cancelled')
    );

DROP TRIGGER tickets_guard_identity ON tickets;
DROP FUNCTION booking_shard_guard_ticket_identity();

DROP TRIGGER ticket_orders_guard_payment_snapshot ON ticket_orders;
DROP FUNCTION booking_shard_guard_ticket_order_payment_snapshot();

DROP INDEX ticket_orders_payment_state_idx;
DROP INDEX ticket_orders_payment_intent_unique_idx;

ALTER TABLE ticket_orders
    DROP CONSTRAINT ticket_orders_reservation_payment_fkey,
    DROP CONSTRAINT ticket_orders_reservation_money_fkey,
    DROP CONSTRAINT ticket_orders_payment_authority_unique,
    DROP CONSTRAINT ticket_orders_payment_money_unique,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    DROP CONSTRAINT ticket_orders_payment_snapshot_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_status_check,
    DROP COLUMN refunded_amount_minor,
    DROP COLUMN captured_amount_minor,
    DROP COLUMN authorized_amount_minor,
    DROP COLUMN payment_currency,
    DROP COLUMN payment_intent_id;

ALTER TABLE ticket_orders
    ADD CONSTRAINT ticket_orders_status_check CHECK (
        status IN ('confirmed', 'cancelled')
    );

DROP TRIGGER reservations_guard_payment_snapshot ON reservations;
DROP FUNCTION booking_shard_guard_reservation_payment_snapshot();

DROP INDEX reservations_payment_work_idx;
DROP INDEX reservations_payment_intent_unique_idx;

ALTER TABLE reservations
    DROP CONSTRAINT reservations_payment_authority_unique,
    DROP CONSTRAINT reservations_payment_money_unique,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    DROP CONSTRAINT reservations_status_check,
    DROP COLUMN payment_grace_expires_at,
    DROP COLUMN payment_currency,
    DROP COLUMN payment_amount_minor,
    DROP COLUMN payment_intent_id;

ALTER TABLE reservations
    ADD CONSTRAINT reservations_status_check CHECK (
        status IN ('held', 'confirmed', 'expired', 'cancelled')
    );

COMMIT;
