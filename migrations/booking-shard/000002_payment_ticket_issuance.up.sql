BEGIN;

-- Payment intent and provider-operation history remain in the control
-- database. A shard stores only the immutable, provider-neutral payment
-- snapshot and receipts needed to protect its reservation, inventory and
-- ticket authority. Control UUIDs intentionally have no cross-database FK.

ALTER TABLE reservations
    DROP CONSTRAINT reservations_status_check;

ALTER TABLE reservations
    ADD COLUMN payment_intent_id uuid,
    ADD COLUMN payment_amount_minor bigint,
    ADD COLUMN payment_currency text,
    ADD COLUMN payment_grace_expires_at timestamptz,
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
    ),
    ADD CONSTRAINT reservations_payment_money_unique
        UNIQUE (
            id, train_run_id, assignment_generation,
            total_amount_minor, currency
        ),
    ADD CONSTRAINT reservations_payment_authority_unique
        UNIQUE (
            id, train_run_id, assignment_generation, payment_intent_id,
            total_amount_minor, currency
        );

CREATE UNIQUE INDEX reservations_payment_intent_unique_idx
    ON reservations (payment_intent_id)
    WHERE payment_intent_id IS NOT NULL;

CREATE INDEX reservations_payment_work_idx
    ON reservations (
        status, payment_grace_expires_at, train_run_id,
        assignment_generation, id
    )
    WHERE status IN ('payment_pending', 'payment_review', 'refund_pending');

CREATE FUNCTION booking_shard_guard_reservation_payment_snapshot()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF OLD.payment_intent_id IS NOT NULL AND (
        NEW.payment_intent_id IS DISTINCT FROM OLD.payment_intent_id
        OR NEW.payment_amount_minor IS DISTINCT FROM OLD.payment_amount_minor
        OR NEW.payment_currency IS DISTINCT FROM OLD.payment_currency
        OR NEW.payment_grace_expires_at
            IS DISTINCT FROM OLD.payment_grace_expires_at
        OR NEW.total_amount_minor IS DISTINCT FROM OLD.total_amount_minor
        OR NEW.currency IS DISTINCT FROM OLD.currency
    ) THEN
        RAISE EXCEPTION 'reservation payment snapshot is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER reservations_guard_payment_snapshot
BEFORE UPDATE ON reservations
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_reservation_payment_snapshot();

ALTER TABLE ticket_orders
    DROP CONSTRAINT ticket_orders_status_check;

ALTER TABLE ticket_orders
    ADD COLUMN payment_intent_id uuid,
    ADD COLUMN payment_currency text,
    ADD COLUMN authorized_amount_minor bigint NOT NULL DEFAULT 0,
    ADD COLUMN captured_amount_minor bigint NOT NULL DEFAULT 0,
    ADD COLUMN refunded_amount_minor bigint NOT NULL DEFAULT 0,
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
    ADD CONSTRAINT ticket_orders_payment_snapshot_check CHECK (
        (
            payment_intent_id IS NULL
            AND payment_currency IS NULL
            AND authorized_amount_minor = 0
            AND captured_amount_minor = 0
            AND refunded_amount_minor = 0
            AND status IN ('confirmed', 'cancelled')
        )
        OR
        (
            payment_intent_id IS NOT NULL
            AND status <> 'confirmed'
            AND payment_currency IS NOT NULL
            AND payment_currency = currency
            AND payment_currency ~ '^[A-Z]{3}$'
        )
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
    ),
    ADD CONSTRAINT ticket_orders_payment_money_unique
        UNIQUE (
            id, train_run_id, assignment_generation,
            total_amount_minor, currency
        ),
    ADD CONSTRAINT ticket_orders_payment_authority_unique
        UNIQUE (
            id, train_run_id, assignment_generation, payment_intent_id,
            total_amount_minor, currency
        ),
    ADD CONSTRAINT ticket_orders_reservation_money_fkey
        FOREIGN KEY (
            reservation_id, train_run_id, assignment_generation,
            total_amount_minor, currency
        ) REFERENCES reservations(
            id, train_run_id, assignment_generation,
            total_amount_minor, currency
        ) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ticket_orders
    ADD CONSTRAINT ticket_orders_reservation_payment_fkey
        FOREIGN KEY (
            reservation_id, train_run_id, assignment_generation,
            payment_intent_id, total_amount_minor, currency
        ) REFERENCES reservations(
            id, train_run_id, assignment_generation, payment_intent_id,
            total_amount_minor, currency
        ) ON UPDATE CASCADE ON DELETE RESTRICT;

CREATE UNIQUE INDEX ticket_orders_payment_intent_unique_idx
    ON ticket_orders (payment_intent_id)
    WHERE payment_intent_id IS NOT NULL;

CREATE INDEX ticket_orders_payment_state_idx
    ON ticket_orders (
        status, train_run_id, assignment_generation, updated_at, id
    )
    WHERE status IN (
        'payment_pending', 'payment_authorized', 'payment_captured',
        'issuance_pending', 'refund_pending', 'manual_review'
    );

CREATE FUNCTION booking_shard_guard_ticket_order_payment_snapshot()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF OLD.payment_intent_id IS NOT NULL AND (
        NEW.payment_intent_id IS DISTINCT FROM OLD.payment_intent_id
        OR NEW.payment_currency IS DISTINCT FROM OLD.payment_currency
        OR NEW.total_amount_minor IS DISTINCT FROM OLD.total_amount_minor
        OR NEW.currency IS DISTINCT FROM OLD.currency
    ) THEN
        RAISE EXCEPTION 'ticket-order payment snapshot is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.authorized_amount_minor < OLD.authorized_amount_minor
       OR NEW.captured_amount_minor < OLD.captured_amount_minor
       OR NEW.refunded_amount_minor < OLD.refunded_amount_minor THEN
        RAISE EXCEPTION 'ticket-order payment totals cannot decrease'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ticket_orders_guard_payment_snapshot
BEFORE UPDATE ON ticket_orders
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_ticket_order_payment_snapshot();

ALTER TABLE tickets
    DROP CONSTRAINT tickets_status_check;

ALTER TABLE tickets
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN ('pending', 'active', 'refund_pending', 'cancelled')
    ),
    ADD CONSTRAINT tickets_opaque_code_check CHECK (
        length(ticket_code) BETWEEN 16 AND 64
    );

CREATE FUNCTION booking_shard_guard_ticket_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.ticket_code IS DISTINCT FROM OLD.ticket_code THEN
        RAISE EXCEPTION 'ticket identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tickets_guard_identity
BEFORE UPDATE ON tickets
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_ticket_identity();

ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_aggregate_type_check;

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_aggregate_type_check CHECK (
        aggregate_type IN (
            'reservation', 'ticket_order', 'ticket', 'payment', 'train_run',
            'booking_command'
        )
    );

CREATE TABLE payment_command_receipts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    command_id uuid NOT NULL UNIQUE,
    payment_intent_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    operation text NOT NULL CHECK (
        operation IN (
            'reservation.payment_begin',
            'payment.authorization_recorded',
            'payment.capture_recorded',
            'reservation.payment_review',
            'reservation.refund_pending',
            'reservation.payment_cancelled'
        )
    ),
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status text NOT NULL DEFAULT 'started'
        CHECK (status IN ('started', 'succeeded', 'rejected')),
    result_resource_id uuid,
    result_status text CHECK (
        result_status IS NULL OR result_status IN (
            'payment_pending', 'payment_review', 'confirmed',
            'refund_pending', 'cancelled'
        )
    ),
    error_code text CHECK (
        error_code IS NULL OR (
            length(error_code) BETWEEN 1 AND 64
            AND error_code ~ '^[a-z][a-z0-9_.-]+$'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    committed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, train_run_id, assignment_generation),
    UNIQUE (payment_intent_id, operation, request_fingerprint),
    FOREIGN KEY (
        reservation_id, train_run_id, assignment_generation,
        payment_intent_id, amount_minor, currency
    ) REFERENCES reservations(
        id, train_run_id, assignment_generation, payment_intent_id,
        total_amount_minor, currency
    )
        ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (
        (status = 'started'
            AND result_resource_id IS NULL
            AND result_status IS NULL
            AND error_code IS NULL
            AND committed_at IS NULL)
        OR
        (status = 'succeeded'
            AND result_resource_id IS NOT NULL
            AND result_status IS NOT NULL
            AND error_code IS NULL
            AND committed_at IS NOT NULL)
        OR
        (status = 'rejected'
            AND result_resource_id IS NULL
            AND result_status IS NULL
            AND error_code IS NOT NULL
            AND committed_at IS NOT NULL)
    )
);

CREATE INDEX payment_command_receipts_intent_idx
    ON payment_command_receipts (
        payment_intent_id, status, created_at, command_id
    );

CREATE UNIQUE INDEX payment_command_receipts_begin_unique_idx
    ON payment_command_receipts (reservation_id)
    WHERE operation = 'reservation.payment_begin'
      AND status <> 'rejected';

CREATE UNIQUE INDEX payment_command_receipts_capture_unique_idx
    ON payment_command_receipts (payment_intent_id)
    WHERE operation = 'payment.capture_recorded'
      AND status <> 'rejected';

CREATE INDEX payment_command_receipts_migration_cursor_idx
    ON payment_command_receipts (
        train_run_id, assignment_generation, id
    );

CREATE TRIGGER payment_command_receipts_set_updated_at
BEFORE UPDATE ON payment_command_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE ticket_issuance_receipts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    issuance_id uuid NOT NULL UNIQUE,
    payment_intent_id uuid NOT NULL UNIQUE,
    reservation_id uuid NOT NULL UNIQUE,
    payment_operation_id uuid NOT NULL UNIQUE,
    ticket_order_id uuid NOT NULL UNIQUE,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    capture_proof_hash bytea NOT NULL
        CHECK (octet_length(capture_proof_hash) = 32),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    issued_ticket_count integer NOT NULL CHECK (issued_ticket_count > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, train_run_id, assignment_generation),
    FOREIGN KEY (
        reservation_id, train_run_id, assignment_generation,
        payment_intent_id, amount_minor, currency
    ) REFERENCES reservations(
        id, train_run_id, assignment_generation, payment_intent_id,
        total_amount_minor, currency
    )
        ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (
        ticket_order_id, train_run_id, assignment_generation,
        payment_intent_id, amount_minor, currency
    ) REFERENCES ticket_orders(
        id, train_run_id, assignment_generation, payment_intent_id,
        total_amount_minor, currency
    )
        ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX ticket_issuance_receipts_migration_cursor_idx
    ON ticket_issuance_receipts (
        train_run_id, assignment_generation, id
    );

CREATE TABLE payment_refund_receipts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    refund_operation_id uuid NOT NULL UNIQUE,
    payment_intent_id uuid NOT NULL UNIQUE,
    reservation_id uuid NOT NULL UNIQUE,
    ticket_order_id uuid NOT NULL UNIQUE,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    refund_proof_hash bytea NOT NULL
        CHECK (octet_length(refund_proof_hash) = 32),
    captured_amount_minor bigint NOT NULL CHECK (captured_amount_minor > 0),
    refunded_amount_minor bigint NOT NULL,
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    refunded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, train_run_id, assignment_generation),
    CONSTRAINT payment_refund_receipts_authority_unique UNIQUE (
        id, train_run_id, assignment_generation, payment_intent_id,
        reservation_id, ticket_order_id
    ),
    FOREIGN KEY (
        reservation_id, train_run_id, assignment_generation,
        payment_intent_id, captured_amount_minor, currency
    ) REFERENCES reservations(
        id, train_run_id, assignment_generation, payment_intent_id,
        total_amount_minor, currency
    )
        ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (
        ticket_order_id, train_run_id, assignment_generation,
        payment_intent_id, captured_amount_minor, currency
    ) REFERENCES ticket_orders(
        id, train_run_id, assignment_generation, payment_intent_id,
        total_amount_minor, currency
    )
        ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (refunded_amount_minor = captured_amount_minor)
);

CREATE INDEX payment_refund_receipts_migration_cursor_idx
    ON payment_refund_receipts (
        train_run_id, assignment_generation, id
    );

CREATE TABLE payment_compensation_receipts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    compensation_id uuid NOT NULL UNIQUE,
    payment_intent_id uuid NOT NULL UNIQUE,
    reservation_id uuid NOT NULL UNIQUE,
    ticket_order_id uuid NOT NULL UNIQUE,
    refund_receipt_id uuid NOT NULL UNIQUE,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    released_seat_count integer NOT NULL CHECK (released_seat_count > 0),
    cancelled_ticket_count integer NOT NULL CHECK (cancelled_ticket_count >= 0),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, train_run_id, assignment_generation),
    FOREIGN KEY (reservation_id, train_run_id, assignment_generation)
        REFERENCES reservations(id, train_run_id, assignment_generation)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (ticket_order_id, train_run_id, assignment_generation)
        REFERENCES ticket_orders(id, train_run_id, assignment_generation)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (
        refund_receipt_id, train_run_id, assignment_generation,
        payment_intent_id, reservation_id, ticket_order_id
    )
        REFERENCES payment_refund_receipts(
            id, train_run_id, assignment_generation, payment_intent_id,
            reservation_id, ticket_order_id
        ) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX payment_compensation_receipts_migration_cursor_idx
    ON payment_compensation_receipts (
        train_run_id, assignment_generation, id
    );

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

-- New receipt tables use a dedicated capture function. Existing authoritative
-- tables retain the version-1 function: updates to their new payment columns
-- already fire the existing row trigger, and replay fetches the complete row.
CREATE FUNCTION booking_shard_capture_payment_mutation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $$
DECLARE
    source_row jsonb;
    captured_train_run_id uuid;
    captured_generation bigint;
    captured_entity_id uuid;
    captured_sequence bigint;
    captured_migration_id uuid;
    captured_metadata jsonb;
    state_enabled boolean;
    state_generation bigint;
BEGIN
    IF TG_TABLE_SCHEMA <> 'public'
       OR TG_TABLE_NAME NOT IN (
            'payment_command_receipts',
            'ticket_issuance_receipts',
            'payment_refund_receipts',
            'payment_compensation_receipts'
       ) THEN
        RAISE EXCEPTION 'unsupported payment mutation-capture table %.%',
            TG_TABLE_SCHEMA, TG_TABLE_NAME
            USING ERRCODE = '0A000';
    END IF;

    IF TG_OP = 'DELETE' THEN
        source_row := to_jsonb(OLD);
    ELSE
        source_row := to_jsonb(NEW);
    END IF;

    captured_train_run_id := (source_row ->> 'train_run_id')::uuid;
    captured_generation :=
        (source_row ->> 'assignment_generation')::bigint;
    captured_entity_id := (source_row ->> 'id')::uuid;

    SELECT migration_id, capture_enabled, source_generation
      INTO captured_migration_id, state_enabled, state_generation
      FROM public.migration_capture_state
     WHERE train_run_id = captured_train_run_id
     FOR UPDATE;

    IF NOT FOUND OR NOT state_enabled THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    IF captured_generation <> state_generation THEN
        RAISE EXCEPTION 'payment capture generation mismatch for train run %',
            captured_train_run_id
            USING ERRCODE = '40001';
    END IF;

    UPDATE public.migration_capture_state
       SET next_sequence = next_sequence + 1
     WHERE train_run_id = captured_train_run_id
     RETURNING next_sequence INTO captured_sequence;

    captured_metadata := CASE TG_TABLE_NAME
        WHEN 'payment_command_receipts' THEN jsonb_build_object(
            'payment_intent_id', source_row -> 'payment_intent_id',
            'reservation_id', source_row -> 'reservation_id',
            'operation', source_row -> 'operation',
            'status', source_row -> 'status',
            'result_resource_id', source_row -> 'result_resource_id',
            'result_status', source_row -> 'result_status',
            'error_code', source_row -> 'error_code'
        )
        WHEN 'ticket_issuance_receipts' THEN jsonb_build_object(
            'payment_intent_id', source_row -> 'payment_intent_id',
            'reservation_id', source_row -> 'reservation_id',
            'ticket_order_id', source_row -> 'ticket_order_id',
            'issued_ticket_count', source_row -> 'issued_ticket_count',
            'amount_minor', source_row -> 'amount_minor',
            'currency', source_row -> 'currency'
        )
        WHEN 'payment_refund_receipts' THEN jsonb_build_object(
            'payment_intent_id', source_row -> 'payment_intent_id',
            'reservation_id', source_row -> 'reservation_id',
            'ticket_order_id', source_row -> 'ticket_order_id',
            'captured_amount_minor', source_row -> 'captured_amount_minor',
            'refunded_amount_minor', source_row -> 'refunded_amount_minor',
            'currency', source_row -> 'currency'
        )
        WHEN 'payment_compensation_receipts' THEN jsonb_build_object(
            'payment_intent_id', source_row -> 'payment_intent_id',
            'reservation_id', source_row -> 'reservation_id',
            'ticket_order_id', source_row -> 'ticket_order_id',
            'refund_receipt_id', source_row -> 'refund_receipt_id',
            'released_seat_count', source_row -> 'released_seat_count',
            'cancelled_ticket_count', source_row -> 'cancelled_ticket_count'
        )
    END;

    INSERT INTO public.train_run_mutation_journal (
        migration_id,
        train_run_id,
        source_generation,
        mutation_sequence,
        table_name,
        operation,
        entity_id,
        primary_key,
        metadata
    ) VALUES (
        captured_migration_id,
        captured_train_run_id,
        captured_generation,
        captured_sequence,
        TG_TABLE_NAME,
        TG_OP,
        captured_entity_id,
        jsonb_build_object('id', captured_entity_id),
        jsonb_strip_nulls(captured_metadata)
    );

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER payment_command_receipts_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON payment_command_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_payment_mutation();

CREATE TRIGGER ticket_issuance_receipts_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON ticket_issuance_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_payment_mutation();

CREATE TRIGGER payment_refund_receipts_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON payment_refund_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_payment_mutation();

CREATE TRIGGER payment_compensation_receipts_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON payment_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_payment_mutation();

COMMIT;
