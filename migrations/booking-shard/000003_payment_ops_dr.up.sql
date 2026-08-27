BEGIN;

-- Existing physical snapshots cannot safely infer a departure instant. New
-- control-to-physical copies populate it; NULL therefore fails refund prepare
-- closed until the train run is rematerialized.
ALTER TABLE train_run_booking_snapshots
    ADD COLUMN scheduled_departure_at timestamptz NOT NULL
        DEFAULT '-infinity'::timestamptz;

-- Milestone 7 adds only shard-owned ticket-refund and regional-fencing facts.
-- Provider requests, ledger postings, settlement evidence, and DR operation
-- journals remain control-plane authority. UUIDs are application generated.

ALTER TABLE reservations
    DROP CONSTRAINT reservations_status_check,
    DROP CONSTRAINT reservations_payment_snapshot_check;

ALTER TABLE reservations
    ADD CONSTRAINT reservations_status_check CHECK (
        status IN (
            'held', 'payment_pending', 'payment_review', 'confirmed',
            'partially_refund_pending', 'partially_refunded',
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
                'payment_pending', 'payment_review',
                'partially_refund_pending', 'partially_refunded',
                'refund_pending'
            )
        )
        OR
        (
            payment_intent_id IS NOT NULL
            AND status IN (
                'payment_pending', 'payment_review', 'confirmed',
                'partially_refund_pending', 'partially_refunded',
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

DROP INDEX reservations_payment_work_idx;
CREATE INDEX reservations_payment_work_idx
    ON reservations (
        status, payment_grace_expires_at, train_run_id,
        assignment_generation, id
    )
    WHERE status IN (
        'payment_pending', 'payment_review',
        'partially_refund_pending', 'refund_pending'
    );

ALTER TABLE ticket_orders
    DROP CONSTRAINT ticket_orders_status_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_state_check;

ALTER TABLE ticket_orders
    ADD CONSTRAINT ticket_orders_status_check CHECK (
        status IN (
            'confirmed', 'payment_pending', 'payment_authorized',
            'payment_captured', 'issuance_pending', 'issued',
            'partial_refund_pending', 'partially_refunded',
            'refund_pending', 'refunded', 'cancelled', 'manual_review'
        )
    ),
    ADD CONSTRAINT ticket_orders_payment_amounts_check CHECK (
        authorized_amount_minor IN (0, total_amount_minor)
        AND captured_amount_minor IN (0, authorized_amount_minor)
        AND refunded_amount_minor >= 0
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
                'partial_refund_pending', 'partially_refunded',
                'refund_pending', 'refunded'
            )
            OR captured_amount_minor = total_amount_minor
        )
        AND (
            status <> 'partially_refunded'
            OR (
                refunded_amount_minor > 0
                AND refunded_amount_minor < captured_amount_minor
            )
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

DROP INDEX ticket_orders_payment_state_idx;
CREATE INDEX ticket_orders_payment_state_idx
    ON ticket_orders (
        status, train_run_id, assignment_generation, updated_at, id
    )
    WHERE status IN (
        'payment_pending', 'payment_authorized', 'payment_captured',
        'issuance_pending', 'partial_refund_pending', 'refund_pending',
        'manual_review'
    );

ALTER TABLE tickets
    DROP CONSTRAINT tickets_status_check;

ALTER TABLE tickets
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN (
            'pending', 'active', 'refund_pending', 'refunded', 'cancelled'
        )
    );

CREATE FUNCTION booking_shard_guard_ticket_refund_transition()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF OLD.status IN ('refunded', 'cancelled')
       AND NEW.status IS DISTINCT FROM OLD.status THEN
        RAISE EXCEPTION 'terminal ticket state is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.status = 'refund_pending'
       AND NEW.status NOT IN ('refund_pending', 'active', 'refunded', 'cancelled') THEN
        RAISE EXCEPTION 'invalid ticket refund transition'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tickets_guard_refund_transition
BEFORE UPDATE OF status ON tickets
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_ticket_refund_transition();

CREATE TABLE regional_write_authority (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    region text NOT NULL CHECK (region IN ('region-a', 'region-b')),
    epoch bigint NOT NULL CHECK (epoch > 0),
    state text NOT NULL CHECK (
        state IN (
            'active', 'draining', 'fenced',
            'promoting', 'recovery', 'failed'
        )
    ),
    writes_enabled boolean NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (NOT writes_enabled OR state = 'active')
);

INSERT INTO regional_write_authority(
    singleton, region, epoch, state, writes_enabled
) VALUES (true, 'region-a', 1, 'active', true);

-- Row-locking SELECT requires table UPDATE privilege in PostgreSQL. Keep the
-- authority table non-writable to application roles while still taking a
-- transaction-scoped shared lock through this narrowly scoped owner function.
CREATE FUNCTION public.lock_regional_write_authority()
RETURNS TABLE (
    region text,
    epoch bigint,
    state text,
    writes_enabled boolean
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $lock_regional_write_authority$
    SELECT authority.region, authority.epoch, authority.state,
           authority.writes_enabled
      FROM public.regional_write_authority AS authority
     WHERE authority.singleton
     FOR SHARE
$lock_regional_write_authority$;

REVOKE ALL ON FUNCTION public.lock_regional_write_authority() FROM PUBLIC;

CREATE FUNCTION booking_shard_guard_regional_write_authority()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF TG_OP = 'DELETE' OR NEW.singleton IS DISTINCT FROM OLD.singleton THEN
        RAISE EXCEPTION 'regional authority identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.epoch < OLD.epoch
       OR (NEW.epoch = OLD.epoch AND NEW.region IS DISTINCT FROM OLD.region) THEN
        RAISE EXCEPTION 'regional authority epoch cannot move backwards or change owner'
            USING ERRCODE = '23514';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER regional_write_authority_guard_transition
BEFORE UPDATE OR DELETE ON regional_write_authority
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_regional_write_authority();

CREATE TABLE ticket_refund_prepare_receipts (
    id uuid PRIMARY KEY,
    command_id uuid NOT NULL UNIQUE,
    refund_request_id uuid NOT NULL UNIQUE,
    refund_operation_id uuid NOT NULL UNIQUE,
    payment_intent_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    ticket_order_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    ticket_ids uuid[] NOT NULL CHECK (cardinality(ticket_ids) BETWEEN 1 AND 1000),
    prior_order_state text NOT NULL CHECK (prior_order_state IN ('issued','partially_refunded')),
    prior_reservation_state text NOT NULL CHECK (prior_reservation_state IN ('confirmed','partially_refunded')),
    state text NOT NULL CHECK (state IN ('prepared','released','applied')),
    requested_at timestamptz NOT NULL,
    eligibility_cutoff_at timestamptz NOT NULL,
    prepared_at timestamptz NOT NULL,
    resolved_at timestamptz,
    UNIQUE (id, train_run_id, assignment_generation),
    FOREIGN KEY (reservation_id, train_run_id, assignment_generation)
        REFERENCES reservations(id, train_run_id, assignment_generation) ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (ticket_order_id, train_run_id, assignment_generation)
        REFERENCES ticket_orders(id, train_run_id, assignment_generation) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (requested_at < eligibility_cutoff_at),
    CHECK ((state='prepared') = (resolved_at IS NULL))
);

CREATE INDEX ticket_refund_prepare_receipts_migration_cursor_idx
    ON ticket_refund_prepare_receipts(train_run_id, assignment_generation, id);

CREATE FUNCTION guard_ticket_refund_prepare_receipt_transition()
RETURNS trigger LANGUAGE plpgsql AS $ticket_refund_prepare_receipt_transition$
BEGIN
    IF NEW.id <> OLD.id OR NEW.command_id <> OLD.command_id
       OR NEW.refund_request_id <> OLD.refund_request_id OR NEW.refund_operation_id <> OLD.refund_operation_id
       OR NEW.payment_intent_id <> OLD.payment_intent_id OR NEW.reservation_id <> OLD.reservation_id
       OR NEW.ticket_order_id <> OLD.ticket_order_id OR NEW.train_run_id <> OLD.train_run_id
       OR NEW.assignment_generation <> OLD.assignment_generation OR NEW.request_fingerprint <> OLD.request_fingerprint
       OR NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency OR NEW.ticket_ids <> OLD.ticket_ids
       OR NEW.prior_order_state <> OLD.prior_order_state OR NEW.prior_reservation_state <> OLD.prior_reservation_state
       OR NEW.requested_at <> OLD.requested_at OR NEW.eligibility_cutoff_at <> OLD.eligibility_cutoff_at
       OR NEW.prepared_at <> OLD.prepared_at THEN
        RAISE EXCEPTION 'ticket refund prepare receipt identity is immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.state <> 'prepared' OR NEW.state NOT IN ('released','applied')
       OR OLD.resolved_at IS NOT NULL OR NEW.resolved_at IS NULL THEN
        RAISE EXCEPTION 'illegal ticket refund prepare receipt transition' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$ticket_refund_prepare_receipt_transition$;
CREATE TRIGGER ticket_refund_prepare_receipts_guard_transition
BEFORE UPDATE ON ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION guard_ticket_refund_prepare_receipt_transition();

CREATE TABLE ticket_refund_compensation_receipts (
    id uuid PRIMARY KEY,
    command_id uuid NOT NULL UNIQUE,
    refund_request_id uuid NOT NULL UNIQUE,
    refund_operation_id uuid NOT NULL UNIQUE,
    payment_intent_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    ticket_order_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    provider_proof_hash bytea NOT NULL
        CHECK (octet_length(provider_proof_hash) = 32),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    selected_ticket_count integer NOT NULL CHECK (selected_ticket_count > 0),
    released_seat_count integer NOT NULL CHECK (released_seat_count > 0),
    resulting_active_ticket_count integer NOT NULL
        CHECK (resulting_active_ticket_count >= 0),
    resulting_order_state text NOT NULL CHECK (
        resulting_order_state IN ('partially_refunded', 'refunded')
    ),
    committed_at timestamptz NOT NULL,
    UNIQUE (id, train_run_id, assignment_generation),
    FOREIGN KEY (reservation_id, train_run_id, assignment_generation)
        REFERENCES reservations(id, train_run_id, assignment_generation)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (ticket_order_id, train_run_id, assignment_generation)
        REFERENCES ticket_orders(id, train_run_id, assignment_generation)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (selected_ticket_count = released_seat_count),
    CHECK (
        (resulting_order_state = 'refunded' AND resulting_active_ticket_count = 0)
        OR
        (resulting_order_state = 'partially_refunded' AND resulting_active_ticket_count > 0)
    )
);

CREATE INDEX ticket_refund_compensation_receipts_migration_cursor_idx
    ON ticket_refund_compensation_receipts(
        train_run_id, assignment_generation, id
    );

CREATE TABLE selected_ticket_refund_receipts (
    id uuid PRIMARY KEY,
    compensation_receipt_id uuid NOT NULL,
    refund_request_id uuid NOT NULL,
    ticket_id uuid NOT NULL,
    reservation_seat_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    fare_amount_minor bigint NOT NULL CHECK (fare_amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    segment_mask_hash bytea NOT NULL
        CHECK (octet_length(segment_mask_hash) = 32),
    released_at timestamptz NOT NULL,
    CONSTRAINT selected_ticket_refund_receipts_ticket_unique
        UNIQUE (ticket_id),
    UNIQUE (reservation_seat_id),
    UNIQUE (compensation_receipt_id, ticket_id),
    FOREIGN KEY (
        compensation_receipt_id, train_run_id, assignment_generation
    ) REFERENCES ticket_refund_compensation_receipts(
        id, train_run_id, assignment_generation
    ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (ticket_id) REFERENCES tickets(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (
        reservation_seat_id, train_run_id, assignment_generation
    ) REFERENCES reservation_seats(id, train_run_id, assignment_generation)
        ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX selected_ticket_refund_receipts_migration_cursor_idx
    ON selected_ticket_refund_receipts(
        train_run_id, assignment_generation, id
    );

-- Retained-predecessor cleanup is the only operation allowed to delete M7
-- receipt evidence. Authorization is a database row bound to the exact local
-- transaction, migration, train run, generation, and table; it is never a
-- session flag that could leak through a pooled connection.
CREATE TABLE migration_evidence_mutation_authorizations (
    transaction_id bigint NOT NULL CHECK (transaction_id > 0),
    migration_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    table_name text NOT NULL CHECK (
        table_name IN (
			'ticket_refund_prepare_receipts',
            'ticket_refund_compensation_receipts',
            'selected_ticket_refund_receipts'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        transaction_id, train_run_id, assignment_generation, table_name
    )
);

CREATE FUNCTION booking_shard_guard_evidence_mutation_authorization()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $booking_shard_evidence_mutation_authorization_guard$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'migration evidence authorization is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF OLD.transaction_id <> txid_current() THEN
            RAISE EXCEPTION 'migration evidence authorization is transaction-bound'
                USING ERRCODE = '55000';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.transaction_id <> txid_current() OR NOT EXISTS (
        SELECT 1
          FROM public.migration_capture_state AS capture
         WHERE capture.migration_id = NEW.migration_id
           AND capture.train_run_id = NEW.train_run_id
           AND capture.source_generation = NEW.assignment_generation
           AND NOT capture.capture_enabled
    ) THEN
        RAISE EXCEPTION 'migration evidence authorization is not exact'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$booking_shard_evidence_mutation_authorization_guard$;

CREATE TRIGGER migration_evidence_mutation_authorizations_guard
BEFORE INSERT OR UPDATE OR DELETE
ON migration_evidence_mutation_authorizations
FOR EACH ROW EXECUTE FUNCTION
    booking_shard_guard_evidence_mutation_authorization();

CREATE FUNCTION booking_shard_require_evidence_authorization_release()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $booking_shard_evidence_authorization_release$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM public.migration_evidence_mutation_authorizations
         WHERE transaction_id = NEW.transaction_id
           AND train_run_id = NEW.train_run_id
           AND assignment_generation = NEW.assignment_generation
           AND table_name = NEW.table_name
    ) THEN
        RAISE EXCEPTION 'migration evidence authorization must be released before commit'
            USING ERRCODE = '55000';
    END IF;
    RETURN NULL;
END;
$booking_shard_evidence_authorization_release$;

CREATE CONSTRAINT TRIGGER migration_evidence_authorization_release
AFTER INSERT ON migration_evidence_mutation_authorizations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION
    booking_shard_require_evidence_authorization_release();

CREATE TABLE dr_reconciliation_checkpoints (
    checkpoint_id uuid PRIMARY KEY,
    scope text NOT NULL CHECK (
        scope IN ('activation', 'steady_state', 'failback', 'restore_validation')
    ),
    region text NOT NULL CHECK (region IN ('region-a', 'region-b')),
    epoch bigint NOT NULL CHECK (epoch > 0),
    rows_examined bigint NOT NULL CHECK (rows_examined >= 0),
    mismatch_count bigint NOT NULL CHECK (mismatch_count >= 0),
    truncated boolean NOT NULL,
    state text NOT NULL CHECK (state IN ('passed', 'failed', 'bounded')),
    evidence_hash bytea NOT NULL CHECK (octet_length(evidence_hash) = 32),
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    CHECK (completed_at >= started_at),
    CHECK (state <> 'passed' OR (NOT truncated AND mismatch_count = 0))
);

CREATE FUNCTION booking_shard_reject_evidence_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    source_row jsonb;
    authorized boolean;
BEGIN
    IF TG_OP = 'DELETE'
       AND TG_TABLE_SCHEMA = 'public'
       AND TG_TABLE_NAME IN (
		   'ticket_refund_prepare_receipts',
           'ticket_refund_compensation_receipts',
           'selected_ticket_refund_receipts'
       ) THEN
        source_row := to_jsonb(OLD);
        SELECT true
          INTO authorized
          FROM public.migration_evidence_mutation_authorizations AS auth
          JOIN public.migration_capture_state AS capture
            ON capture.train_run_id = auth.train_run_id
           AND capture.migration_id = auth.migration_id
           AND capture.source_generation = auth.assignment_generation
         WHERE auth.transaction_id = txid_current()
           AND auth.train_run_id = (source_row ->> 'train_run_id')::uuid
           AND auth.assignment_generation =
               (source_row ->> 'assignment_generation')::bigint
           AND auth.table_name = TG_TABLE_NAME
           AND NOT capture.capture_enabled
         FOR UPDATE OF capture;
        IF authorized THEN
            RETURN OLD;
        END IF;
    END IF;
    RAISE EXCEPTION 'booking-shard evidence is immutable'
        USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER ticket_refund_compensation_receipts_guard_immutable
BEFORE UPDATE OR DELETE ON ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_reject_evidence_mutation();

CREATE TRIGGER selected_ticket_refund_receipts_guard_immutable
BEFORE UPDATE OR DELETE ON selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_reject_evidence_mutation();

CREATE TRIGGER dr_reconciliation_checkpoints_guard_immutable
BEFORE UPDATE OR DELETE ON dr_reconciliation_checkpoints
FOR EACH ROW EXECUTE FUNCTION booking_shard_reject_evidence_mutation();

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
            'payment_compensation_receipts',
			'ticket_refund_prepare_receipts',
            'ticket_refund_compensation_receipts',
            'selected_ticket_refund_receipts'
        )
    );

CREATE FUNCTION booking_shard_capture_ticket_refund_mutation()
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
			'ticket_refund_prepare_receipts',
            'ticket_refund_compensation_receipts',
            'selected_ticket_refund_receipts'
       ) THEN
        RAISE EXCEPTION 'unsupported ticket-refund capture table %.%',
            TG_TABLE_SCHEMA, TG_TABLE_NAME
            USING ERRCODE = '0A000';
    END IF;

    IF TG_OP = 'DELETE' THEN
        source_row := to_jsonb(OLD);
    ELSE
        source_row := to_jsonb(NEW);
    END IF;
    captured_train_run_id := (source_row ->> 'train_run_id')::uuid;
    captured_generation := (source_row ->> 'assignment_generation')::bigint;
    captured_entity_id := (source_row ->> 'id')::uuid;

    SELECT migration_id, capture_enabled, source_generation
      INTO captured_migration_id, state_enabled, state_generation
      FROM public.migration_capture_state
     WHERE train_run_id = captured_train_run_id
     FOR UPDATE;

    IF NOT FOUND OR NOT state_enabled THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF captured_generation <> state_generation THEN
        RAISE EXCEPTION 'ticket-refund capture generation mismatch for train run %',
            captured_train_run_id USING ERRCODE = '40001';
    END IF;

    UPDATE public.migration_capture_state
       SET next_sequence = next_sequence + 1
     WHERE train_run_id = captured_train_run_id
     RETURNING next_sequence INTO captured_sequence;

    captured_metadata := CASE TG_TABLE_NAME
		WHEN 'ticket_refund_prepare_receipts' THEN jsonb_build_object(
			'refund_request_id', source_row -> 'refund_request_id',
			'refund_operation_id', source_row -> 'refund_operation_id',
			'payment_intent_id', source_row -> 'payment_intent_id',
			'reservation_id', source_row -> 'reservation_id',
			'ticket_order_id', source_row -> 'ticket_order_id',
			'amount_minor', source_row -> 'amount_minor',
			'currency', source_row -> 'currency',
			'state', source_row -> 'state'
		)
        WHEN 'ticket_refund_compensation_receipts' THEN jsonb_build_object(
            'refund_request_id', source_row -> 'refund_request_id',
            'refund_operation_id', source_row -> 'refund_operation_id',
            'payment_intent_id', source_row -> 'payment_intent_id',
            'reservation_id', source_row -> 'reservation_id',
            'ticket_order_id', source_row -> 'ticket_order_id',
            'amount_minor', source_row -> 'amount_minor',
            'currency', source_row -> 'currency',
            'selected_ticket_count', source_row -> 'selected_ticket_count',
            'released_seat_count', source_row -> 'released_seat_count',
            'resulting_active_ticket_count', source_row -> 'resulting_active_ticket_count',
            'resulting_order_state', source_row -> 'resulting_order_state'
        )
        WHEN 'selected_ticket_refund_receipts' THEN jsonb_build_object(
            'compensation_receipt_id', source_row -> 'compensation_receipt_id',
            'refund_request_id', source_row -> 'refund_request_id',
            'ticket_id', source_row -> 'ticket_id',
            'reservation_seat_id', source_row -> 'reservation_seat_id',
            'fare_amount_minor', source_row -> 'fare_amount_minor',
            'currency', source_row -> 'currency'
        )
    END;

    INSERT INTO public.train_run_mutation_journal(
        migration_id, train_run_id, source_generation, mutation_sequence,
        table_name, operation, entity_id, primary_key, metadata
    ) VALUES (
        captured_migration_id, captured_train_run_id, captured_generation,
        captured_sequence, TG_TABLE_NAME, TG_OP, captured_entity_id,
        jsonb_build_object('id', captured_entity_id),
        jsonb_strip_nulls(captured_metadata)
    );

    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ticket_refund_prepare_receipts_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_ticket_refund_mutation();

CREATE TRIGGER ticket_refund_compensation_receipts_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_ticket_refund_mutation();

CREATE TRIGGER selected_ticket_refund_receipts_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_ticket_refund_mutation();

-- Every physical-shard DML path must carry bounded deployment identity in the
-- same local transaction. Callers set these values with SET LOCAL; missing,
-- passive, stale-epoch, or disabled contexts fail closed before row mutation.
CREATE FUNCTION booking_shard_guard_regional_application_write()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $booking_shard_regional_application_write_guard$
DECLARE
    deployment_region text;
    deployment_role text;
    deployment_epoch_text text;
    deployment_epoch bigint;
    deployment_writes_enabled text;
    authority_region text;
    authority_epoch bigint;
    authority_state text;
    authority_writes_enabled boolean;
BEGIN
    deployment_region := current_setting(
        'railway.deployment_region', true
    );
    deployment_role := current_setting(
        'railway.deployment_role', true
    );
    deployment_epoch_text := current_setting(
        'railway.region_epoch', true
    );
    deployment_writes_enabled := current_setting(
        'railway.regional_writes_enabled', true
    );
    IF deployment_region NOT IN ('region-a', 'region-b')
       OR deployment_role <> 'active'
       OR deployment_writes_enabled <> 'true'
       OR deployment_epoch_text IS NULL
       OR deployment_epoch_text !~ '^[1-9][0-9]{0,18}$' THEN
        RAISE EXCEPTION 'regional application write context is absent or disabled'
            USING ERRCODE = '55000';
    END IF;
    BEGIN
        deployment_epoch := deployment_epoch_text::bigint;
    EXCEPTION WHEN numeric_value_out_of_range THEN
        RAISE EXCEPTION 'regional application epoch is out of range'
            USING ERRCODE = '55000';
    END;

    SELECT authority.region, authority.epoch, authority.state,
           authority.writes_enabled
      INTO authority_region, authority_epoch, authority_state,
           authority_writes_enabled
      FROM public.lock_regional_write_authority() AS authority;
    IF NOT FOUND
       OR authority_state <> 'active'
       OR NOT authority_writes_enabled
       OR deployment_region <> authority_region
       OR deployment_epoch <> authority_epoch THEN
        RAISE EXCEPTION 'regional application write is fenced'
            USING ERRCODE = '55000';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$booking_shard_regional_application_write_guard$;

-- DR reconciliation may run before customer writes are enabled. The recovery
-- exception is attached only to its append-only checkpoint relation.
CREATE FUNCTION booking_shard_guard_regional_operational_write()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $booking_shard_regional_operational_write_guard$
DECLARE
    deployment_region text;
    deployment_role text;
    deployment_epoch_text text;
    deployment_epoch bigint;
    deployment_writes_enabled text;
    authority_region text;
    authority_epoch bigint;
    authority_state text;
    authority_writes_enabled boolean;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'DR reconciliation checkpoint is append-only'
            USING ERRCODE = '23514';
    END IF;
    deployment_region := current_setting(
        'railway.deployment_region', true
    );
    deployment_role := current_setting(
        'railway.deployment_role', true
    );
    deployment_epoch_text := current_setting(
        'railway.region_epoch', true
    );
    deployment_writes_enabled := current_setting(
        'railway.regional_writes_enabled', true
    );
    IF deployment_region NOT IN ('region-a', 'region-b')
       OR deployment_role NOT IN ('active', 'recovery')
       OR deployment_epoch_text IS NULL
       OR deployment_epoch_text !~ '^[1-9][0-9]{0,18}$'
       OR (
           deployment_role = 'active'
           AND deployment_writes_enabled <> 'true'
       )
       OR (
           deployment_role = 'recovery'
           AND deployment_writes_enabled <> 'false'
       ) THEN
        RAISE EXCEPTION 'regional operational write context is invalid'
            USING ERRCODE = '55000';
    END IF;
    BEGIN
        deployment_epoch := deployment_epoch_text::bigint;
    EXCEPTION WHEN numeric_value_out_of_range THEN
        RAISE EXCEPTION 'regional operational epoch is out of range'
            USING ERRCODE = '55000';
    END;
    SELECT authority.region, authority.epoch, authority.state,
           authority.writes_enabled
      INTO authority_region, authority_epoch, authority_state,
           authority_writes_enabled
      FROM public.lock_regional_write_authority() AS authority;
    IF NOT FOUND OR (
        deployment_role = 'active'
        AND (
            authority_state <> 'active'
            OR NOT authority_writes_enabled
            OR deployment_region <> authority_region
            OR deployment_epoch <> authority_epoch
        )
    ) OR (
        deployment_role = 'recovery'
        AND deployment_epoch < authority_epoch
    ) THEN
        RAISE EXCEPTION 'regional operational write is fenced'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.region <> deployment_region OR NEW.epoch <> deployment_epoch THEN
        RAISE EXCEPTION 'DR reconciliation checkpoint is not bound to deployment'
            USING ERRCODE = '55000';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$booking_shard_regional_operational_write_guard$;

CREATE FUNCTION booking_shard_guard_regional_authority_command()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $booking_shard_regional_authority_command_guard$
DECLARE
    deployment_region text;
    deployment_role text;
    deployment_epoch_text text;
    deployment_epoch bigint;
    deployment_writes_enabled text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'regional authority cannot be deleted'
            USING ERRCODE = '23514';
    END IF;
    deployment_region := current_setting(
        'railway.deployment_region', true
    );
    deployment_role := current_setting(
        'railway.deployment_role', true
    );
    deployment_epoch_text := current_setting(
        'railway.region_epoch', true
    );
    deployment_writes_enabled := current_setting(
        'railway.regional_writes_enabled', true
    );
    IF deployment_region NOT IN ('region-a', 'region-b')
       OR deployment_role <> 'recovery'
       OR deployment_writes_enabled <> 'false'
       OR deployment_epoch_text IS NULL
       OR deployment_epoch_text !~ '^[1-9][0-9]{0,18}$' THEN
        RAISE EXCEPTION 'regional authority command requires recovery context'
            USING ERRCODE = '55000';
    END IF;
    BEGIN
        deployment_epoch := deployment_epoch_text::bigint;
    EXCEPTION WHEN numeric_value_out_of_range THEN
        RAISE EXCEPTION 'regional authority command epoch is out of range'
            USING ERRCODE = '55000';
    END;
    IF NEW.region <> deployment_region OR NEW.epoch <> deployment_epoch THEN
        RAISE EXCEPTION 'regional authority command does not match deployment'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$booking_shard_regional_authority_command_guard$;

CREATE TRIGGER regional_write_context_guard
BEFORE INSERT OR UPDATE OR DELETE ON dr_reconciliation_checkpoints
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_regional_operational_write();

CREATE TRIGGER regional_write_context_guard
BEFORE UPDATE OR DELETE ON regional_write_authority
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_regional_authority_command();

DO $install_booking_shard_regional_application_guards$
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
           AND table_row.relname NOT IN (
               'schema_migrations', 'regional_write_authority',
               'dr_reconciliation_checkpoints'
           )
         ORDER BY table_row.relname
    LOOP
        EXECUTE format(
            'CREATE TRIGGER regional_write_context_guard '
            'BEFORE INSERT OR UPDATE OR DELETE ON public.%I '
            'FOR EACH ROW EXECUTE FUNCTION '
            'public.booking_shard_guard_regional_application_write()',
            guarded_relation.table_name
        );
    END LOOP;
END;
$install_booking_shard_regional_application_guards$;

CREATE FUNCTION booking_shard_reject_regional_truncate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $booking_shard_reject_regional_truncate$
BEGIN
    RAISE EXCEPTION 'regional tables cannot be truncated'
        USING ERRCODE = '23514';
END;
$booking_shard_reject_regional_truncate$;

DO $install_booking_shard_regional_truncate_guards$
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
           AND table_row.relname <> 'schema_migrations'
         ORDER BY table_row.relname
    LOOP
        EXECUTE format(
            'CREATE TRIGGER regional_truncate_guard '
            'BEFORE TRUNCATE ON public.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION '
            'public.booking_shard_reject_regional_truncate()',
            guarded_relation.table_name
        );
    END LOOP;
END;
$install_booking_shard_regional_truncate_guards$;

COMMIT;
