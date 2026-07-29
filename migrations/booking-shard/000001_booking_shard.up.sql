BEGIN;

-- This migration is applied to an independent physical booking database.
-- UUIDs that identify control-plane rows are intentionally not foreign keys:
-- no cross-database referential constraint is assumed.

CREATE FUNCTION booking_shard_set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TABLE train_run_booking_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    train_id uuid NOT NULL,
    route_id uuid NOT NULL,
    service_date date NOT NULL,
    segment_count integer NOT NULL CHECK (segment_count > 0),
    route_version bigint NOT NULL CHECK (route_version > 0),
    booking_policy_version bigint NOT NULL CHECK (booking_policy_version > 0),
    source_version bigint NOT NULL CHECK (source_version > 0),
    status text NOT NULL CHECK (
        status IN ('scheduled', 'boarding', 'departed', 'cancelled', 'completed')
    ),
    bookable boolean NOT NULL DEFAULT false,
    active boolean NOT NULL DEFAULT true,
    source_updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (train_run_id, assignment_generation),
    UNIQUE (train_run_id, assignment_generation, segment_count),
    UNIQUE (train_run_id, assignment_generation, train_id),
    CHECK (NOT bookable OR (active AND status IN ('scheduled', 'boarding')))
);

CREATE UNIQUE INDEX train_run_booking_snapshots_active_idx
    ON train_run_booking_snapshots (train_run_id)
    WHERE active;

CREATE INDEX train_run_booking_snapshots_service_idx
    ON train_run_booking_snapshots (service_date, status, train_run_id);

CREATE TRIGGER train_run_booking_snapshots_set_updated_at
BEFORE UPDATE ON train_run_booking_snapshots
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE booking_seat_catalog (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    train_id uuid NOT NULL,
    coach_id uuid NOT NULL,
    seat_id uuid NOT NULL,
    coach_order integer NOT NULL CHECK (coach_order >= 0),
    seat_order integer NOT NULL CHECK (seat_order >= 0),
    seat_class text NOT NULL
        CHECK (seat_class IN ('standard', 'business', 'first')),
    active boolean NOT NULL DEFAULT true,
    source_version bigint NOT NULL CHECK (source_version > 0),
    source_updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (train_run_id, assignment_generation, seat_id),
    UNIQUE (train_run_id, assignment_generation, seat_id, seat_class),
    UNIQUE (train_run_id, assignment_generation, coach_order, seat_order),
    FOREIGN KEY (train_run_id, assignment_generation, train_id)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation, train_id
        ) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX booking_seat_catalog_allocation_idx
    ON booking_seat_catalog (
        train_run_id, assignment_generation, seat_class, active, coach_order,
        seat_order, seat_id
    );

CREATE TRIGGER booking_seat_catalog_set_updated_at
BEFORE UPDATE ON booking_seat_catalog
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE booking_fare_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    segment_count integer NOT NULL CHECK (segment_count > 0),
    from_stop_index integer NOT NULL CHECK (from_stop_index >= 0),
    to_stop_index integer NOT NULL,
    seat_class text NOT NULL
        CHECK (seat_class IN ('standard', 'business', 'first')),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    source_version bigint NOT NULL CHECK (source_version > 0),
    active boolean NOT NULL DEFAULT true,
    source_updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, train_run_id, assignment_generation),
    FOREIGN KEY (train_run_id, assignment_generation, segment_count)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation, segment_count
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (from_stop_index < to_stop_index),
    CHECK (to_stop_index <= segment_count)
);

CREATE UNIQUE INDEX booking_fare_snapshots_active_version_unique_idx
    ON booking_fare_snapshots (
        train_run_id, assignment_generation, from_stop_index, to_stop_index,
        seat_class, source_version
    )
    WHERE active;

CREATE INDEX booking_fare_snapshots_lookup_idx
    ON booking_fare_snapshots (
        train_run_id, assignment_generation, seat_class, from_stop_index,
        to_stop_index, active, source_version DESC
    );

CREATE TRIGGER booking_fare_snapshots_set_updated_at
BEFORE UPDATE ON booking_fare_snapshots
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE seat_inventory (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    segment_count integer NOT NULL CHECK (segment_count > 0),
    seat_id uuid NOT NULL,
    seat_class text NOT NULL
        CHECK (seat_class IN ('standard', 'business', 'first')),
    occupied_segments bit varying NOT NULL,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (train_run_id, assignment_generation, seat_id),
    FOREIGN KEY (train_run_id, assignment_generation, segment_count)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation, segment_count
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (
        train_run_id, assignment_generation, seat_id, seat_class
    ) REFERENCES booking_seat_catalog(
        train_run_id, assignment_generation, seat_id, seat_class
    ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (bit_length(occupied_segments) = segment_count)
);

CREATE INDEX seat_inventory_allocation_idx
    ON seat_inventory (
        train_run_id, assignment_generation, seat_class, seat_id
    );

CREATE TRIGGER seat_inventory_set_updated_at
BEFORE UPDATE ON seat_inventory
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE reservations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    segment_count integer NOT NULL CHECK (segment_count > 0),
    from_stop_index integer NOT NULL CHECK (from_stop_index >= 0),
    to_stop_index integer NOT NULL,
    seat_class text NOT NULL
        CHECK (seat_class IN ('standard', 'business', 'first')),
    status text NOT NULL DEFAULT 'held'
        CHECK (status IN ('held', 'confirmed', 'expired', 'cancelled')),
    expires_at timestamptz NOT NULL,
    total_amount_minor bigint NOT NULL CHECK (total_amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, train_run_id, assignment_generation),
    UNIQUE (id, train_run_id, assignment_generation, segment_count),
    FOREIGN KEY (train_run_id, assignment_generation, segment_count)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation, segment_count
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (from_stop_index < to_stop_index),
    CHECK (to_stop_index <= segment_count),
    CHECK (expires_at > created_at)
);

CREATE INDEX reservations_owner_created_idx
    ON reservations (user_id, created_at DESC, id);

CREATE INDEX reservations_train_run_status_idx
    ON reservations (
        train_run_id, assignment_generation, status, id
    );

CREATE INDEX reservations_expiry_idx
    ON reservations (expires_at, id)
    WHERE status = 'held';

CREATE TRIGGER reservations_set_updated_at
BEFORE UPDATE ON reservations
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE reservation_seats (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    segment_count integer NOT NULL CHECK (segment_count > 0),
    seat_id uuid NOT NULL,
    passenger_id uuid NOT NULL,
    fare_snapshot_id uuid NOT NULL,
    segment_mask bit varying NOT NULL,
    fare_amount_minor bigint NOT NULL CHECK (fare_amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, train_run_id, assignment_generation),
    UNIQUE (reservation_id, seat_id),
    UNIQUE (reservation_id, passenger_id),
    FOREIGN KEY (
        reservation_id, train_run_id, assignment_generation, segment_count
    ) REFERENCES reservations(
        id, train_run_id, assignment_generation, segment_count
    ) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (train_run_id, assignment_generation, seat_id)
        REFERENCES seat_inventory(
            train_run_id, assignment_generation, seat_id
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (fare_snapshot_id, train_run_id, assignment_generation)
        REFERENCES booking_fare_snapshots(
            id, train_run_id, assignment_generation
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (bit_length(segment_mask) = segment_count),
    CHECK (bit_count(segment_mask) > 0)
);

CREATE INDEX reservation_seats_seat_idx
    ON reservation_seats (
        train_run_id, assignment_generation, seat_id, reservation_id
    );

CREATE INDEX reservation_seats_passenger_idx
    ON reservation_seats (passenger_id, reservation_id);

CREATE TRIGGER reservation_seats_set_updated_at
BEFORE UPDATE ON reservation_seats
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE ticket_orders (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id uuid NOT NULL UNIQUE,
    user_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    status text NOT NULL DEFAULT 'confirmed'
        CHECK (status IN ('confirmed', 'cancelled')),
    total_amount_minor bigint NOT NULL CHECK (total_amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, train_run_id, assignment_generation),
    FOREIGN KEY (reservation_id, train_run_id, assignment_generation)
        REFERENCES reservations(id, train_run_id, assignment_generation)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX ticket_orders_owner_created_idx
    ON ticket_orders (user_id, created_at DESC, id);

CREATE INDEX ticket_orders_train_run_idx
    ON ticket_orders (train_run_id, assignment_generation, id);

CREATE TRIGGER ticket_orders_set_updated_at
BEFORE UPDATE ON ticket_orders
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE tickets (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_order_id uuid NOT NULL,
    reservation_seat_id uuid NOT NULL UNIQUE,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    ticket_code text NOT NULL UNIQUE,
    status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'cancelled')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (ticket_order_id, train_run_id, assignment_generation)
        REFERENCES ticket_orders(id, train_run_id, assignment_generation)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (reservation_seat_id, train_run_id, assignment_generation)
        REFERENCES reservation_seats(id, train_run_id, assignment_generation)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (length(ticket_code) BETWEEN 16 AND 64)
);

CREATE INDEX tickets_order_idx ON tickets (ticket_order_id, id);

CREATE INDEX tickets_train_run_idx
    ON tickets (train_run_id, assignment_generation, id);

CREATE TRIGGER tickets_set_updated_at
BEFORE UPDATE ON tickets
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE idempotency_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    user_id uuid NOT NULL,
    operation text NOT NULL CHECK (
        operation IN (
            'reservation.create', 'reservation.confirm', 'reservation.cancel'
        )
    ),
    key_hash bytea NOT NULL CHECK (octet_length(key_hash) = 32),
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    status text NOT NULL DEFAULT 'in_progress'
        CHECK (status IN ('in_progress', 'completed')),
    resource_type text,
    resource_id uuid,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (user_id, operation, key_hash),
    FOREIGN KEY (train_run_id, assignment_generation)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (expires_at > created_at),
    CHECK (
        (status = 'in_progress'
            AND resource_type IS NULL
            AND resource_id IS NULL)
        OR
        (status = 'completed'
            AND resource_type = 'reservation'
            AND resource_id IS NOT NULL)
    )
);

CREATE INDEX idempotency_records_expiry_idx
    ON idempotency_records (expires_at, id);

CREATE INDEX idempotency_records_train_run_idx
    ON idempotency_records (
        train_run_id, assignment_generation, expires_at, id
    );

CREATE INDEX idempotency_records_resource_idx
    ON idempotency_records (resource_type, resource_id)
    WHERE status = 'completed';

CREATE TRIGGER idempotency_records_set_updated_at
BEFORE UPDATE ON idempotency_records
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE booking_command_receipts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    command_id uuid NOT NULL UNIQUE,
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    command_type text NOT NULL CHECK (
        command_type IN (
            'reservation.create', 'reservation.confirm', 'reservation.cancel',
			'train_run.cancel', 'fare.install', 'seat.disable', 'seat.enable',
			'booking_policy.bump'
        )
    ),
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    status text NOT NULL DEFAULT 'started'
        CHECK (status IN ('started', 'succeeded', 'rejected')),
    result_type text,
    result_id uuid,
    result_source_version bigint CHECK (result_source_version > 0),
    result_booking_policy_version bigint
        CHECK (result_booking_policy_version > 0),
    error_code text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (train_run_id, assignment_generation)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (error_code IS NULL OR length(error_code) BETWEEN 1 AND 64),
    CHECK (
        (status = 'started'
            AND completed_at IS NULL
            AND result_type IS NULL
            AND result_id IS NULL
            AND result_source_version IS NULL
            AND result_booking_policy_version IS NULL
            AND error_code IS NULL)
        OR
        (status = 'succeeded'
            AND completed_at IS NOT NULL
            AND result_type IN ('reservation', 'train_run', 'fare', 'seat')
            AND result_id IS NOT NULL
            AND (
                (command_type = 'booking_policy.bump'
                    AND result_source_version IS NOT NULL
                    AND result_booking_policy_version IS NOT NULL)
                OR
                (command_type IN ('fare.install', 'seat.disable', 'seat.enable')
                    AND result_source_version IS NOT NULL
                    AND result_booking_policy_version IS NULL)
                OR
                (command_type NOT IN ('fare.install', 'seat.disable', 'seat.enable', 'booking_policy.bump')
                    AND result_source_version IS NULL
                    AND result_booking_policy_version IS NULL)
            )
            AND error_code IS NULL)
        OR
        (status = 'rejected'
            AND completed_at IS NOT NULL
            AND result_type IS NULL
            AND result_id IS NULL
            AND result_source_version IS NULL
            AND result_booking_policy_version IS NULL
            AND error_code IS NOT NULL)
    )
);

CREATE INDEX booking_command_receipts_train_run_idx
    ON booking_command_receipts (
        train_run_id, assignment_generation, status, started_at, command_id
    );

CREATE TRIGGER booking_command_receipts_set_updated_at
BEFORE UPDATE ON booking_command_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    aggregate_type text NOT NULL CHECK (
        aggregate_type IN ('reservation', 'ticket', 'train_run', 'booking_command')
    ),
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL CHECK (
        length(event_type) BETWEEN 3 AND 64
        AND event_type ~ '^[a-z][a-z0-9_.-]+$'
    ),
    event_version integer NOT NULL DEFAULT 1 CHECK (event_version > 0),
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'processing', 'published', 'dead_letter')
    ),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    locked_at timestamptz,
    locked_by text,
    lease_token uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    FOREIGN KEY (train_run_id, assignment_generation)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (octet_length(payload::text) <= 65536),
    CHECK (NOT (payload ?| ARRAY[
        'passenger_name', 'email', 'identity_document', 'dsn', 'password',
        'token', 'raw_idempotency_key'
    ])),
    CHECK (locked_by IS NULL OR length(locked_by) BETWEEN 1 AND 128),
    CHECK (
        (status = 'processing'
            AND locked_at IS NOT NULL
            AND locked_by IS NOT NULL
            AND lease_token IS NOT NULL)
        OR
        (status <> 'processing')
    ),
    CHECK (published_at IS NULL OR status = 'published')
);

CREATE INDEX outbox_events_pending_idx
    ON outbox_events (next_attempt_at, created_at, id)
    WHERE status = 'pending';

CREATE INDEX outbox_events_processing_idx
    ON outbox_events (locked_at, id)
    WHERE status = 'processing';

CREATE INDEX outbox_events_aggregate_idx
    ON outbox_events (
        aggregate_type, aggregate_id, created_at, id
    );

CREATE INDEX outbox_events_train_run_idx
    ON outbox_events (
        train_run_id, assignment_generation, created_at, id
    );

CREATE TRIGGER outbox_events_set_updated_at
BEFORE UPDATE ON outbox_events
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

-- Every base-copy table uses the same keyset cursor. These indexes prevent
-- each 500-row page from rescanning and resorting the remaining train-run
-- boundary while source writes continue.
CREATE INDEX train_run_booking_snapshots_migration_cursor_idx
    ON train_run_booking_snapshots (train_run_id, assignment_generation, id);
CREATE INDEX booking_seat_catalog_migration_cursor_idx
    ON booking_seat_catalog (train_run_id, assignment_generation, id);
CREATE INDEX booking_fare_snapshots_migration_cursor_idx
    ON booking_fare_snapshots (train_run_id, assignment_generation, id);
CREATE INDEX seat_inventory_migration_cursor_idx
    ON seat_inventory (train_run_id, assignment_generation, id);
CREATE INDEX reservations_migration_cursor_idx
    ON reservations (train_run_id, assignment_generation, id);
CREATE INDEX reservation_seats_migration_cursor_idx
    ON reservation_seats (train_run_id, assignment_generation, id);
CREATE INDEX ticket_orders_migration_cursor_idx
    ON ticket_orders (train_run_id, assignment_generation, id);
CREATE INDEX tickets_migration_cursor_idx
    ON tickets (train_run_id, assignment_generation, id);
CREATE INDEX idempotency_records_migration_cursor_idx
    ON idempotency_records (train_run_id, assignment_generation, id);
CREATE INDEX booking_command_receipts_migration_cursor_idx
    ON booking_command_receipts (train_run_id, assignment_generation, id);
CREATE INDEX outbox_events_migration_cursor_idx
    ON outbox_events (train_run_id, assignment_generation, id);

-- Migration tooling stages bounded normalized outbox batches here, then
-- atomically promotes the complete replacement. The authoritative outbox is
-- never partially replaced and retries can discard incomplete staging safely.
CREATE TABLE migration_outbox_staging (
    migration_id uuid NOT NULL,
    source_event_id uuid NOT NULL,
    row_data jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (migration_id, source_event_id),
    CHECK (jsonb_typeof(row_data) = 'object'),
    CHECK (octet_length(row_data::text) <= 131072)
);

CREATE INDEX migration_outbox_staging_created_at_idx
    ON migration_outbox_staging (created_at, migration_id);

CREATE TABLE train_run_write_fences (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL UNIQUE,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    state text NOT NULL DEFAULT 'standby' CHECK (
        state IN ('standby', 'active', 'quiescing', 'disabled', 'retained')
    ),
    write_enabled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (train_run_id, assignment_generation)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (NOT write_enabled OR state = 'active')
);

CREATE INDEX train_run_write_fences_generation_idx
    ON train_run_write_fences (
        train_run_id, assignment_generation, write_enabled
    );

CREATE FUNCTION booking_shard_guard_fence_generation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.id <> OLD.id OR NEW.train_run_id <> OLD.train_run_id THEN
        RAISE EXCEPTION 'train-run fence identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.assignment_generation < OLD.assignment_generation THEN
        RAISE EXCEPTION 'train-run fence generation cannot decrease'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER train_run_write_fences_guard_generation
BEFORE UPDATE ON train_run_write_fences
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_fence_generation();

CREATE TRIGGER train_run_write_fences_set_updated_at
BEFORE UPDATE ON train_run_write_fences
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE train_run_target_write_evidence (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    successful_write_count bigint NOT NULL DEFAULT 0
        CHECK (successful_write_count >= 0),
    first_successful_write_at timestamptz,
    last_successful_write_at timestamptz,
    last_command_id uuid,
    baseline_initialized boolean NOT NULL DEFAULT false,
    baseline_reservation_count bigint NOT NULL DEFAULT 0
        CHECK (baseline_reservation_count >= 0),
    baseline_command_receipt_count bigint NOT NULL DEFAULT 0
        CHECK (baseline_command_receipt_count >= 0),
    baseline_outbox_count bigint NOT NULL DEFAULT 0
        CHECK (baseline_outbox_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (train_run_id, assignment_generation),
    FOREIGN KEY (train_run_id, assignment_generation)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (
        (successful_write_count = 0
            AND first_successful_write_at IS NULL
            AND last_successful_write_at IS NULL
            AND last_command_id IS NULL)
        OR
        (successful_write_count > 0
            AND first_successful_write_at IS NOT NULL
            AND last_successful_write_at IS NOT NULL
            AND last_successful_write_at >= first_successful_write_at)
    )
);

CREATE FUNCTION booking_shard_guard_target_write_evidence()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.id <> OLD.id
       OR NEW.train_run_id <> OLD.train_run_id
       OR NEW.assignment_generation < OLD.assignment_generation THEN
        RAISE EXCEPTION 'target-write evidence identity is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.baseline_initialized AND (
        NOT NEW.baseline_initialized
        OR NEW.baseline_reservation_count <> OLD.baseline_reservation_count
        OR NEW.baseline_command_receipt_count <> OLD.baseline_command_receipt_count
        OR NEW.baseline_outbox_count <> OLD.baseline_outbox_count
    ) THEN
        RAISE EXCEPTION 'target-write evidence baseline is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.successful_write_count < OLD.successful_write_count THEN
        RAISE EXCEPTION 'target-write count cannot decrease'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.first_successful_write_at IS DISTINCT FROM OLD.first_successful_write_at
       AND OLD.first_successful_write_at IS NOT NULL THEN
        RAISE EXCEPTION 'first target-write timestamp is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.last_successful_write_at IS NOT NULL
       AND NEW.last_successful_write_at < OLD.last_successful_write_at THEN
        RAISE EXCEPTION 'last target-write timestamp cannot decrease'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER train_run_target_write_evidence_guard
BEFORE UPDATE ON train_run_target_write_evidence
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_target_write_evidence();

CREATE TRIGGER train_run_target_write_evidence_set_updated_at
BEFORE UPDATE ON train_run_target_write_evidence
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

-- One stable row per train run avoids session-variable capture state. The row
-- must be installed disabled before capture activation. Every authoritative
-- mutation trigger locks it before checking capture_enabled, so activation
-- waits for older mutation transactions and cannot miss a late commit.
CREATE TABLE migration_capture_state (
    train_run_id uuid PRIMARY KEY,
    migration_id uuid NOT NULL UNIQUE,
    source_generation bigint NOT NULL CHECK (source_generation > 0),
    capture_enabled boolean NOT NULL DEFAULT false,
    next_sequence bigint NOT NULL DEFAULT 0 CHECK (next_sequence >= 0),
    enabled_at timestamptz,
    disabled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (train_run_id, source_generation)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (NOT capture_enabled OR (enabled_at IS NOT NULL AND disabled_at IS NULL))
);

CREATE INDEX migration_capture_state_enabled_idx
    ON migration_capture_state (
        capture_enabled, train_run_id, source_generation
    );

CREATE FUNCTION booking_shard_guard_capture_state()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.train_run_id <> OLD.train_run_id THEN
        RAISE EXCEPTION 'capture train-run identity is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.migration_id <> OLD.migration_id THEN
        IF OLD.capture_enabled
           OR NEW.capture_enabled
           OR NEW.next_sequence <> 0 THEN
            RAISE EXCEPTION 'capture migration reset requires disabled state and zero sequence'
                USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.next_sequence < OLD.next_sequence THEN
        RAISE EXCEPTION 'capture sequence cannot decrease'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER migration_capture_state_guard
BEFORE UPDATE ON migration_capture_state
FOR EACH ROW EXECUTE FUNCTION booking_shard_guard_capture_state();

CREATE TRIGGER migration_capture_state_set_updated_at
BEFORE UPDATE ON migration_capture_state
FOR EACH ROW EXECUTE FUNCTION booking_shard_set_updated_at();

CREATE TABLE train_run_mutation_journal (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    migration_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    source_generation bigint NOT NULL CHECK (source_generation > 0),
    mutation_sequence bigint NOT NULL CHECK (mutation_sequence > 0),
    table_name text NOT NULL CHECK (table_name IN (
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
    )),
    operation text NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    entity_id uuid NOT NULL,
    primary_key jsonb NOT NULL,
    row_version bigint CHECK (row_version IS NULL OR row_version >= 0),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (migration_id, mutation_sequence),
    UNIQUE (migration_id, id),
    FOREIGN KEY (train_run_id, source_generation)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation
        ) ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (jsonb_typeof(primary_key) = 'object'),
    CHECK (jsonb_typeof(metadata) = 'object'),
    CHECK (octet_length(primary_key::text) <= 1024),
    CHECK (octet_length(metadata::text) <= 4096),
    CHECK (NOT (metadata ?| ARRAY[
        'user_id', 'passenger_id', 'passenger_name', 'email',
        'identity_document', 'key_hash', 'request_fingerprint', 'dsn',
        'password', 'token', 'payload', 'occupied_segments', 'segment_mask'
    ]))
);

CREATE INDEX train_run_mutation_journal_replay_idx
    ON train_run_mutation_journal (
        migration_id, mutation_sequence, id
    );

CREATE INDEX train_run_mutation_journal_train_run_idx
    ON train_run_mutation_journal (
        train_run_id, source_generation, mutation_sequence
    );

-- These receipts live on the target. They intentionally do not reference the
-- source journal, which is in another PostgreSQL database.
CREATE TABLE migration_apply_receipts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    migration_id uuid NOT NULL,
    source_journal_id uuid NOT NULL,
    train_run_id uuid NOT NULL,
    target_generation bigint NOT NULL CHECK (target_generation > 0),
    mutation_sequence bigint NOT NULL CHECK (mutation_sequence > 0),
    apply_fingerprint bytea NOT NULL CHECK (octet_length(apply_fingerprint) = 32),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (migration_id, source_journal_id),
    UNIQUE (migration_id, mutation_sequence),
    FOREIGN KEY (train_run_id, target_generation)
        REFERENCES train_run_booking_snapshots(
            train_run_id, assignment_generation
        ) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX migration_apply_receipts_train_run_idx
    ON migration_apply_receipts (
        train_run_id, target_generation, mutation_sequence
    );

CREATE FUNCTION booking_shard_capture_mutation()
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
    captured_row_version bigint;
    captured_sequence bigint;
    captured_migration_id uuid;
    captured_metadata jsonb;
    state_enabled boolean;
    state_generation bigint;
BEGIN
    IF TG_TABLE_SCHEMA <> 'public'
       OR TG_TABLE_NAME NOT IN (
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
       ) THEN
        RAISE EXCEPTION 'unsupported mutation-capture table %.%',
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
    captured_row_version := NULLIF(source_row ->> 'version', '')::bigint;

    -- Lock the stable row even while disabled. This is deliberately table
    -- state rather than a session GUC, so pooled sessions cannot leak capture
    -- mode between requests.
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
        RAISE EXCEPTION 'capture generation mismatch for train run %',
            captured_train_run_id
            USING ERRCODE = '40001';
    END IF;

    UPDATE public.migration_capture_state
       SET next_sequence = next_sequence + 1
     WHERE train_run_id = captured_train_run_id
     RETURNING next_sequence INTO captured_sequence;

    captured_metadata := CASE TG_TABLE_NAME
        WHEN 'train_run_booking_snapshots' THEN jsonb_build_object(
            'source_version', source_row -> 'source_version',
            'status', source_row -> 'status',
            'bookable', source_row -> 'bookable',
            'active', source_row -> 'active'
        )
        WHEN 'booking_seat_catalog' THEN jsonb_build_object(
            'source_version', source_row -> 'source_version',
            'seat_class', source_row -> 'seat_class',
            'active', source_row -> 'active'
        )
        WHEN 'booking_fare_snapshots' THEN jsonb_build_object(
            'source_version', source_row -> 'source_version',
            'seat_class', source_row -> 'seat_class',
            'active', source_row -> 'active'
        )
        WHEN 'seat_inventory' THEN jsonb_build_object(
            'version', source_row -> 'version',
            'segment_count', source_row -> 'segment_count'
        )
        WHEN 'reservations' THEN jsonb_build_object(
            'status', source_row -> 'status',
            'segment_count', source_row -> 'segment_count'
        )
        WHEN 'reservation_seats' THEN jsonb_build_object(
            'segment_count', source_row -> 'segment_count'
        )
        WHEN 'ticket_orders' THEN jsonb_build_object(
            'status', source_row -> 'status'
        )
        WHEN 'tickets' THEN jsonb_build_object(
            'status', source_row -> 'status'
        )
        WHEN 'idempotency_records' THEN jsonb_build_object(
            'operation', source_row -> 'operation',
            'status', source_row -> 'status',
            'resource_type', source_row -> 'resource_type',
            'resource_id', source_row -> 'resource_id'
        )
        WHEN 'booking_command_receipts' THEN jsonb_build_object(
            'command_type', source_row -> 'command_type',
            'status', source_row -> 'status',
            'result_type', source_row -> 'result_type',
            'result_id', source_row -> 'result_id',
            'result_source_version', source_row -> 'result_source_version',
            'result_booking_policy_version',
                source_row -> 'result_booking_policy_version',
            'error_code', source_row -> 'error_code'
        )
    END;

    captured_metadata := jsonb_strip_nulls(captured_metadata);

    INSERT INTO public.train_run_mutation_journal (
        migration_id,
        train_run_id,
        source_generation,
        mutation_sequence,
        table_name,
        operation,
        entity_id,
        primary_key,
        row_version,
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
        captured_row_version,
        captured_metadata
    );

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER train_run_booking_snapshots_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON train_run_booking_snapshots
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

CREATE TRIGGER booking_seat_catalog_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON booking_seat_catalog
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

CREATE TRIGGER booking_fare_snapshots_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON booking_fare_snapshots
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

CREATE TRIGGER seat_inventory_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON seat_inventory
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

CREATE TRIGGER reservations_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON reservations
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

CREATE TRIGGER reservation_seats_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON reservation_seats
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

CREATE TRIGGER ticket_orders_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON ticket_orders
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

CREATE TRIGGER tickets_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON tickets
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

CREATE TRIGGER idempotency_records_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON idempotency_records
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

CREATE TRIGGER booking_command_receipts_capture_mutation
AFTER INSERT OR UPDATE OR DELETE ON booking_command_receipts
FOR EACH ROW EXECUTE FUNCTION booking_shard_capture_mutation();

COMMIT;
