BEGIN;

CREATE TABLE seat_inventory (
    train_run_id uuid NOT NULL,
    segment_count integer NOT NULL,
    seat_id uuid NOT NULL REFERENCES seats(id) ON DELETE RESTRICT,
    seat_class text NOT NULL
        CHECK (seat_class IN ('standard', 'business', 'first')),
    occupied_segments bit varying NOT NULL,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (train_run_id, seat_id),
    FOREIGN KEY (train_run_id, segment_count)
        REFERENCES train_runs(id, segment_count) ON DELETE CASCADE,
    CHECK (segment_count > 0),
    CHECK (bit_length(occupied_segments) = segment_count)
);

CREATE INDEX seat_inventory_allocation_idx
    ON seat_inventory (train_run_id, seat_class, seat_id);

CREATE TRIGGER seat_inventory_set_updated_at
BEFORE UPDATE ON seat_inventory
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE FUNCTION validate_inventory_seat_class()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM seats AS s
        JOIN coaches AS c ON c.id = s.coach_id
        WHERE s.id = NEW.seat_id
          AND s.active
          AND c.seat_class = NEW.seat_class
    ) THEN
        RAISE EXCEPTION 'inventory seat must be active and match its coach class'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER seat_inventory_validate_class
BEFORE INSERT OR UPDATE OF seat_id, seat_class ON seat_inventory
FOR EACH ROW EXECUTE FUNCTION validate_inventory_seat_class();

CREATE TABLE reservations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    train_run_id uuid NOT NULL,
    segment_count integer NOT NULL,
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
    FOREIGN KEY (train_run_id, segment_count)
        REFERENCES train_runs(id, segment_count) ON DELETE RESTRICT,
    UNIQUE (id, segment_count),
    CHECK (segment_count > 0),
    CHECK (from_stop_index < to_stop_index),
    CHECK (to_stop_index <= segment_count),
    CHECK (expires_at > created_at)
);

CREATE INDEX reservations_owner_created_idx
    ON reservations (user_id, created_at DESC, id);
CREATE INDEX reservations_train_run_status_idx
    ON reservations (train_run_id, status, id);
CREATE INDEX reservations_expiry_idx
    ON reservations (expires_at, id)
    WHERE status = 'held';

CREATE TRIGGER reservations_set_updated_at
BEFORE UPDATE ON reservations
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE reservation_seats (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id uuid NOT NULL,
    segment_count integer NOT NULL,
    seat_id uuid NOT NULL REFERENCES seats(id) ON DELETE RESTRICT,
    passenger_id uuid NOT NULL REFERENCES passengers(id) ON DELETE RESTRICT,
    segment_mask bit varying NOT NULL,
    fare_amount_minor bigint NOT NULL CHECK (fare_amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (reservation_id, segment_count)
        REFERENCES reservations(id, segment_count) ON DELETE CASCADE,
    UNIQUE (reservation_id, seat_id),
    UNIQUE (reservation_id, passenger_id),
    CHECK (segment_count > 0),
    CHECK (bit_length(segment_mask) = segment_count),
    CHECK (bit_count(segment_mask) > 0)
);

CREATE INDEX reservation_seats_seat_idx
    ON reservation_seats (seat_id, reservation_id);
CREATE INDEX reservation_seats_passenger_idx
    ON reservation_seats (passenger_id, reservation_id);

CREATE FUNCTION validate_reservation_seat()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM reservations AS r
        JOIN passengers AS p
          ON p.id = NEW.passenger_id
         AND p.user_id = r.user_id
        JOIN seat_inventory AS si
          ON si.train_run_id = r.train_run_id
         AND si.seat_id = NEW.seat_id
         AND si.segment_count = NEW.segment_count
         AND si.seat_class = r.seat_class
        WHERE r.id = NEW.reservation_id
          AND r.segment_count = NEW.segment_count
          AND NEW.segment_mask = repeat('0', r.from_stop_index)::bit varying
                                 || repeat('1', r.to_stop_index - r.from_stop_index)::bit varying
                                 || repeat('0', r.segment_count - r.to_stop_index)::bit varying
          AND NEW.currency = r.currency
    ) THEN
        RAISE EXCEPTION 'reservation seat violates ownership, class, inventory, mask, or currency invariants'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER reservation_seats_validate
AFTER INSERT OR UPDATE ON reservation_seats
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_reservation_seat();

CREATE TABLE ticket_orders (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id uuid NOT NULL UNIQUE REFERENCES reservations(id) ON DELETE RESTRICT,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    status text NOT NULL DEFAULT 'confirmed'
        CHECK (status IN ('confirmed', 'cancelled')),
    total_amount_minor bigint NOT NULL CHECK (total_amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX ticket_orders_owner_created_idx
    ON ticket_orders (user_id, created_at DESC, id);

CREATE TRIGGER ticket_orders_set_updated_at
BEFORE UPDATE ON ticket_orders
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE tickets (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_order_id uuid NOT NULL REFERENCES ticket_orders(id) ON DELETE RESTRICT,
    reservation_seat_id uuid NOT NULL UNIQUE REFERENCES reservation_seats(id) ON DELETE RESTRICT,
    ticket_code text NOT NULL UNIQUE,
    status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'cancelled')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (length(ticket_code) BETWEEN 16 AND 64)
);

CREATE INDEX tickets_order_idx ON tickets (ticket_order_id, id);

CREATE TRIGGER tickets_set_updated_at
BEFORE UPDATE ON tickets
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
