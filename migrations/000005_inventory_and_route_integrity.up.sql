BEGIN;

CREATE OR REPLACE FUNCTION validate_inventory_seat_class()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM seats AS s
        JOIN coaches AS c ON c.id = s.coach_id
        JOIN train_runs AS tr
          ON tr.id = NEW.train_run_id
         AND tr.train_id = c.train_id
        WHERE s.id = NEW.seat_id
          AND s.active
          AND c.seat_class = NEW.seat_class
    ) THEN
        RAISE EXCEPTION 'inventory seat must belong to the run train, be active, and match its coach class'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM seat_inventory AS si
        JOIN train_runs AS tr ON tr.id = si.train_run_id
        JOIN seats AS s ON s.id = si.seat_id
        JOIN coaches AS c ON c.id = s.coach_id
        WHERE c.seat_class <> si.seat_class
           OR c.train_id <> tr.train_id
    ) THEN
        RAISE EXCEPTION 'existing inventory seat violates run train or class integrity'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

DROP TRIGGER seat_inventory_validate_class ON seat_inventory;
CREATE TRIGGER seat_inventory_validate_class
BEFORE INSERT OR UPDATE OF train_run_id, seat_id, seat_class ON seat_inventory
FOR EACH ROW EXECUTE FUNCTION validate_inventory_seat_class();

ALTER TABLE reservations
    ADD CONSTRAINT reservations_id_train_run_segment_count_key
    UNIQUE (id, train_run_id, segment_count);

ALTER TABLE reservation_seats
    ADD COLUMN train_run_id uuid;

-- Keep the expand migration compatible with version-4 application instances
-- during a rolling deploy. New writers provide train_run_id directly; an old
-- writer derives it from the authoritative reservation row.
CREATE FUNCTION populate_reservation_seat_train_run_id()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.train_run_id IS NULL THEN
        SELECT train_run_id
        INTO NEW.train_run_id
        FROM reservations
        WHERE id = NEW.reservation_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER reservation_seats_populate_train_run_id
BEFORE INSERT OR UPDATE OF reservation_id, train_run_id ON reservation_seats
FOR EACH ROW EXECUTE FUNCTION populate_reservation_seat_train_run_id();

UPDATE reservation_seats AS rs
SET train_run_id = r.train_run_id
FROM reservations AS r
WHERE r.id = rs.reservation_id;

-- The backfill queues the existing deferred reservation-seat validation
-- trigger. Drain it before changing constraints on the same table.
SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE reservation_seats
    ALTER COLUMN train_run_id SET NOT NULL,
    DROP CONSTRAINT reservation_seats_reservation_id_segment_count_fkey,
    ADD CONSTRAINT reservation_seats_reservation_run_segment_fkey
        FOREIGN KEY (reservation_id, train_run_id, segment_count)
        REFERENCES reservations(id, train_run_id, segment_count) ON DELETE CASCADE,
    ADD CONSTRAINT reservation_seats_inventory_fkey
        FOREIGN KEY (train_run_id, seat_id)
        REFERENCES seat_inventory(train_run_id, seat_id) ON DELETE RESTRICT;

CREATE OR REPLACE FUNCTION validate_route_stop_sequence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_route_id uuid;
    affected_route_ids uuid[];
BEGIN
    IF TG_OP = 'DELETE' THEN
        affected_route_ids := ARRAY[OLD.route_id];
    ELSIF TG_OP = 'UPDATE' AND OLD.route_id IS DISTINCT FROM NEW.route_id THEN
        affected_route_ids := ARRAY[OLD.route_id, NEW.route_id];
    ELSE
        affected_route_ids := ARRAY[NEW.route_id];
    END IF;

    FOREACH affected_route_id IN ARRAY affected_route_ids LOOP
        IF EXISTS (
            SELECT 1
            FROM (
                SELECT stop_index,
                       row_number() OVER (ORDER BY stop_index) - 1 AS expected_index
                FROM route_stops
                WHERE route_id = affected_route_id
            ) AS ordered
            WHERE stop_index <> expected_index
        ) THEN
            RAISE EXCEPTION 'route stops must be contiguous and zero-based'
                USING ERRCODE = '23514';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM (
                SELECT arrival_offset_minutes,
                       departure_offset_minutes,
                       lag(departure_offset_minutes) OVER (ORDER BY stop_index) AS previous_departure
                FROM route_stops
                WHERE route_id = affected_route_id
            ) AS ordered
            WHERE previous_departure IS NOT NULL
              AND arrival_offset_minutes < previous_departure
        ) THEN
            RAISE EXCEPTION 'route stop offsets must be non-decreasing'
                USING ERRCODE = '23514';
        END IF;
    END LOOP;

    RETURN NULL;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM (
            SELECT route_id,
                   stop_index,
                   row_number() OVER (PARTITION BY route_id ORDER BY stop_index) - 1 AS expected_index
            FROM route_stops
        ) AS ordered
        WHERE stop_index <> expected_index
    ) THEN
        RAISE EXCEPTION 'existing route stops must be contiguous and zero-based'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM (
            SELECT route_id,
                   arrival_offset_minutes,
                   lag(departure_offset_minutes) OVER (
                       PARTITION BY route_id ORDER BY stop_index
                   ) AS previous_departure
            FROM route_stops
        ) AS ordered
        WHERE previous_departure IS NOT NULL
          AND arrival_offset_minutes < previous_departure
    ) THEN
        RAISE EXCEPTION 'existing route stop offsets must be non-decreasing'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM routes AS r
        WHERE (SELECT count(*) FROM route_stops AS rs WHERE rs.route_id = r.id) < 2
    ) THEN
        RAISE EXCEPTION 'existing route requires at least two stops'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION validate_route_minimum_stops()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_route_id uuid;
    affected_route_ids uuid[];
BEGIN
    IF TG_TABLE_NAME = 'routes' THEN
        affected_route_ids := ARRAY[NEW.id];
    ELSIF TG_OP = 'DELETE' THEN
        affected_route_ids := ARRAY[OLD.route_id];
    ELSIF TG_OP = 'UPDATE' AND OLD.route_id IS DISTINCT FROM NEW.route_id THEN
        affected_route_ids := ARRAY[OLD.route_id, NEW.route_id];
    ELSE
        affected_route_ids := ARRAY[NEW.route_id];
    END IF;

    FOREACH affected_route_id IN ARRAY affected_route_ids LOOP
        IF EXISTS (SELECT 1 FROM routes WHERE id = affected_route_id)
           AND (SELECT count(*) FROM route_stops WHERE route_id = affected_route_id) < 2 THEN
            RAISE EXCEPTION 'route requires at least two stops'
                USING ERRCODE = '23514';
        END IF;
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER routes_validate_minimum_stops
AFTER INSERT OR UPDATE ON routes
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_route_minimum_stops();

CREATE CONSTRAINT TRIGGER route_stops_validate_minimum
AFTER INSERT OR UPDATE OR DELETE ON route_stops
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_route_minimum_stops();

COMMIT;
