BEGIN;

DROP TRIGGER route_stops_validate_minimum ON route_stops;
DROP TRIGGER routes_validate_minimum_stops ON routes;
DROP FUNCTION validate_route_minimum_stops();

CREATE OR REPLACE FUNCTION validate_route_stop_sequence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_route_id uuid;
BEGIN
    IF TG_OP = 'DELETE' THEN
        affected_route_id := OLD.route_id;
    ELSE
        affected_route_id := NEW.route_id;
    END IF;

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

    RETURN NULL;
END;
$$;

DROP TRIGGER reservation_seats_populate_train_run_id ON reservation_seats;
DROP FUNCTION populate_reservation_seat_train_run_id();

ALTER TABLE reservation_seats
    DROP CONSTRAINT reservation_seats_inventory_fkey,
    DROP CONSTRAINT reservation_seats_reservation_run_segment_fkey,
    ADD CONSTRAINT reservation_seats_reservation_id_segment_count_fkey
        FOREIGN KEY (reservation_id, segment_count)
        REFERENCES reservations(id, segment_count) ON DELETE CASCADE,
    DROP COLUMN train_run_id;

ALTER TABLE reservations
    DROP CONSTRAINT reservations_id_train_run_segment_count_key;

CREATE OR REPLACE FUNCTION validate_inventory_seat_class()
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

DROP TRIGGER seat_inventory_validate_class ON seat_inventory;
CREATE TRIGGER seat_inventory_validate_class
BEFORE INSERT OR UPDATE OF seat_id, seat_class ON seat_inventory
FOR EACH ROW EXECUTE FUNCTION validate_inventory_seat_class();

COMMIT;
