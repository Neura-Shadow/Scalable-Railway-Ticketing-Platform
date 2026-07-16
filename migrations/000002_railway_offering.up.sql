BEGIN;

CREATE TABLE stations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code text NOT NULL UNIQUE,
    name text NOT NULL,
    timezone text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (code ~ '^[A-Z0-9]{2,12}$'),
    CHECK (length(btrim(name)) BETWEEN 1 AND 120),
    CHECK (length(btrim(timezone)) BETWEEN 1 AND 64)
);

CREATE TRIGGER stations_set_updated_at
BEFORE UPDATE ON stations
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE routes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code text NOT NULL UNIQUE,
    name text NOT NULL,
    operating_timezone text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (code ~ '^[A-Z0-9][A-Z0-9_-]{1,31}$'),
    CHECK (length(btrim(name)) BETWEEN 1 AND 120),
    CHECK (length(btrim(operating_timezone)) BETWEEN 1 AND 64)
);

CREATE TRIGGER routes_set_updated_at
BEFORE UPDATE ON routes
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE route_stops (
    route_id uuid NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    station_id uuid NOT NULL REFERENCES stations(id) ON DELETE RESTRICT,
    stop_index integer NOT NULL CHECK (stop_index >= 0),
    arrival_offset_minutes integer NOT NULL CHECK (arrival_offset_minutes >= 0),
    departure_offset_minutes integer NOT NULL CHECK (departure_offset_minutes >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (route_id, stop_index),
    UNIQUE (route_id, station_id),
    CHECK (arrival_offset_minutes <= departure_offset_minutes)
);

CREATE INDEX route_stops_station_idx ON route_stops (station_id, route_id);

CREATE TRIGGER route_stops_set_updated_at
BEFORE UPDATE ON route_stops
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE FUNCTION validate_route_stop_sequence()
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

CREATE CONSTRAINT TRIGGER route_stops_validate_sequence
AFTER INSERT OR UPDATE OR DELETE ON route_stops
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_route_stop_sequence();

CREATE TABLE trains (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code text NOT NULL UNIQUE,
    name text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (code ~ '^[A-Z0-9][A-Z0-9_-]{1,31}$'),
    CHECK (length(btrim(name)) BETWEEN 1 AND 120)
);

CREATE TRIGGER trains_set_updated_at
BEFORE UPDATE ON trains
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE coaches (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_id uuid NOT NULL REFERENCES trains(id) ON DELETE CASCADE,
    coach_number text NOT NULL,
    seat_class text NOT NULL
        CHECK (seat_class IN ('standard', 'business', 'first')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (train_id, coach_number),
    CHECK (length(btrim(coach_number)) BETWEEN 1 AND 16)
);

CREATE INDEX coaches_train_class_idx ON coaches (train_id, seat_class, coach_number);

CREATE TRIGGER coaches_set_updated_at
BEFORE UPDATE ON coaches
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE seats (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    coach_id uuid NOT NULL REFERENCES coaches(id) ON DELETE CASCADE,
    seat_number text NOT NULL,
    seat_type text NOT NULL DEFAULT 'other'
        CHECK (seat_type IN ('window', 'aisle', 'middle', 'other')),
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (coach_id, seat_number),
    CHECK (length(btrim(seat_number)) BETWEEN 1 AND 16)
);

CREATE INDEX seats_coach_active_idx ON seats (coach_id, active, seat_number);

CREATE TRIGGER seats_set_updated_at
BEFORE UPDATE ON seats
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE train_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_id uuid NOT NULL REFERENCES trains(id) ON DELETE RESTRICT,
    route_id uuid NOT NULL REFERENCES routes(id) ON DELETE RESTRICT,
    service_date date NOT NULL,
    scheduled_departure_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'scheduled'
        CHECK (status IN ('scheduled', 'boarding', 'departed', 'cancelled', 'completed')),
    segment_count integer NOT NULL CHECK (segment_count > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, segment_count),
    UNIQUE (train_id, route_id, service_date, scheduled_departure_at)
);

CREATE INDEX train_runs_search_idx
    ON train_runs (service_date, route_id, status, scheduled_departure_at, id);
CREATE INDEX train_runs_train_date_idx
    ON train_runs (train_id, service_date, scheduled_departure_at);

CREATE TRIGGER train_runs_set_updated_at
BEFORE UPDATE ON train_runs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE fares (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid REFERENCES train_runs(id) ON DELETE CASCADE,
    route_id uuid REFERENCES routes(id) ON DELETE CASCADE,
    from_stop_index integer NOT NULL CHECK (from_stop_index >= 0),
    to_stop_index integer NOT NULL,
    seat_class text NOT NULL
        CHECK (seat_class IN ('standard', 'business', 'first')),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((train_run_id IS NULL) <> (route_id IS NULL)),
    CHECK (from_stop_index < to_stop_index)
);

CREATE UNIQUE INDEX fares_active_train_run_interval_idx
    ON fares (train_run_id, from_stop_index, to_stop_index, seat_class)
    WHERE active AND train_run_id IS NOT NULL;
CREATE UNIQUE INDEX fares_active_route_interval_idx
    ON fares (route_id, from_stop_index, to_stop_index, seat_class)
    WHERE active AND route_id IS NOT NULL;
CREATE INDEX fares_train_run_lookup_idx
    ON fares (train_run_id, seat_class, from_stop_index, to_stop_index)
    WHERE active;
CREATE INDEX fares_route_lookup_idx
    ON fares (route_id, seat_class, from_stop_index, to_stop_index)
    WHERE active;

CREATE TRIGGER fares_set_updated_at
BEFORE UPDATE ON fares
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
