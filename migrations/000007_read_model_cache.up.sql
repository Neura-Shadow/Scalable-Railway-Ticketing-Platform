BEGIN;

CREATE TABLE train_run_journey_read_model (
    train_run_id uuid NOT NULL
        REFERENCES train_runs(id) ON DELETE CASCADE,
    route_id uuid NOT NULL,
    train_id uuid NOT NULL,
    train_code text NOT NULL,
    service_date date NOT NULL,
    train_run_status text NOT NULL,
    from_station_id uuid NOT NULL,
    from_station_code text NOT NULL,
    from_station_name text NOT NULL,
    from_stop_index integer NOT NULL,
    to_station_id uuid NOT NULL,
    to_station_code text NOT NULL,
    to_station_name text NOT NULL,
    to_stop_index integer NOT NULL,
    departure_at timestamptz NOT NULL,
    arrival_at timestamptz NOT NULL,
    seat_class text NOT NULL,
    fare_amount_minor bigint NOT NULL,
    currency text NOT NULL,
    source_updated_at timestamptz NOT NULL,
    rebuilt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        train_run_id,
        from_stop_index,
        to_stop_index,
        seat_class
    ),
    CONSTRAINT train_run_journey_read_model_journey_order_check
        CHECK (from_stop_index >= 0 AND from_stop_index < to_stop_index),
    CONSTRAINT train_run_journey_read_model_fare_amount_minor_check
        CHECK (fare_amount_minor >= 0),
    CONSTRAINT train_run_journey_read_model_currency_check
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT train_run_journey_read_model_status_check
        CHECK (train_run_status IN (
            'scheduled',
            'boarding',
            'departed',
            'completed',
            'cancelled'
        )),
    CONSTRAINT train_run_journey_read_model_seat_class_check
        CHECK (seat_class IN ('standard', 'business', 'first')),
    CONSTRAINT train_run_journey_read_model_times_check
        CHECK (departure_at < arrival_at),
    CONSTRAINT train_run_journey_read_model_station_codes_check
        CHECK (
            from_station_code = btrim(from_station_code)
            AND to_station_code = btrim(to_station_code)
            AND from_station_code <> ''
            AND to_station_code <> ''
        ),
    CONSTRAINT train_run_journey_read_model_station_ids_check
        CHECK (
            from_station_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND to_station_id <> '00000000-0000-0000-0000-000000000000'::uuid
        )
);

CREATE INDEX train_run_journey_read_model_search_idx
    ON train_run_journey_read_model (
        from_station_code,
        to_station_code,
        service_date,
        seat_class,
        train_run_status,
        departure_at,
        train_run_id
    );

CREATE INDEX train_run_journey_read_model_fare_search_idx
    ON train_run_journey_read_model (
        from_station_code,
        to_station_code,
        service_date,
        seat_class,
        train_run_status,
        fare_amount_minor,
        departure_at,
        train_run_id
    );

CREATE INDEX train_run_journey_read_model_source_updated_at_idx
    ON train_run_journey_read_model (source_updated_at, train_run_id);

CREATE TABLE read_model_event_receipts (
    consumer_name text NOT NULL,
    event_id uuid NOT NULL,
    event_type text NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_name, event_id),
    CONSTRAINT read_model_event_receipts_consumer_name_check
        CHECK (
            consumer_name = btrim(consumer_name)
            AND length(consumer_name) BETWEEN 1 AND 128
        ),
    CONSTRAINT read_model_event_receipts_event_type_check
        CHECK (
            event_type = btrim(event_type)
            AND length(event_type) BETWEEN 1 AND 128
        ),
    CONSTRAINT read_model_event_receipts_aggregate_type_check
        CHECK (
            aggregate_type = btrim(aggregate_type)
            AND length(aggregate_type) BETWEEN 1 AND 64
        )
);

CREATE INDEX read_model_event_receipts_aggregate_idx
    ON read_model_event_receipts (
        aggregate_type,
        aggregate_id,
        processed_at DESC
    );

CREATE INDEX read_model_event_receipts_processed_at_idx
    ON read_model_event_receipts (processed_at, event_id);

CREATE TABLE read_model_event_progress (
    consumer_name text NOT NULL,
    event_id uuid NOT NULL,
    event_type text NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    projection_affecting boolean NOT NULL,
    phase text NOT NULL DEFAULT 'invalidating',
    after_train_run_id uuid,
    processed_train_runs integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_name, event_id),
    CONSTRAINT read_model_event_progress_consumer_name_check
        CHECK (
            consumer_name = btrim(consumer_name)
            AND length(consumer_name) BETWEEN 1 AND 128
        ),
    CONSTRAINT read_model_event_progress_event_type_check
        CHECK (
            event_type = btrim(event_type)
            AND length(event_type) BETWEEN 1 AND 128
        ),
    CONSTRAINT read_model_event_progress_aggregate_type_check
        CHECK (
            aggregate_type = btrim(aggregate_type)
            AND length(aggregate_type) BETWEEN 1 AND 64
        ),
    CONSTRAINT read_model_event_progress_phase_check
        CHECK (phase IN ('invalidating', 'processing', 'finalizing')),
    CONSTRAINT read_model_event_progress_processed_check
        CHECK (processed_train_runs >= 0)
);

CREATE INDEX read_model_event_progress_projection_idx
    ON read_model_event_progress (projection_affecting, phase, updated_at)
    WHERE projection_affecting;

CREATE TABLE read_model_projection_state (
    projection_name text PRIMARY KEY,
    ready boolean NOT NULL DEFAULT false,
    rebuild_after text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT read_model_projection_state_name_check
        CHECK (projection_name = 'journey_search'),
    CONSTRAINT read_model_projection_state_cursor_check
        CHECK (length(rebuild_after) <= 128)
);

INSERT INTO read_model_projection_state (projection_name)
VALUES ('journey_search');

ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_aggregate_type_check,
    ADD CONSTRAINT outbox_events_aggregate_type_check
        CHECK (aggregate_type IN (
            'reservation',
            'ticket',
            'train_run',
            'hot_train_policy',
            'station',
            'route',
            'train',
            'coach',
            'seat',
            'fare'
        )),
    DROP CONSTRAINT outbox_events_event_type_check,
    ADD CONSTRAINT outbox_events_event_type_check
        CHECK (event_type IN (
            'reservation.held',
            'reservation.confirmed',
            'reservation.expired',
            'reservation.cancelled',
            'ticket.created',
            'trainrun.created',
            'trainrun.updated',
            'trainrun.cancelled',
            'hot_train_policy.created',
            'hot_train_policy.updated',
            'hot_train_policy.disabled',
            'station.created',
            'station.updated',
            'station.disabled',
            'route.created',
            'route.updated',
            'route.disabled',
            'train.updated',
            'coach.updated',
            'seat.updated',
            'fare.created',
            'fare.updated',
            'fare.disabled'
        ));

COMMIT;
