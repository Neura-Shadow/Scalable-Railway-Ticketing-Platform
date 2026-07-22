BEGIN;

INSERT INTO train_run_journey_read_model (
    train_run_id, route_id, train_id, train_code, service_date, train_run_status,
    from_station_id, from_station_code, from_station_name, from_stop_index,
    to_station_id, to_station_code, to_station_name, to_stop_index,
    departure_at, arrival_at, seat_class, fare_amount_minor, currency,
    source_updated_at, rebuilt_at
) VALUES (
    '66666666-6666-4666-8666-666666666666',
    '44444444-4444-4444-8444-444444444444',
    '55555555-5555-4555-8555-555555555555',
    'FIXTURE_TRAIN', '2099-01-01', 'scheduled',
    '22222222-2222-4222-8222-222222222222', 'FXA', 'Fixture Alpha', 0,
    '33333333-3333-4333-8333-333333333333', 'FXB', 'Fixture Beta', 1,
    '2099-01-01 08:00:00+00', '2099-01-01 09:00:00+00',
    'standard', 12500, 'TWD',
    '2026-01-01 00:00:00+00', '2026-01-01 00:01:00+00'
);

INSERT INTO read_model_event_receipts (
    consumer_name, event_id, event_type, aggregate_type, aggregate_id, processed_at
) VALUES (
    'railway-read-model',
    'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
    'trainrun.updated', 'train_run',
    '66666666-6666-4666-8666-666666666666',
    '2026-01-01 00:01:00+00'
);

INSERT INTO outbox_events (
    id, aggregate_type, aggregate_id, event_type, event_version, payload,
    status, attempts, next_attempt_at, created_at
) VALUES (
    'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
    'train_run', '66666666-6666-4666-8666-666666666666',
    'trainrun.updated', 1, '{}'::jsonb,
    'pending', 0, '2026-01-01 00:01:00+00', '2026-01-01 00:01:00+00'
);

COMMIT;
