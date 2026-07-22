BEGIN;

SET LOCAL TIME ZONE 'UTC';

INSERT INTO users (
    id,
    email,
    password_hash,
    role,
    token_version,
    active,
    created_at,
    updated_at
) VALUES (
    '11111111-1111-4111-8111-111111111111',
    'migration-fixture@example.com',
    'fixture-password-hash-value',
    'customer',
    1,
    true,
    '2026-01-01 00:00:00+00',
    '2026-01-01 00:00:00+00'
);

INSERT INTO stations (
    id,
    code,
    name,
    timezone,
    active,
    created_at,
    updated_at
) VALUES
    (
        '22222222-2222-4222-8222-222222222222',
        'FXA',
        'Fixture Alpha',
        'UTC',
        true,
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00'
    ),
    (
        '33333333-3333-4333-8333-333333333333',
        'FXB',
        'Fixture Beta',
        'UTC',
        true,
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00'
    );

INSERT INTO routes (
    id,
    code,
    name,
    operating_timezone,
    active,
    created_at,
    updated_at
) VALUES (
    '44444444-4444-4444-8444-444444444444',
    'FIXTURE_ROUTE',
    'Migration Fixture Route',
    'UTC',
    true,
    '2026-01-01 00:00:00+00',
    '2026-01-01 00:00:00+00'
);

INSERT INTO route_stops (
    route_id,
    station_id,
    stop_index,
    arrival_offset_minutes,
    departure_offset_minutes,
    created_at,
    updated_at
) VALUES
    (
        '44444444-4444-4444-8444-444444444444',
        '22222222-2222-4222-8222-222222222222',
        0,
        0,
        0,
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00'
    ),
    (
        '44444444-4444-4444-8444-444444444444',
        '33333333-3333-4333-8333-333333333333',
        1,
        60,
        60,
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00'
    );

INSERT INTO trains (
    id,
    code,
    name,
    active,
    created_at,
    updated_at
) VALUES (
    '55555555-5555-4555-8555-555555555555',
    'FIXTURE_TRAIN',
    'Migration Fixture Train',
    true,
    '2026-01-01 00:00:00+00',
    '2026-01-01 00:00:00+00'
);

INSERT INTO train_runs (
    id,
    train_id,
    route_id,
    service_date,
    scheduled_departure_at,
    status,
    segment_count,
    created_at,
    updated_at
) VALUES (
    '66666666-6666-4666-8666-666666666666',
    '55555555-5555-4555-8555-555555555555',
    '44444444-4444-4444-8444-444444444444',
    '2099-01-01',
    '2099-01-01 08:00:00+00',
    'scheduled',
    1,
    '2026-01-01 00:00:00+00',
    '2026-01-01 00:00:00+00'
);

INSERT INTO reservations (
    id,
    user_id,
    train_run_id,
    segment_count,
    from_stop_index,
    to_stop_index,
    seat_class,
    status,
    expires_at,
    total_amount_minor,
    currency,
    created_at,
    updated_at
) VALUES (
    '77777777-7777-4777-8777-777777777777',
    '11111111-1111-4111-8111-111111111111',
    '66666666-6666-4666-8666-666666666666',
    1,
    0,
    1,
    'standard',
    'held',
    '2098-12-31 23:59:00+00',
    12500,
    'TWD',
    '2026-01-01 00:00:00+00',
    '2026-01-01 00:00:00+00'
);

INSERT INTO outbox_events (
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    event_version,
    payload,
    status,
    attempts,
    next_attempt_at,
    created_at
) VALUES (
    '88888888-8888-4888-8888-888888888888',
    'reservation',
    '77777777-7777-4777-8777-777777777777',
    'reservation.held',
    1,
    '{"fixture":"populated-v5","reservation_id":"77777777-7777-4777-8777-777777777777"}',
    'pending',
    0,
    '2026-01-01 00:00:00+00',
    '2026-01-01 00:00:00+00'
);

COMMIT;
