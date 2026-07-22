\set ON_ERROR_STOP on

BEGIN;

INSERT INTO stations (id, code, name, timezone)
VALUES
    ('21000000-0000-4000-8000-000000000001', 'M2A', 'Milestone 2 Alpha', 'UTC'),
    ('21000000-0000-4000-8000-000000000002', 'M2B', 'Milestone 2 Beta', 'UTC')
ON CONFLICT (id) DO UPDATE
SET code = EXCLUDED.code,
    name = EXCLUDED.name,
    timezone = EXCLUDED.timezone,
    active = true;

INSERT INTO routes (id, code, name, operating_timezone)
VALUES (
    '21000000-0000-4000-8000-000000000100',
    'M2_EVIDENCE',
    'Milestone 2 Evidence Route',
    'UTC'
)
ON CONFLICT (id) DO UPDATE
SET code = EXCLUDED.code,
    name = EXCLUDED.name,
    operating_timezone = EXCLUDED.operating_timezone,
    active = true;

INSERT INTO route_stops (
    route_id,
    station_id,
    stop_index,
    arrival_offset_minutes,
    departure_offset_minutes
)
VALUES
    (
        '21000000-0000-4000-8000-000000000100',
        '21000000-0000-4000-8000-000000000001',
        0,
        0,
        0
    ),
    (
        '21000000-0000-4000-8000-000000000100',
        '21000000-0000-4000-8000-000000000002',
        1,
        60,
        60
    )
ON CONFLICT (route_id, stop_index) DO UPDATE
SET station_id = EXCLUDED.station_id,
    arrival_offset_minutes = EXCLUDED.arrival_offset_minutes,
    departure_offset_minutes = EXCLUDED.departure_offset_minutes;

INSERT INTO trains (id, code, name)
VALUES (
    '21000000-0000-4000-8000-000000000200',
    'M2_TRAIN',
    'Milestone 2 Evidence Train'
)
ON CONFLICT (id) DO UPDATE
SET code = EXCLUDED.code,
    name = EXCLUDED.name,
    active = true;

INSERT INTO coaches (id, train_id, coach_number, seat_class)
VALUES (
    '21000000-0000-4000-8000-000000000300',
    '21000000-0000-4000-8000-000000000200',
    'M2C1',
    'standard'
)
ON CONFLICT (id) DO UPDATE
SET train_id = EXCLUDED.train_id,
    coach_number = EXCLUDED.coach_number,
    seat_class = EXCLUDED.seat_class;

INSERT INTO seats (id, coach_id, seat_number, seat_type, active)
SELECT
    md5('milestone-2-evidence-seat-' || seat_number)::uuid,
    '21000000-0000-4000-8000-000000000300'::uuid,
    'S' || lpad(seat_number::text, 3, '0'),
    CASE WHEN seat_number % 2 = 0 THEN 'aisle' ELSE 'window' END,
    true
FROM generate_series(1, 80) AS generated(seat_number)
ON CONFLICT (id) DO UPDATE
SET coach_id = EXCLUDED.coach_id,
    seat_number = EXCLUDED.seat_number,
    seat_type = EXCLUDED.seat_type,
    active = true;

INSERT INTO train_runs (
    id,
    train_id,
    route_id,
    service_date,
    scheduled_departure_at,
    status,
    segment_count
)
VALUES
    (
        '21000000-0000-4000-8000-000000000401',
        '21000000-0000-4000-8000-000000000200',
        '21000000-0000-4000-8000-000000000100',
        CURRENT_DATE + 30,
        (CURRENT_DATE + 30)::timestamp AT TIME ZONE 'UTC',
        'scheduled',
        1
    ),
    (
        '21000000-0000-4000-8000-000000000402',
        '21000000-0000-4000-8000-000000000200',
        '21000000-0000-4000-8000-000000000100',
        CURRENT_DATE + 31,
        (CURRENT_DATE + 31)::timestamp AT TIME ZONE 'UTC',
        'scheduled',
        1
    )
ON CONFLICT (id) DO UPDATE
SET service_date = EXCLUDED.service_date,
    scheduled_departure_at = EXCLUDED.scheduled_departure_at,
    status = 'scheduled',
    segment_count = EXCLUDED.segment_count;

INSERT INTO seat_inventory (
    train_run_id,
    segment_count,
    seat_id,
    seat_class,
    occupied_segments
)
SELECT
    train_run.id,
    1,
    seat.id,
    'standard',
    B'0'
FROM (
    VALUES
        ('21000000-0000-4000-8000-000000000401'::uuid),
        ('21000000-0000-4000-8000-000000000402'::uuid)
) AS train_run(id)
CROSS JOIN seats AS seat
WHERE seat.coach_id = '21000000-0000-4000-8000-000000000300'
ON CONFLICT (train_run_id, seat_id) DO UPDATE
SET segment_count = EXCLUDED.segment_count,
    seat_class = EXCLUDED.seat_class;

INSERT INTO fares (
    id,
    train_run_id,
    route_id,
    from_stop_index,
    to_stop_index,
    seat_class,
    amount_minor,
    currency,
    active
)
VALUES
    (
        '21000000-0000-4000-8000-000000000601',
        '21000000-0000-4000-8000-000000000401',
        NULL,
        0,
        1,
        'standard',
        10000,
        'TWD',
        true
    ),
    (
        '21000000-0000-4000-8000-000000000602',
        '21000000-0000-4000-8000-000000000402',
        NULL,
        0,
        1,
        'standard',
        10000,
        'TWD',
        true
    )
ON CONFLICT (id) DO UPDATE
SET train_run_id = EXCLUDED.train_run_id,
    route_id = EXCLUDED.route_id,
    from_stop_index = EXCLUDED.from_stop_index,
    to_stop_index = EXCLUDED.to_stop_index,
    seat_class = EXCLUDED.seat_class,
    amount_minor = EXCLUDED.amount_minor,
    currency = EXCLUDED.currency,
    active = true;

INSERT INTO hot_train_policies (
    id,
    train_run_id,
    seat_class,
    enabled,
    version,
    max_queue_size,
    admission_rate_per_second,
    max_inflight_admissions,
    admission_token_ttl_seconds,
    processing_lease_seconds,
    queue_entry_ttl_seconds
)
VALUES (
    '21000000-0000-4000-8000-000000000500',
    '21000000-0000-4000-8000-000000000401',
    'standard',
    true,
    1,
    1000,
    5,
    5,
    240,
    5,
    300
)
ON CONFLICT (id) DO UPDATE
SET enabled = true,
    max_queue_size = EXCLUDED.max_queue_size,
    admission_rate_per_second = EXCLUDED.admission_rate_per_second,
    max_inflight_admissions = EXCLUDED.max_inflight_admissions,
    admission_token_ttl_seconds = EXCLUDED.admission_token_ttl_seconds,
    processing_lease_seconds = EXCLUDED.processing_lease_seconds,
    queue_entry_ttl_seconds = EXCLUDED.queue_entry_ttl_seconds;

COMMIT;

SELECT
    '21000000-0000-4000-8000-000000000401'::uuid AS hot_train_run_id,
    '21000000-0000-4000-8000-000000000402'::uuid AS non_hot_train_run_id,
    count(*) FILTER (
        WHERE train_run_id = '21000000-0000-4000-8000-000000000401'
    ) AS hot_inventory_rows,
    count(*) FILTER (
        WHERE train_run_id = '21000000-0000-4000-8000-000000000402'
    ) AS non_hot_inventory_rows
FROM seat_inventory
WHERE train_run_id IN (
    '21000000-0000-4000-8000-000000000401',
    '21000000-0000-4000-8000-000000000402'
);
