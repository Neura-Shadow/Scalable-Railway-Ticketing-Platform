BEGIN;

SET LOCAL TIME ZONE 'UTC';

-- This fixture extends populated_v5_fixture.sql after migrations 6 and 7.
-- It supplies the passenger/seat state required for an exact quota-claim
-- bootstrap and adds confirmed order/ticket/idempotency rows for locator and
-- provenance coverage.
INSERT INTO public.coaches (
    id, train_id, coach_number, seat_class, created_at, updated_at
) VALUES (
    'a0000000-0000-4000-8000-000000000001',
    '55555555-5555-4555-8555-555555555555',
    'M4-C1', 'standard',
    '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'
);

INSERT INTO public.seats (
    id, coach_id, seat_number, seat_type, active, created_at, updated_at
) VALUES
    (
        'a0000000-0000-4000-8000-000000000002',
        'a0000000-0000-4000-8000-000000000001',
        '1A', 'window', true,
        '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'
    ),
    (
        'a0000000-0000-4000-8000-000000000003',
        'a0000000-0000-4000-8000-000000000001',
        '1B', 'aisle', true,
        '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'
    );

INSERT INTO public.passengers (
    id, user_id, display_name, created_at, updated_at
) VALUES
    (
        'b0000000-0000-4000-8000-000000000001',
        '11111111-1111-4111-8111-111111111111',
        'Milestone Four Held Passenger',
        '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'
    ),
    (
        'b0000000-0000-4000-8000-000000000002',
        '11111111-1111-4111-8111-111111111111',
        'Milestone Four Confirmed Passenger',
        '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'
    );

INSERT INTO public.seat_inventory (
    train_run_id, segment_count, seat_id, seat_class, occupied_segments,
    version, created_at, updated_at
) VALUES
    (
        '66666666-6666-4666-8666-666666666666', 1,
        'a0000000-0000-4000-8000-000000000002', 'standard', B'1', 1,
        '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'
    ),
    (
        '66666666-6666-4666-8666-666666666666', 1,
        'a0000000-0000-4000-8000-000000000003', 'standard', B'1', 1,
        '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'
    );

INSERT INTO public.reservation_seats (
    id, reservation_id, train_run_id, segment_count, seat_id, passenger_id,
    segment_mask, fare_amount_minor, currency, created_at
) VALUES (
    'c0000000-0000-4000-8000-000000000001',
    '77777777-7777-4777-8777-777777777777',
    '66666666-6666-4666-8666-666666666666', 1,
    'a0000000-0000-4000-8000-000000000002',
    'b0000000-0000-4000-8000-000000000001',
    B'1', 12500, 'TWD', '2026-01-01 00:00:00+00'
);

INSERT INTO public.reservations (
    id, user_id, train_run_id, segment_count, from_stop_index, to_stop_index,
    seat_class, status, expires_at, total_amount_minor, currency,
    created_at, updated_at
) VALUES (
    'd0000000-0000-4000-8000-000000000001',
    '11111111-1111-4111-8111-111111111111',
    '66666666-6666-4666-8666-666666666666',
    1, 0, 1, 'standard', 'confirmed',
    '2098-12-31 23:59:00+00', 15000, 'TWD',
    '2026-01-01 00:00:01+00', '2026-01-01 00:00:02+00'
);

INSERT INTO public.reservation_seats (
    id, reservation_id, train_run_id, segment_count, seat_id, passenger_id,
    segment_mask, fare_amount_minor, currency, created_at
) VALUES (
    'c0000000-0000-4000-8000-000000000002',
    'd0000000-0000-4000-8000-000000000001',
    '66666666-6666-4666-8666-666666666666', 1,
    'a0000000-0000-4000-8000-000000000003',
    'b0000000-0000-4000-8000-000000000002',
    B'1', 15000, 'TWD', '2026-01-01 00:00:01+00'
);

INSERT INTO public.ticket_orders (
    id, reservation_id, user_id, status, total_amount_minor, currency,
    created_at, updated_at
) VALUES (
    'd0000000-0000-4000-8000-000000000002',
    'd0000000-0000-4000-8000-000000000001',
    '11111111-1111-4111-8111-111111111111',
    'confirmed', 15000, 'TWD',
    '2026-01-01 00:00:02+00', '2026-01-01 00:00:02+00'
);

INSERT INTO public.tickets (
    id, ticket_order_id, reservation_seat_id, ticket_code, status,
    created_at, updated_at
) VALUES (
    'd0000000-0000-4000-8000-000000000003',
    'd0000000-0000-4000-8000-000000000002',
    'c0000000-0000-4000-8000-000000000002',
    'M4.legacy/ticket?0001', 'active',
    '2026-01-01 00:00:02+00', '2026-01-01 00:00:02+00'
);

INSERT INTO public.idempotency_records (
    id, user_id, operation, key_hash, request_fingerprint, status,
    resource_type, resource_id, expires_at, created_at, updated_at
) VALUES
    (
        'e0000000-0000-4000-8000-000000000001',
        '11111111-1111-4111-8111-111111111111',
        'reservation.create', decode(repeat('a1', 32), 'hex'),
        decode(repeat('b1', 32), 'hex'), 'completed', 'reservation',
        '77777777-7777-4777-8777-777777777777',
        '2099-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'
    ),
    (
        'e0000000-0000-4000-8000-000000000002',
        '11111111-1111-4111-8111-111111111111',
        'reservation.confirm', decode(repeat('a2', 32), 'hex'),
        decode(repeat('b2', 32), 'hex'), 'completed', 'reservation',
        'd0000000-0000-4000-8000-000000000001',
        '2099-01-01 00:00:00+00',
        '2026-01-01 00:00:02+00', '2026-01-01 00:00:02+00'
    ),
    (
        'e0000000-0000-4000-8000-000000000003',
        '11111111-1111-4111-8111-111111111111',
        'reservation.cancel', decode(repeat('a3', 32), 'hex'),
        decode(repeat('b3', 32), 'hex'), 'in_progress', NULL, NULL,
        '2099-01-01 00:00:00+00',
        '2026-01-01 00:00:03+00', '2026-01-01 00:00:03+00'
    );

INSERT INTO public.outbox_events (
    id, aggregate_type, aggregate_id, event_type, event_version, payload,
    status, attempts, next_attempt_at, created_at
) VALUES (
    'f0000000-0000-4000-8000-000000000001',
    'ticket', 'd0000000-0000-4000-8000-000000000003',
    'ticket.created', 1, '{"fixture":"milestone-4-v7"}'::jsonb,
    'pending', 0, '2026-01-01 00:00:02+00', '2026-01-01 00:00:02+00'
);

COMMIT;
