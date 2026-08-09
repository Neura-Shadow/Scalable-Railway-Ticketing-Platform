-- Synthetic version-1 booking data used to prove a populated 1-to-2 upgrade.
-- No payment method, provider credential, raw idempotency key or PII is used.

INSERT INTO public.train_run_booking_snapshots (
    id, train_run_id, assignment_generation, train_id, route_id,
    service_date, segment_count, route_version, booking_policy_version,
    source_version, status, bookable, active, source_updated_at
) VALUES (
    '61000000-0000-0000-0000-000000000001',
    '61000000-0000-0000-0000-000000000002', 1,
    '61000000-0000-0000-0000-000000000003',
    '61000000-0000-0000-0000-000000000004',
    current_date + 1, 2, 1, 1, 1, 'scheduled', true, true,
    clock_timestamp()
);

INSERT INTO public.booking_seat_catalog (
    id, train_run_id, assignment_generation, train_id, coach_id, seat_id,
    coach_order, seat_order, seat_class, active, source_version,
    source_updated_at
) VALUES (
    '61000000-0000-0000-0000-000000000005',
    '61000000-0000-0000-0000-000000000002', 1,
    '61000000-0000-0000-0000-000000000003',
    '61000000-0000-0000-0000-000000000006',
    '61000000-0000-0000-0000-000000000007',
    0, 0, 'standard', true, 1, clock_timestamp()
);

INSERT INTO public.booking_fare_snapshots (
    id, train_run_id, assignment_generation, segment_count,
    from_stop_index, to_stop_index, seat_class, amount_minor, currency,
    source_version, active, source_updated_at
) VALUES (
    '61000000-0000-0000-0000-000000000008',
    '61000000-0000-0000-0000-000000000002', 1, 2,
    0, 2, 'standard', 12500, 'TWD', 1, true, clock_timestamp()
);

INSERT INTO public.seat_inventory (
    id, train_run_id, assignment_generation, segment_count, seat_id,
    seat_class, occupied_segments, version
) VALUES (
    '61000000-0000-0000-0000-000000000009',
    '61000000-0000-0000-0000-000000000002', 1, 2,
    '61000000-0000-0000-0000-000000000007',
    'standard', B'11'::bit varying, 1
);

INSERT INTO public.reservations (
    id, user_id, train_run_id, assignment_generation, segment_count,
    from_stop_index, to_stop_index, seat_class, status, expires_at,
    total_amount_minor, currency
) VALUES (
    '61000000-0000-0000-0000-000000000010',
    '61000000-0000-0000-0000-000000000011',
    '61000000-0000-0000-0000-000000000002', 1, 2,
    0, 2, 'standard', 'confirmed', clock_timestamp() + interval '1 hour',
    12500, 'TWD'
);

INSERT INTO public.reservation_seats (
    id, reservation_id, train_run_id, assignment_generation, segment_count,
    seat_id, passenger_id, fare_snapshot_id, segment_mask,
    fare_amount_minor, currency
) VALUES (
    '61000000-0000-0000-0000-000000000012',
    '61000000-0000-0000-0000-000000000010',
    '61000000-0000-0000-0000-000000000002', 1, 2,
    '61000000-0000-0000-0000-000000000007',
    '61000000-0000-0000-0000-000000000013',
    '61000000-0000-0000-0000-000000000008',
    B'11'::bit varying, 12500, 'TWD'
);

INSERT INTO public.ticket_orders (
    id, reservation_id, user_id, train_run_id, assignment_generation,
    status, total_amount_minor, currency
) VALUES (
    '61000000-0000-0000-0000-000000000014',
    '61000000-0000-0000-0000-000000000010',
    '61000000-0000-0000-0000-000000000011',
    '61000000-0000-0000-0000-000000000002', 1,
    'confirmed', 12500, 'TWD'
);

INSERT INTO public.tickets (
    id, ticket_order_id, reservation_seat_id, train_run_id,
    assignment_generation, ticket_code, status
) VALUES (
    '61000000-0000-0000-0000-000000000015',
    '61000000-0000-0000-0000-000000000014',
    '61000000-0000-0000-0000-000000000012',
    '61000000-0000-0000-0000-000000000002', 1,
    'TKT.61000000/legacy?00000015', 'active'
);
