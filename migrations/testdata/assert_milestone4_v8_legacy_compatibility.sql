BEGIN;

INSERT INTO public.seats (
    id, coach_id, seat_number, seat_type, active
) VALUES (
    'a4000000-0000-4000-8000-000000000010',
    'a0000000-0000-4000-8000-000000000001',
    '1C', 'other', true
);

INSERT INTO public.passengers (id, user_id, display_name)
VALUES (
    'b4000000-0000-4000-8000-000000000010',
    '11111111-1111-4111-8111-111111111111',
    'Legacy Compatibility Passenger'
);

-- A schema-aware legacy writer acquires the global key claim before its
-- shard-local insert. The compatibility trigger must accept only the same
-- fingerprint, train-run integrity reference, and database expiry.
INSERT INTO public.booking_idempotency_key_claims (
    user_id, operation, key_hash, request_fingerprint, train_run_id, expires_at
) VALUES (
    '11111111-1111-4111-8111-111111111111',
    'reservation.create', decode(repeat('b0', 32), 'hex'),
    decode(repeat('a0', 32), 'hex'),
    '66666666-6666-4666-8666-666666666666',
    '2099-01-01 00:00:00+00'
);

INSERT INTO public.idempotency_records (
    id, user_id, operation, key_hash, request_fingerprint, status,
    expires_at, train_run_id
) VALUES (
    'e4000000-0000-4000-8000-000000000001',
    '11111111-1111-4111-8111-111111111111',
    'reservation.create', decode(repeat('b0', 32), 'hex'),
    decode(repeat('a0', 32), 'hex'), 'in_progress',
    '2099-01-01 00:00:00+00',
    '66666666-6666-4666-8666-666666666666'
);

DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.booking_idempotency_key_claims
        WHERE user_id = '11111111-1111-4111-8111-111111111111'
          AND operation = 'reservation.create'
          AND key_hash = decode(repeat('b0', 32), 'hex')
          AND train_run_id = '66666666-6666-4666-8666-666666666666'
          AND expires_at = '2099-01-01 00:00:00+00'
    ) THEN
        RAISE EXCEPTION 'routed legacy key placeholder was not bound';
    END IF;
END
$assert$;

INSERT INTO public.seat_inventory (
    train_run_id, segment_count, seat_id, seat_class, occupied_segments,
    version
) VALUES (
    '66666666-6666-4666-8666-666666666666', 1,
    'a4000000-0000-4000-8000-000000000010', 'standard', B'1', 1
);

-- Version-7 writers acquire idempotency before they can store a train run.
INSERT INTO public.idempotency_records (
    id, user_id, operation, key_hash, request_fingerprint, status, expires_at
) VALUES (
    'e4000000-0000-4000-8000-000000000010',
    '11111111-1111-4111-8111-111111111111',
    'reservation.create', decode(repeat('c1', 32), 'hex'),
    decode(repeat('d1', 32), 'hex'), 'in_progress',
    '2099-01-01 00:00:00+00'
);

DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.booking_idempotency_key_claims
        WHERE user_id = '11111111-1111-4111-8111-111111111111'
          AND operation = 'reservation.create'
          AND key_hash = decode(repeat('c1', 32), 'hex')
          AND train_run_id IS NULL
    ) THEN
        RAISE EXCEPTION 'unresolved legacy in-progress key claim was not preserved';
    END IF;
END
$assert$;

INSERT INTO public.reservations (
    id, user_id, train_run_id, segment_count, from_stop_index, to_stop_index,
    seat_class, status, expires_at, total_amount_minor, currency
) VALUES (
    'd4000000-0000-4000-8000-000000000010',
    '11111111-1111-4111-8111-111111111111',
    '66666666-6666-4666-8666-666666666666',
    1, 0, 1, 'standard', 'held',
    '2098-12-31 23:59:00+00', 17000, 'TWD'
);

INSERT INTO public.reservation_seats (
    id, reservation_id, train_run_id, segment_count, seat_id, passenger_id,
    segment_mask, fare_amount_minor, currency
) VALUES (
    'c4000000-0000-4000-8000-000000000010',
    'd4000000-0000-4000-8000-000000000010',
    '66666666-6666-4666-8666-666666666666', 1,
    'a4000000-0000-4000-8000-000000000010',
    'b4000000-0000-4000-8000-000000000010',
    B'1', 17000, 'TWD'
);

UPDATE public.idempotency_records
SET status = 'completed',
    resource_type = 'reservation',
    resource_id = 'd4000000-0000-4000-8000-000000000010'
WHERE id = 'e4000000-0000-4000-8000-000000000010';

INSERT INTO public.outbox_events (
    id, aggregate_type, aggregate_id, event_type, payload
) VALUES (
    'f4000000-0000-4000-8000-000000000010',
    'reservation', 'd4000000-0000-4000-8000-000000000010',
    'reservation.held', '{}'::jsonb
);

DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.reservation_shard_locators
        WHERE reservation_id = 'd4000000-0000-4000-8000-000000000010'
          AND shard_id = 'legacy' AND assignment_generation = 1
    ) OR NOT EXISTS (
        SELECT 1 FROM public.reservation_quota_claims
        WHERE reservation_id = 'd4000000-0000-4000-8000-000000000010'
          AND passenger_count = 1 AND active
    ) OR NOT EXISTS (
        SELECT 1 FROM public.booking_idempotency_key_claims
        WHERE user_id = '11111111-1111-4111-8111-111111111111'
          AND operation = 'reservation.create'
          AND key_hash = decode(repeat('c1', 32), 'hex')
          AND train_run_id = '66666666-6666-4666-8666-666666666666'
    ) OR NOT EXISTS (
        SELECT 1 FROM public.outbox_events
        WHERE id = 'f4000000-0000-4000-8000-000000000010'
          AND train_run_id = '66666666-6666-4666-8666-666666666666'
          AND shard_id = 'legacy' AND assignment_generation = 1
    ) THEN
        RAISE EXCEPTION 'legacy expand compatibility did not maintain global state';
    END IF;
END
$assert$;

UPDATE public.reservations
SET status = 'confirmed'
WHERE id = 'd4000000-0000-4000-8000-000000000010';

INSERT INTO public.ticket_orders (
    id, reservation_id, user_id, status, total_amount_minor, currency
) VALUES (
    'd4000000-0000-4000-8000-000000000011',
    'd4000000-0000-4000-8000-000000000010',
    '11111111-1111-4111-8111-111111111111',
    'confirmed', 17000, 'TWD'
);

INSERT INTO public.tickets (
    id, ticket_order_id, reservation_seat_id, ticket_code, status
) VALUES (
    'd4000000-0000-4000-8000-000000000012',
    'd4000000-0000-4000-8000-000000000011',
    'c4000000-0000-4000-8000-000000000010',
    'M4LEGACYCOMPAT0001', 'active'
);

UPDATE public.ticket_orders
SET status = 'cancelled'
WHERE id = 'd4000000-0000-4000-8000-000000000011';

DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.reservation_quota_claims
        WHERE reservation_id = 'd4000000-0000-4000-8000-000000000010'
          AND NOT active AND closed_at IS NOT NULL
    ) OR NOT EXISTS (
        SELECT 1 FROM public.ticket_order_shard_locators
        WHERE ticket_order_id = 'd4000000-0000-4000-8000-000000000011'
          AND status = 'cancelled' AND total_amount_minor = 17000
          AND currency = 'TWD'
    ) OR NOT EXISTS (
        SELECT 1 FROM public.ticket_shard_locators
        WHERE ticket_id = 'd4000000-0000-4000-8000-000000000012'
          AND ticket_order_id = 'd4000000-0000-4000-8000-000000000011'
    ) THEN
        RAISE EXCEPTION 'legacy lifecycle compatibility did not maintain global state';
    END IF;
END
$assert$;

INSERT INTO public.idempotency_records (
    id, user_id, operation, key_hash, request_fingerprint, status,
    expires_at, created_at, updated_at
) VALUES (
    'e4000000-0000-4000-8000-000000000020',
    '11111111-1111-4111-8111-111111111111',
    'reservation.cancel', decode(repeat('c2', 32), 'hex'),
    decode(repeat('d2', 32), 'hex'), 'in_progress',
    '2026-01-02 00:00:00+00',
    '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'
);

INSERT INTO public.idempotency_records (
    id, user_id, operation, key_hash, request_fingerprint, status, expires_at
) VALUES (
    'e4000000-0000-4000-8000-000000000021',
    '11111111-1111-4111-8111-111111111111',
    'reservation.cancel', decode(repeat('c2', 32), 'hex'),
    decode(repeat('d3', 32), 'hex'), 'in_progress',
    '2099-01-01 00:00:00+00'
)
ON CONFLICT (user_id, operation, key_hash) DO UPDATE
SET id = EXCLUDED.id,
    request_fingerprint = EXCLUDED.request_fingerprint,
    status = 'in_progress',
    resource_type = NULL,
    resource_id = NULL,
    expires_at = EXCLUDED.expires_at,
    created_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE public.idempotency_records.expires_at <= clock_timestamp();

DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.idempotency_records AS record
        JOIN public.booking_idempotency_key_claims AS claim
          ON claim.user_id = record.user_id
         AND claim.operation = record.operation
         AND claim.key_hash = record.key_hash
         AND claim.request_fingerprint = record.request_fingerprint
        WHERE record.id = 'e4000000-0000-4000-8000-000000000021'
          AND record.request_fingerprint = decode(repeat('d3', 32), 'hex')
          AND record.train_run_id IS NULL
          AND claim.train_run_id IS NULL
          AND claim.expires_at = record.expires_at
    ) THEN
        RAISE EXCEPTION 'expired unresolved key was not atomically reacquired';
    END IF;

END
$assert$;

ROLLBACK;
