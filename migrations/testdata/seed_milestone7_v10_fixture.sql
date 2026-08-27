BEGIN;

-- Synthetic version-10 rows exercise every Milestone 7 saga-action backfill.
-- The directory entries are deliberately control-plane-only: the migration
-- must preserve existing data without requiring a provider or shard write.
INSERT INTO public.reservation_directory (
    reservation_id, train_run_id, owner_user_id, state,
    last_known_shard_id, last_known_generation, legacy_imported, active_at
)
SELECT fixture.reservation_id,
       '66666666-6666-4666-8666-666666666666'::uuid,
       '11111111-1111-4111-8111-111111111111'::uuid,
       'active', 'legacy', 1, true, clock_timestamp()
FROM (VALUES
    ('71000000-0000-4000-8000-000000000001'::uuid),
    ('71000000-0000-4000-8000-000000000002'::uuid),
    ('71000000-0000-4000-8000-000000000003'::uuid),
    ('71000000-0000-4000-8000-000000000004'::uuid)
) AS fixture(reservation_id);

INSERT INTO public.reservation_shard_locators (
    reservation_id, train_run_id, shard_id,
    assignment_generation, owner_user_id
)
SELECT fixture.reservation_id,
       '66666666-6666-4666-8666-666666666666'::uuid,
       'legacy', 1,
       '11111111-1111-4111-8111-111111111111'::uuid
FROM (VALUES
    ('71000000-0000-4000-8000-000000000001'::uuid),
    ('71000000-0000-4000-8000-000000000002'::uuid),
    ('71000000-0000-4000-8000-000000000003'::uuid),
    ('71000000-0000-4000-8000-000000000004'::uuid)
) AS fixture(reservation_id);

INSERT INTO public.payment_intents (
    payment_intent_id, reservation_id, train_run_id, owner_user_id,
    provider, amount_minor, currency,
    idempotency_key_hash, request_fingerprint
)
SELECT fixture.payment_intent_id, fixture.reservation_id,
       '66666666-6666-4666-8666-666666666666'::uuid,
       '11111111-1111-4111-8111-111111111111'::uuid,
       'sandbox', 12500, 'TWD',
       decode(repeat(fixture.hash_byte, 32), 'hex'),
       decode(repeat(fixture.fingerprint_byte, 32), 'hex')
FROM (VALUES
    ('72000000-0000-4000-8000-000000000001'::uuid,
     '71000000-0000-4000-8000-000000000001'::uuid, '11', '21'),
    ('72000000-0000-4000-8000-000000000002'::uuid,
     '71000000-0000-4000-8000-000000000002'::uuid, '12', '22'),
    ('72000000-0000-4000-8000-000000000003'::uuid,
     '71000000-0000-4000-8000-000000000003'::uuid, '13', '23'),
    ('72000000-0000-4000-8000-000000000004'::uuid,
     '71000000-0000-4000-8000-000000000004'::uuid, '14', '24')
) AS fixture(
    payment_intent_id, reservation_id, hash_byte, fingerprint_byte
);

-- Two compensating states are legal recovery entry points in version 10.
INSERT INTO public.payment_sagas (
    saga_id, payment_intent_id, reservation_id, current_step, state
) VALUES
    ('73000000-0000-4000-8000-000000000002',
     '72000000-0000-4000-8000-000000000002',
     '71000000-0000-4000-8000-000000000002', 'refund', 'compensating'),
    ('73000000-0000-4000-8000-000000000003',
     '72000000-0000-4000-8000-000000000003',
     '71000000-0000-4000-8000-000000000003', 'compensate', 'compensating');

-- Reach issuing/refunding through the public version-10 transition guards.
INSERT INTO public.payment_sagas (
    saga_id, payment_intent_id, reservation_id
) VALUES
    ('73000000-0000-4000-8000-000000000001',
     '72000000-0000-4000-8000-000000000001',
     '71000000-0000-4000-8000-000000000001'),
    ('73000000-0000-4000-8000-000000000004',
     '72000000-0000-4000-8000-000000000004',
     '71000000-0000-4000-8000-000000000004');

UPDATE public.payment_sagas
   SET state = 'reservation_secured', current_step = 'create_checkout'
 WHERE saga_id IN (
    '73000000-0000-4000-8000-000000000001',
    '73000000-0000-4000-8000-000000000004'
 );
UPDATE public.payment_sagas
   SET state = 'checkout_created', current_step = 'await_provider'
 WHERE saga_id IN (
    '73000000-0000-4000-8000-000000000001',
    '73000000-0000-4000-8000-000000000004'
 );
UPDATE public.payment_sagas
   SET state = 'awaiting_provider', current_step = 'authorize'
 WHERE saga_id IN (
    '73000000-0000-4000-8000-000000000001',
    '73000000-0000-4000-8000-000000000004'
 );
UPDATE public.payment_sagas
   SET state = 'authorized', current_step = 'capture'
 WHERE saga_id IN (
    '73000000-0000-4000-8000-000000000001',
    '73000000-0000-4000-8000-000000000004'
 );
UPDATE public.payment_sagas
   SET state = 'capturing', current_step = 'capture'
 WHERE saga_id IN (
    '73000000-0000-4000-8000-000000000001',
    '73000000-0000-4000-8000-000000000004'
 );
UPDATE public.payment_sagas
   SET state = 'captured', current_step = 'issue_tickets'
 WHERE saga_id IN (
    '73000000-0000-4000-8000-000000000001',
    '73000000-0000-4000-8000-000000000004'
 );
UPDATE public.payment_sagas
   SET state = 'issuing_tickets', current_step = 'issue_tickets'
 WHERE saga_id = '73000000-0000-4000-8000-000000000001';
UPDATE public.payment_sagas
   SET state = 'compensating', current_step = 'refund'
 WHERE saga_id = '73000000-0000-4000-8000-000000000004';
UPDATE public.payment_sagas
   SET state = 'refunding', current_step = 'compensate'
 WHERE saga_id = '73000000-0000-4000-8000-000000000004';

-- Historical M6 financial effects must remain reconstructible when the
-- operational double-entry ledger is introduced by version 11. The three
-- cases distinguish an issued sale, an issued-then-refunded sale, and a
-- captured-then-refunded payment for which ticket issuance never succeeded.
INSERT INTO public.reservation_directory (
    reservation_id, train_run_id, owner_user_id, state,
    last_known_shard_id, last_known_generation, legacy_imported, active_at
)
SELECT fixture.reservation_id,
       '66666666-6666-4666-8666-666666666666'::uuid,
       '11111111-1111-4111-8111-111111111111'::uuid,
       'active', 'legacy', 1, true, '2026-01-02 00:00:00+00'
FROM (VALUES
    ('74000000-0000-4000-8000-000000000001'::uuid),
    ('74000000-0000-4000-8000-000000000002'::uuid),
    ('74000000-0000-4000-8000-000000000003'::uuid)
) AS fixture(reservation_id);

INSERT INTO public.reservation_shard_locators (
    reservation_id, train_run_id, shard_id,
    assignment_generation, owner_user_id, created_at, updated_at
)
SELECT fixture.reservation_id,
       '66666666-6666-4666-8666-666666666666'::uuid,
       'legacy', 1,
       '11111111-1111-4111-8111-111111111111'::uuid,
       '2026-01-02 00:00:00+00', '2026-01-02 00:00:00+00'
FROM (VALUES
    ('74000000-0000-4000-8000-000000000001'::uuid),
    ('74000000-0000-4000-8000-000000000002'::uuid),
    ('74000000-0000-4000-8000-000000000003'::uuid)
) AS fixture(reservation_id);

INSERT INTO public.payment_intents (
    payment_intent_id, reservation_id, train_run_id, owner_user_id,
    provider, amount_minor, currency,
    idempotency_key_hash, request_fingerprint, created_at, updated_at
)
SELECT fixture.payment_intent_id, fixture.reservation_id,
       '66666666-6666-4666-8666-666666666666'::uuid,
       '11111111-1111-4111-8111-111111111111'::uuid,
       'sandbox', 12500, 'TWD',
       decode(repeat(fixture.hash_byte, 32), 'hex'),
       decode(repeat(fixture.fingerprint_byte, 32), 'hex'),
       '2026-01-02 00:00:00+00', '2026-01-02 00:00:00+00'
FROM (VALUES
    ('75000000-0000-4000-8000-000000000001'::uuid,
     '74000000-0000-4000-8000-000000000001'::uuid, '31', '41'),
    ('75000000-0000-4000-8000-000000000002'::uuid,
     '74000000-0000-4000-8000-000000000002'::uuid, '32', '42'),
    ('75000000-0000-4000-8000-000000000003'::uuid,
     '74000000-0000-4000-8000-000000000003'::uuid, '33', '43')
) AS fixture(
    payment_intent_id, reservation_id, hash_byte, fingerprint_byte
);

-- Exercise the version-10 intent guards instead of seeding terminal rows by
-- bypassing constraints.
UPDATE public.payment_intents
   SET state = 'reservation_securing'
 WHERE payment_intent_id::text LIKE '75000000-0000-4000-8000-%';
UPDATE public.payment_intents
   SET state = 'checkout_pending',
       provider_payment_id = 'm7-historical-' || right(payment_intent_id::text, 12)
 WHERE payment_intent_id::text LIKE '75000000-0000-4000-8000-%';
UPDATE public.payment_intents
   SET state = 'awaiting_customer'
 WHERE payment_intent_id::text LIKE '75000000-0000-4000-8000-%';
UPDATE public.payment_intents
   SET state = 'authorization_pending'
 WHERE payment_intent_id::text LIKE '75000000-0000-4000-8000-%';
UPDATE public.payment_intents
   SET state = 'authorized'
 WHERE payment_intent_id::text LIKE '75000000-0000-4000-8000-%';
UPDATE public.payment_intents
   SET state = 'capture_pending'
 WHERE payment_intent_id::text LIKE '75000000-0000-4000-8000-%';
UPDATE public.payment_intents
   SET state = 'captured'
 WHERE payment_intent_id::text LIKE '75000000-0000-4000-8000-%';

INSERT INTO public.payment_operations (
    operation_id, payment_intent_id, provider, operation_type,
    provider_idempotency_key_hash, amount_minor, currency,
    created_at, updated_at
)
SELECT fixture.operation_id, fixture.payment_intent_id, 'sandbox',
       fixture.operation_type,
       decode(repeat(fixture.hash_byte, 32), 'hex'),
       12500, 'TWD', fixture.created_at, fixture.created_at
FROM (VALUES
    ('76000000-0000-4000-8000-000000000001'::uuid,
     '75000000-0000-4000-8000-000000000001'::uuid,
     'capture', '51', '2026-01-02 00:00:01+00'::timestamptz),
    ('76000000-0000-4000-8000-000000000002'::uuid,
     '75000000-0000-4000-8000-000000000002'::uuid,
     'capture', '52', '2026-01-02 00:00:02+00'::timestamptz),
    ('76000000-0000-4000-8000-000000000003'::uuid,
     '75000000-0000-4000-8000-000000000003'::uuid,
     'capture', '53', '2026-01-02 00:00:03+00'::timestamptz)
) AS fixture(
    operation_id, payment_intent_id, operation_type, hash_byte, created_at
);

UPDATE public.payment_operations
   SET state = 'claimed', lease_owner = 'm7-v10-fixture',
       lease_until = '2026-01-02 00:05:00+00'
 WHERE operation_id::text LIKE '76000000-0000-4000-8000-%';
UPDATE public.payment_operations
   SET state = 'in_flight'
 WHERE operation_id::text LIKE '76000000-0000-4000-8000-%';
UPDATE public.payment_operations
   SET state = 'succeeded',
       provider_operation_id = 'm7-capture-' || right(operation_id::text, 12),
       normalized_provider_state = 'captured',
       response_fingerprint = sha256(convert_to(operation_id::text, 'UTF8')),
       completed_at = created_at + interval '10 seconds',
       lease_owner = NULL, lease_until = NULL
 WHERE operation_id::text LIKE '76000000-0000-4000-8000-%';

INSERT INTO public.ticket_order_shard_locators (
    ticket_order_id, reservation_id, train_run_id, shard_id,
    assignment_generation, owner_user_id, status, total_amount_minor,
    currency, created_at, updated_at
) VALUES
    (
        '78000000-0000-4000-8000-000000000001',
        '74000000-0000-4000-8000-000000000001',
        '66666666-6666-4666-8666-666666666666', 'legacy', 1,
        '11111111-1111-4111-8111-111111111111', 'confirmed', 12500, 'TWD',
        '2026-01-02 00:01:01+00', '2026-01-02 00:01:01+00'
    ),
    (
        '78000000-0000-4000-8000-000000000002',
        '74000000-0000-4000-8000-000000000002',
        '66666666-6666-4666-8666-666666666666', 'legacy', 1,
        '11111111-1111-4111-8111-111111111111', 'cancelled', 12500, 'TWD',
        '2026-01-02 00:01:02+00', '2026-01-02 00:02:02+00'
    );

UPDATE public.payment_intents
   SET state = 'ticket_issue_pending'
 WHERE payment_intent_id IN (
    '75000000-0000-4000-8000-000000000001',
    '75000000-0000-4000-8000-000000000002'
 );
UPDATE public.payment_intents
   SET state = 'completed', completed_at = '2026-01-02 00:01:10+00'
 WHERE payment_intent_id IN (
    '75000000-0000-4000-8000-000000000001',
    '75000000-0000-4000-8000-000000000002'
 );
UPDATE public.payment_intents
   SET state = 'refund_pending'
 WHERE payment_intent_id IN (
    '75000000-0000-4000-8000-000000000002',
    '75000000-0000-4000-8000-000000000003'
 );

INSERT INTO public.payment_operations (
    operation_id, payment_intent_id, provider, operation_type,
    provider_idempotency_key_hash, amount_minor, currency,
    created_at, updated_at
)
SELECT fixture.operation_id, fixture.payment_intent_id, 'sandbox', 'refund',
       decode(repeat(fixture.hash_byte, 32), 'hex'),
       12500, 'TWD', fixture.created_at, fixture.created_at
FROM (VALUES
    ('77000000-0000-4000-8000-000000000002'::uuid,
     '75000000-0000-4000-8000-000000000002'::uuid,
     '62', '2026-01-02 00:02:02+00'::timestamptz),
    ('77000000-0000-4000-8000-000000000003'::uuid,
     '75000000-0000-4000-8000-000000000003'::uuid,
     '63', '2026-01-02 00:02:03+00'::timestamptz)
) AS fixture(operation_id, payment_intent_id, hash_byte, created_at);

UPDATE public.payment_operations
   SET state = 'claimed', lease_owner = 'm7-v10-fixture',
       lease_until = '2026-01-02 00:10:00+00'
 WHERE operation_id::text LIKE '77000000-0000-4000-8000-%';
UPDATE public.payment_operations
   SET state = 'in_flight'
 WHERE operation_id::text LIKE '77000000-0000-4000-8000-%';
UPDATE public.payment_operations
   SET state = 'succeeded',
       provider_operation_id = 'm7-refund-' || right(operation_id::text, 12),
       normalized_provider_state = 'refunded',
       response_fingerprint = sha256(convert_to(operation_id::text, 'UTF8')),
       completed_at = created_at + interval '10 seconds',
       lease_owner = NULL, lease_until = NULL
 WHERE operation_id::text LIKE '77000000-0000-4000-8000-%';

UPDATE public.payment_intents
   SET state = 'refunded',
       completed_at = COALESCE(completed_at, '2026-01-02 00:02:20+00')
 WHERE payment_intent_id IN (
    '75000000-0000-4000-8000-000000000002',
    '75000000-0000-4000-8000-000000000003'
 );

-- Historical issuance identity is derived from the durable saga, never from
-- a version-11 payment_saga_actions row (whose backfilled ID is intentionally
-- generated at upgrade time). Exercise legal version-10 transitions so the
-- populated migration proves the same constructor as worker and repair.
INSERT INTO public.payment_sagas (
    saga_id, payment_intent_id, reservation_id, created_at, updated_at
) VALUES
    ('79000000-0000-4000-8000-000000000001',
     '75000000-0000-4000-8000-000000000001',
     '74000000-0000-4000-8000-000000000001',
     '2026-01-02 00:00:00+00', '2026-01-02 00:00:00+00'),
    ('79000000-0000-4000-8000-000000000002',
     '75000000-0000-4000-8000-000000000002',
     '74000000-0000-4000-8000-000000000002',
     '2026-01-02 00:00:00+00', '2026-01-02 00:00:00+00'),
    ('79000000-0000-4000-8000-000000000003',
     '75000000-0000-4000-8000-000000000003',
     '74000000-0000-4000-8000-000000000003',
     '2026-01-02 00:00:00+00', '2026-01-02 00:00:00+00');

UPDATE public.payment_sagas SET state='reservation_secured',current_step='create_checkout'
 WHERE saga_id::text LIKE '79000000-0000-4000-8000-%';
UPDATE public.payment_sagas SET state='checkout_created',current_step='await_provider'
 WHERE saga_id::text LIKE '79000000-0000-4000-8000-%';
UPDATE public.payment_sagas SET state='awaiting_provider',current_step='authorize'
 WHERE saga_id::text LIKE '79000000-0000-4000-8000-%';
UPDATE public.payment_sagas SET state='authorized',current_step='capture'
 WHERE saga_id::text LIKE '79000000-0000-4000-8000-%';
UPDATE public.payment_sagas SET state='capturing',current_step='capture'
 WHERE saga_id::text LIKE '79000000-0000-4000-8000-%';
UPDATE public.payment_sagas SET state='captured',current_step='issue_tickets'
 WHERE saga_id::text LIKE '79000000-0000-4000-8000-%';
UPDATE public.payment_sagas SET state='issuing_tickets',current_step='issue_tickets'
 WHERE saga_id IN (
    '79000000-0000-4000-8000-000000000001',
    '79000000-0000-4000-8000-000000000002'
 );
UPDATE public.payment_sagas SET state='completed',current_step='complete',completed_at='2026-01-02 00:01:10+00'
 WHERE saga_id IN (
    '79000000-0000-4000-8000-000000000001',
    '79000000-0000-4000-8000-000000000002'
 );
UPDATE public.payment_sagas SET state='compensating',current_step='refund',completed_at=NULL
 WHERE saga_id IN (
    '79000000-0000-4000-8000-000000000002',
    '79000000-0000-4000-8000-000000000003'
 );
UPDATE public.payment_sagas SET state='refunding',current_step='compensate'
 WHERE saga_id IN (
    '79000000-0000-4000-8000-000000000002',
    '79000000-0000-4000-8000-000000000003'
 );
UPDATE public.payment_sagas SET state='compensated',current_step='complete',completed_at='2026-01-02 00:02:20+00'
 WHERE saga_id IN (
    '79000000-0000-4000-8000-000000000002',
    '79000000-0000-4000-8000-000000000003'
 );

COMMIT;
