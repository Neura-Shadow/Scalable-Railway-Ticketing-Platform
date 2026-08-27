BEGIN;

SELECT set_config('railway.deployment_region', 'region-a', true),
       set_config('railway.deployment_role', 'active', true),
       set_config('railway.region_epoch', '1', true),
       set_config('railway.regional_writes_enabled', 'true', true);

INSERT INTO public.ticket_refund_compensation_receipts (
    id, command_id, refund_request_id, refund_operation_id,
    payment_intent_id, reservation_id, ticket_order_id,
    train_run_id, assignment_generation,
    request_fingerprint, provider_proof_hash,
    amount_minor, currency, selected_ticket_count, released_seat_count,
    resulting_active_ticket_count, resulting_order_state, committed_at
) VALUES (
    '78000000-0000-4000-8000-000000000001',
    '78000000-0000-4000-8000-000000000002',
    '78000000-0000-4000-8000-000000000003',
    '78000000-0000-4000-8000-000000000004',
    '78000000-0000-4000-8000-000000000005',
    '61000000-0000-0000-0000-000000000010',
    '61000000-0000-0000-0000-000000000014',
    '61000000-0000-0000-0000-000000000002', 1,
    decode(repeat('61', 32), 'hex'),
    decode(repeat('62', 32), 'hex'),
    12500, 'TWD', 1, 1, 0, 'refunded', clock_timestamp()
);

INSERT INTO public.selected_ticket_refund_receipts (
    id, compensation_receipt_id, refund_request_id, ticket_id,
    reservation_seat_id, train_run_id, assignment_generation,
    fare_amount_minor, currency, segment_mask_hash, released_at
) VALUES (
    '78000000-0000-4000-8000-000000000006',
    '78000000-0000-4000-8000-000000000001',
    '78000000-0000-4000-8000-000000000003',
    '61000000-0000-0000-0000-000000000015',
    '61000000-0000-0000-0000-000000000012',
    '61000000-0000-0000-0000-000000000002', 1,
    12500, 'TWD', decode(repeat('63', 32), 'hex'), clock_timestamp()
);

COMMIT;
