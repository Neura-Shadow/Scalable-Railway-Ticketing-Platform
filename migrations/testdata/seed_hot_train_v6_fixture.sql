BEGIN;

SET LOCAL TIME ZONE 'UTC';

INSERT INTO hot_train_policies (
    id,
    train_run_id,
    seat_class,
    enabled,
    version,
    redis_initialized_version,
    max_queue_size,
    admission_rate_per_second,
    max_inflight_admissions,
    admission_token_ttl_seconds,
    processing_lease_seconds,
    queue_entry_ttl_seconds,
    created_at,
    updated_at
) VALUES (
    '99999999-9999-4999-8999-999999999999',
    '66666666-6666-4666-8666-666666666666',
    'standard',
    true,
    1,
    NULL,
    1000,
    100,
    100,
    120,
    30,
    600,
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
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    'hot_train_policy',
    '99999999-9999-4999-8999-999999999999',
    'hot_train_policy.created',
    1,
    '{"fixture":"populated-v6","policy_id":"99999999-9999-4999-8999-999999999999"}',
    'pending',
    0,
    '2026-01-01 00:00:00+00',
    '2026-01-01 00:00:00+00'
);

COMMIT;
