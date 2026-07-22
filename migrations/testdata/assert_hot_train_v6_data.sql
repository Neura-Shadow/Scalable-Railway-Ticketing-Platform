DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM hot_train_policies
        WHERE id = '99999999-9999-4999-8999-999999999999'
          AND train_run_id = '66666666-6666-4666-8666-666666666666'
          AND seat_class = 'standard'
          AND enabled
          AND version = 1
          AND redis_initialized_version IS NULL
          AND max_queue_size = 1000
          AND admission_rate_per_second = 100
          AND max_inflight_admissions = 100
          AND admission_token_ttl_seconds = 120
          AND processing_lease_seconds = 30
          AND queue_entry_ttl_seconds = 600
    ) THEN
        RAISE EXCEPTION 'populated version-6 hot-train policy was lost or changed';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM outbox_events
        WHERE id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
          AND aggregate_type = 'hot_train_policy'
          AND aggregate_id = '99999999-9999-4999-8999-999999999999'
          AND event_type = 'hot_train_policy.created'
          AND event_version = 1
          AND payload = '{"fixture":"populated-v6","policy_id":"99999999-9999-4999-8999-999999999999"}'::jsonb
          AND status = 'pending'
          AND attempts = 0
    ) THEN
        RAISE EXCEPTION 'populated version-6 hot-train policy outbox event was lost or changed';
    END IF;
END;
$assert$;
