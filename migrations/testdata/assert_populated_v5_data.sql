DO $assert$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM reservations
        WHERE id = '77777777-7777-4777-8777-777777777777'
          AND user_id = '11111111-1111-4111-8111-111111111111'
          AND train_run_id = '66666666-6666-4666-8666-666666666666'
          AND segment_count = 1
          AND from_stop_index = 0
          AND to_stop_index = 1
          AND seat_class = 'standard'
          AND status = 'held'
          AND expires_at = '2098-12-31 23:59:00+00'
          AND total_amount_minor = 12500
          AND currency = 'TWD'
    ) THEN
        RAISE EXCEPTION 'populated version-5 reservation was lost or changed';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM outbox_events
        WHERE id = '88888888-8888-4888-8888-888888888888'
          AND aggregate_type = 'reservation'
          AND aggregate_id = '77777777-7777-4777-8777-777777777777'
          AND event_type = 'reservation.held'
          AND event_version = 1
          AND payload = '{"fixture":"populated-v5","reservation_id":"77777777-7777-4777-8777-777777777777"}'::jsonb
          AND status = 'pending'
          AND attempts = 0
    ) THEN
        RAISE EXCEPTION 'populated version-5 outbox event was lost or changed';
    END IF;
END;
$assert$;
