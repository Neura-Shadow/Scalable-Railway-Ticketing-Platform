DO $assert$
BEGIN
    IF (SELECT count(*) FROM train_run_journey_read_model WHERE train_run_id = '66666666-6666-4666-8666-666666666666') <> 1 THEN
        RAISE EXCEPTION 'read-model fixture row missing';
    END IF;
    IF (SELECT count(*) FROM read_model_event_receipts WHERE event_id = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb') <> 1 THEN
        RAISE EXCEPTION 'read-model receipt fixture missing';
    END IF;
    IF (SELECT count(*) FROM outbox_events WHERE id = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc') <> 1 THEN
        RAISE EXCEPTION 'read-model outbox fixture missing';
    END IF;
END
$assert$;
