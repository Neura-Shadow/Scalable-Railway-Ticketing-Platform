DO $assert$
BEGIN
    IF (SELECT count(*) FROM train_run_journey_read_model) <> 0 THEN
        RAISE EXCEPTION 'read-model projection unexpectedly survived one-step down/up';
    END IF;
    IF (SELECT count(*) FROM read_model_event_receipts) <> 0 THEN
        RAISE EXCEPTION 'read-model receipts unexpectedly survived one-step down/up';
    END IF;
    IF (SELECT count(*) FROM outbox_events WHERE id = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc') <> 0 THEN
        RAISE EXCEPTION 'version-7-only outbox event unexpectedly survived one-step down/up';
    END IF;
END
$assert$;
