BEGIN;

DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS idempotency_records;

COMMIT;
