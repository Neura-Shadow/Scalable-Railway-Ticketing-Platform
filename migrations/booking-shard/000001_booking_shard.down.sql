BEGIN;

-- Drop only version-1 booking-shard objects. No CASCADE is used: a later
-- migration that depends on these objects must be rolled back first.

DROP TABLE IF EXISTS migration_apply_receipts;
DROP TABLE IF EXISTS train_run_mutation_journal;
DROP TABLE IF EXISTS migration_capture_state;
DROP TABLE IF EXISTS train_run_target_write_evidence;
DROP TABLE IF EXISTS migration_outbox_staging;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS tickets;
DROP TABLE IF EXISTS ticket_orders;
DROP TABLE IF EXISTS reservation_seats;
DROP TABLE IF EXISTS reservations;
DROP TABLE IF EXISTS seat_inventory;
DROP TABLE IF EXISTS idempotency_records;
DROP TABLE IF EXISTS booking_command_receipts;
DROP TABLE IF EXISTS booking_fare_snapshots;
DROP TABLE IF EXISTS booking_seat_catalog;
DROP TABLE IF EXISTS train_run_write_fences;
DROP TABLE IF EXISTS train_run_booking_snapshots;

DROP FUNCTION IF EXISTS booking_shard_capture_mutation();
DROP FUNCTION IF EXISTS booking_shard_guard_capture_state();
DROP FUNCTION IF EXISTS booking_shard_guard_target_write_evidence();
DROP FUNCTION IF EXISTS booking_shard_guard_fence_generation();
DROP FUNCTION IF EXISTS booking_shard_set_updated_at();

COMMIT;
