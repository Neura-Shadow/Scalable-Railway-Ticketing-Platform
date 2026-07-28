BEGIN;

SELECT pg_advisory_xact_lock(804230008);

-- Version 7 cannot represent schema-local authoritative booking state. A
-- downgrade is therefore permitted only after every run is back on legacy,
-- every migration is terminal, and both logical schemas contain no copied or
-- authoritative booking rows. The check prevents a destructive CASCADE.
DO $m4_down_preflight$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.train_run_shard_assignments
        WHERE shard_id <> 'legacy'
           OR assignment_state <> 'stable'
           OR active_migration_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while a train run is non-legacy or migrating'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.train_run_shard_migrations
        WHERE state NOT IN ('completed', 'failed', 'rolled_back')
    ) THEN
        RAISE EXCEPTION 'cannot downgrade with an active shard migration'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1 FROM booking_shard_0.seat_inventory
        UNION ALL SELECT 1 FROM booking_shard_0.reservations
        UNION ALL SELECT 1 FROM booking_shard_0.reservation_seats
        UNION ALL SELECT 1 FROM booking_shard_0.ticket_orders
        UNION ALL SELECT 1 FROM booking_shard_0.tickets
        UNION ALL SELECT 1 FROM booking_shard_0.idempotency_records
        UNION ALL SELECT 1 FROM booking_shard_1.seat_inventory
        UNION ALL SELECT 1 FROM booking_shard_1.reservations
        UNION ALL SELECT 1 FROM booking_shard_1.reservation_seats
        UNION ALL SELECT 1 FROM booking_shard_1.ticket_orders
        UNION ALL SELECT 1 FROM booking_shard_1.tickets
        UNION ALL SELECT 1 FROM booking_shard_1.idempotency_records
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while a logical shard retains booking data'
            USING ERRCODE = '55000';
    END IF;
END
$m4_down_preflight$;

DROP TRIGGER IF EXISTS tickets_sync_legacy_locator ON public.tickets;
DROP TRIGGER IF EXISTS ticket_orders_sync_legacy_locator ON public.ticket_orders;
DROP TRIGGER IF EXISTS reservation_seats_sync_legacy_quota_claim
    ON public.reservation_seats;
DROP TRIGGER IF EXISTS reservations_sync_legacy_quota_claim
    ON public.reservations;
DROP TRIGGER IF EXISTS reservations_sync_legacy_locator ON public.reservations;
DROP TRIGGER IF EXISTS idempotency_records_sync_key_claim
    ON public.idempotency_records;
DROP TRIGGER IF EXISTS idempotency_records_populate_train_run
    ON public.idempotency_records;

DROP FUNCTION IF EXISTS public.sync_legacy_ticket_locator();
DROP FUNCTION IF EXISTS public.sync_legacy_ticket_order_locator();
DROP FUNCTION IF EXISTS public.sync_legacy_reservation_quota_claim();
DROP FUNCTION IF EXISTS public.sync_legacy_reservation_locator();
DROP FUNCTION IF EXISTS public.sync_legacy_idempotency_key_claim();
DROP FUNCTION IF EXISTS public.populate_legacy_idempotency_train_run();

DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.tickets;
DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.ticket_orders;
DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.reservation_seats;
DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.reservations;
DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.seat_inventory;
DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.idempotency_records;

DROP FUNCTION IF EXISTS public.guard_legacy_booking_write();
DROP FUNCTION IF EXISTS public.assert_legacy_train_run_writable(uuid);
DROP FUNCTION IF EXISTS public.legacy_cleanup_allowed(uuid);
DROP FUNCTION IF EXISTS public.legacy_migration_copy_allowed(uuid);

DROP TRIGGER IF EXISTS train_runs_bootstrap_shard_assignment
    ON public.train_runs;
DROP FUNCTION IF EXISTS public.bootstrap_train_run_shard_assignment();

DROP TRIGGER IF EXISTS train_run_write_fences_validate
    ON booking_shard_1.train_run_write_fences;
DROP TRIGGER IF EXISTS train_run_write_fences_validate
    ON booking_shard_0.train_run_write_fences;
DROP TRIGGER IF EXISTS train_run_write_fences_validate
    ON public.train_run_write_fences;
DROP TRIGGER IF EXISTS train_run_shard_assignments_validate_fences
    ON public.train_run_shard_assignments;
DROP FUNCTION IF EXISTS public.validate_train_run_fence_invariant();
DROP FUNCTION IF EXISTS public.assert_train_run_fence_invariant(uuid);

DROP TRIGGER IF EXISTS train_run_shard_migrations_transition
    ON public.train_run_shard_migrations;
DROP FUNCTION IF EXISTS public.guard_migration_state_transition();

DROP TRIGGER IF EXISTS train_run_write_fences_monotonic
    ON booking_shard_1.train_run_write_fences;
DROP TRIGGER IF EXISTS train_run_write_fences_monotonic
    ON booking_shard_0.train_run_write_fences;
DROP TRIGGER IF EXISTS train_run_write_fences_monotonic
    ON public.train_run_write_fences;
DROP FUNCTION IF EXISTS public.guard_fence_monotonicity();

DROP TRIGGER IF EXISTS train_run_shard_assignments_monotonic
    ON public.train_run_shard_assignments;
DROP FUNCTION IF EXISTS public.guard_assignment_monotonicity();

DROP TRIGGER IF EXISTS outbox_events_populate_shard_provenance
    ON public.outbox_events;
DROP FUNCTION IF EXISTS public.populate_outbox_shard_provenance();

DROP INDEX IF EXISTS public.outbox_events_train_run_provenance_idx;
ALTER TABLE public.outbox_events
    DROP CONSTRAINT IF EXISTS outbox_events_shard_provenance_check,
    DROP COLUMN IF EXISTS assignment_generation,
    DROP COLUMN IF EXISTS shard_id,
    DROP COLUMN IF EXISTS train_run_id;

DROP INDEX IF EXISTS public.idempotency_records_train_run_idx;
ALTER TABLE public.idempotency_records
    DROP COLUMN IF EXISTS train_run_id;

DROP TABLE IF EXISTS public.ticket_shard_locators;
DROP TABLE IF EXISTS public.ticket_order_shard_locators;
DROP TABLE IF EXISTS public.reservation_shard_locators;
DROP TABLE IF EXISTS public.reservation_quota_claims;
DROP TABLE IF EXISTS public.booking_idempotency_key_claims;

-- Remove only objects owned by this migration. Every DROP uses PostgreSQL's
-- default RESTRICT behavior, so an unmanaged object or a dependency outside
-- the logical shard aborts and rolls back the entire downgrade.
DROP TABLE IF EXISTS booking_shard_1.tickets;
DROP TABLE IF EXISTS booking_shard_1.ticket_orders;
DROP TABLE IF EXISTS booking_shard_1.reservation_seats;
DROP TABLE IF EXISTS booking_shard_1.reservations;
DROP TABLE IF EXISTS booking_shard_1.seat_inventory;
DROP TABLE IF EXISTS booking_shard_1.idempotency_records;
DROP TABLE IF EXISTS booking_shard_1.train_run_write_fences;
DROP FUNCTION IF EXISTS booking_shard_1.validate_reservation_seat();
DROP FUNCTION IF EXISTS booking_shard_1.validate_inventory_seat_class();
DROP SCHEMA IF EXISTS booking_shard_1 RESTRICT;

DROP TABLE IF EXISTS booking_shard_0.tickets;
DROP TABLE IF EXISTS booking_shard_0.ticket_orders;
DROP TABLE IF EXISTS booking_shard_0.reservation_seats;
DROP TABLE IF EXISTS booking_shard_0.reservations;
DROP TABLE IF EXISTS booking_shard_0.seat_inventory;
DROP TABLE IF EXISTS booking_shard_0.idempotency_records;
DROP TABLE IF EXISTS booking_shard_0.train_run_write_fences;
DROP FUNCTION IF EXISTS booking_shard_0.validate_reservation_seat();
DROP FUNCTION IF EXISTS booking_shard_0.validate_inventory_seat_class();
DROP SCHEMA IF EXISTS booking_shard_0 RESTRICT;

ALTER TABLE public.train_run_shard_assignments
    DROP CONSTRAINT IF EXISTS train_run_shard_assignments_active_migration_fkey;

DROP TABLE IF EXISTS public.train_run_generation_writes;
DROP TABLE IF EXISTS public.train_run_shard_migrations;
DROP TABLE IF EXISTS public.train_run_write_fences;
DROP TABLE IF EXISTS public.train_run_shard_assignments;
DROP TABLE IF EXISTS public.booking_shards;

COMMIT;
