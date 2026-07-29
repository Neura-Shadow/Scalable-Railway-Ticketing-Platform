BEGIN;

SELECT pg_advisory_xact_lock(804230009);

-- Version 8 cannot represent physical assignments or physical recovery state.
-- Refuse a destructive downgrade until all version-9-only durable work has
-- been explicitly retired. Imported reservation-directory rows are safe to
-- remove because their complete version-8 locators remain unchanged.
DO $m5_down_preflight$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.train_run_shard_assignments
        WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while a train run is assigned to a physical shard'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.train_run_shard_migrations
        WHERE source_shard_id IN ('physical-shard-0', 'physical-shard-1')
           OR target_shard_id IN ('physical-shard-0', 'physical-shard-1')
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while the legacy migration ledger references a physical shard'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.physical_shard_migrations) THEN
        RAISE EXCEPTION 'cannot downgrade while physical migration history is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.booking_commands) THEN
        RAISE EXCEPTION 'cannot downgrade while version-9 booking commands are retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.operator_booking_commands) THEN
        RAISE EXCEPTION 'cannot downgrade while version-9 operator commands are retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.booking_quota_leases) THEN
        RAISE EXCEPTION 'cannot downgrade while version-9 quota leases are retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.reservation_directory AS directory
        FULL JOIN public.reservation_shard_locators AS locator
          ON locator.reservation_id = directory.reservation_id
        WHERE directory.reservation_id IS NULL
           OR locator.reservation_id IS NULL
           OR NOT directory.legacy_imported
           OR directory.command_id IS NOT NULL
           OR directory.state <> 'active'
           OR directory.train_run_id <> locator.train_run_id
           OR directory.owner_user_id <> locator.owner_user_id
           OR directory.last_known_shard_id <> locator.shard_id
           OR directory.last_known_generation <> locator.assignment_generation
    ) THEN
        RAISE EXCEPTION 'cannot downgrade after the version-9 reservation directory diverged from version-8 locators'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.physical_shard_reconciliation_runs) THEN
        RAISE EXCEPTION 'cannot downgrade while version-9 reconciliation evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.outbox_events
        WHERE aggregate_type IN ('booking_command', 'physical_shard_migration')
           OR event_type IN (
                'booking_command.finalized', 'booking_command.repaired',
                'booking_command.failed', 'physical_shard_migration.cutover',
                'physical_shard_migration.rolled_back',
                'physical_shard_migration.reverse_cutover',
                'physical_shard_migration.completed'
           )
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while version-9 control outbox intent is retained'
            USING ERRCODE = '55000';
    END IF;
END
$m5_down_preflight$;

DROP TRIGGER physical_source_capture_train_run ON public.train_runs;
DROP TRIGGER physical_source_capture_fare ON public.fares;
DROP TRIGGER physical_source_capture_seat ON public.seats;
DROP TRIGGER physical_source_capture ON public.outbox_events;
DROP TRIGGER physical_source_capture ON public.idempotency_records;
DROP TRIGGER physical_source_capture ON public.tickets;
DROP TRIGGER physical_source_capture ON public.ticket_orders;
DROP TRIGGER physical_source_capture ON public.reservation_seats;
DROP TRIGGER physical_source_capture ON public.reservations;
DROP TRIGGER physical_source_capture ON public.seat_inventory;
DROP TRIGGER physical_source_capture ON booking_shard_0.idempotency_records;
DROP TRIGGER physical_source_capture ON booking_shard_0.tickets;
DROP TRIGGER physical_source_capture ON booking_shard_0.ticket_orders;
DROP TRIGGER physical_source_capture ON booking_shard_0.reservation_seats;
DROP TRIGGER physical_source_capture ON booking_shard_0.reservations;
DROP TRIGGER physical_source_capture ON booking_shard_0.seat_inventory;
DROP TRIGGER physical_source_capture ON booking_shard_1.idempotency_records;
DROP TRIGGER physical_source_capture ON booking_shard_1.tickets;
DROP TRIGGER physical_source_capture ON booking_shard_1.ticket_orders;
DROP TRIGGER physical_source_capture ON booking_shard_1.reservation_seats;
DROP TRIGGER physical_source_capture ON booking_shard_1.reservations;
DROP TRIGGER physical_source_capture ON booking_shard_1.seat_inventory;

DROP FUNCTION public.capture_physical_source_seat_reference();
DROP FUNCTION public.capture_physical_source_fare_reference();
DROP FUNCTION public.capture_physical_source_train_run_reference();
DROP FUNCTION public.capture_physical_source_booking_mutation();
DROP FUNCTION public.append_physical_source_mutation(uuid, text, text, text, uuid, jsonb);

DROP VIEW public.physical_source_outbox_rows;
DROP VIEW public.physical_source_idempotency_rows;
DROP VIEW public.physical_source_ticket_rows;
DROP VIEW public.physical_source_ticket_order_rows;
DROP VIEW public.physical_source_reservation_seat_rows;
DROP VIEW public.physical_source_reservation_rows;
DROP VIEW public.physical_source_seat_inventory_rows;

DROP FUNCTION public.physical_source_entity_id(uuid, text, uuid);
DROP TABLE public.physical_control_target_apply_authorizations;
DROP TABLE public.physical_control_target_apply_receipts;
DROP TABLE public.physical_source_train_run_mutation_journal;
DROP TABLE public.physical_source_migration_capture_state;

ALTER TABLE public.outbox_events
    DROP CONSTRAINT outbox_events_event_pair_check,
    DROP CONSTRAINT outbox_events_aggregate_type_check,
    DROP CONSTRAINT outbox_events_event_type_check,
    ADD CONSTRAINT outbox_events_aggregate_type_check CHECK (
        aggregate_type IN (
            'reservation', 'ticket', 'train_run', 'hot_train_policy',
            'station', 'route', 'train', 'coach', 'seat', 'fare'
        )
    ),
    ADD CONSTRAINT outbox_events_event_type_check CHECK (
        event_type IN (
            'reservation.held', 'reservation.confirmed',
            'reservation.expired', 'reservation.cancelled', 'ticket.created',
            'trainrun.created', 'trainrun.updated', 'trainrun.cancelled',
            'hot_train_policy.created', 'hot_train_policy.updated',
            'hot_train_policy.disabled', 'station.created', 'station.updated',
            'station.disabled', 'route.created', 'route.updated',
            'route.disabled', 'train.updated', 'coach.updated', 'seat.updated',
            'fare.created', 'fare.updated', 'fare.disabled'
        )
    ),
    ADD CONSTRAINT outbox_events_event_pair_check CHECK (
        (aggregate_type = 'reservation' AND event_type IN (
            'reservation.held', 'reservation.confirmed',
            'reservation.expired', 'reservation.cancelled'
        ))
        OR (aggregate_type = 'ticket' AND event_type = 'ticket.created')
        OR (aggregate_type = 'train_run' AND event_type IN (
            'trainrun.created', 'trainrun.updated', 'trainrun.cancelled'
        ))
        OR (aggregate_type = 'hot_train_policy' AND event_type IN (
            'hot_train_policy.created', 'hot_train_policy.updated',
            'hot_train_policy.disabled'
        ))
        OR (aggregate_type = 'station' AND event_type IN (
            'station.created', 'station.updated', 'station.disabled'
        ))
        OR (aggregate_type = 'route' AND event_type IN (
            'route.created', 'route.updated', 'route.disabled'
        ))
        OR (aggregate_type = 'train' AND event_type = 'train.updated')
        OR (aggregate_type = 'coach' AND event_type = 'coach.updated')
        OR (aggregate_type = 'seat' AND event_type = 'seat.updated')
        OR (aggregate_type = 'fare' AND event_type IN (
            'fare.created', 'fare.updated', 'fare.disabled'
        ))
    );

DROP TABLE public.physical_shard_reconciliation_runs;
DROP TABLE public.physical_shard_target_write_observations;
DROP TABLE public.physical_shard_migration_checkpoints;

-- Restore the version-8 invariant before removing the physical migration
-- ledger and assignment column on which the version-9 implementation depends.
CREATE OR REPLACE FUNCTION public.assert_train_run_fence_invariant(
    checked_train_run_id uuid
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    assigned_shard_id text;
    assigned_generation bigint;
    assigned_state text;
    catalog_enabled boolean;
    catalog_write_enabled boolean;
    enabled_fence_count integer;
    matching_fence_count integer;
BEGIN
    SELECT assignment.shard_id,
           assignment.assignment_generation,
           assignment.assignment_state,
           shard.enabled,
           shard.write_enabled
    INTO assigned_shard_id,
         assigned_generation,
         assigned_state,
         catalog_enabled,
         catalog_write_enabled
    FROM public.train_run_shard_assignments AS assignment
    JOIN public.booking_shards AS shard
      ON shard.shard_id = assignment.shard_id
    WHERE assignment.train_run_id = checked_train_run_id;

    SELECT count(*) FILTER (WHERE fence.write_enabled),
           count(*) FILTER (
               WHERE fence.write_enabled
                 AND fence.shard_id = assigned_shard_id
                 AND fence.assignment_generation = assigned_generation
           )
    INTO enabled_fence_count, matching_fence_count
    FROM (
        SELECT 'legacy'::text AS shard_id,
               assignment_generation,
               write_enabled
        FROM public.train_run_write_fences
        WHERE train_run_id = checked_train_run_id
        UNION ALL
        SELECT 'shard-0'::text,
               assignment_generation,
               write_enabled
        FROM booking_shard_0.train_run_write_fences
        WHERE train_run_id = checked_train_run_id
        UNION ALL
        SELECT 'shard-1'::text,
               assignment_generation,
               write_enabled
        FROM booking_shard_1.train_run_write_fences
        WHERE train_run_id = checked_train_run_id
    ) AS fence;

    IF assigned_shard_id IS NULL THEN
        IF enabled_fence_count <> 0 THEN
            RAISE EXCEPTION 'enabled booking fence lacks an assignment'
                USING ERRCODE = '23514';
        END IF;
        RETURN;
    END IF;

    IF enabled_fence_count > 1 THEN
        RAISE EXCEPTION 'multiple booking writers are enabled for one train run'
            USING ERRCODE = '23514';
    END IF;

    IF assigned_state = 'migrating' THEN
        IF enabled_fence_count <> 0 THEN
            RAISE EXCEPTION 'migrating assignment must be in a zero-writer state'
                USING ERRCODE = '23514';
        END IF;
        RETURN;
    END IF;

    IF enabled_fence_count <> 1
       OR matching_fence_count <> 1
       OR NOT catalog_enabled
       OR NOT catalog_write_enabled THEN
        RAISE EXCEPTION 'stable assignment lacks exactly one matching enabled fence'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.assert_legacy_train_run_writable(
    checked_train_run_id uuid
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    assigned_generation bigint;
    assigned_state text;
    shard_enabled boolean;
    shard_write_enabled boolean;
    fence_generation bigint;
    fence_write_enabled boolean;
BEGIN
    SELECT assignment.assignment_generation,
           assignment.assignment_state,
           shard.enabled,
           shard.write_enabled
    INTO assigned_generation,
         assigned_state,
         shard_enabled,
         shard_write_enabled
    FROM public.train_run_shard_assignments AS assignment
    JOIN public.booking_shards AS shard
      ON shard.shard_id = assignment.shard_id
    WHERE assignment.train_run_id = checked_train_run_id
      AND assignment.shard_id = 'legacy'
    FOR UPDATE OF assignment;

    IF assigned_generation IS NULL
       OR assigned_state = 'migrating'
       OR NOT shard_enabled
       OR NOT shard_write_enabled THEN
        RAISE EXCEPTION 'booking write is fenced'
            USING ERRCODE = '55000';
    END IF;

    SELECT fence.assignment_generation, fence.write_enabled
    INTO fence_generation, fence_write_enabled
    FROM public.train_run_write_fences AS fence
    WHERE fence.train_run_id = checked_train_run_id
    FOR UPDATE;

    IF fence_generation IS NULL
       OR fence_generation <> assigned_generation
       OR NOT fence_write_enabled THEN
        RAISE EXCEPTION 'booking write is fenced'
            USING ERRCODE = '55000';
    END IF;
END;
$$;

DROP INDEX public.train_run_shard_assignments_active_physical_migration_idx;
ALTER TABLE public.train_run_shard_assignments
    DROP CONSTRAINT train_run_shard_assignments_one_migration_kind_check,
    DROP CONSTRAINT train_run_shard_assignments_active_physical_migration_fkey,
    DROP COLUMN active_physical_migration_id;

DROP TRIGGER physical_shard_migrations_operator_command_guard
    ON public.physical_shard_migrations;
DROP FUNCTION public.reject_physical_migration_with_operator_command();
DROP TABLE public.physical_shard_migrations;
DROP TABLE public.reservation_directory;
DROP TABLE public.booking_quota_leases;
DROP TABLE public.booking_commands;
DROP TABLE public.train_run_booking_policy_versions;
DROP TABLE public.train_run_seat_booking_overrides;
DROP INDEX public.fares_last_booking_command_unique_idx;
ALTER TABLE public.fares
    DROP COLUMN last_booking_command_id,
    DROP COLUMN source_version;
DROP TABLE public.operator_booking_commands;

DELETE FROM public.booking_shards
WHERE shard_id IN ('physical-shard-0', 'physical-shard-1');

DROP INDEX public.booking_shards_connection_ref_unique_idx;

ALTER TABLE public.booking_shards
    DROP CONSTRAINT booking_shards_write_metadata_check,
    DROP CONSTRAINT booking_shards_health_state_check,
    DROP CONSTRAINT booking_shards_version_check,
    DROP CONSTRAINT booking_shards_connection_ref_check,
    DROP CONSTRAINT booking_shards_fixed_topology_check,
    DROP CONSTRAINT booking_shards_storage_kind_check;

UPDATE public.booking_shards
SET storage_kind = CASE storage_kind
    WHEN 'legacy_schema' THEN 'legacy'
    WHEN 'logical_schema' THEN 'schema'
    ELSE storage_kind
END;

ALTER TABLE public.booking_shards
    DROP COLUMN write_disabled_reason,
    DROP COLUMN last_health_checked_at,
    DROP COLUMN health_state,
    DROP COLUMN schema_version,
    DROP COLUMN protocol_version,
    DROP COLUMN connection_ref,
    ADD CONSTRAINT booking_shards_storage_kind_check CHECK (
        storage_kind IN ('legacy', 'schema')
    ),
    ADD CONSTRAINT booking_shards_fixed_topology_check CHECK (
        (shard_id = 'legacy' AND storage_kind = 'legacy')
        OR
        (shard_id IN ('shard-0', 'shard-1') AND storage_kind = 'schema')
    );

COMMIT;
