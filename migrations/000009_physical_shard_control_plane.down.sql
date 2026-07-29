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

DROP INDEX public.train_run_shard_assignments_active_physical_migration_idx;
ALTER TABLE public.train_run_shard_assignments
    DROP CONSTRAINT train_run_shard_assignments_one_migration_kind_check,
    DROP CONSTRAINT train_run_shard_assignments_active_physical_migration_fkey,
    DROP COLUMN active_physical_migration_id;

DROP TABLE public.physical_shard_migrations;
DROP TABLE public.reservation_directory;
DROP TABLE public.booking_quota_leases;
DROP TABLE public.booking_commands;

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
