BEGIN;

SELECT pg_advisory_xact_lock(804230009);

-- Version 9 extends the fixed catalog. Connection references are bounded
-- configuration keys only; DSNs, endpoints, credentials, and arbitrary shard
-- discovery never enter the control database.
ALTER TABLE public.booking_shards
    DROP CONSTRAINT booking_shards_storage_kind_check,
    DROP CONSTRAINT booking_shards_fixed_topology_check;

UPDATE public.booking_shards
SET storage_kind = CASE storage_kind
    WHEN 'legacy' THEN 'legacy_schema'
    WHEN 'schema' THEN 'logical_schema'
    ELSE storage_kind
END;

ALTER TABLE public.booking_shards
    ADD COLUMN connection_ref text,
    ADD COLUMN protocol_version integer NOT NULL DEFAULT 1,
    ADD COLUMN schema_version integer NOT NULL DEFAULT 8,
    ADD COLUMN health_state text NOT NULL DEFAULT 'unknown',
    ADD COLUMN last_health_checked_at timestamptz,
    ADD COLUMN write_disabled_reason text;

ALTER TABLE public.booking_shards
    ADD CONSTRAINT booking_shards_storage_kind_check CHECK (
        storage_kind IN ('legacy_schema', 'logical_schema', 'postgres')
    ),
    ADD CONSTRAINT booking_shards_fixed_topology_check CHECK (
        (shard_id = 'legacy'
            AND storage_kind = 'legacy_schema'
            AND connection_ref IS NULL)
        OR
        (shard_id IN ('shard-0', 'shard-1')
            AND storage_kind = 'logical_schema'
            AND connection_ref IS NULL)
        OR
        (shard_id = 'physical-shard-0'
            AND storage_kind = 'postgres'
            AND connection_ref = 'physical-shard-0')
        OR
        (shard_id = 'physical-shard-1'
            AND storage_kind = 'postgres'
            AND connection_ref = 'physical-shard-1')
    ),
    ADD CONSTRAINT booking_shards_connection_ref_check CHECK (
        connection_ref IS NULL
        OR connection_ref ~ '^[a-z][a-z0-9_-]{0,63}$'
    ),
    ADD CONSTRAINT booking_shards_version_check CHECK (
        protocol_version > 0 AND schema_version > 0
    ),
    ADD CONSTRAINT booking_shards_health_state_check CHECK (
        health_state IN ('unknown', 'healthy', 'degraded', 'unavailable')
    ),
    ADD CONSTRAINT booking_shards_write_metadata_check CHECK (
        (
            write_enabled
            AND write_disabled_reason IS NULL
            AND (
                storage_kind <> 'postgres'
                OR (
                    enabled
                    AND health_state = 'healthy'
                    AND last_health_checked_at IS NOT NULL
                )
            )
        )
        OR
        (
            NOT write_enabled
            AND (
                write_disabled_reason IS NULL
                OR write_disabled_reason ~ '^[a-z][a-z0-9_]{0,63}$'
            )
        )
    );

CREATE UNIQUE INDEX booking_shards_connection_ref_unique_idx
    ON public.booking_shards (connection_ref)
    WHERE connection_ref IS NOT NULL;

INSERT INTO public.booking_shards (
    shard_id,
    storage_kind,
    connection_ref,
    protocol_version,
    schema_version,
    enabled,
    write_enabled,
    health_state,
    state,
    minimum_fencing_protocol_version,
    write_disabled_reason
) VALUES
    (
        'physical-shard-0', 'postgres', 'physical-shard-0', 1, 1,
        false, false, 'unknown', 'disabled', 1, 'pilot_not_enabled'
    ),
    (
        'physical-shard-1', 'postgres', 'physical-shard-1', 1, 1,
        false, false, 'unknown', 'disabled', 1, 'pilot_not_enabled'
    );

CREATE TABLE public.booking_commands (
    command_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    operation text NOT NULL
        CHECK (operation IN (
            'reservation.create', 'reservation.confirm', 'reservation.cancel'
        )),
    owner_user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    reservation_id uuid NOT NULL,
    idempotency_key_hash bytea NOT NULL
        CHECK (octet_length(idempotency_key_hash) = 32),
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    target_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    assignment_generation bigint NOT NULL
        CHECK (assignment_generation > 0),
    state text NOT NULL DEFAULT 'reserved'
        CHECK (state IN (
            'reserved', 'executing', 'committed_on_shard', 'finalized',
            'failed', 'expired', 'needs_repair'
        )),
    lease_owner text,
    lease_until timestamptz,
    result_resource_id uuid,
    bounded_error_category text,
    finalized_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (owner_user_id, operation, idempotency_key_hash),
    UNIQUE (command_id, owner_user_id, train_run_id),
    UNIQUE (command_id, owner_user_id, train_run_id, reservation_id),
    CONSTRAINT booking_commands_lease_check CHECK (
        (lease_owner IS NULL AND lease_until IS NULL)
        OR
        (
            lease_owner IS NOT NULL
            AND lease_until IS NOT NULL
            AND
            lease_owner ~ '^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$'
        )
    ),
    CONSTRAINT booking_commands_result_check CHECK (
        result_resource_id IS NULL OR result_resource_id = reservation_id
    ),
    CONSTRAINT booking_commands_finalized_check CHECK (
        state <> 'finalized'
        OR (
            result_resource_id IS NOT NULL
            AND result_resource_id = reservation_id
            AND finalized_at IS NOT NULL
        )
    ),
    CONSTRAINT booking_commands_error_category_check CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    )
);

CREATE INDEX booking_commands_state_lease_idx
    ON public.booking_commands (state, lease_until, updated_at, command_id);
CREATE INDEX booking_commands_train_run_idx
    ON public.booking_commands (train_run_id, created_at, command_id);
CREATE INDEX booking_commands_reservation_idx
    ON public.booking_commands (reservation_id, command_id);

CREATE TRIGGER booking_commands_set_updated_at
BEFORE UPDATE ON public.booking_commands
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE public.booking_quota_leases (
    lease_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    command_id uuid NOT NULL UNIQUE,
    owner_user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    passenger_count integer NOT NULL CHECK (passenger_count > 0),
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN (
            'pending', 'active_hold', 'released', 'expired', 'repair_required'
        )),
    expires_at timestamptz NOT NULL,
    released_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT booking_quota_leases_expiry_check CHECK (
        expires_at > created_at
    ),
    CONSTRAINT booking_quota_leases_release_check CHECK (
        (state IN ('released', 'expired') AND released_at IS NOT NULL)
        OR
        (state NOT IN ('released', 'expired') AND released_at IS NULL)
    ),
    CONSTRAINT booking_quota_leases_command_fkey FOREIGN KEY (
        command_id, owner_user_id, train_run_id
    ) REFERENCES public.booking_commands (
        command_id, owner_user_id, train_run_id
    ) ON DELETE RESTRICT
);

CREATE INDEX booking_quota_leases_counted_user_idx
    ON public.booking_quota_leases (owner_user_id, lease_id)
    WHERE state IN ('pending', 'active_hold', 'repair_required');
CREATE INDEX booking_quota_leases_counted_user_train_run_idx
    ON public.booking_quota_leases (owner_user_id, train_run_id, lease_id)
    WHERE state IN ('pending', 'active_hold', 'repair_required');
CREATE INDEX booking_quota_leases_repair_idx
    ON public.booking_quota_leases (state, expires_at, lease_id)
    WHERE state IN ('pending', 'repair_required');

CREATE TRIGGER booking_quota_leases_set_updated_at
BEFORE UPDATE ON public.booking_quota_leases
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE public.reservation_directory (
    reservation_id uuid PRIMARY KEY,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    owner_user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    command_id uuid UNIQUE,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'active', 'failed', 'moving', 'tombstoned')),
    last_known_shard_id text
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    last_known_generation bigint,
    legacy_imported boolean NOT NULL DEFAULT false,
    bounded_error_category text,
    active_at timestamptz,
    tombstoned_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT reservation_directory_route_hint_check CHECK (
        (last_known_shard_id IS NULL AND last_known_generation IS NULL)
        OR
        (last_known_shard_id IS NOT NULL AND last_known_generation > 0)
    ),
    CONSTRAINT reservation_directory_command_check CHECK (
        (
            legacy_imported
            AND command_id IS NULL
            AND state = 'active'
            AND active_at IS NOT NULL
        )
        OR
        (NOT legacy_imported AND command_id IS NOT NULL)
    ),
    CONSTRAINT reservation_directory_active_check CHECK (
        state NOT IN ('active', 'moving') OR active_at IS NOT NULL
    ),
    CONSTRAINT reservation_directory_tombstone_check CHECK (
        (state = 'tombstoned' AND tombstoned_at IS NOT NULL)
        OR
        (state <> 'tombstoned' AND tombstoned_at IS NULL)
    ),
    CONSTRAINT reservation_directory_error_category_check CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    CONSTRAINT reservation_directory_command_fkey FOREIGN KEY (
        command_id, owner_user_id, train_run_id, reservation_id
    ) REFERENCES public.booking_commands (
        command_id, owner_user_id, train_run_id, reservation_id
    ) ON DELETE RESTRICT
);

CREATE INDEX reservation_directory_owner_created_idx
    ON public.reservation_directory (
        owner_user_id, created_at DESC, reservation_id DESC
    );
CREATE INDEX reservation_directory_train_run_state_idx
    ON public.reservation_directory (train_run_id, state, reservation_id);
CREATE INDEX reservation_directory_repair_idx
    ON public.reservation_directory (state, updated_at, reservation_id)
    WHERE state IN ('pending', 'failed', 'moving');

CREATE TRIGGER reservation_directory_set_updated_at
BEFORE UPDATE ON public.reservation_directory
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- Preserve every populated version-8 locator. Imported rows retain their v8
-- locator as the rollback representation and intentionally have no synthetic
-- command or fingerprint.
INSERT INTO public.reservation_directory (
    reservation_id,
    train_run_id,
    owner_user_id,
    state,
    last_known_shard_id,
    last_known_generation,
    legacy_imported,
    active_at,
    created_at,
    updated_at
)
SELECT locator.reservation_id,
       locator.train_run_id,
       locator.owner_user_id,
       'active',
       locator.shard_id,
       locator.assignment_generation,
       true,
       locator.created_at,
       locator.created_at,
       locator.updated_at
FROM public.reservation_shard_locators AS locator;

CREATE TABLE public.physical_shard_migrations (
    migration_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_migration_id uuid
        REFERENCES public.physical_shard_migrations(migration_id)
        ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    source_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    target_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    source_generation bigint NOT NULL CHECK (source_generation > 0),
    target_generation bigint NOT NULL CHECK (target_generation > 0),
    reverse_migration boolean NOT NULL DEFAULT false,
    state text NOT NULL DEFAULT 'planned'
        CHECK (state IN (
            'planned', 'preparing_target', 'capture_enabled', 'base_copying',
            'catching_up', 'validating_online', 'draining', 'source_fenced',
            'final_catchup', 'final_validating', 'target_enabled',
            'switching_assignment', 'rollback_window', 'completed',
            'reverse_migration_required', 'failed', 'rolled_back'
        )),
    source_journal_start_sequence bigint,
    last_replayed_sequence bigint,
    final_source_sequence bigint,
    rows_copied bigint NOT NULL DEFAULT 0 CHECK (rows_copied >= 0),
    rows_replayed bigint NOT NULL DEFAULT 0 CHECK (rows_replayed >= 0),
    validation_version bigint NOT NULL DEFAULT 0
        CHECK (validation_version >= 0),
    source_fenced_at timestamptz,
    target_enabled_at timestamptz,
    assignment_switched_at timestamptz,
    target_successful_write_count bigint NOT NULL DEFAULT 0
        CHECK (target_successful_write_count >= 0),
    rollback_window_seconds integer NOT NULL DEFAULT 300
        CHECK (rollback_window_seconds BETWEEN 1 AND 86400),
    rollback_deadline_at timestamptz,
    source_retention_until timestamptz,
    cleanup_state text NOT NULL DEFAULT 'not_requested'
        CHECK (cleanup_state IN (
            'not_requested', 'eligible', 'confirmed', 'running',
            'completed', 'failed'
        )),
    cleanup_confirmation_hash bytea,
    cleanup_completed_at timestamptz,
    bounded_error_category text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (migration_id, train_run_id),
    UNIQUE (migration_id, train_run_id, target_shard_id, target_generation),
    CONSTRAINT physical_shard_migrations_distinct_shards_check CHECK (
        source_shard_id <> target_shard_id
    ),
    CONSTRAINT physical_shard_migrations_generation_check CHECK (
        target_generation > source_generation
    ),
    CONSTRAINT physical_shard_migrations_sequence_check CHECK (
        (source_journal_start_sequence IS NULL
            OR source_journal_start_sequence >= 0)
        AND
        (last_replayed_sequence IS NULL OR last_replayed_sequence >= 0)
        AND
        (final_source_sequence IS NULL OR final_source_sequence >= 0)
        AND
        (
            final_source_sequence IS NULL
            OR last_replayed_sequence IS NULL
            OR final_source_sequence >= last_replayed_sequence
        )
    ),
    CONSTRAINT physical_shard_migrations_cutover_order_check CHECK (
        (target_enabled_at IS NULL OR source_fenced_at IS NOT NULL)
        AND
        (
            target_enabled_at IS NULL
            OR target_enabled_at >= source_fenced_at
        )
        AND
        (assignment_switched_at IS NULL OR target_enabled_at IS NOT NULL)
        AND
        (
            assignment_switched_at IS NULL
            OR assignment_switched_at >= target_enabled_at
        )
    ),
    CONSTRAINT physical_shard_migrations_retention_check CHECK (
        rollback_deadline_at IS NULL
        OR (
            assignment_switched_at IS NOT NULL
            AND rollback_deadline_at > assignment_switched_at
            AND source_retention_until IS NOT NULL
            AND source_retention_until >= rollback_deadline_at
        )
    ),
    CONSTRAINT physical_shard_migrations_cleanup_check CHECK (
        (
            cleanup_state IN ('confirmed', 'running', 'completed')
            AND cleanup_confirmation_hash IS NOT NULL
            AND octet_length(cleanup_confirmation_hash) = 32
        )
        OR
        (
            cleanup_state NOT IN ('confirmed', 'running', 'completed')
            AND cleanup_confirmation_hash IS NULL
        )
    ),
    CONSTRAINT physical_shard_migrations_cleanup_completed_check CHECK (
        (cleanup_state = 'completed' AND cleanup_completed_at IS NOT NULL)
        OR
        (cleanup_state <> 'completed' AND cleanup_completed_at IS NULL)
    ),
    CONSTRAINT physical_shard_migrations_error_category_check CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    )
);

CREATE UNIQUE INDEX physical_shard_migrations_one_active_idx
    ON public.physical_shard_migrations (train_run_id)
    WHERE state NOT IN (
        'completed', 'reverse_migration_required', 'failed', 'rolled_back'
    );
CREATE INDEX physical_shard_migrations_state_idx
    ON public.physical_shard_migrations (state, updated_at, migration_id);
CREATE INDEX physical_shard_migrations_source_idx
    ON public.physical_shard_migrations (
        source_shard_id, state, updated_at, migration_id
    );
CREATE INDEX physical_shard_migrations_target_idx
    ON public.physical_shard_migrations (
        target_shard_id, state, updated_at, migration_id
    );

CREATE TRIGGER physical_shard_migrations_set_updated_at
BEFORE UPDATE ON public.physical_shard_migrations
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.train_run_shard_assignments
    ADD COLUMN active_physical_migration_id uuid,
    ADD CONSTRAINT train_run_shard_assignments_active_physical_migration_fkey
        FOREIGN KEY (active_physical_migration_id, train_run_id)
        REFERENCES public.physical_shard_migrations(migration_id, train_run_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT train_run_shard_assignments_one_migration_kind_check CHECK (
        active_migration_id IS NULL OR active_physical_migration_id IS NULL
    );

CREATE INDEX train_run_shard_assignments_active_physical_migration_idx
    ON public.train_run_shard_assignments (active_physical_migration_id)
    WHERE active_physical_migration_id IS NOT NULL;

CREATE TABLE public.physical_shard_migration_checkpoints (
    migration_id uuid NOT NULL
        REFERENCES public.physical_shard_migrations(migration_id)
        ON DELETE RESTRICT,
    checkpoint_kind text NOT NULL
        CHECK (checkpoint_kind IN (
            'target_prepare', 'capture', 'base_copy', 'journal_replay',
            'online_validation', 'final_validation', 'cutover',
            'cleanup', 'reconciliation'
        )),
    object_name text NOT NULL
        CHECK (object_name ~ '^[a-z][a-z0-9_]{0,63}$'),
    cursor_value text NOT NULL DEFAULT ''
        CHECK (octet_length(cursor_value) <= 256),
    source_sequence bigint,
    target_sequence bigint,
    rows_processed bigint NOT NULL DEFAULT 0 CHECK (rows_processed >= 0),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    bounded_error_category text,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (migration_id, checkpoint_kind, object_name),
    CONSTRAINT physical_shard_migration_checkpoints_sequence_check CHECK (
        (source_sequence IS NULL OR source_sequence >= 0)
        AND (target_sequence IS NULL OR target_sequence >= 0)
    ),
    CONSTRAINT physical_shard_migration_checkpoints_completed_check CHECK (
        (status = 'completed' AND completed_at IS NOT NULL)
        OR
        (status <> 'completed' AND completed_at IS NULL)
    ),
    CONSTRAINT physical_shard_migration_checkpoints_error_check CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    )
);

CREATE INDEX physical_shard_migration_checkpoints_work_idx
    ON public.physical_shard_migration_checkpoints (
        status, updated_at, migration_id, checkpoint_kind, object_name
    );

CREATE TRIGGER physical_shard_migration_checkpoints_set_updated_at
BEFORE UPDATE ON public.physical_shard_migration_checkpoints
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- This control-plane table stores bounded observations copied from durable
-- shard-local receipts. It is not the authority that permits a shard write.
CREATE TABLE public.physical_shard_target_write_observations (
    migration_id uuid PRIMARY KEY,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    target_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    target_generation bigint NOT NULL CHECK (target_generation > 0),
    successful_write_count bigint NOT NULL DEFAULT 0
        CHECK (successful_write_count >= 0),
    first_successful_write_at timestamptz,
    last_successful_write_at timestamptz,
    verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT physical_shard_target_write_observations_time_check CHECK (
        (
            successful_write_count = 0
            AND first_successful_write_at IS NULL
            AND last_successful_write_at IS NULL
        )
        OR
        (
            successful_write_count > 0
            AND first_successful_write_at IS NOT NULL
            AND last_successful_write_at IS NOT NULL
            AND last_successful_write_at >= first_successful_write_at
        )
    ),
    CONSTRAINT physical_shard_target_write_observations_migration_fkey
        FOREIGN KEY (
            migration_id, train_run_id, target_shard_id, target_generation
        ) REFERENCES public.physical_shard_migrations (
            migration_id, train_run_id, target_shard_id, target_generation
        ) ON DELETE RESTRICT
);

CREATE INDEX physical_shard_target_write_observations_route_idx
    ON public.physical_shard_target_write_observations (
        train_run_id, target_generation, migration_id
    );

CREATE TRIGGER physical_shard_target_write_observations_set_updated_at
BEFORE UPDATE ON public.physical_shard_target_write_observations
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE public.physical_shard_reconciliation_runs (
    reconciliation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    scope text NOT NULL
        CHECK (scope IN (
            'booking_command', 'quota_lease', 'reservation_directory',
            'assignment_fence', 'migration', 'outbox', 'source_target'
        )),
    train_run_id uuid
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    migration_id uuid
        REFERENCES public.physical_shard_migrations(migration_id)
        ON DELETE RESTRICT,
    status text NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'passed', 'mismatch', 'partial', 'failed')),
    rows_examined bigint NOT NULL DEFAULT 0 CHECK (rows_examined >= 0),
    mismatch_count bigint NOT NULL DEFAULT 0 CHECK (mismatch_count >= 0),
    truncated boolean NOT NULL DEFAULT false,
    bounded_error_category text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CONSTRAINT physical_shard_reconciliation_runs_target_check CHECK (
        train_run_id IS NOT NULL OR migration_id IS NOT NULL
    ),
    CONSTRAINT physical_shard_reconciliation_runs_completed_check CHECK (
        (status = 'running' AND completed_at IS NULL)
        OR
        (status <> 'running' AND completed_at IS NOT NULL)
    ),
    CONSTRAINT physical_shard_reconciliation_runs_error_check CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    )
);

CREATE INDEX physical_shard_reconciliation_runs_train_run_idx
    ON public.physical_shard_reconciliation_runs (
        train_run_id, started_at DESC, reconciliation_id
    );
CREATE INDEX physical_shard_reconciliation_runs_migration_idx
    ON public.physical_shard_reconciliation_runs (
        migration_id, started_at DESC, reconciliation_id
    );
CREATE INDEX physical_shard_reconciliation_runs_result_idx
    ON public.physical_shard_reconciliation_runs (
        status, started_at, reconciliation_id
    )
    WHERE status IN ('mismatch', 'partial', 'failed');

-- Control-plane intent stays in the existing control outbox. Booking-domain
-- intent belongs to each physical shard and is defined by the independent
-- booking-shard migration history.
ALTER TABLE public.outbox_events
    DROP CONSTRAINT outbox_events_event_pair_check,
    DROP CONSTRAINT outbox_events_aggregate_type_check,
    DROP CONSTRAINT outbox_events_event_type_check,
    ADD CONSTRAINT outbox_events_aggregate_type_check CHECK (
        aggregate_type IN (
            'reservation', 'ticket', 'train_run', 'hot_train_policy',
            'station', 'route', 'train', 'coach', 'seat', 'fare',
            'booking_command', 'physical_shard_migration'
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
            'fare.created', 'fare.updated', 'fare.disabled',
            'booking_command.finalized', 'booking_command.repaired',
            'booking_command.failed', 'physical_shard_migration.cutover',
            'physical_shard_migration.rolled_back',
            'physical_shard_migration.reverse_cutover',
            'physical_shard_migration.completed'
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
        OR (aggregate_type = 'booking_command' AND event_type IN (
            'booking_command.finalized', 'booking_command.repaired',
            'booking_command.failed'
        ))
        OR (aggregate_type = 'physical_shard_migration' AND event_type IN (
            'physical_shard_migration.cutover',
            'physical_shard_migration.rolled_back',
            'physical_shard_migration.reverse_cutover',
            'physical_shard_migration.completed'
        ))
    );

COMMIT;
