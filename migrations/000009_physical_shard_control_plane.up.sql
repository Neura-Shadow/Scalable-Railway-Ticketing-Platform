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

-- Operator mutations reserve one durable control identity before touching an
-- independently committed physical shard. Only bounded projection fields are
-- retained; raw idempotency keys and operator/customer profile data are not.
CREATE TABLE public.operator_booking_commands (
    command_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    operation text NOT NULL CHECK (
        operation IN (
            'fare.install', 'seat.disable', 'seat.enable',
            'booking_policy.bump'
        )
    ),
    idempotency_key_hash bytea NOT NULL
        CHECK (octet_length(idempotency_key_hash) = 32),
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    resource_id uuid NOT NULL,
    target_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT
        CHECK (target_shard_id IN ('physical-shard-0', 'physical-shard-1')),
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    expected_source_version bigint NOT NULL CHECK (expected_source_version > 0),
    expected_booking_policy_version bigint,
    finalize_from_stop_index integer,
    finalize_to_stop_index integer,
    finalize_seat_class text,
    finalize_amount_minor bigint,
    finalize_currency text,
    finalize_seat_active boolean,
    result_source_version bigint,
    result_booking_policy_version bigint,
    state text NOT NULL DEFAULT 'reserved' CHECK (
        state IN (
            'reserved', 'committed_on_shard', 'needs_repair',
            'finalized', 'failed'
        )
    ),
    lease_owner text,
    lease_until timestamptz,
    attempt_count integer NOT NULL DEFAULT 0
        CHECK (attempt_count BETWEEN 0 AND 1000000),
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category IN (
            'shard_rejected', 'receipt_mismatch', 'control_conflict',
            'route_unavailable', 'finalization_failed'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (actor_id, operation, idempotency_key_hash),
    CONSTRAINT operator_booking_commands_lease_check CHECK (
        (lease_owner IS NULL AND lease_until IS NULL)
        OR (
            lease_owner IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_owner ~ '^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$'
        )
    ),
    CONSTRAINT operator_booking_commands_policy_check CHECK (
        (
            operation = 'booking_policy.bump'
            AND resource_id = train_run_id
            AND expected_booking_policy_version IS NOT NULL
            AND expected_booking_policy_version > 0
        )
        OR (
            operation <> 'booking_policy.bump'
            AND expected_booking_policy_version IS NULL
        )
    ),
    CONSTRAINT operator_booking_commands_payload_check CHECK (
        (
            operation = 'fare.install'
            AND finalize_from_stop_index IS NOT NULL
            AND finalize_to_stop_index IS NOT NULL
            AND finalize_seat_class IS NOT NULL
            AND finalize_amount_minor IS NOT NULL
            AND finalize_currency IS NOT NULL
            AND finalize_from_stop_index >= 0
            AND finalize_to_stop_index > finalize_from_stop_index
            AND finalize_seat_class IN ('standard', 'business', 'first')
            AND finalize_amount_minor >= 0
            AND finalize_currency ~ '^[A-Z]{3}$'
            AND finalize_seat_active IS NULL
        )
        OR (
            operation IN ('seat.disable', 'seat.enable')
            AND finalize_from_stop_index IS NULL
            AND finalize_to_stop_index IS NULL
            AND finalize_seat_class IS NULL
            AND finalize_amount_minor IS NULL
            AND finalize_currency IS NULL
            AND finalize_seat_active IS NOT NULL
            AND finalize_seat_active = (operation = 'seat.enable')
        )
        OR (
            operation = 'booking_policy.bump'
            AND finalize_from_stop_index IS NULL
            AND finalize_to_stop_index IS NULL
            AND finalize_seat_class IS NULL
            AND finalize_amount_minor IS NULL
            AND finalize_currency IS NULL
            AND finalize_seat_active IS NULL
        )
    ),
    CONSTRAINT operator_booking_commands_completion_check CHECK (
        (
            state = 'finalized'
            AND result_source_version IS NOT NULL
            AND result_source_version = expected_source_version + 1
            AND lease_owner IS NULL
            AND lease_until IS NULL
            AND completed_at IS NOT NULL
            AND bounded_error_category IS NULL
            AND (
                (
                    operation = 'booking_policy.bump'
                    AND result_booking_policy_version IS NOT NULL
                    AND result_booking_policy_version =
                        expected_booking_policy_version + 1
                )
                OR (
                    operation <> 'booking_policy.bump'
                    AND result_booking_policy_version IS NULL
                )
            )
        )
        OR (
            state = 'failed'
            AND result_source_version IS NULL
            AND result_booking_policy_version IS NULL
            AND lease_owner IS NULL
            AND lease_until IS NULL
            AND completed_at IS NOT NULL
            AND bounded_error_category IS NOT NULL
        )
        OR (
            state IN ('reserved', 'committed_on_shard', 'needs_repair')
            AND result_source_version IS NULL
            AND result_booking_policy_version IS NULL
            AND completed_at IS NULL
            AND bounded_error_category IS NULL
        )
    )
);

CREATE INDEX operator_booking_commands_recovery_idx
    ON public.operator_booking_commands (
        state, lease_until, updated_at, command_id
    )
    WHERE state IN ('reserved', 'committed_on_shard', 'needs_repair');
CREATE INDEX operator_booking_commands_train_run_idx
    ON public.operator_booking_commands (
        train_run_id, state, updated_at, command_id
    );

CREATE TRIGGER operator_booking_commands_set_updated_at
BEFORE UPDATE ON public.operator_booking_commands
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.fares
    ADD COLUMN source_version bigint,
    ADD COLUMN last_booking_command_id uuid
        REFERENCES public.operator_booking_commands(command_id)
        ON DELETE RESTRICT;

UPDATE public.fares
SET source_version = GREATEST(
    1::bigint,
    (extract(epoch FROM updated_at) * 1000000)::bigint
);

ALTER TABLE public.fares
    ALTER COLUMN source_version SET NOT NULL,
    ALTER COLUMN source_version SET DEFAULT 1,
    ADD CONSTRAINT fares_source_version_check CHECK (source_version > 0);

CREATE UNIQUE INDEX fares_last_booking_command_unique_idx
    ON public.fares (last_booking_command_id)
    WHERE last_booking_command_id IS NOT NULL;

CREATE TABLE public.train_run_seat_booking_overrides (
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    seat_id uuid NOT NULL
        REFERENCES public.seats(id) ON DELETE RESTRICT,
    active boolean NOT NULL,
    source_version bigint NOT NULL CHECK (source_version > 0),
    command_id uuid NOT NULL UNIQUE
        REFERENCES public.operator_booking_commands(command_id)
        ON DELETE RESTRICT,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (train_run_id, seat_id)
);

CREATE TABLE public.train_run_booking_policy_versions (
    train_run_id uuid PRIMARY KEY
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    booking_policy_version bigint NOT NULL
        CHECK (booking_policy_version > 0),
    source_version bigint NOT NULL CHECK (source_version > 0),
    command_id uuid NOT NULL UNIQUE
        REFERENCES public.operator_booking_commands(command_id)
        ON DELETE RESTRICT,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

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

-- Lifecycle projection is monotonic across independently finalized confirm and
-- cancel commands. A cancellation tombstone may precede the delayed confirm
-- locator and can never be downgraded by replaying an older receipt.
CREATE TABLE public.reservation_lifecycle_states (
    reservation_id uuid PRIMARY KEY
        REFERENCES public.reservation_directory(reservation_id) ON DELETE RESTRICT,
    owner_user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    status text NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    lifecycle_rank smallint NOT NULL CHECK (lifecycle_rank IN (1, 2)),
    last_command_id uuid NOT NULL
        REFERENCES public.booking_commands(command_id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT reservation_lifecycle_states_rank_check CHECK (
        (status = 'confirmed' AND lifecycle_rank = 1)
        OR (status = 'cancelled' AND lifecycle_rank = 2)
    )
);

CREATE INDEX reservation_lifecycle_states_owner_idx
    ON public.reservation_lifecycle_states (owner_user_id, reservation_id);

CREATE TRIGGER reservation_lifecycle_states_set_updated_at
BEFORE UPDATE ON public.reservation_lifecycle_states
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
    rollback_assignment_generation bigint,
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
    CONSTRAINT physical_shard_migrations_rollback_generation_check CHECK (
        rollback_assignment_generation IS NULL
        OR rollback_assignment_generation > target_generation
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

CREATE FUNCTION public.reject_physical_migration_with_operator_command()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF TG_OP = 'INSERT' OR NEW.state IS DISTINCT FROM OLD.state THEN
        -- Operator reserve and physical migration control both serialize on
        -- this stable assignment row. A READ COMMITTED transition observes
        -- any command that acquired the assignment lock first.
        PERFORM 1
        FROM public.train_run_shard_assignments AS assignment
        WHERE assignment.train_run_id = NEW.train_run_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION
                'physical migration requires a stable train-run assignment'
                USING ERRCODE = '55000';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM public.operator_booking_commands AS command_row
            WHERE command_row.train_run_id = NEW.train_run_id
              AND command_row.state IN (
                  'reserved', 'committed_on_shard', 'needs_repair'
              )
        ) THEN
            RAISE EXCEPTION
                'physical migration is blocked by a nonterminal operator command'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER physical_shard_migrations_operator_command_guard
BEFORE INSERT OR UPDATE OF state ON public.physical_shard_migrations
FOR EACH ROW
EXECUTE FUNCTION public.reject_physical_migration_with_operator_command();

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

-- Version 8 could inspect every writer because all three booking schemas lived
-- in this database. A physical fence is deliberately local to another
-- PostgreSQL instance, so the control constraint validates the durable
-- migration ledger and the remaining control-local fences instead. The
-- migration engine still owns the cross-database ordering: disable source,
-- enable target, then switch this assignment.
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
    active_physical_migration_id uuid;
    catalog_enabled boolean;
    catalog_write_enabled boolean;
    enabled_fence_count integer;
    matching_fence_count integer;
    migration_source_shard_id text;
    migration_target_shard_id text;
    migration_source_generation bigint;
    migration_target_generation bigint;
    migration_rollback_generation bigint;
    migration_state text;
    migration_target_fence_count integer;
    migration_target_catalog_enabled boolean;
    migration_target_catalog_write_enabled boolean;
BEGIN
    SELECT assignment.shard_id,
           assignment.assignment_generation,
           assignment.assignment_state,
           assignment.active_physical_migration_id,
           shard.enabled,
           shard.write_enabled
    INTO assigned_shard_id,
         assigned_generation,
         assigned_state,
         active_physical_migration_id,
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
        RAISE EXCEPTION 'multiple control-local booking writers are enabled for one train run'
            USING ERRCODE = '23514';
    END IF;

    IF active_physical_migration_id IS NOT NULL THEN
        SELECT migration.source_shard_id,
               migration.target_shard_id,
               migration.source_generation,
               migration.target_generation,
               migration.rollback_assignment_generation,
               migration.state
        INTO STRICT migration_source_shard_id,
                    migration_target_shard_id,
                    migration_source_generation,
                    migration_target_generation,
                    migration_rollback_generation,
                    migration_state
        FROM public.physical_shard_migrations AS migration
        WHERE migration.migration_id = active_physical_migration_id
          AND migration.train_run_id = checked_train_run_id;

        SELECT count(*) FILTER (
                   WHERE fence.write_enabled
                     AND fence.shard_id = migration_target_shard_id
                     AND fence.assignment_generation = migration_target_generation
               )
        INTO migration_target_fence_count
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

        SELECT shard.enabled, shard.write_enabled
        INTO STRICT migration_target_catalog_enabled,
                    migration_target_catalog_write_enabled
        FROM public.booking_shards AS shard
        WHERE shard.shard_id = migration_target_shard_id;

        IF assigned_shard_id = migration_source_shard_id
           AND assigned_generation = migration_source_generation
           AND assigned_state IN ('migrating', 'draining') THEN
            IF migration_state IN (
                'planned', 'preparing_target', 'capture_enabled',
                'base_copying', 'catching_up', 'validating_online'
            ) THEN
                IF assigned_state <> 'migrating' THEN
                    RAISE EXCEPTION 'online migration source entered draining assignment state early'
                        USING ERRCODE = '23514';
                END IF;
                IF migration_source_shard_id IN ('legacy', 'shard-0', 'shard-1') THEN
                    IF enabled_fence_count <> 1 OR matching_fence_count <> 1 THEN
                        RAISE EXCEPTION 'online physical migration lacks one matching control-local source fence'
                            USING ERRCODE = '23514';
                    END IF;
                ELSIF enabled_fence_count <> 0 THEN
                    RAISE EXCEPTION 'physical source migration has an unexpected control-local writer'
                        USING ERRCODE = '23514';
                END IF;
            ELSIF migration_state = 'draining' THEN
                IF assigned_state <> 'draining' THEN
                    RAISE EXCEPTION 'draining migration lacks a draining source assignment'
                        USING ERRCODE = '23514';
                END IF;
                IF migration_source_shard_id IN ('legacy', 'shard-0', 'shard-1') THEN
                    IF enabled_fence_count > 1
                       OR (enabled_fence_count = 1 AND matching_fence_count <> 1) THEN
                        RAISE EXCEPTION 'draining migration has a stale or duplicate control-local source fence'
                            USING ERRCODE = '23514';
                    END IF;
                ELSIF enabled_fence_count <> 0 THEN
                    RAISE EXCEPTION 'draining physical source has an unexpected control-local writer'
                        USING ERRCODE = '23514';
                END IF;
            ELSIF migration_state IN ('source_fenced', 'final_catchup') THEN
                IF enabled_fence_count <> 0 THEN
                    RAISE EXCEPTION 'fenced physical migration retains a control-local writer'
                        USING ERRCODE = '23514';
                END IF;
            ELSIF migration_state = 'final_validating' THEN
                IF migration_target_shard_id IN ('legacy', 'shard-0', 'shard-1') THEN
                    IF enabled_fence_count > 1
                       OR (enabled_fence_count = 1 AND migration_target_fence_count <> 1) THEN
                        RAISE EXCEPTION 'final validation has a stale or duplicate control-local target fence'
                            USING ERRCODE = '23514';
                    END IF;
                ELSIF enabled_fence_count <> 0 THEN
                    RAISE EXCEPTION 'final validation for a physical target has an unexpected control-local writer'
                        USING ERRCODE = '23514';
                END IF;
            ELSIF migration_state = 'target_enabled' THEN
                IF migration_target_shard_id IN ('legacy', 'shard-0', 'shard-1') THEN
                    IF enabled_fence_count <> 1 OR migration_target_fence_count <> 1 THEN
                        RAISE EXCEPTION 'enabled control-local migration target lacks one writer'
                            USING ERRCODE = '23514';
                    END IF;
                ELSIF enabled_fence_count <> 0 THEN
                    RAISE EXCEPTION 'enabled physical target has an unexpected control-local writer'
                        USING ERRCODE = '23514';
                END IF;
            ELSE
                RAISE EXCEPTION 'source assignment does not match the active physical migration state'
                    USING ERRCODE = '23514';
            END IF;

            IF migration_state = 'target_enabled'
               AND (
                   NOT migration_target_catalog_enabled
                   OR NOT migration_target_catalog_write_enabled
               ) THEN
                RAISE EXCEPTION 'enabled migration target uses a disabled catalog entry'
                    USING ERRCODE = '23514';
            END IF;
        ELSIF assigned_shard_id = migration_target_shard_id
              AND assigned_generation = migration_target_generation
              AND assigned_state = 'rollback_window'
              AND migration_state IN ('switching_assignment', 'rollback_window') THEN
            IF migration_target_shard_id IN ('legacy', 'shard-0', 'shard-1') THEN
                IF enabled_fence_count <> 1 OR matching_fence_count <> 1 THEN
                    RAISE EXCEPTION 'control-local migration target lacks one matching writer'
                        USING ERRCODE = '23514';
                END IF;
            ELSIF enabled_fence_count <> 0 THEN
                RAISE EXCEPTION 'physical migration target has an unexpected control-local writer'
                    USING ERRCODE = '23514';
            END IF;
            IF NOT migration_target_catalog_enabled
               OR NOT migration_target_catalog_write_enabled THEN
                RAISE EXCEPTION 'migration target assignment uses a disabled catalog entry'
                    USING ERRCODE = '23514';
            END IF;
        ELSE
            RAISE EXCEPTION 'assignment does not match the active physical migration ledger'
                USING ERRCODE = '23514';
        END IF;

        IF NOT catalog_enabled OR NOT catalog_write_enabled THEN
            RAISE EXCEPTION 'physical migration assignment uses a disabled catalog entry'
                USING ERRCODE = '23514';
        END IF;
        RETURN;
    END IF;

    IF assigned_shard_id IN ('physical-shard-0', 'physical-shard-1') THEN
        IF assigned_state <> 'stable'
           OR enabled_fence_count <> 0
           OR NOT catalog_enabled
           OR NOT catalog_write_enabled
           OR NOT EXISTS (
                SELECT 1
                FROM public.physical_shard_migrations AS migration
                WHERE migration.train_run_id = checked_train_run_id
                  AND (
                    (migration.state = 'completed'
                     AND migration.target_shard_id = assigned_shard_id
                     AND migration.target_generation = assigned_generation)
                    OR
                    (migration.state = 'rolled_back'
                     AND migration.source_shard_id = assigned_shard_id
                     AND migration.rollback_assignment_generation = assigned_generation)
                  )
           ) THEN
            RAISE EXCEPTION 'stable physical assignment lacks exact durable migration evidence'
                USING ERRCODE = '23514';
        END IF;
        RETURN;
    END IF;

    IF assigned_state = 'migrating' THEN
        RAISE EXCEPTION 'logical migration lacks an active physical migration ledger'
            USING ERRCODE = '23514';
    END IF;

    IF enabled_fence_count <> 1
       OR matching_fence_count <> 1
       OR NOT catalog_enabled
       OR NOT catalog_write_enabled THEN
        RAISE EXCEPTION 'stable assignment lacks exactly one matching enabled fence'
            USING ERRCODE = '23514';
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'active physical migration ledger is missing'
            USING ERRCODE = '23514';
END;
$$;

-- Version 8 fences every legacy write while assignment_state is migrating.
-- A physical migration instead relies on the durable v9 ledger and the
-- still-enabled source-local fence until the explicit source_fenced phase.
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
    active_physical_migration_id uuid;
    shard_enabled boolean;
    shard_write_enabled boolean;
    fence_generation bigint;
    fence_write_enabled boolean;
    online_physical_migration boolean := false;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.physical_control_target_apply_authorizations AS apply_auth
        JOIN public.physical_shard_migrations AS migration
          ON migration.migration_id = apply_auth.migration_id
        JOIN public.train_run_shard_assignments AS assignment
          ON assignment.train_run_id = migration.train_run_id
        WHERE apply_auth.transaction_id = txid_current()
          AND apply_auth.train_run_id = checked_train_run_id
          AND apply_auth.target_shard_id = 'legacy'
          AND apply_auth.target_generation = migration.target_generation
          AND migration.train_run_id = checked_train_run_id
          AND migration.reverse_migration
          AND migration.source_shard_id IN (
              'physical-shard-0', 'physical-shard-1'
          )
          AND migration.target_shard_id = 'legacy'
          AND migration.state IN (
              'preparing_target', 'capture_enabled', 'base_copying',
              'catching_up', 'validating_online', 'draining',
              'source_fenced', 'final_catchup', 'final_validating'
          )
          AND assignment.shard_id = migration.source_shard_id
          AND assignment.assignment_generation = migration.source_generation
          AND assignment.active_physical_migration_id = migration.migration_id
          AND assignment.assignment_state IN ('migrating', 'draining')
    ) THEN
        RETURN;
    END IF;

    SELECT assignment.assignment_generation,
           assignment.assignment_state,
           assignment.active_physical_migration_id,
           shard.enabled,
           shard.write_enabled
    INTO assigned_generation,
         assigned_state,
         active_physical_migration_id,
         shard_enabled,
         shard_write_enabled
    FROM public.train_run_shard_assignments AS assignment
    JOIN public.booking_shards AS shard
      ON shard.shard_id = assignment.shard_id
    WHERE assignment.train_run_id = checked_train_run_id
      AND assignment.shard_id = 'legacy'
    FOR UPDATE OF assignment;

    IF assigned_state = 'migrating'
       AND active_physical_migration_id IS NOT NULL THEN
        SELECT EXISTS (
            SELECT 1
            FROM public.physical_shard_migrations AS migration
            WHERE migration.migration_id = active_physical_migration_id
              AND migration.train_run_id = checked_train_run_id
              AND migration.source_shard_id = 'legacy'
              AND migration.source_generation = assigned_generation
              AND migration.state IN (
                  'planned', 'preparing_target', 'capture_enabled',
                  'base_copying', 'catching_up', 'validating_online',
                  'draining'
              )
        ) INTO online_physical_migration;
    END IF;

    IF assigned_generation IS NULL
       OR (assigned_state = 'migrating' AND NOT online_physical_migration)
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

-- A selected legacy/logical-schema train run remains writable while its
-- physical-shard base image is copied.  This control-database-local capture
-- ledger closes that copy gap without writing to the target database.  Only
-- opaque identifiers and a fixed, bounded primary-key object are retained;
-- passenger/customer data and row payloads never enter this journal.
CREATE TABLE public.physical_source_migration_capture_state (
    train_run_id uuid PRIMARY KEY
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    migration_id uuid NOT NULL UNIQUE
        REFERENCES public.physical_shard_migrations(migration_id)
        ON DELETE RESTRICT,
    source_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    source_generation bigint NOT NULL CHECK (source_generation > 0),
    capture_enabled boolean NOT NULL DEFAULT false,
    next_sequence bigint NOT NULL DEFAULT 0 CHECK (next_sequence >= 0),
    enabled_at timestamptz,
    disabled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (source_shard_id IN ('legacy', 'shard-0', 'shard-1')),
    CHECK (
        (capture_enabled AND enabled_at IS NOT NULL AND disabled_at IS NULL)
        OR (NOT capture_enabled)
    )
);

CREATE TRIGGER physical_source_migration_capture_state_set_updated_at
BEFORE UPDATE ON public.physical_source_migration_capture_state
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE public.physical_source_train_run_mutation_journal (
    journal_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    migration_id uuid NOT NULL
        REFERENCES public.physical_shard_migrations(migration_id)
        ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    source_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    source_generation bigint NOT NULL CHECK (source_generation > 0),
    mutation_sequence bigint NOT NULL CHECK (mutation_sequence > 0),
    table_name text NOT NULL CHECK (table_name IN (
        'train_run_booking_snapshots', 'booking_seat_catalog',
        'booking_fare_snapshots', 'seat_inventory', 'reservations',
        'reservation_seats', 'ticket_orders', 'tickets',
        'idempotency_records', 'outbox_events'
    )),
    operation text NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    entity_id uuid NOT NULL,
    primary_key jsonb NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (migration_id, mutation_sequence),
    UNIQUE (migration_id, journal_id),
    CHECK (source_shard_id IN ('legacy', 'shard-0', 'shard-1')),
    CHECK (jsonb_typeof(primary_key) = 'object'),
    CHECK (jsonb_typeof(metadata) = 'object'),
    CHECK (octet_length(primary_key::text) <= 512),
    CHECK (octet_length(metadata::text) <= 256),
    CHECK (NOT (primary_key ?| ARRAY[
        'passenger_name', 'email', 'identity_document', 'dsn', 'password',
        'token', 'raw_idempotency_key'
    ])),
    CHECK (NOT (metadata ?| ARRAY[
        'passenger_name', 'email', 'identity_document', 'dsn', 'password',
        'token', 'raw_idempotency_key'
    ]))
);

CREATE INDEX physical_source_mutation_journal_replay_idx
    ON public.physical_source_train_run_mutation_journal (
        migration_id, train_run_id, source_generation,
        mutation_sequence, journal_id
    );

-- Reverse replay lands in the version-8 control schemas, so its idempotency
-- receipt must also be control-database-local. It stores only bounded
-- migration metadata and a digest; source row payloads and customer data are
-- deliberately excluded.
CREATE TABLE public.physical_control_target_apply_receipts (
    receipt_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    migration_id uuid NOT NULL
        REFERENCES public.physical_shard_migrations(migration_id)
        ON DELETE RESTRICT,
    source_journal_id uuid NOT NULL,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    target_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    target_generation bigint NOT NULL CHECK (target_generation > 0),
    mutation_sequence bigint NOT NULL CHECK (mutation_sequence > 0),
    apply_fingerprint bytea NOT NULL
        CHECK (octet_length(apply_fingerprint) = 32),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (migration_id, source_journal_id),
    UNIQUE (migration_id, mutation_sequence),
    CHECK (target_shard_id IN ('legacy', 'shard-0', 'shard-1'))
);

CREATE INDEX physical_control_target_apply_receipts_run_idx
    ON public.physical_control_target_apply_receipts (
        train_run_id, target_shard_id, target_generation, mutation_sequence
    );

-- A reverse apply must write a retained control-local layout while the live
-- assignment still points at its physical source. Authorization is scoped to
-- one PostgreSQL transaction and removed before commit; a crash rolls it back,
-- so ordinary legacy writers never inherit a pooled-session capability.
CREATE TABLE public.physical_control_target_apply_authorizations (
    migration_id uuid NOT NULL
        REFERENCES public.physical_shard_migrations(migration_id)
        ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    target_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    target_generation bigint NOT NULL CHECK (target_generation > 0),
    transaction_id bigint NOT NULL CHECK (transaction_id > 0),
    authorized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (migration_id, transaction_id),
    CHECK (target_shard_id IN ('legacy', 'shard-0', 'shard-1'))
);

-- Fixed deterministic target identifiers let independently copied train runs
-- reuse global seat/fare identifiers without colliding in a physical database.
-- The identifier is placement data, never an authorization token.
CREATE FUNCTION public.physical_source_entity_id(
    namespace_id uuid,
    entity_kind text,
    source_id uuid
)
RETURNS uuid
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT (
        substring(md5(namespace_id::text || ':' || entity_kind || ':' || source_id::text), 1, 8)
        || '-' || substring(md5(namespace_id::text || ':' || entity_kind || ':' || source_id::text), 9, 4)
        || '-5' || substring(md5(namespace_id::text || ':' || entity_kind || ':' || source_id::text), 14, 3)
        || '-a' || substring(md5(namespace_id::text || ':' || entity_kind || ':' || source_id::text), 18, 3)
        || '-' || substring(md5(namespace_id::text || ':' || entity_kind || ':' || source_id::text), 21, 12)
    )::uuid
$$;

-- Read-only fixed-shape views expose the three approved source schemas through
-- one adapter boundary.  They neither duplicate authority nor accept a schema
-- name at runtime.
CREATE VIEW public.physical_source_seat_inventory_rows AS
SELECT 'legacy'::text AS source_shard_id, inventory.*
FROM public.seat_inventory AS inventory
UNION ALL
SELECT 'shard-0'::text AS source_shard_id, inventory.*
FROM booking_shard_0.seat_inventory AS inventory
UNION ALL
SELECT 'shard-1'::text AS source_shard_id, inventory.*
FROM booking_shard_1.seat_inventory AS inventory;

CREATE VIEW public.physical_source_reservation_rows AS
SELECT 'legacy'::text AS source_shard_id, reservation.*
FROM public.reservations AS reservation
UNION ALL
SELECT 'shard-0'::text AS source_shard_id, reservation.*
FROM booking_shard_0.reservations AS reservation
UNION ALL
SELECT 'shard-1'::text AS source_shard_id, reservation.*
FROM booking_shard_1.reservations AS reservation;

CREATE VIEW public.physical_source_reservation_seat_rows AS
SELECT 'legacy'::text AS source_shard_id, seat.*
FROM public.reservation_seats AS seat
UNION ALL
SELECT 'shard-0'::text AS source_shard_id, seat.*
FROM booking_shard_0.reservation_seats AS seat
UNION ALL
SELECT 'shard-1'::text AS source_shard_id, seat.*
FROM booking_shard_1.reservation_seats AS seat;

CREATE VIEW public.physical_source_ticket_order_rows AS
SELECT 'legacy'::text AS source_shard_id, orders.*,
       reservation.train_run_id
FROM public.ticket_orders AS orders
JOIN public.reservations AS reservation ON reservation.id = orders.reservation_id
UNION ALL
SELECT 'shard-0'::text AS source_shard_id, orders.*,
       reservation.train_run_id
FROM booking_shard_0.ticket_orders AS orders
JOIN booking_shard_0.reservations AS reservation ON reservation.id = orders.reservation_id
UNION ALL
SELECT 'shard-1'::text AS source_shard_id, orders.*,
       reservation.train_run_id
FROM booking_shard_1.ticket_orders AS orders
JOIN booking_shard_1.reservations AS reservation ON reservation.id = orders.reservation_id;

CREATE VIEW public.physical_source_ticket_rows AS
SELECT 'legacy'::text AS source_shard_id, ticket.*,
       reservation.train_run_id
FROM public.tickets AS ticket
JOIN public.ticket_orders AS orders ON orders.id = ticket.ticket_order_id
JOIN public.reservations AS reservation ON reservation.id = orders.reservation_id
UNION ALL
SELECT 'shard-0'::text AS source_shard_id, ticket.*,
       reservation.train_run_id
FROM booking_shard_0.tickets AS ticket
JOIN booking_shard_0.ticket_orders AS orders ON orders.id = ticket.ticket_order_id
JOIN booking_shard_0.reservations AS reservation ON reservation.id = orders.reservation_id
UNION ALL
SELECT 'shard-1'::text AS source_shard_id, ticket.*,
       reservation.train_run_id
FROM booking_shard_1.tickets AS ticket
JOIN booking_shard_1.ticket_orders AS orders ON orders.id = ticket.ticket_order_id
JOIN booking_shard_1.reservations AS reservation ON reservation.id = orders.reservation_id;

CREATE VIEW public.physical_source_idempotency_rows AS
SELECT 'legacy'::text AS source_shard_id, record.*
FROM public.idempotency_records AS record
WHERE record.train_run_id IS NOT NULL
UNION ALL
SELECT 'shard-0'::text AS source_shard_id, record.*
FROM booking_shard_0.idempotency_records AS record
UNION ALL
SELECT 'shard-1'::text AS source_shard_id, record.*
FROM booking_shard_1.idempotency_records AS record;

CREATE VIEW public.physical_source_outbox_rows AS
SELECT event.shard_id AS source_shard_id, event.*
FROM public.outbox_events AS event
WHERE event.shard_id IN ('legacy', 'shard-0', 'shard-1')
  AND event.train_run_id IS NOT NULL
  AND event.assignment_generation IS NOT NULL;

CREATE FUNCTION public.append_physical_source_mutation(
    selected_train_run_id uuid,
    selected_source_shard_id text,
    target_table_name text,
    mutation_operation text,
    target_entity_id uuid,
    bounded_primary_key jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    capture_migration_id uuid;
    capture_generation bigint;
    allocated_sequence bigint;
BEGIN
    IF selected_train_run_id IS NULL
       OR selected_source_shard_id NOT IN ('legacy', 'shard-0', 'shard-1')
       OR target_table_name NOT IN (
           'train_run_booking_snapshots', 'booking_seat_catalog',
           'booking_fare_snapshots', 'seat_inventory', 'reservations',
           'reservation_seats', 'ticket_orders', 'tickets',
           'idempotency_records', 'outbox_events'
       )
       OR mutation_operation NOT IN ('INSERT', 'UPDATE', 'DELETE')
       OR target_entity_id IS NULL
       OR jsonb_typeof(bounded_primary_key) <> 'object'
       OR octet_length(bounded_primary_key::text) > 512 THEN
        RAISE EXCEPTION 'invalid physical source mutation capture input'
            USING ERRCODE = '22023';
    END IF;

    UPDATE public.physical_source_migration_capture_state AS capture
    SET next_sequence = capture.next_sequence + 1
    FROM public.train_run_shard_assignments AS assignment
    WHERE capture.train_run_id = selected_train_run_id
      AND capture.source_shard_id = selected_source_shard_id
      AND capture.capture_enabled
      AND assignment.train_run_id = capture.train_run_id
      AND assignment.shard_id = capture.source_shard_id
      AND assignment.assignment_generation = capture.source_generation
      AND assignment.assignment_state IN ('stable', 'draining', 'migrating')
    RETURNING capture.migration_id, capture.source_generation,
              capture.next_sequence
    INTO capture_migration_id, capture_generation, allocated_sequence;

    IF capture_migration_id IS NULL THEN
        RETURN;
    END IF;

    INSERT INTO public.physical_source_train_run_mutation_journal (
        migration_id, train_run_id, source_shard_id, source_generation,
        mutation_sequence, table_name, operation, entity_id,
        primary_key, metadata
    ) VALUES (
        capture_migration_id, selected_train_run_id,
        selected_source_shard_id, capture_generation, allocated_sequence,
        target_table_name, mutation_operation, target_entity_id,
        bounded_primary_key,
        jsonb_build_object('source_shard_id', selected_source_shard_id)
    );
END;
$$;

-- One trigger body covers only the three compile-time source schemas and the
-- seven fixed booking relations.  It contains no EXECUTE and accepts no SQL
-- identifier, relation, DSN, or endpoint from an operator request.
CREATE FUNCTION public.capture_physical_source_booking_mutation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, booking_shard_0, booking_shard_1
AS $$
DECLARE
    source_shard_id text;
    affected_train_run_id uuid;
    source_entity_id uuid;
    target_entity_id uuid;
    target_table_name text;
    bounded_key jsonb;
BEGIN
    source_shard_id := CASE TG_TABLE_SCHEMA
        WHEN 'public' THEN 'legacy'
        WHEN 'booking_shard_0' THEN 'shard-0'
        WHEN 'booking_shard_1' THEN 'shard-1'
        ELSE NULL
    END;
    IF source_shard_id IS NULL THEN
        RAISE EXCEPTION 'unapproved physical migration source schema'
            USING ERRCODE = '22023';
    END IF;

    IF TG_TABLE_NAME = 'seat_inventory' THEN
        affected_train_run_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.train_run_id ELSE NEW.train_run_id END;
        source_entity_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.seat_id ELSE NEW.seat_id END;
        target_entity_id := public.physical_source_entity_id(affected_train_run_id, 'inventory', source_entity_id);
        target_table_name := 'seat_inventory';
    ELSIF TG_TABLE_NAME = 'reservations' THEN
        affected_train_run_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.train_run_id ELSE NEW.train_run_id END;
        source_entity_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END;
        PERFORM public.append_physical_source_mutation(
            affected_train_run_id, source_shard_id,
            'booking_fare_snapshots', TG_OP,
            public.physical_source_entity_id(affected_train_run_id, 'reservation-fare', source_entity_id),
            jsonb_build_object('source_id', source_entity_id, 'source_kind', 'reservation')
        );
        target_entity_id := source_entity_id;
        target_table_name := 'reservations';
    ELSIF TG_TABLE_NAME = 'reservation_seats' THEN
        affected_train_run_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.train_run_id ELSE NEW.train_run_id END;
        source_entity_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END;
        target_entity_id := source_entity_id;
        target_table_name := 'reservation_seats';
    ELSIF TG_TABLE_NAME = 'idempotency_records' THEN
        affected_train_run_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.train_run_id ELSE NEW.train_run_id END;
        source_entity_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END;
        IF affected_train_run_id IS NULL THEN
            RETURN COALESCE(NEW, OLD);
        END IF;
        target_entity_id := source_entity_id;
        target_table_name := 'idempotency_records';
    ELSIF TG_TABLE_NAME = 'outbox_events' THEN
        affected_train_run_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.train_run_id ELSE NEW.train_run_id END;
        source_entity_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END;
        source_shard_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.shard_id ELSE NEW.shard_id END;
        IF affected_train_run_id IS NULL THEN
            RETURN COALESCE(NEW, OLD);
        END IF;
        IF source_shard_id NOT IN ('legacy', 'shard-0', 'shard-1') THEN
            RETURN COALESCE(NEW, OLD);
        END IF;
        target_entity_id := source_entity_id;
        target_table_name := 'outbox_events';
    ELSIF TG_TABLE_NAME = 'ticket_orders' THEN
        source_entity_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END;
        IF TG_TABLE_SCHEMA = 'public' THEN
            SELECT reservation.train_run_id INTO affected_train_run_id
            FROM public.reservations AS reservation
            WHERE reservation.id = CASE WHEN TG_OP = 'DELETE' THEN OLD.reservation_id ELSE NEW.reservation_id END;
        ELSIF TG_TABLE_SCHEMA = 'booking_shard_0' THEN
            SELECT reservation.train_run_id INTO affected_train_run_id
            FROM booking_shard_0.reservations AS reservation
            WHERE reservation.id = CASE WHEN TG_OP = 'DELETE' THEN OLD.reservation_id ELSE NEW.reservation_id END;
        ELSE
            SELECT reservation.train_run_id INTO affected_train_run_id
            FROM booking_shard_1.reservations AS reservation
            WHERE reservation.id = CASE WHEN TG_OP = 'DELETE' THEN OLD.reservation_id ELSE NEW.reservation_id END;
        END IF;
        target_entity_id := source_entity_id;
        target_table_name := 'ticket_orders';
    ELSIF TG_TABLE_NAME = 'tickets' THEN
        source_entity_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END;
        IF TG_TABLE_SCHEMA = 'public' THEN
            SELECT reservation.train_run_id INTO affected_train_run_id
            FROM public.ticket_orders AS orders
            JOIN public.reservations AS reservation ON reservation.id = orders.reservation_id
            WHERE orders.id = CASE WHEN TG_OP = 'DELETE' THEN OLD.ticket_order_id ELSE NEW.ticket_order_id END;
        ELSIF TG_TABLE_SCHEMA = 'booking_shard_0' THEN
            SELECT reservation.train_run_id INTO affected_train_run_id
            FROM booking_shard_0.ticket_orders AS orders
            JOIN booking_shard_0.reservations AS reservation ON reservation.id = orders.reservation_id
            WHERE orders.id = CASE WHEN TG_OP = 'DELETE' THEN OLD.ticket_order_id ELSE NEW.ticket_order_id END;
        ELSE
            SELECT reservation.train_run_id INTO affected_train_run_id
            FROM booking_shard_1.ticket_orders AS orders
            JOIN booking_shard_1.reservations AS reservation ON reservation.id = orders.reservation_id
            WHERE orders.id = CASE WHEN TG_OP = 'DELETE' THEN OLD.ticket_order_id ELSE NEW.ticket_order_id END;
        END IF;
        target_entity_id := source_entity_id;
        target_table_name := 'tickets';
    ELSE
        RAISE EXCEPTION 'unapproved physical migration source relation'
            USING ERRCODE = '22023';
    END IF;

    bounded_key := jsonb_build_object('source_id', source_entity_id);
    PERFORM public.append_physical_source_mutation(
        affected_train_run_id, source_shard_id, target_table_name,
        TG_OP, target_entity_id, bounded_key
    );
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE FUNCTION public.capture_physical_source_train_run_reference()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    selected_shard_id text;
BEGIN
    SELECT assignment.shard_id INTO selected_shard_id
    FROM public.train_run_shard_assignments AS assignment
    WHERE assignment.train_run_id = COALESCE(NEW.id, OLD.id)
      AND assignment.shard_id IN ('legacy', 'shard-0', 'shard-1');
    IF selected_shard_id IS NOT NULL THEN
        PERFORM public.append_physical_source_mutation(
            COALESCE(NEW.id, OLD.id), selected_shard_id,
            'train_run_booking_snapshots', TG_OP,
            public.physical_source_entity_id(COALESCE(NEW.id, OLD.id), 'snapshot', COALESCE(NEW.id, OLD.id)),
            jsonb_build_object('source_id', COALESCE(NEW.id, OLD.id))
        );
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE FUNCTION public.capture_physical_source_fare_reference()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    selected_run record;
    changed_fare_id uuid := COALESCE(NEW.id, OLD.id);
    selected_train_run_id uuid := COALESCE(NEW.train_run_id, OLD.train_run_id);
    selected_route_id uuid := COALESCE(NEW.route_id, OLD.route_id);
BEGIN
    FOR selected_run IN
        SELECT capture.train_run_id, capture.source_shard_id
        FROM public.physical_source_migration_capture_state AS capture
        JOIN public.train_runs AS train_run ON train_run.id = capture.train_run_id
        WHERE capture.capture_enabled
          AND (capture.train_run_id = selected_train_run_id
               OR (selected_train_run_id IS NULL AND train_run.route_id = selected_route_id))
        ORDER BY capture.train_run_id
        LIMIT 64
    LOOP
        PERFORM public.append_physical_source_mutation(
            selected_run.train_run_id, selected_run.source_shard_id,
            'booking_fare_snapshots', TG_OP,
            public.physical_source_entity_id(selected_run.train_run_id, 'fare', changed_fare_id),
            jsonb_build_object('source_id', changed_fare_id, 'source_kind', 'fare')
        );
    END LOOP;
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE FUNCTION public.capture_physical_source_seat_reference()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    selected_run record;
    changed_seat_id uuid := COALESCE(NEW.id, OLD.id);
    selected_train_id uuid;
BEGIN
    SELECT coach.train_id INTO selected_train_id
    FROM public.coaches AS coach
    WHERE coach.id = COALESCE(NEW.coach_id, OLD.coach_id);
    FOR selected_run IN
        SELECT capture.train_run_id, capture.source_shard_id
        FROM public.physical_source_migration_capture_state AS capture
        JOIN public.train_runs AS train_run ON train_run.id = capture.train_run_id
        WHERE capture.capture_enabled AND train_run.train_id = selected_train_id
        ORDER BY capture.train_run_id
        LIMIT 64
    LOOP
        PERFORM public.append_physical_source_mutation(
            selected_run.train_run_id, selected_run.source_shard_id,
            'booking_seat_catalog', TG_OP,
            public.physical_source_entity_id(selected_run.train_run_id, 'seat', changed_seat_id),
            jsonb_build_object('source_id', changed_seat_id)
        );
    END LOOP;
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON public.seat_inventory
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON public.reservations
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON public.reservation_seats
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON public.ticket_orders
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON public.tickets
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON public.idempotency_records
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON public.outbox_events
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();

CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_0.seat_inventory
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_0.reservations
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_0.reservation_seats
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_0.ticket_orders
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_0.tickets
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_0.idempotency_records
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();

CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_1.seat_inventory
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_1.reservations
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_1.reservation_seats
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_1.ticket_orders
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_1.tickets
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_1.idempotency_records
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_booking_mutation();

CREATE TRIGGER physical_source_capture_train_run
AFTER UPDATE ON public.train_runs
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_train_run_reference();

CREATE TRIGGER physical_source_capture_fare
AFTER INSERT OR UPDATE OR DELETE ON public.fares
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_fare_reference();

CREATE TRIGGER physical_source_capture_seat
AFTER UPDATE ON public.seats
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_seat_reference();

COMMIT;
