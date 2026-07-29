DO $assert$
DECLARE
    current_version bigint;
    current_dirty boolean;
BEGIN
    SELECT version, dirty
    INTO current_version, current_dirty
    FROM public.schema_migrations
    LIMIT 1;

    IF current_version <> 9 OR current_dirty THEN
        RAISE EXCEPTION 'control migration must be clean at version 9';
    END IF;

    IF (SELECT count(*) FROM public.booking_shards) <> 5
       OR (SELECT count(*) FROM public.booking_shards
           WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
             AND storage_kind = 'postgres'
             AND connection_ref = shard_id
             AND protocol_version = 1
             AND schema_version = 1
             AND NOT enabled
             AND NOT write_enabled) <> 2 THEN
        RAISE EXCEPTION 'fixed physical shard catalog is invalid';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN (
              'booking_commands',
              'operator_booking_commands',
              'booking_quota_leases',
              'reservation_directory',
              'train_run_seat_booking_overrides',
              'train_run_booking_policy_versions',
              'physical_shard_migrations',
              'physical_shard_migration_checkpoints',
              'physical_shard_target_write_observations',
              'physical_shard_reconciliation_runs',
              'physical_source_migration_capture_state',
              'physical_source_train_run_mutation_journal',
              'physical_control_target_apply_receipts',
              'physical_control_target_apply_authorizations'
          )) <> 14 THEN
        RAISE EXCEPTION 'control-plane version 9 tables are incomplete';
    END IF;

    IF (SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fares'
          AND column_name IN ('source_version', 'last_booking_command_id')) <> 2 THEN
        RAISE EXCEPTION 'operator fare version contract is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
        WHERE schema_row.nspname = 'public'
          AND table_row.relname = 'operator_booking_commands'
          AND constraint_row.contype = 'c'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%finalization_failed%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%receipt_mismatch%'
          AND pg_get_constraintdef(constraint_row.oid) LIKE '%control_conflict%'
    ) THEN
        RAISE EXCEPTION 'operator command bounded error allowlist is incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
        WHERE schema_row.nspname = 'public'
          AND table_row.relname = 'operator_booking_commands'
          AND constraint_row.conname = 'operator_booking_commands_policy_check'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%expected_booking_policy_version IS NOT NULL%'
    ) THEN
        RAISE EXCEPTION 'operator policy command permits a null expected version';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
        WHERE schema_row.nspname = 'public'
          AND table_row.relname = 'operator_booking_commands'
          AND constraint_row.conname = 'operator_booking_commands_payload_check'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%finalize_from_stop_index IS NOT NULL%'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%finalize_to_stop_index IS NOT NULL%'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%finalize_seat_class IS NOT NULL%'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%finalize_amount_minor IS NOT NULL%'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%finalize_currency IS NOT NULL%'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%finalize_seat_active IS NOT NULL%'
    ) THEN
        RAISE EXCEPTION 'operator command permits a null required payload field';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
        WHERE schema_row.nspname = 'public'
          AND table_row.relname = 'operator_booking_commands'
          AND constraint_row.conname =
              'operator_booking_commands_completion_check'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%result_source_version IS NOT NULL%'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%result_booking_policy_version IS NOT NULL%'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%lease_owner IS NULL%'
          AND pg_get_constraintdef(constraint_row.oid)
              LIKE '%lease_until IS NULL%'
    ) THEN
        RAISE EXCEPTION 'operator terminal state permits null results or a retained lease';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.triggers
        WHERE trigger_schema = 'public'
          AND event_object_table = 'physical_shard_migrations'
          AND trigger_name = 'physical_shard_migrations_operator_command_guard'
          AND action_statement LIKE '%reject_physical_migration_with_operator_command%'
    ) THEN
        RAISE EXCEPTION 'physical migration operator-command guard is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc AS function_row
        JOIN pg_namespace AS schema_row
          ON schema_row.oid = function_row.pronamespace
        WHERE schema_row.nspname = 'public'
          AND function_row.proname =
              'reject_physical_migration_with_operator_command'
          AND pg_get_functiondef(function_row.oid)
              LIKE '%operator_booking_commands%'
          AND pg_get_functiondef(function_row.oid)
              LIKE '%train_run_shard_assignments%'
          AND pg_get_functiondef(function_row.oid) LIKE '%FOR UPDATE%'
          AND pg_get_functiondef(function_row.oid) LIKE '%reserved%'
          AND pg_get_functiondef(function_row.oid) LIKE '%committed_on_shard%'
          AND pg_get_functiondef(function_row.oid) LIKE '%needs_repair%'
    ) THEN
        RAISE EXCEPTION 'physical migration operator-command guard is not fail closed';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.booking_shards
        WHERE connection_ref LIKE '%://%'
           OR connection_ref LIKE '%@%'
    ) THEN
        RAISE EXCEPTION 'catalog contains endpoint-like connection reference';
    END IF;
END;
$assert$;

-- LIKE copies the bounded CHECK contract without foreign keys, allowing the
-- schema assertion to prove NULL rejection without retaining fixture rows.
CREATE TEMP TABLE operator_booking_command_constraint_probe
    (LIKE public.operator_booking_commands INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

DO $assert_null_rejection$
BEGIN
    BEGIN
        INSERT INTO operator_booking_command_constraint_probe (
            command_id, actor_id, operation, idempotency_key_hash,
            request_fingerprint, train_run_id, resource_id, target_shard_id,
            assignment_generation, expected_source_version,
            expected_booking_policy_version, state
        ) VALUES (
            '10000000-0000-0000-0000-000000000001',
            '20000000-0000-0000-0000-000000000001',
            'booking_policy.bump', decode(repeat('01', 32), 'hex'),
            decode(repeat('02', 32), 'hex'),
            '30000000-0000-0000-0000-000000000001',
            '30000000-0000-0000-0000-000000000001',
            'physical-shard-0', 1, 1, NULL, 'reserved'
        );
        RAISE EXCEPTION 'null expected policy version was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO operator_booking_command_constraint_probe (
            command_id, actor_id, operation, idempotency_key_hash,
            request_fingerprint, train_run_id, resource_id, target_shard_id,
            assignment_generation, expected_source_version,
            finalize_from_stop_index, finalize_to_stop_index,
            finalize_seat_class, finalize_amount_minor, finalize_currency,
            state
        ) VALUES (
            '10000000-0000-0000-0000-000000000002',
            '20000000-0000-0000-0000-000000000002',
            'fare.install', decode(repeat('03', 32), 'hex'),
            decode(repeat('04', 32), 'hex'),
            '30000000-0000-0000-0000-000000000002',
            '40000000-0000-0000-0000-000000000002',
            'physical-shard-0', 1, 1,
            NULL, 2, 'standard', 100, 'TWD', 'reserved'
        );
        RAISE EXCEPTION 'null fare payload field was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO operator_booking_command_constraint_probe (
            command_id, actor_id, operation, idempotency_key_hash,
            request_fingerprint, train_run_id, resource_id, target_shard_id,
            assignment_generation, expected_source_version,
            finalize_seat_active, state
        ) VALUES (
            '10000000-0000-0000-0000-000000000003',
            '20000000-0000-0000-0000-000000000003',
            'seat.enable', decode(repeat('05', 32), 'hex'),
            decode(repeat('06', 32), 'hex'),
            '30000000-0000-0000-0000-000000000003',
            '40000000-0000-0000-0000-000000000003',
            'physical-shard-0', 1, 1, NULL, 'reserved'
        );
        RAISE EXCEPTION 'null seat payload field was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO operator_booking_command_constraint_probe (
            command_id, actor_id, operation, idempotency_key_hash,
            request_fingerprint, train_run_id, resource_id, target_shard_id,
            assignment_generation, expected_source_version,
            finalize_from_stop_index, finalize_to_stop_index,
            finalize_seat_class, finalize_amount_minor, finalize_currency,
            result_source_version, state, completed_at
        ) VALUES (
            '10000000-0000-0000-0000-000000000004',
            '20000000-0000-0000-0000-000000000004',
            'fare.install', decode(repeat('07', 32), 'hex'),
            decode(repeat('08', 32), 'hex'),
            '30000000-0000-0000-0000-000000000004',
            '40000000-0000-0000-0000-000000000004',
            'physical-shard-0', 1, 1,
            0, 2, 'standard', 100, 'TWD', NULL, 'finalized', clock_timestamp()
        );
        RAISE EXCEPTION 'null finalized result version was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO operator_booking_command_constraint_probe (
            command_id, actor_id, operation, idempotency_key_hash,
            request_fingerprint, train_run_id, resource_id, target_shard_id,
            assignment_generation, expected_source_version,
            finalize_from_stop_index, finalize_to_stop_index,
            finalize_seat_class, finalize_amount_minor, finalize_currency,
            result_source_version, state, lease_owner, lease_until,
            completed_at
        ) VALUES (
            '10000000-0000-0000-0000-000000000005',
            '20000000-0000-0000-0000-000000000005',
            'fare.install', decode(repeat('09', 32), 'hex'),
            decode(repeat('10', 32), 'hex'),
            '30000000-0000-0000-0000-000000000005',
            '40000000-0000-0000-0000-000000000005',
            'physical-shard-0', 1, 1,
            0, 2, 'standard', 100, 'TWD', 2, 'finalized',
            'worker-1', clock_timestamp(), clock_timestamp()
        );
        RAISE EXCEPTION 'terminal command retained an active lease';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$assert_null_rejection$;
