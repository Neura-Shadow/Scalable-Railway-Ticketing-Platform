BEGIN;

-- Schema-isolated booking shards are process-wide database objects. Some
-- integration suites apply migrations in isolated search_path schemas, so a
-- transaction-scoped advisory lock serializes this fixed, idempotent topology
-- bootstrap without accepting a dynamic schema identifier.
SELECT pg_advisory_xact_lock(804230008);

CREATE SCHEMA IF NOT EXISTS booking_shard_0;
CREATE SCHEMA IF NOT EXISTS booking_shard_1;

CREATE TABLE IF NOT EXISTS public.booking_shards (
    shard_id text PRIMARY KEY,
    storage_kind text NOT NULL
        CHECK (storage_kind IN ('legacy', 'schema')),
    enabled boolean NOT NULL DEFAULT true,
    write_enabled boolean NOT NULL DEFAULT true,
    state text NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'degraded', 'draining', 'disabled')),
    minimum_fencing_protocol_version integer NOT NULL DEFAULT 1
        CHECK (minimum_fencing_protocol_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT booking_shards_fixed_topology_check CHECK (
        (shard_id = 'legacy' AND storage_kind = 'legacy')
        OR (shard_id IN ('shard-0', 'shard-1') AND storage_kind = 'schema')
    ),
    CONSTRAINT booking_shards_enabled_state_check CHECK (
        state <> 'disabled' OR (NOT enabled AND NOT write_enabled)
    )
);

DROP TRIGGER IF EXISTS booking_shards_set_updated_at ON public.booking_shards;
CREATE TRIGGER booking_shards_set_updated_at
BEFORE UPDATE ON public.booking_shards
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.train_run_shard_assignments (
    train_run_id uuid PRIMARY KEY
        REFERENCES public.train_runs(id) ON DELETE CASCADE,
    shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    assignment_state text NOT NULL DEFAULT 'stable'
        CHECK (assignment_state IN ('stable', 'draining', 'migrating', 'rollback_window')),
    active_migration_id uuid,
    availability_generation bigint NOT NULL DEFAULT 1
        CHECK (availability_generation > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (train_run_id, shard_id, assignment_generation),
    UNIQUE (train_run_id, assignment_generation)
);

CREATE INDEX IF NOT EXISTS train_run_shard_assignments_shard_idx
    ON public.train_run_shard_assignments (shard_id, assignment_state, train_run_id);
CREATE INDEX IF NOT EXISTS train_run_shard_assignments_active_migration_idx
    ON public.train_run_shard_assignments (active_migration_id)
    WHERE active_migration_id IS NOT NULL;

DROP TRIGGER IF EXISTS train_run_shard_assignments_set_updated_at
    ON public.train_run_shard_assignments;
CREATE TRIGGER train_run_shard_assignments_set_updated_at
BEFORE UPDATE ON public.train_run_shard_assignments
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.train_run_write_fences (
    train_run_id uuid PRIMARY KEY
        REFERENCES public.train_runs(id) ON DELETE CASCADE,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    write_enabled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS booking_shard_0.train_run_write_fences (
    train_run_id uuid PRIMARY KEY
        REFERENCES public.train_runs(id) ON DELETE CASCADE,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    write_enabled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS booking_shard_1.train_run_write_fences (
    train_run_id uuid PRIMARY KEY
        REFERENCES public.train_runs(id) ON DELETE CASCADE,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    write_enabled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

DROP TRIGGER IF EXISTS train_run_write_fences_set_updated_at
    ON public.train_run_write_fences;
CREATE TRIGGER train_run_write_fences_set_updated_at
BEFORE UPDATE ON public.train_run_write_fences
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS train_run_write_fences_set_updated_at
    ON booking_shard_0.train_run_write_fences;
CREATE TRIGGER train_run_write_fences_set_updated_at
BEFORE UPDATE ON booking_shard_0.train_run_write_fences
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS train_run_write_fences_set_updated_at
    ON booking_shard_1.train_run_write_fences;
CREATE TRIGGER train_run_write_fences_set_updated_at
BEFORE UPDATE ON booking_shard_1.train_run_write_fences
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.train_run_shard_migrations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    source_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    target_shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    source_generation bigint NOT NULL CHECK (source_generation > 0),
    target_generation bigint NOT NULL CHECK (target_generation > 0),
    state text NOT NULL DEFAULT 'planned'
        CHECK (state IN (
            'planned', 'draining', 'copying', 'validating', 'cutover_ready',
            'cutting_over', 'rollback_window', 'completed', 'failed', 'rolled_back'
        )),
    copy_phase text NOT NULL DEFAULT 'inventory'
        CHECK (copy_phase IN (
            'inventory', 'reservations', 'reservation_seats', 'ticket_orders',
            'tickets', 'idempotency_records', 'fence', 'complete'
        )),
    copy_cursor uuid,
    copy_checkpoint text NOT NULL DEFAULT '',
    copied_rows bigint NOT NULL DEFAULT 0,
    copy_complete boolean NOT NULL DEFAULT false,
    inventory_rows_copied bigint NOT NULL DEFAULT 0 CHECK (inventory_rows_copied >= 0),
    reservation_rows_copied bigint NOT NULL DEFAULT 0 CHECK (reservation_rows_copied >= 0),
    reservation_seat_rows_copied bigint NOT NULL DEFAULT 0 CHECK (reservation_seat_rows_copied >= 0),
    ticket_order_rows_copied bigint NOT NULL DEFAULT 0 CHECK (ticket_order_rows_copied >= 0),
    ticket_rows_copied bigint NOT NULL DEFAULT 0 CHECK (ticket_rows_copied >= 0),
    idempotency_rows_copied bigint NOT NULL DEFAULT 0 CHECK (idempotency_rows_copied >= 0),
    validation_status text NOT NULL DEFAULT 'pending'
        CHECK (validation_status IN ('pending', 'running', 'passed', 'failed')),
    last_validation jsonb,
    error_category text,
    rollback_window_seconds integer NOT NULL DEFAULT 300,
    rollback_generation bigint,
    planned_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    drain_started_at timestamptz,
    quiesced_at timestamptz,
    copy_started_at timestamptz,
    validated_at timestamptz,
    cutover_at timestamptz,
    rollback_deadline_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT train_run_shard_migrations_distinct_shards_check
        CHECK (source_shard_id <> target_shard_id),
    CONSTRAINT train_run_shard_migrations_generation_order_check
        CHECK (target_generation > source_generation),
    CONSTRAINT train_run_shard_migrations_aggregate_copy_check
        CHECK (
            length(copy_checkpoint) <= 128
            AND copied_rows >= 0
            AND rollback_window_seconds BETWEEN 1 AND 86400
            AND (
                rollback_generation IS NULL
                OR rollback_generation > target_generation
            )
            AND (
                last_validation IS NULL
                OR (
                    jsonb_typeof(last_validation) = 'object'
                    AND octet_length(last_validation::text) <= 8192
                )
            )
        ),
    CONSTRAINT train_run_shard_migrations_error_category_check
        CHECK (
            error_category IS NULL
            OR error_category ~ '^[a-z][a-z0-9_]{0,63}$'
        ),
    CONSTRAINT train_run_shard_migrations_rollback_deadline_check
        CHECK (
            rollback_deadline_at IS NULL
            OR (cutover_at IS NOT NULL AND rollback_deadline_at > cutover_at)
        )
);

-- Keep direct SQL application idempotent for integration suites that install
-- the fixed public topology from several isolated search_path schemas.
ALTER TABLE public.train_run_shard_migrations
    ADD COLUMN IF NOT EXISTS copy_checkpoint text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS copied_rows bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS copy_complete boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS last_validation jsonb,
    ADD COLUMN IF NOT EXISTS rollback_window_seconds integer NOT NULL DEFAULT 300,
    ADD COLUMN IF NOT EXISTS rollback_generation bigint;

DO $m4_constraint$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.train_run_shard_migrations'::regclass
          AND conname = 'train_run_shard_migrations_aggregate_copy_check'
    ) THEN
        ALTER TABLE public.train_run_shard_migrations
            ADD CONSTRAINT train_run_shard_migrations_aggregate_copy_check
            CHECK (
                length(copy_checkpoint) <= 128
                AND copied_rows >= 0
                AND rollback_window_seconds BETWEEN 1 AND 86400
                AND (
                    rollback_generation IS NULL
                    OR rollback_generation > target_generation
                )
                AND (
                    last_validation IS NULL
                    OR (
                        jsonb_typeof(last_validation) = 'object'
                        AND octet_length(last_validation::text) <= 8192
                    )
                )
            );
    END IF;
END
$m4_constraint$;

CREATE UNIQUE INDEX IF NOT EXISTS train_run_shard_migrations_one_active_idx
    ON public.train_run_shard_migrations (train_run_id)
    WHERE state NOT IN ('completed', 'failed', 'rolled_back');
CREATE INDEX IF NOT EXISTS train_run_shard_migrations_state_idx
    ON public.train_run_shard_migrations (state, updated_at, id);

DROP TRIGGER IF EXISTS train_run_shard_migrations_set_updated_at
    ON public.train_run_shard_migrations;
CREATE TRIGGER train_run_shard_migrations_set_updated_at
BEFORE UPDATE ON public.train_run_shard_migrations
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DO $m4_constraint$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.train_run_shard_assignments'::regclass
          AND conname = 'train_run_shard_assignments_active_migration_fkey'
    ) THEN
        ALTER TABLE public.train_run_shard_assignments
            ADD CONSTRAINT train_run_shard_assignments_active_migration_fkey
            FOREIGN KEY (active_migration_id)
            REFERENCES public.train_run_shard_migrations(id)
            ON DELETE RESTRICT
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$m4_constraint$;

CREATE TABLE IF NOT EXISTS public.train_run_generation_writes (
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    shard_id text NOT NULL
        REFERENCES public.booking_shards(shard_id) ON DELETE RESTRICT,
    migration_id uuid NOT NULL
        REFERENCES public.train_run_shard_migrations(id) ON DELETE RESTRICT,
    successful_write_count bigint NOT NULL DEFAULT 0
        CHECK (successful_write_count >= 0),
    first_successful_write_at timestamptz,
    last_successful_write_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (train_run_id, assignment_generation),
    CONSTRAINT train_run_generation_writes_timestamp_check CHECK (
        (successful_write_count = 0
            AND first_successful_write_at IS NULL
            AND last_successful_write_at IS NULL)
        OR
        (successful_write_count > 0
            AND first_successful_write_at IS NOT NULL
            AND last_successful_write_at IS NOT NULL
            AND last_successful_write_at >= first_successful_write_at)
    )
);

CREATE INDEX IF NOT EXISTS train_run_generation_writes_migration_idx
    ON public.train_run_generation_writes (migration_id, train_run_id);

DROP TRIGGER IF EXISTS train_run_generation_writes_set_updated_at
    ON public.train_run_generation_writes;
CREATE TRIGGER train_run_generation_writes_set_updated_at
BEFORE UPDATE ON public.train_run_generation_writes
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.idempotency_records
    ADD COLUMN IF NOT EXISTS train_run_id uuid
        REFERENCES public.train_runs(id) ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idempotency_records_train_run_idx
    ON public.idempotency_records (train_run_id, expires_at, id)
    WHERE train_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.booking_idempotency_key_claims (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE CASCADE,
    operation text NOT NULL
        CHECK (operation IN ('reservation.create', 'reservation.confirm', 'reservation.cancel')),
    key_hash bytea NOT NULL CHECK (octet_length(key_hash) = 32),
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    train_run_id uuid
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (user_id, operation, key_hash),
    CHECK (expires_at > created_at)
);

-- A committed version-7 in-progress record has no resource from which to
-- derive train_run_id. This tightly constrained nullable form preserves the
-- global key conflict until completion/expiry; routed version-8 acquisition
-- always supplies a train run. The claim deliberately carries no storage
-- route, generation, local record identity, or replay result authority.
ALTER TABLE public.booking_idempotency_key_claims
    ALTER COLUMN train_run_id DROP NOT NULL;

CREATE INDEX IF NOT EXISTS booking_idempotency_key_claims_expiry_idx
    ON public.booking_idempotency_key_claims (expires_at, id);
CREATE INDEX IF NOT EXISTS booking_idempotency_key_claims_train_run_idx
    ON public.booking_idempotency_key_claims (train_run_id, expires_at, id);

DROP TRIGGER IF EXISTS booking_idempotency_key_claims_set_updated_at
    ON public.booking_idempotency_key_claims;
CREATE TRIGGER booking_idempotency_key_claims_set_updated_at
BEFORE UPDATE ON public.booking_idempotency_key_claims
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.reservation_quota_claims (
    reservation_id uuid PRIMARY KEY,
    user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    passenger_count integer NOT NULL CHECK (passenger_count > 0),
    active boolean NOT NULL DEFAULT true,
    closed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT reservation_quota_claims_closed_check CHECK (
        (active AND closed_at IS NULL)
        OR (NOT active AND closed_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS reservation_quota_claims_active_user_idx
    ON public.reservation_quota_claims (user_id, reservation_id)
    WHERE active;
CREATE INDEX IF NOT EXISTS reservation_quota_claims_active_user_train_run_idx
    ON public.reservation_quota_claims (user_id, train_run_id, reservation_id)
    WHERE active;

DROP TRIGGER IF EXISTS reservation_quota_claims_set_updated_at
    ON public.reservation_quota_claims;
CREATE TRIGGER reservation_quota_claims_set_updated_at
BEFORE UPDATE ON public.reservation_quota_claims
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.reservation_shard_locators (
    reservation_id uuid PRIMARY KEY,
    train_run_id uuid NOT NULL,
    shard_id text NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    owner_user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT reservation_shard_locators_assignment_fkey
        FOREIGN KEY (train_run_id, shard_id, assignment_generation)
        REFERENCES public.train_run_shard_assignments(
            train_run_id, shard_id, assignment_generation
        ) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS reservation_shard_locators_train_run_idx
    ON public.reservation_shard_locators (train_run_id, reservation_id);
CREATE INDEX IF NOT EXISTS reservation_shard_locators_owner_idx
    ON public.reservation_shard_locators (owner_user_id, created_at DESC, reservation_id DESC);

DROP TRIGGER IF EXISTS reservation_shard_locators_set_updated_at
    ON public.reservation_shard_locators;
CREATE TRIGGER reservation_shard_locators_set_updated_at
BEFORE UPDATE ON public.reservation_shard_locators
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.ticket_order_shard_locators (
    ticket_order_id uuid PRIMARY KEY,
    reservation_id uuid NOT NULL UNIQUE
        REFERENCES public.reservation_shard_locators(reservation_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    train_run_id uuid NOT NULL,
    shard_id text NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    owner_user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    status text NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    total_amount_minor bigint NOT NULL CHECK (total_amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ticket_order_shard_locators_assignment_fkey
        FOREIGN KEY (train_run_id, shard_id, assignment_generation)
        REFERENCES public.train_run_shard_assignments(
            train_run_id, shard_id, assignment_generation
        ) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS ticket_order_shard_locators_train_run_idx
    ON public.ticket_order_shard_locators (train_run_id, ticket_order_id);
CREATE INDEX IF NOT EXISTS ticket_order_shard_locators_owner_created_idx
    ON public.ticket_order_shard_locators (
        owner_user_id, created_at DESC, ticket_order_id DESC
    );
CREATE INDEX IF NOT EXISTS ticket_order_shard_locators_owner_status_idx
    ON public.ticket_order_shard_locators (
        owner_user_id, status, created_at DESC, ticket_order_id DESC
    );

DROP TRIGGER IF EXISTS ticket_order_shard_locators_set_updated_at
    ON public.ticket_order_shard_locators;
CREATE TRIGGER ticket_order_shard_locators_set_updated_at
BEFORE UPDATE ON public.ticket_order_shard_locators
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.ticket_shard_locators (
    ticket_id uuid PRIMARY KEY,
    ticket_order_id uuid NOT NULL
        REFERENCES public.ticket_order_shard_locators(ticket_order_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    reservation_id uuid NOT NULL
        REFERENCES public.reservation_shard_locators(reservation_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    train_run_id uuid NOT NULL,
    shard_id text NOT NULL,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    owner_user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ticket_shard_locators_assignment_fkey
        FOREIGN KEY (train_run_id, shard_id, assignment_generation)
        REFERENCES public.train_run_shard_assignments(
            train_run_id, shard_id, assignment_generation
        ) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS ticket_shard_locators_train_run_idx
    ON public.ticket_shard_locators (train_run_id, ticket_id);
CREATE INDEX IF NOT EXISTS ticket_shard_locators_order_idx
    ON public.ticket_shard_locators (ticket_order_id, ticket_id);

DROP TRIGGER IF EXISTS ticket_shard_locators_set_updated_at
    ON public.ticket_shard_locators;
CREATE TRIGGER ticket_shard_locators_set_updated_at
BEFORE UPDATE ON public.ticket_shard_locators
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.outbox_events
    ADD COLUMN IF NOT EXISTS train_run_id uuid
        REFERENCES public.train_runs(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS shard_id text NOT NULL DEFAULT 'global',
    ADD COLUMN IF NOT EXISTS assignment_generation bigint;

DO $m4_constraint$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.outbox_events'::regclass
          AND conname = 'outbox_events_shard_provenance_check'
    ) THEN
        ALTER TABLE public.outbox_events
            ADD CONSTRAINT outbox_events_shard_provenance_check CHECK (
                (shard_id = 'global' AND assignment_generation IS NULL)
                OR
                (
                    shard_id IN ('legacy', 'shard-0', 'shard-1')
                    AND train_run_id IS NOT NULL
                    AND assignment_generation > 0
                )
            );
    END IF;
END
$m4_constraint$;

CREATE INDEX IF NOT EXISTS outbox_events_train_run_provenance_idx
    ON public.outbox_events (
        train_run_id, shard_id, assignment_generation, created_at, id
    )
    WHERE train_run_id IS NOT NULL;

-- The two logical shards copy the version-7 booking shape, then add explicit
-- cross-schema foreign keys. LIKE does not copy foreign keys or triggers.
CREATE TABLE IF NOT EXISTS booking_shard_0.seat_inventory (
    LIKE public.seat_inventory INCLUDING ALL,
    CONSTRAINT seat_inventory_seat_id_fkey
        FOREIGN KEY (seat_id) REFERENCES public.seats(id) ON DELETE RESTRICT,
    CONSTRAINT seat_inventory_train_run_id_segment_count_fkey
        FOREIGN KEY (train_run_id, segment_count)
        REFERENCES public.train_runs(id, segment_count) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS booking_shard_0.reservations (
    LIKE public.reservations INCLUDING ALL,
    CONSTRAINT reservations_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE RESTRICT,
    CONSTRAINT reservations_train_run_id_segment_count_fkey
        FOREIGN KEY (train_run_id, segment_count)
        REFERENCES public.train_runs(id, segment_count) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS booking_shard_0.reservation_seats (
    LIKE public.reservation_seats INCLUDING ALL,
    CONSTRAINT reservation_seats_seat_id_fkey
        FOREIGN KEY (seat_id) REFERENCES public.seats(id) ON DELETE RESTRICT,
    CONSTRAINT reservation_seats_passenger_id_fkey
        FOREIGN KEY (passenger_id) REFERENCES public.passengers(id) ON DELETE RESTRICT,
    CONSTRAINT reservation_seats_reservation_run_segment_fkey
        FOREIGN KEY (reservation_id, train_run_id, segment_count)
        REFERENCES booking_shard_0.reservations(id, train_run_id, segment_count)
        ON DELETE CASCADE,
    CONSTRAINT reservation_seats_inventory_fkey
        FOREIGN KEY (train_run_id, seat_id)
        REFERENCES booking_shard_0.seat_inventory(train_run_id, seat_id)
        ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS booking_shard_0.ticket_orders (
    LIKE public.ticket_orders INCLUDING ALL,
    CONSTRAINT ticket_orders_reservation_id_fkey
        FOREIGN KEY (reservation_id)
        REFERENCES booking_shard_0.reservations(id) ON DELETE RESTRICT,
    CONSTRAINT ticket_orders_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS booking_shard_0.tickets (
    LIKE public.tickets INCLUDING ALL,
    CONSTRAINT tickets_ticket_order_id_fkey
        FOREIGN KEY (ticket_order_id)
        REFERENCES booking_shard_0.ticket_orders(id) ON DELETE RESTRICT,
    CONSTRAINT tickets_reservation_seat_id_fkey
        FOREIGN KEY (reservation_seat_id)
        REFERENCES booking_shard_0.reservation_seats(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS booking_shard_0.idempotency_records (
    LIKE public.idempotency_records INCLUDING ALL,
    CONSTRAINT idempotency_records_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT idempotency_records_train_run_id_fkey
        FOREIGN KEY (train_run_id) REFERENCES public.train_runs(id) ON DELETE RESTRICT
);

ALTER TABLE booking_shard_0.idempotency_records
    ALTER COLUMN train_run_id SET NOT NULL;

CREATE TABLE IF NOT EXISTS booking_shard_1.seat_inventory (
    LIKE public.seat_inventory INCLUDING ALL,
    CONSTRAINT seat_inventory_seat_id_fkey
        FOREIGN KEY (seat_id) REFERENCES public.seats(id) ON DELETE RESTRICT,
    CONSTRAINT seat_inventory_train_run_id_segment_count_fkey
        FOREIGN KEY (train_run_id, segment_count)
        REFERENCES public.train_runs(id, segment_count) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS booking_shard_1.reservations (
    LIKE public.reservations INCLUDING ALL,
    CONSTRAINT reservations_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE RESTRICT,
    CONSTRAINT reservations_train_run_id_segment_count_fkey
        FOREIGN KEY (train_run_id, segment_count)
        REFERENCES public.train_runs(id, segment_count) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS booking_shard_1.reservation_seats (
    LIKE public.reservation_seats INCLUDING ALL,
    CONSTRAINT reservation_seats_seat_id_fkey
        FOREIGN KEY (seat_id) REFERENCES public.seats(id) ON DELETE RESTRICT,
    CONSTRAINT reservation_seats_passenger_id_fkey
        FOREIGN KEY (passenger_id) REFERENCES public.passengers(id) ON DELETE RESTRICT,
    CONSTRAINT reservation_seats_reservation_run_segment_fkey
        FOREIGN KEY (reservation_id, train_run_id, segment_count)
        REFERENCES booking_shard_1.reservations(id, train_run_id, segment_count)
        ON DELETE CASCADE,
    CONSTRAINT reservation_seats_inventory_fkey
        FOREIGN KEY (train_run_id, seat_id)
        REFERENCES booking_shard_1.seat_inventory(train_run_id, seat_id)
        ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS booking_shard_1.ticket_orders (
    LIKE public.ticket_orders INCLUDING ALL,
    CONSTRAINT ticket_orders_reservation_id_fkey
        FOREIGN KEY (reservation_id)
        REFERENCES booking_shard_1.reservations(id) ON DELETE RESTRICT,
    CONSTRAINT ticket_orders_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS booking_shard_1.tickets (
    LIKE public.tickets INCLUDING ALL,
    CONSTRAINT tickets_ticket_order_id_fkey
        FOREIGN KEY (ticket_order_id)
        REFERENCES booking_shard_1.ticket_orders(id) ON DELETE RESTRICT,
    CONSTRAINT tickets_reservation_seat_id_fkey
        FOREIGN KEY (reservation_seat_id)
        REFERENCES booking_shard_1.reservation_seats(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS booking_shard_1.idempotency_records (
    LIKE public.idempotency_records INCLUDING ALL,
    CONSTRAINT idempotency_records_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT idempotency_records_train_run_id_fkey
        FOREIGN KEY (train_run_id) REFERENCES public.train_runs(id) ON DELETE RESTRICT
);

ALTER TABLE booking_shard_1.idempotency_records
    ALTER COLUMN train_run_id SET NOT NULL;

CREATE OR REPLACE FUNCTION booking_shard_0.validate_inventory_seat_class()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.seats AS s
        JOIN public.coaches AS c ON c.id = s.coach_id
        JOIN public.train_runs AS tr
          ON tr.id = NEW.train_run_id
         AND tr.train_id = c.train_id
        WHERE s.id = NEW.seat_id
          AND s.active
          AND c.seat_class = NEW.seat_class
    ) THEN
        RAISE EXCEPTION 'inventory seat violates run train or class integrity'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS seat_inventory_validate_class
    ON booking_shard_0.seat_inventory;
CREATE TRIGGER seat_inventory_validate_class
BEFORE INSERT OR UPDATE OF train_run_id, seat_id, seat_class
ON booking_shard_0.seat_inventory
FOR EACH ROW EXECUTE FUNCTION booking_shard_0.validate_inventory_seat_class();

CREATE OR REPLACE FUNCTION booking_shard_1.validate_inventory_seat_class()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.seats AS s
        JOIN public.coaches AS c ON c.id = s.coach_id
        JOIN public.train_runs AS tr
          ON tr.id = NEW.train_run_id
         AND tr.train_id = c.train_id
        WHERE s.id = NEW.seat_id
          AND s.active
          AND c.seat_class = NEW.seat_class
    ) THEN
        RAISE EXCEPTION 'inventory seat violates run train or class integrity'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS seat_inventory_validate_class
    ON booking_shard_1.seat_inventory;
CREATE TRIGGER seat_inventory_validate_class
BEFORE INSERT OR UPDATE OF train_run_id, seat_id, seat_class
ON booking_shard_1.seat_inventory
FOR EACH ROW EXECUTE FUNCTION booking_shard_1.validate_inventory_seat_class();

CREATE OR REPLACE FUNCTION booking_shard_0.validate_reservation_seat()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM booking_shard_0.reservations AS r
        JOIN public.passengers AS p
          ON p.id = NEW.passenger_id
         AND p.user_id = r.user_id
        JOIN booking_shard_0.seat_inventory AS si
          ON si.train_run_id = r.train_run_id
         AND si.seat_id = NEW.seat_id
         AND si.segment_count = NEW.segment_count
         AND si.seat_class = r.seat_class
        WHERE r.id = NEW.reservation_id
          AND r.train_run_id = NEW.train_run_id
          AND r.segment_count = NEW.segment_count
          AND NEW.segment_mask = repeat('0', r.from_stop_index)::bit varying
                                 || repeat('1', r.to_stop_index - r.from_stop_index)::bit varying
                                 || repeat('0', r.segment_count - r.to_stop_index)::bit varying
          AND NEW.currency = r.currency
    ) THEN
        RAISE EXCEPTION 'reservation seat violates ownership, class, inventory, mask, or currency invariants'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS reservation_seats_validate
    ON booking_shard_0.reservation_seats;
CREATE CONSTRAINT TRIGGER reservation_seats_validate
AFTER INSERT OR UPDATE ON booking_shard_0.reservation_seats
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION booking_shard_0.validate_reservation_seat();

CREATE OR REPLACE FUNCTION booking_shard_1.validate_reservation_seat()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM booking_shard_1.reservations AS r
        JOIN public.passengers AS p
          ON p.id = NEW.passenger_id
         AND p.user_id = r.user_id
        JOIN booking_shard_1.seat_inventory AS si
          ON si.train_run_id = r.train_run_id
         AND si.seat_id = NEW.seat_id
         AND si.segment_count = NEW.segment_count
         AND si.seat_class = r.seat_class
        WHERE r.id = NEW.reservation_id
          AND r.train_run_id = NEW.train_run_id
          AND r.segment_count = NEW.segment_count
          AND NEW.segment_mask = repeat('0', r.from_stop_index)::bit varying
                                 || repeat('1', r.to_stop_index - r.from_stop_index)::bit varying
                                 || repeat('0', r.segment_count - r.to_stop_index)::bit varying
          AND NEW.currency = r.currency
    ) THEN
        RAISE EXCEPTION 'reservation seat violates ownership, class, inventory, mask, or currency invariants'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS reservation_seats_validate
    ON booking_shard_1.reservation_seats;
CREATE CONSTRAINT TRIGGER reservation_seats_validate
AFTER INSERT OR UPDATE ON booking_shard_1.reservation_seats
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION booking_shard_1.validate_reservation_seat();

DROP TRIGGER IF EXISTS seat_inventory_set_updated_at
    ON booking_shard_0.seat_inventory;
CREATE TRIGGER seat_inventory_set_updated_at
BEFORE UPDATE ON booking_shard_0.seat_inventory
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS reservations_set_updated_at
    ON booking_shard_0.reservations;
CREATE TRIGGER reservations_set_updated_at
BEFORE UPDATE ON booking_shard_0.reservations
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS ticket_orders_set_updated_at
    ON booking_shard_0.ticket_orders;
CREATE TRIGGER ticket_orders_set_updated_at
BEFORE UPDATE ON booking_shard_0.ticket_orders
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS tickets_set_updated_at
    ON booking_shard_0.tickets;
CREATE TRIGGER tickets_set_updated_at
BEFORE UPDATE ON booking_shard_0.tickets
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS idempotency_records_set_updated_at
    ON booking_shard_0.idempotency_records;
CREATE TRIGGER idempotency_records_set_updated_at
BEFORE UPDATE ON booking_shard_0.idempotency_records
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS seat_inventory_set_updated_at
    ON booking_shard_1.seat_inventory;
CREATE TRIGGER seat_inventory_set_updated_at
BEFORE UPDATE ON booking_shard_1.seat_inventory
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS reservations_set_updated_at
    ON booking_shard_1.reservations;
CREATE TRIGGER reservations_set_updated_at
BEFORE UPDATE ON booking_shard_1.reservations
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS ticket_orders_set_updated_at
    ON booking_shard_1.ticket_orders;
CREATE TRIGGER ticket_orders_set_updated_at
BEFORE UPDATE ON booking_shard_1.ticket_orders
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS tickets_set_updated_at
    ON booking_shard_1.tickets;
CREATE TRIGGER tickets_set_updated_at
BEFORE UPDATE ON booking_shard_1.tickets
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS idempotency_records_set_updated_at
    ON booking_shard_1.idempotency_records;
CREATE TRIGGER idempotency_records_set_updated_at
BEFORE UPDATE ON booking_shard_1.idempotency_records
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

INSERT INTO public.booking_shards (
    shard_id, storage_kind, enabled, write_enabled, state,
    minimum_fencing_protocol_version
) VALUES
    ('legacy', 'legacy', true, true, 'active', 1),
    ('shard-0', 'schema', true, true, 'active', 1),
    ('shard-1', 'schema', true, true, 'active', 1)
ON CONFLICT (shard_id) DO NOTHING;

INSERT INTO public.train_run_shard_assignments (
    train_run_id, shard_id, assignment_generation, assignment_state,
    availability_generation
)
SELECT tr.id, 'legacy', 1, 'stable', 1
FROM public.train_runs AS tr
ON CONFLICT (train_run_id) DO NOTHING;

INSERT INTO public.train_run_write_fences (
    train_run_id, assignment_generation, write_enabled
)
SELECT assignment.train_run_id, assignment.assignment_generation, true
FROM public.train_run_shard_assignments AS assignment
WHERE assignment.shard_id = 'legacy'
ON CONFLICT (train_run_id) DO NOTHING;

-- Version 7 can resolve completed idempotency rows through their reservation.
-- In-progress legacy rows have no resource ID and remain nullable until expiry
-- or a fenced version-8 writer reacquires them with an explicit train run.
UPDATE public.idempotency_records AS record
SET train_run_id = reservation.train_run_id
FROM public.reservations AS reservation
WHERE record.train_run_id IS NULL
  AND record.resource_type = 'reservation'
  AND record.resource_id = reservation.id;

INSERT INTO public.booking_idempotency_key_claims (
    user_id, operation, key_hash, request_fingerprint, train_run_id, expires_at,
    created_at, updated_at
)
SELECT record.user_id,
       record.operation,
       record.key_hash,
       record.request_fingerprint,
       record.train_run_id,
       record.expires_at,
       record.created_at,
       record.updated_at
FROM public.idempotency_records AS record
WHERE record.train_run_id IS NOT NULL
ON CONFLICT (user_id, operation, key_hash) DO NOTHING;

INSERT INTO public.booking_idempotency_key_claims (
    user_id, operation, key_hash, request_fingerprint, train_run_id, expires_at,
    created_at, updated_at
)
SELECT record.user_id,
       record.operation,
       record.key_hash,
       record.request_fingerprint,
       NULL,
       record.expires_at,
       record.created_at,
       record.updated_at
FROM public.idempotency_records AS record
WHERE record.train_run_id IS NULL
  AND record.status = 'in_progress'
ON CONFLICT (user_id, operation, key_hash) DO NOTHING;

DO $m4_quota_preflight$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.reservations AS reservation
        LEFT JOIN public.reservation_seats AS seat
          ON seat.reservation_id = reservation.id
        GROUP BY reservation.id
        HAVING count(seat.id) = 0
    ) THEN
        RAISE EXCEPTION 'existing reservation lacks passenger-seat state required by quota claims'
            USING ERRCODE = '23514';
    END IF;
END
$m4_quota_preflight$;

INSERT INTO public.reservation_quota_claims (
    reservation_id, user_id, train_run_id, passenger_count, active,
    closed_at, created_at, updated_at
)
SELECT reservation.id,
       reservation.user_id,
       reservation.train_run_id,
       count(seat.id)::integer,
       reservation.status = 'held',
       CASE
           WHEN reservation.status = 'held' THEN NULL
           ELSE reservation.updated_at
       END,
       reservation.created_at,
       reservation.updated_at
FROM public.reservations AS reservation
JOIN public.reservation_seats AS seat
  ON seat.reservation_id = reservation.id
GROUP BY reservation.id
ON CONFLICT (reservation_id) DO NOTHING;

INSERT INTO public.reservation_shard_locators (
    reservation_id, train_run_id, shard_id, assignment_generation,
    owner_user_id, created_at, updated_at
)
SELECT reservation.id,
       reservation.train_run_id,
       assignment.shard_id,
       assignment.assignment_generation,
       reservation.user_id,
       reservation.created_at,
       reservation.updated_at
FROM public.reservations AS reservation
JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = reservation.train_run_id
ON CONFLICT (reservation_id) DO NOTHING;

INSERT INTO public.ticket_order_shard_locators (
    ticket_order_id, reservation_id, train_run_id, shard_id,
    assignment_generation, owner_user_id, status, total_amount_minor,
    currency, created_at, updated_at
)
SELECT ticket_order.id,
       ticket_order.reservation_id,
       reservation.train_run_id,
       assignment.shard_id,
       assignment.assignment_generation,
       ticket_order.user_id,
       ticket_order.status,
       ticket_order.total_amount_minor,
       ticket_order.currency,
       ticket_order.created_at,
       ticket_order.updated_at
FROM public.ticket_orders AS ticket_order
JOIN public.reservations AS reservation
  ON reservation.id = ticket_order.reservation_id
JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = reservation.train_run_id
ON CONFLICT (ticket_order_id) DO NOTHING;

INSERT INTO public.ticket_shard_locators (
    ticket_id, ticket_order_id, reservation_id, train_run_id, shard_id,
    assignment_generation, owner_user_id, created_at, updated_at
)
SELECT ticket.id,
       ticket.ticket_order_id,
       ticket_order.reservation_id,
       reservation.train_run_id,
       assignment.shard_id,
       assignment.assignment_generation,
       ticket_order.user_id,
       ticket.created_at,
       ticket.updated_at
FROM public.tickets AS ticket
JOIN public.ticket_orders AS ticket_order
  ON ticket_order.id = ticket.ticket_order_id
JOIN public.reservations AS reservation
  ON reservation.id = ticket_order.reservation_id
JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = reservation.train_run_id
ON CONFLICT (ticket_id) DO NOTHING;

UPDATE public.outbox_events AS event
SET train_run_id = reservation.train_run_id,
    shard_id = assignment.shard_id,
    assignment_generation = assignment.assignment_generation
FROM public.reservations AS reservation
JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = reservation.train_run_id
WHERE event.aggregate_type = 'reservation'
  AND event.aggregate_id = reservation.id
  AND event.shard_id = 'global';

UPDATE public.outbox_events AS event
SET train_run_id = reservation.train_run_id,
    shard_id = assignment.shard_id,
    assignment_generation = assignment.assignment_generation
FROM public.tickets AS ticket
JOIN public.ticket_orders AS ticket_order
  ON ticket_order.id = ticket.ticket_order_id
JOIN public.reservations AS reservation
  ON reservation.id = ticket_order.reservation_id
JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = reservation.train_run_id
WHERE event.aggregate_type = 'ticket'
  AND event.aggregate_id = ticket.id
  AND event.shard_id = 'global';

CREATE OR REPLACE FUNCTION public.populate_outbox_shard_provenance()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    resolved_train_run_id uuid;
    resolved_shard_id text;
    resolved_generation bigint;
BEGIN
    IF NEW.shard_id <> 'global' OR NEW.assignment_generation IS NOT NULL THEN
        RETURN NEW;
    END IF;

    IF NEW.aggregate_type = 'reservation' THEN
        SELECT reservation.train_run_id,
               assignment.shard_id,
               assignment.assignment_generation
        INTO resolved_train_run_id, resolved_shard_id, resolved_generation
        FROM public.reservations AS reservation
        JOIN public.train_run_shard_assignments AS assignment
          ON assignment.train_run_id = reservation.train_run_id
        WHERE reservation.id = NEW.aggregate_id;
    ELSIF NEW.aggregate_type = 'ticket' THEN
        SELECT reservation.train_run_id,
               assignment.shard_id,
               assignment.assignment_generation
        INTO resolved_train_run_id, resolved_shard_id, resolved_generation
        FROM public.tickets AS ticket
        JOIN public.ticket_orders AS ticket_order
          ON ticket_order.id = ticket.ticket_order_id
        JOIN public.reservations AS reservation
          ON reservation.id = ticket_order.reservation_id
        JOIN public.train_run_shard_assignments AS assignment
          ON assignment.train_run_id = reservation.train_run_id
        WHERE ticket.id = NEW.aggregate_id;
    END IF;

    IF resolved_train_run_id IS NOT NULL THEN
        NEW.train_run_id := resolved_train_run_id;
        NEW.shard_id := resolved_shard_id;
        NEW.assignment_generation := resolved_generation;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS outbox_events_populate_shard_provenance
    ON public.outbox_events;
CREATE TRIGGER outbox_events_populate_shard_provenance
BEFORE INSERT OR UPDATE OF aggregate_type, aggregate_id, train_run_id,
    shard_id, assignment_generation
ON public.outbox_events
FOR EACH ROW EXECUTE FUNCTION public.populate_outbox_shard_provenance();

CREATE OR REPLACE FUNCTION public.guard_assignment_monotonicity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.train_run_id <> OLD.train_run_id THEN
        RAISE EXCEPTION 'train-run assignment identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.assignment_generation < OLD.assignment_generation THEN
        RAISE EXCEPTION 'assignment generation cannot decrease'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.shard_id IS DISTINCT FROM OLD.shard_id
       AND NEW.assignment_generation <= OLD.assignment_generation THEN
        RAISE EXCEPTION 'ownership change requires a newer generation'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.availability_generation < OLD.availability_generation THEN
        RAISE EXCEPTION 'availability generation cannot decrease'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS train_run_shard_assignments_monotonic
    ON public.train_run_shard_assignments;
CREATE TRIGGER train_run_shard_assignments_monotonic
BEFORE UPDATE ON public.train_run_shard_assignments
FOR EACH ROW EXECUTE FUNCTION public.guard_assignment_monotonicity();

CREATE OR REPLACE FUNCTION public.guard_fence_monotonicity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.train_run_id <> OLD.train_run_id THEN
        RAISE EXCEPTION 'train-run fence identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.assignment_generation < OLD.assignment_generation THEN
        RAISE EXCEPTION 'fence generation cannot decrease'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS train_run_write_fences_monotonic
    ON public.train_run_write_fences;
CREATE TRIGGER train_run_write_fences_monotonic
BEFORE UPDATE ON public.train_run_write_fences
FOR EACH ROW EXECUTE FUNCTION public.guard_fence_monotonicity();

DROP TRIGGER IF EXISTS train_run_write_fences_monotonic
    ON booking_shard_0.train_run_write_fences;
CREATE TRIGGER train_run_write_fences_monotonic
BEFORE UPDATE ON booking_shard_0.train_run_write_fences
FOR EACH ROW EXECUTE FUNCTION public.guard_fence_monotonicity();

DROP TRIGGER IF EXISTS train_run_write_fences_monotonic
    ON booking_shard_1.train_run_write_fences;
CREATE TRIGGER train_run_write_fences_monotonic
BEFORE UPDATE ON booking_shard_1.train_run_write_fences
FOR EACH ROW EXECUTE FUNCTION public.guard_fence_monotonicity();

CREATE OR REPLACE FUNCTION public.guard_migration_state_transition()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    transition_allowed boolean := false;
BEGIN
    IF NEW.id <> OLD.id
       OR NEW.train_run_id <> OLD.train_run_id
       OR NEW.source_shard_id <> OLD.source_shard_id
       OR NEW.target_shard_id <> OLD.target_shard_id
       OR NEW.source_generation <> OLD.source_generation
       OR NEW.target_generation <> OLD.target_generation THEN
        RAISE EXCEPTION 'migration identity and ownership plan are immutable'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state IN ('completed', 'failed', 'rolled_back') THEN
        RAISE EXCEPTION 'terminal migration state is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;

    transition_allowed := CASE OLD.state
        WHEN 'planned' THEN NEW.state IN ('draining', 'failed', 'rolled_back')
        WHEN 'draining' THEN NEW.state IN ('copying', 'failed', 'rolled_back')
        WHEN 'copying' THEN NEW.state IN ('validating', 'failed', 'rolled_back')
        WHEN 'validating' THEN NEW.state IN ('cutover_ready', 'failed', 'rolled_back')
        WHEN 'cutover_ready' THEN NEW.state IN ('cutting_over', 'failed', 'rolled_back')
        WHEN 'cutting_over' THEN NEW.state IN ('rollback_window', 'failed')
        WHEN 'rollback_window' THEN NEW.state IN ('completed', 'failed', 'rolled_back')
        ELSE false
    END;

    IF NOT transition_allowed THEN
        RAISE EXCEPTION 'invalid migration state transition'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'rollback_window' AND NEW.state = 'rolled_back'
       AND NOT EXISTS (
           SELECT 1
           FROM public.train_run_generation_writes AS evidence
           WHERE evidence.train_run_id = OLD.train_run_id
             AND evidence.assignment_generation = OLD.target_generation
             AND evidence.migration_id = OLD.id
             AND evidence.successful_write_count = 0
       ) THEN
        RAISE EXCEPTION 'direct rollback requires locked zero target-write evidence'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS train_run_shard_migrations_transition
    ON public.train_run_shard_migrations;
CREATE TRIGGER train_run_shard_migrations_transition
BEFORE UPDATE ON public.train_run_shard_migrations
FOR EACH ROW EXECUTE FUNCTION public.guard_migration_state_transition();

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

CREATE OR REPLACE FUNCTION public.validate_train_run_fence_invariant()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    checked_train_run_id uuid;
BEGIN
    checked_train_run_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.train_run_id
        ELSE NEW.train_run_id
    END;
    PERFORM public.assert_train_run_fence_invariant(checked_train_run_id);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS train_run_shard_assignments_validate_fences
    ON public.train_run_shard_assignments;
CREATE CONSTRAINT TRIGGER train_run_shard_assignments_validate_fences
AFTER INSERT OR UPDATE OR DELETE ON public.train_run_shard_assignments
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.validate_train_run_fence_invariant();

DROP TRIGGER IF EXISTS train_run_write_fences_validate
    ON public.train_run_write_fences;
CREATE CONSTRAINT TRIGGER train_run_write_fences_validate
AFTER INSERT OR UPDATE OR DELETE ON public.train_run_write_fences
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.validate_train_run_fence_invariant();

DROP TRIGGER IF EXISTS train_run_write_fences_validate
    ON booking_shard_0.train_run_write_fences;
CREATE CONSTRAINT TRIGGER train_run_write_fences_validate
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_0.train_run_write_fences
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.validate_train_run_fence_invariant();

DROP TRIGGER IF EXISTS train_run_write_fences_validate
    ON booking_shard_1.train_run_write_fences;
CREATE CONSTRAINT TRIGGER train_run_write_fences_validate
AFTER INSERT OR UPDATE OR DELETE ON booking_shard_1.train_run_write_fences
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.validate_train_run_fence_invariant();

CREATE OR REPLACE FUNCTION public.bootstrap_train_run_shard_assignment()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    INSERT INTO public.train_run_shard_assignments (
        train_run_id, shard_id, assignment_generation, assignment_state,
        availability_generation
    ) VALUES (NEW.id, 'legacy', 1, 'stable', 1);

    INSERT INTO public.train_run_write_fences (
        train_run_id, assignment_generation, write_enabled
    ) VALUES (NEW.id, 1, true);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS train_runs_bootstrap_shard_assignment
    ON public.train_runs;
CREATE TRIGGER train_runs_bootstrap_shard_assignment
AFTER INSERT ON public.train_runs
FOR EACH ROW EXECUTE FUNCTION public.bootstrap_train_run_shard_assignment();

CREATE OR REPLACE FUNCTION public.legacy_migration_copy_allowed(
    checked_train_run_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    configured_migration_id uuid;
    allowed boolean := false;
BEGIN
    BEGIN
        configured_migration_id := nullif(
            current_setting('railway.booking_migration_id', true), ''
        )::uuid;
    EXCEPTION WHEN invalid_text_representation THEN
        RETURN false;
    END;

    IF configured_migration_id IS NULL THEN
        RETURN false;
    END IF;

    SELECT true
    INTO allowed
    FROM public.train_run_shard_migrations AS migration
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id = migration.train_run_id
     AND assignment.active_migration_id = migration.id
    JOIN public.train_run_write_fences AS fence
      ON fence.train_run_id = migration.train_run_id
    WHERE migration.id = configured_migration_id
      AND migration.train_run_id = checked_train_run_id
      AND migration.target_shard_id = 'legacy'
      AND migration.state IN ('copying', 'validating')
      AND assignment.shard_id = migration.source_shard_id
      AND NOT fence.write_enabled
    FOR UPDATE OF migration, assignment, fence;

    RETURN coalesce(allowed, false);
END;
$$;

CREATE OR REPLACE FUNCTION public.legacy_cleanup_allowed(
    checked_train_run_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    configured_migration_id uuid;
    allowed boolean := false;
BEGIN
    BEGIN
        configured_migration_id := nullif(
            current_setting('railway.booking_cleanup_migration_id', true), ''
        )::uuid;
    EXCEPTION WHEN invalid_text_representation THEN
        RETURN false;
    END;

    IF configured_migration_id IS NULL THEN
        RETURN false;
    END IF;

    SELECT true
    INTO allowed
    FROM public.train_run_shard_migrations AS migration
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id = migration.train_run_id
    JOIN public.train_run_write_fences AS fence
      ON fence.train_run_id = migration.train_run_id
    WHERE migration.id = configured_migration_id
      AND migration.train_run_id = checked_train_run_id
      AND migration.source_shard_id = 'legacy'
      AND migration.target_shard_id = assignment.shard_id
      AND migration.state = 'completed'
      AND migration.rollback_deadline_at <= clock_timestamp()
      AND assignment.shard_id <> 'legacy'
      AND NOT fence.write_enabled
    FOR UPDATE OF migration, assignment, fence;

    RETURN coalesce(allowed, false);
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

CREATE OR REPLACE FUNCTION public.guard_legacy_booking_write()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    old_train_run_id uuid;
    new_train_run_id uuid;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        IF TG_TABLE_NAME IN ('seat_inventory', 'reservations', 'reservation_seats') THEN
            old_train_run_id := OLD.train_run_id;
        ELSIF TG_TABLE_NAME = 'ticket_orders' THEN
            SELECT reservation.train_run_id
            INTO old_train_run_id
            FROM public.reservations AS reservation
            WHERE reservation.id = OLD.reservation_id;
        ELSIF TG_TABLE_NAME = 'tickets' THEN
            SELECT reservation.train_run_id
            INTO old_train_run_id
            FROM public.ticket_orders AS ticket_order
            JOIN public.reservations AS reservation
              ON reservation.id = ticket_order.reservation_id
            WHERE ticket_order.id = OLD.ticket_order_id;
        ELSIF TG_TABLE_NAME = 'idempotency_records' THEN
            old_train_run_id := OLD.train_run_id;
            IF old_train_run_id IS NULL
               AND OLD.resource_type = 'reservation'
               AND OLD.resource_id IS NOT NULL THEN
                SELECT reservation.train_run_id
                INTO old_train_run_id
                FROM public.reservations AS reservation
                WHERE reservation.id = OLD.resource_id;
            END IF;
        END IF;
    END IF;

    IF TG_OP <> 'DELETE' THEN
        IF TG_TABLE_NAME IN ('seat_inventory', 'reservations', 'reservation_seats') THEN
            new_train_run_id := NEW.train_run_id;
        ELSIF TG_TABLE_NAME = 'ticket_orders' THEN
            SELECT reservation.train_run_id
            INTO new_train_run_id
            FROM public.reservations AS reservation
            WHERE reservation.id = NEW.reservation_id;
        ELSIF TG_TABLE_NAME = 'tickets' THEN
            SELECT reservation.train_run_id
            INTO new_train_run_id
            FROM public.ticket_orders AS ticket_order
            JOIN public.reservations AS reservation
              ON reservation.id = ticket_order.reservation_id
            WHERE ticket_order.id = NEW.ticket_order_id;
        ELSIF TG_TABLE_NAME = 'idempotency_records' THEN
            new_train_run_id := NEW.train_run_id;
            IF new_train_run_id IS NULL
               AND NEW.resource_type = 'reservation'
               AND NEW.resource_id IS NOT NULL THEN
                SELECT reservation.train_run_id
                INTO new_train_run_id
                FROM public.reservations AS reservation
                WHERE reservation.id = NEW.resource_id;
            END IF;
        END IF;
    END IF;

    IF old_train_run_id IS NOT NULL
       AND NOT public.legacy_migration_copy_allowed(old_train_run_id)
       AND NOT public.legacy_cleanup_allowed(old_train_run_id) THEN
        PERFORM public.assert_legacy_train_run_writable(old_train_run_id);
    END IF;

    IF new_train_run_id IS DISTINCT FROM old_train_run_id
       AND new_train_run_id IS NOT NULL
       AND NOT public.legacy_migration_copy_allowed(new_train_run_id)
       AND NOT public.legacy_cleanup_allowed(new_train_run_id) THEN
        PERFORM public.assert_legacy_train_run_writable(new_train_run_id);
    END IF;

    IF old_train_run_id IS NULL AND new_train_run_id IS NULL
       AND TG_TABLE_NAME <> 'idempotency_records' THEN
        RAISE EXCEPTION 'booking write lacks a train-run ownership key'
            USING ERRCODE = '23514';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.seat_inventory;
CREATE TRIGGER legacy_booking_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.seat_inventory
FOR EACH ROW EXECUTE FUNCTION public.guard_legacy_booking_write();

DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.reservations;
CREATE TRIGGER legacy_booking_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.reservations
FOR EACH ROW EXECUTE FUNCTION public.guard_legacy_booking_write();

DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.reservation_seats;
CREATE TRIGGER legacy_booking_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.reservation_seats
FOR EACH ROW EXECUTE FUNCTION public.guard_legacy_booking_write();

DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.ticket_orders;
CREATE TRIGGER legacy_booking_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.ticket_orders
FOR EACH ROW EXECUTE FUNCTION public.guard_legacy_booking_write();

DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.tickets;
CREATE TRIGGER legacy_booking_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.tickets
FOR EACH ROW EXECUTE FUNCTION public.guard_legacy_booking_write();

DROP TRIGGER IF EXISTS legacy_booking_write_guard ON public.idempotency_records;
CREATE TRIGGER legacy_booking_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.idempotency_records
FOR EACH ROW EXECUTE FUNCTION public.guard_legacy_booking_write();

-- Compatibility maintenance keeps version-7 legacy writers safe during the
-- expand/drain interval. These triggers only observe the explicit legacy
-- assignment; migration-copy and cleanup transactions are excluded so a
-- partial target copy can never switch global locators or claims.
CREATE OR REPLACE FUNCTION public.populate_legacy_idempotency_train_run()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND NEW.id IS DISTINCT FROM OLD.id
       AND NEW.status = 'in_progress'
       AND NEW.resource_id IS NULL THEN
        -- Version-7 expiry reacquisition changes the local record ID but does
        -- not know the next command's train run until completion.
        NEW.train_run_id := NULL;
    END IF;

    IF NEW.train_run_id IS NULL
       AND NEW.resource_type = 'reservation'
       AND NEW.resource_id IS NOT NULL THEN
        SELECT reservation.train_run_id
        INTO NEW.train_run_id
        FROM public.reservations AS reservation
        WHERE reservation.id = NEW.resource_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS idempotency_records_populate_train_run
    ON public.idempotency_records;
CREATE TRIGGER idempotency_records_populate_train_run
BEFORE INSERT OR UPDATE ON public.idempotency_records
FOR EACH ROW EXECUTE FUNCTION public.populate_legacy_idempotency_train_run();

CREATE OR REPLACE FUNCTION public.sync_legacy_idempotency_key_claim()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    current_generation bigint;
    synchronized_claim_id uuid;
BEGIN
    IF NEW.train_run_id IS NOT NULL
       AND (
           public.legacy_migration_copy_allowed(NEW.train_run_id)
           OR public.legacy_cleanup_allowed(NEW.train_run_id)
       ) THEN
        RETURN NEW;
    END IF;

    IF NEW.train_run_id IS NULL THEN
        IF NEW.status <> 'in_progress' OR NEW.resource_id IS NOT NULL THEN
            RAISE EXCEPTION 'unresolved legacy idempotency claim must be in progress'
                USING ERRCODE = '23514';
        END IF;
        current_generation := 1;
    ELSE
        SELECT assignment.assignment_generation
        INTO current_generation
        FROM public.train_run_shard_assignments AS assignment
        WHERE assignment.train_run_id = NEW.train_run_id
          AND assignment.shard_id = 'legacy';
    END IF;

    IF current_generation IS NULL THEN
        RAISE EXCEPTION 'legacy idempotency completion lacks current ownership'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO public.booking_idempotency_key_claims (
        user_id, operation, key_hash, request_fingerprint, train_run_id, expires_at,
        created_at, updated_at
    ) VALUES (
        NEW.user_id, NEW.operation, NEW.key_hash, NEW.request_fingerprint,
        NEW.train_run_id, NEW.expires_at, NEW.created_at, NEW.updated_at
    )
    ON CONFLICT (user_id, operation, key_hash) DO UPDATE
    SET request_fingerprint = EXCLUDED.request_fingerprint,
        train_run_id = EXCLUDED.train_run_id,
        expires_at = EXCLUDED.expires_at,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at
    WHERE public.booking_idempotency_key_claims.expires_at <= clock_timestamp()
       OR (
            public.booking_idempotency_key_claims.request_fingerprint
                = EXCLUDED.request_fingerprint
            AND public.booking_idempotency_key_claims.train_run_id
                IS NOT DISTINCT FROM EXCLUDED.train_run_id
            AND public.booking_idempotency_key_claims.expires_at
                = EXCLUDED.expires_at
       )
       OR (
            -- A version-7 in-progress row receives its train-run integrity
            -- reference only when completion resolves its reservation.
            public.booking_idempotency_key_claims.request_fingerprint
                = EXCLUDED.request_fingerprint
            AND public.booking_idempotency_key_claims.train_run_id IS NULL
            AND EXCLUDED.train_run_id IS NOT NULL
            AND public.booking_idempotency_key_claims.expires_at
                = EXCLUDED.expires_at
       )
    RETURNING id INTO synchronized_claim_id;

    IF synchronized_claim_id IS NULL THEN
        RAISE EXCEPTION 'idempotency key claim conflict'
            USING ERRCODE = '23505';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS idempotency_records_sync_key_claim
    ON public.idempotency_records;
CREATE TRIGGER idempotency_records_sync_key_claim
AFTER INSERT OR UPDATE ON public.idempotency_records
FOR EACH ROW EXECUTE FUNCTION public.sync_legacy_idempotency_key_claim();

CREATE OR REPLACE FUNCTION public.sync_legacy_reservation_locator()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    current_generation bigint;
BEGIN
    IF public.legacy_migration_copy_allowed(NEW.train_run_id)
       OR public.legacy_cleanup_allowed(NEW.train_run_id) THEN
        RETURN NEW;
    END IF;

    SELECT assignment.assignment_generation
    INTO current_generation
    FROM public.train_run_shard_assignments AS assignment
    WHERE assignment.train_run_id = NEW.train_run_id
      AND assignment.shard_id = 'legacy';

    IF current_generation IS NULL THEN
        RAISE EXCEPTION 'legacy reservation lacks current ownership'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO public.reservation_shard_locators (
        reservation_id, train_run_id, shard_id, assignment_generation,
        owner_user_id, created_at, updated_at
    ) VALUES (
        NEW.id, NEW.train_run_id, 'legacy', current_generation,
        NEW.user_id, NEW.created_at, NEW.updated_at
    )
    ON CONFLICT (reservation_id) DO UPDATE
    SET train_run_id = EXCLUDED.train_run_id,
        shard_id = EXCLUDED.shard_id,
        assignment_generation = EXCLUDED.assignment_generation,
        owner_user_id = EXCLUDED.owner_user_id,
        updated_at = EXCLUDED.updated_at;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS reservations_sync_legacy_locator
    ON public.reservations;
CREATE TRIGGER reservations_sync_legacy_locator
AFTER INSERT OR UPDATE OF user_id, train_run_id ON public.reservations
FOR EACH ROW EXECUTE FUNCTION public.sync_legacy_reservation_locator();

CREATE OR REPLACE FUNCTION public.sync_legacy_reservation_quota_claim()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    affected_reservation_id uuid;
    reservation_row public.reservations%ROWTYPE;
    current_passenger_count integer;
BEGIN
    IF TG_TABLE_NAME = 'reservations' THEN
        affected_reservation_id := NEW.id;
    ELSIF TG_OP = 'DELETE' THEN
        affected_reservation_id := OLD.reservation_id;
    ELSE
        affected_reservation_id := NEW.reservation_id;
    END IF;

    SELECT *
    INTO reservation_row
    FROM public.reservations
    WHERE id = affected_reservation_id;

    IF reservation_row.id IS NULL
       OR public.legacy_migration_copy_allowed(reservation_row.train_run_id)
       OR public.legacy_cleanup_allowed(reservation_row.train_run_id) THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    SELECT count(*)::integer
    INTO current_passenger_count
    FROM public.reservation_seats
    WHERE reservation_id = affected_reservation_id;

    -- Reservation insertion precedes its seat rows in the legacy transaction.
    IF current_passenger_count = 0 AND TG_TABLE_NAME = 'reservations'
       AND TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;

    IF current_passenger_count <= 0 THEN
        RAISE EXCEPTION 'reservation quota claim requires passenger-seat state'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO public.reservation_quota_claims (
        reservation_id, user_id, train_run_id, passenger_count, active,
        closed_at, created_at, updated_at
    ) VALUES (
        reservation_row.id,
        reservation_row.user_id,
        reservation_row.train_run_id,
        current_passenger_count,
        reservation_row.status = 'held',
        CASE
            WHEN reservation_row.status = 'held' THEN NULL
            ELSE reservation_row.updated_at
        END,
        reservation_row.created_at,
        reservation_row.updated_at
    )
    ON CONFLICT (reservation_id) DO UPDATE
    SET user_id = EXCLUDED.user_id,
        train_run_id = EXCLUDED.train_run_id,
        passenger_count = EXCLUDED.passenger_count,
        active = EXCLUDED.active,
        closed_at = EXCLUDED.closed_at,
        updated_at = EXCLUDED.updated_at;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS reservations_sync_legacy_quota_claim
    ON public.reservations;
CREATE TRIGGER reservations_sync_legacy_quota_claim
AFTER INSERT OR UPDATE OF user_id, train_run_id, status
ON public.reservations
FOR EACH ROW EXECUTE FUNCTION public.sync_legacy_reservation_quota_claim();

DROP TRIGGER IF EXISTS reservation_seats_sync_legacy_quota_claim
    ON public.reservation_seats;
CREATE TRIGGER reservation_seats_sync_legacy_quota_claim
AFTER INSERT OR UPDATE OR DELETE ON public.reservation_seats
FOR EACH ROW EXECUTE FUNCTION public.sync_legacy_reservation_quota_claim();

CREATE OR REPLACE FUNCTION public.sync_legacy_ticket_order_locator()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    reservation_train_run_id uuid;
    current_generation bigint;
BEGIN
    SELECT reservation.train_run_id, assignment.assignment_generation
    INTO reservation_train_run_id, current_generation
    FROM public.reservations AS reservation
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id = reservation.train_run_id
     AND assignment.shard_id = 'legacy'
    WHERE reservation.id = NEW.reservation_id;

    IF reservation_train_run_id IS NULL THEN
        RAISE EXCEPTION 'legacy ticket order lacks current ownership'
            USING ERRCODE = '55000';
    END IF;

    IF public.legacy_migration_copy_allowed(reservation_train_run_id)
       OR public.legacy_cleanup_allowed(reservation_train_run_id) THEN
        RETURN NEW;
    END IF;

    INSERT INTO public.ticket_order_shard_locators (
        ticket_order_id, reservation_id, train_run_id, shard_id,
        assignment_generation, owner_user_id, status, total_amount_minor,
        currency, created_at, updated_at
    ) VALUES (
        NEW.id, NEW.reservation_id, reservation_train_run_id, 'legacy',
        current_generation, NEW.user_id, NEW.status, NEW.total_amount_minor,
        NEW.currency, NEW.created_at, NEW.updated_at
    )
    ON CONFLICT (ticket_order_id) DO UPDATE
    SET reservation_id = EXCLUDED.reservation_id,
        train_run_id = EXCLUDED.train_run_id,
        shard_id = EXCLUDED.shard_id,
        assignment_generation = EXCLUDED.assignment_generation,
        owner_user_id = EXCLUDED.owner_user_id,
        status = EXCLUDED.status,
        total_amount_minor = EXCLUDED.total_amount_minor,
        currency = EXCLUDED.currency,
        updated_at = EXCLUDED.updated_at;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ticket_orders_sync_legacy_locator
    ON public.ticket_orders;
CREATE TRIGGER ticket_orders_sync_legacy_locator
AFTER INSERT OR UPDATE OF reservation_id, user_id, status,
    total_amount_minor, currency
ON public.ticket_orders
FOR EACH ROW EXECUTE FUNCTION public.sync_legacy_ticket_order_locator();

CREATE OR REPLACE FUNCTION public.sync_legacy_ticket_locator()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    order_row public.ticket_orders%ROWTYPE;
    reservation_train_run_id uuid;
    current_generation bigint;
BEGIN
    SELECT ticket_order.*
    INTO order_row
    FROM public.ticket_orders AS ticket_order
    WHERE ticket_order.id = NEW.ticket_order_id;

    SELECT reservation.train_run_id, assignment.assignment_generation
    INTO reservation_train_run_id, current_generation
    FROM public.reservations AS reservation
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id = reservation.train_run_id
     AND assignment.shard_id = 'legacy'
    WHERE reservation.id = order_row.reservation_id;

    IF reservation_train_run_id IS NULL THEN
        RAISE EXCEPTION 'legacy ticket lacks current ownership'
            USING ERRCODE = '55000';
    END IF;

    IF public.legacy_migration_copy_allowed(reservation_train_run_id)
       OR public.legacy_cleanup_allowed(reservation_train_run_id) THEN
        RETURN NEW;
    END IF;

    INSERT INTO public.ticket_shard_locators (
        ticket_id, ticket_order_id, reservation_id, train_run_id, shard_id,
        assignment_generation, owner_user_id, created_at, updated_at
    ) VALUES (
        NEW.id, NEW.ticket_order_id, order_row.reservation_id,
        reservation_train_run_id, 'legacy', current_generation,
        order_row.user_id, NEW.created_at, NEW.updated_at
    )
    ON CONFLICT (ticket_id) DO UPDATE
    SET ticket_order_id = EXCLUDED.ticket_order_id,
        reservation_id = EXCLUDED.reservation_id,
        train_run_id = EXCLUDED.train_run_id,
        shard_id = EXCLUDED.shard_id,
        assignment_generation = EXCLUDED.assignment_generation,
        owner_user_id = EXCLUDED.owner_user_id,
        updated_at = EXCLUDED.updated_at;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tickets_sync_legacy_locator ON public.tickets;
CREATE TRIGGER tickets_sync_legacy_locator
AFTER INSERT OR UPDATE OF ticket_order_id ON public.tickets
FOR EACH ROW EXECUTE FUNCTION public.sync_legacy_ticket_locator();

COMMIT;
