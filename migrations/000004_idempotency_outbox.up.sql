BEGIN;

CREATE TABLE idempotency_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    operation text NOT NULL
        CHECK (operation IN ('reservation.create', 'reservation.confirm', 'reservation.cancel')),
    key_hash bytea NOT NULL CHECK (octet_length(key_hash) = 32),
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    status text NOT NULL DEFAULT 'in_progress'
        CHECK (status IN ('in_progress', 'completed')),
    resource_type text,
    resource_id uuid,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (user_id, operation, key_hash),
    CHECK (expires_at > created_at),
    CHECK (
        (status = 'in_progress' AND resource_type IS NULL AND resource_id IS NULL)
        OR
        (status = 'completed' AND resource_type IS NOT NULL AND resource_id IS NOT NULL)
    ),
    CHECK (resource_type IS NULL OR resource_type IN ('reservation'))
);

CREATE INDEX idempotency_records_expiry_idx
    ON idempotency_records (expires_at, id);
CREATE INDEX idempotency_records_resource_idx
    ON idempotency_records (resource_type, resource_id)
    WHERE status = 'completed';

CREATE TRIGGER idempotency_records_set_updated_at
BEFORE UPDATE ON idempotency_records
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type text NOT NULL
        CHECK (aggregate_type IN ('reservation', 'ticket', 'train_run')),
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL
        CHECK (event_type IN (
            'reservation.held',
            'reservation.confirmed',
            'reservation.expired',
            'reservation.cancelled',
            'ticket.created',
            'trainrun.cancelled'
        )),
    event_version integer NOT NULL DEFAULT 1 CHECK (event_version > 0),
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'published', 'dead_letter')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    locked_at timestamptz,
    locked_by text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (octet_length(payload::text) <= 65536),
    CHECK (locked_by IS NULL OR length(locked_by) BETWEEN 1 AND 128),
    CHECK (
        (status = 'processing' AND locked_at IS NOT NULL AND locked_by IS NOT NULL)
        OR
        (status <> 'processing')
    ),
    CHECK (published_at IS NULL OR status = 'published')
);

CREATE INDEX outbox_events_pending_idx
    ON outbox_events (next_attempt_at, created_at, id)
    WHERE status = 'pending';
CREATE INDEX outbox_events_processing_stale_idx
    ON outbox_events (locked_at, id)
    WHERE status = 'processing';
CREATE INDEX outbox_events_aggregate_idx
    ON outbox_events (aggregate_type, aggregate_id, created_at, id);

COMMIT;
