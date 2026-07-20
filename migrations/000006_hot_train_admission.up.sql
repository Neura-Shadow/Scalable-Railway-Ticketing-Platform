BEGIN;

CREATE TABLE hot_train_policies (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    train_run_id uuid NOT NULL REFERENCES train_runs(id) ON DELETE RESTRICT,
    seat_class text NOT NULL
        CHECK (seat_class IN ('standard', 'business', 'first')),
    enabled boolean NOT NULL DEFAULT true,
    version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    redis_initialized_version bigint,
    max_queue_size integer NOT NULL
        CHECK (max_queue_size BETWEEN 1 AND 100000),
    admission_rate_per_second integer NOT NULL
        CHECK (admission_rate_per_second BETWEEN 1 AND 10000),
    max_inflight_admissions integer NOT NULL
        CHECK (max_inflight_admissions BETWEEN 1 AND 10000),
    admission_token_ttl_seconds integer NOT NULL
        CHECK (admission_token_ttl_seconds BETWEEN 6 AND 900),
    processing_lease_seconds integer NOT NULL
        CHECK (processing_lease_seconds BETWEEN 5 AND 120),
    queue_entry_ttl_seconds integer NOT NULL
        CHECK (queue_entry_ttl_seconds BETWEEN 60 AND 86400),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (train_run_id, seat_class),
    CHECK (processing_lease_seconds < admission_token_ttl_seconds),
    CHECK (
        queue_entry_ttl_seconds
        >= admission_token_ttl_seconds + processing_lease_seconds
    ),
    CHECK (
        redis_initialized_version IS NULL
        OR redis_initialized_version BETWEEN 1 AND version
    )
);

CREATE INDEX hot_train_policies_enabled_lookup_idx
    ON hot_train_policies (train_run_id, seat_class, version)
    WHERE enabled;

CREATE TRIGGER hot_train_policies_set_updated_at
BEFORE UPDATE ON hot_train_policies
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- The counters are intentionally derived from durable held rows under the
-- Booking per-user advisory transaction lock. They are not quota counters.
CREATE INDEX reservations_held_user_train_run_idx
    ON reservations (user_id, train_run_id)
    WHERE status = 'held';
CREATE INDEX reservation_seats_reservation_id_idx
    ON reservation_seats (reservation_id);

ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_aggregate_type_check,
    ADD CONSTRAINT outbox_events_aggregate_type_check
        CHECK (aggregate_type IN ('reservation', 'ticket', 'train_run', 'hot_train_policy')),
    DROP CONSTRAINT outbox_events_event_type_check,
    ADD CONSTRAINT outbox_events_event_type_check
        CHECK (event_type IN (
            'reservation.held',
            'reservation.confirmed',
            'reservation.expired',
            'reservation.cancelled',
            'ticket.created',
            'trainrun.cancelled',
            'hot_train_policy.created',
            'hot_train_policy.updated',
            'hot_train_policy.disabled'
        ));

COMMIT;
