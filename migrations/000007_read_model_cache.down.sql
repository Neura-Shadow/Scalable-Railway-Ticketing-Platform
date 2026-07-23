BEGIN;

-- Version 6 cannot represent Milestone 3 Railway Offering events. A schema
-- rollback therefore discards only that derived read-model delivery intent and
-- preserves every pre-existing reservation, ticket, cancellation, and hot
-- policy event.
DELETE FROM outbox_events
WHERE aggregate_type IN ('station', 'route', 'train', 'coach', 'seat', 'fare')
   OR event_type IN (
    'trainrun.created',
    'trainrun.updated',
    'station.created',
    'station.updated',
    'station.disabled',
    'route.created',
    'route.updated',
    'route.disabled',
    'train.updated',
    'coach.updated',
    'seat.updated',
    'fare.created',
    'fare.updated',
    'fare.disabled'
);

DROP INDEX outbox_events_read_model_replay_idx;
DROP INDEX outbox_events_read_model_lag_idx;

ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_event_pair_check,
    DROP CONSTRAINT outbox_events_aggregate_type_check,
    ADD CONSTRAINT outbox_events_aggregate_type_check
        CHECK (aggregate_type IN (
            'reservation',
            'ticket',
            'train_run',
            'hot_train_policy'
        )),
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

DROP TABLE read_model_projection_state;
DROP TABLE read_model_event_progress;
DROP TABLE read_model_event_receipts;
DROP TABLE train_run_journey_read_model;

COMMIT;
