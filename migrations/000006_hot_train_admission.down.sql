BEGIN;

-- A rollback intentionally removes feature-owned audit intent together with
-- the feature table, because version-4's outbox constraints cannot represent
-- these events.
DELETE FROM outbox_events
WHERE aggregate_type = 'hot_train_policy';

ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_aggregate_type_check,
    ADD CONSTRAINT outbox_events_aggregate_type_check
        CHECK (aggregate_type IN ('reservation', 'ticket', 'train_run')),
    DROP CONSTRAINT outbox_events_event_type_check,
    ADD CONSTRAINT outbox_events_event_type_check
        CHECK (event_type IN (
            'reservation.held',
            'reservation.confirmed',
            'reservation.expired',
            'reservation.cancelled',
            'ticket.created',
            'trainrun.cancelled'
        ));

DROP INDEX reservation_seats_reservation_id_idx;
DROP INDEX reservations_held_user_train_run_idx;

DROP TRIGGER hot_train_policies_set_updated_at ON hot_train_policies;
DROP TABLE hot_train_policies;

COMMIT;
