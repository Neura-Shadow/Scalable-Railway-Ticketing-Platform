DO $assert$
DECLARE
    feature_index text;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM schema_migrations
        WHERE version = 5
          AND NOT dirty
    ) THEN
        RAISE EXCEPTION 'expected clean migration version 5';
    END IF;

    IF to_regclass('public.hot_train_policies') IS NOT NULL THEN
        RAISE EXCEPTION 'hot_train_policies must not exist at version 5';
    END IF;

    FOREACH feature_index IN ARRAY ARRAY[
        'public.hot_train_policies_enabled_lookup_idx',
        'public.reservations_held_user_train_run_idx',
        'public.reservation_seats_reservation_id_idx'
    ] LOOP
        IF to_regclass(feature_index) IS NOT NULL THEN
            RAISE EXCEPTION 'Milestone 2 index % must not exist at version 5',
                feature_index;
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.outbox_events'::regclass
          AND conname = 'outbox_events_aggregate_type_check'
          AND position('hot_train_policy' IN pg_get_constraintdef(oid)) > 0
    ) THEN
        RAISE EXCEPTION 'version-5 aggregate constraint still admits hot-train policy events';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.outbox_events'::regclass
          AND conname = 'outbox_events_event_type_check'
          AND position('hot_train_policy.' IN pg_get_constraintdef(oid)) > 0
    ) THEN
        RAISE EXCEPTION 'version-5 event constraint still admits hot-train policy events';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM outbox_events
        WHERE id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
           OR aggregate_type = 'hot_train_policy'
           OR event_type LIKE 'hot_train_policy.%'
    ) THEN
        RAISE EXCEPTION 'version-5 rollback retained hot-train policy outbox intent';
    END IF;
END;
$assert$;
