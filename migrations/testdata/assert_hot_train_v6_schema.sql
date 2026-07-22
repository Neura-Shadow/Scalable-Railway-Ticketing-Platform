DO $assert$
DECLARE
    feature_constraint record;
    feature_index record;
    actual_definition text;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM schema_migrations
        WHERE version = 6
          AND NOT dirty
    ) THEN
        RAISE EXCEPTION 'expected clean migration version 6';
    END IF;

    IF to_regclass('public.hot_train_policies') IS NULL THEN
        RAISE EXCEPTION 'hot_train_policies must exist at version 6';
    END IF;

    IF (
        SELECT count(*)
        FROM pg_constraint
        WHERE conrelid = 'public.hot_train_policies'::regclass
    ) <> 14 THEN
        RAISE EXCEPTION 'hot_train_policies must have exactly 14 declared constraints';
    END IF;

    FOR feature_constraint IN
        SELECT *
        FROM (
            VALUES
                ('hot_train_policies_pkey', 'p',
                    'PRIMARY KEY (id)'),
                ('hot_train_policies_train_run_id_seat_class_key', 'u',
                    'UNIQUE (train_run_id, seat_class)'),
                ('hot_train_policies_train_run_id_fkey', 'f',
                    'FOREIGN KEY (train_run_id) REFERENCES train_runs(id) ON DELETE RESTRICT'),
                ('hot_train_policies_seat_class_check', 'c',
                    'CHECK ((seat_class = ANY (ARRAY[''standard''::text, ''business''::text, ''first''::text])))'),
                ('hot_train_policies_version_check', 'c',
                    'CHECK ((version >= 1))'),
                ('hot_train_policies_max_queue_size_check', 'c',
                    'CHECK (((max_queue_size >= 1) AND (max_queue_size <= 100000)))'),
                ('hot_train_policies_admission_rate_per_second_check', 'c',
                    'CHECK (((admission_rate_per_second >= 1) AND (admission_rate_per_second <= 10000)))'),
                ('hot_train_policies_max_inflight_admissions_check', 'c',
                    'CHECK (((max_inflight_admissions >= 1) AND (max_inflight_admissions <= 10000)))'),
                ('hot_train_policies_admission_token_ttl_seconds_check', 'c',
                    'CHECK (((admission_token_ttl_seconds >= 6) AND (admission_token_ttl_seconds <= 900)))'),
                ('hot_train_policies_processing_lease_seconds_check', 'c',
                    'CHECK (((processing_lease_seconds >= 5) AND (processing_lease_seconds <= 120)))'),
                ('hot_train_policies_queue_entry_ttl_seconds_check', 'c',
                    'CHECK (((queue_entry_ttl_seconds >= 60) AND (queue_entry_ttl_seconds <= 86400)))'),
                ('hot_train_policies_check', 'c',
                    'CHECK ((processing_lease_seconds < admission_token_ttl_seconds))'),
                ('hot_train_policies_check1', 'c',
                    'CHECK ((queue_entry_ttl_seconds >= (admission_token_ttl_seconds + processing_lease_seconds)))'),
                ('hot_train_policies_check2', 'c',
                    'CHECK (((redis_initialized_version IS NULL) OR ((redis_initialized_version >= 1) AND (redis_initialized_version <= version))))')
        ) AS expected(name, type, definition)
    LOOP
        SELECT pg_get_constraintdef(oid)
        INTO actual_definition
        FROM pg_constraint
        WHERE conrelid = 'public.hot_train_policies'::regclass
          AND conname = feature_constraint.name
          AND contype = feature_constraint.type::"char"
          AND convalidated;

        IF actual_definition IS DISTINCT FROM feature_constraint.definition THEN
            RAISE EXCEPTION
                'hot_train_policies constraint % mismatch: got %, expected %',
                feature_constraint.name,
                actual_definition,
                feature_constraint.definition;
        END IF;
    END LOOP;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgrelid = 'public.hot_train_policies'::regclass
          AND tgname = 'hot_train_policies_set_updated_at'
          AND NOT tgisinternal
          AND tgenabled = 'O'
          AND pg_get_triggerdef(oid) =
              'CREATE TRIGGER hot_train_policies_set_updated_at BEFORE UPDATE ON public.hot_train_policies FOR EACH ROW EXECUTE FUNCTION set_updated_at()'
    ) THEN
        RAISE EXCEPTION 'hot_train_policies updated-at trigger is missing or misconfigured';
    END IF;

    FOR feature_index IN
        SELECT *
        FROM (
            VALUES
                ('public.hot_train_policies_enabled_lookup_idx',
                    'CREATE INDEX hot_train_policies_enabled_lookup_idx ON public.hot_train_policies USING btree (train_run_id, seat_class, version) WHERE enabled'),
                ('public.reservations_held_user_train_run_idx',
                    'CREATE INDEX reservations_held_user_train_run_idx ON public.reservations USING btree (user_id, train_run_id) WHERE (status = ''held''::text)'),
                ('public.reservation_seats_reservation_id_idx',
                    'CREATE INDEX reservation_seats_reservation_id_idx ON public.reservation_seats USING btree (reservation_id)')
        ) AS expected(name, definition)
    LOOP
        SELECT pg_get_indexdef(indexrelid)
        INTO actual_definition
        FROM pg_index
        WHERE indexrelid = to_regclass(feature_index.name)
          AND indisvalid
          AND indisready;

        IF actual_definition IS DISTINCT FROM feature_index.definition THEN
            RAISE EXCEPTION
                'Milestone 2 index % mismatch: got %, expected %',
                feature_index.name,
                actual_definition,
                feature_index.definition;
        END IF;
    END LOOP;

    SELECT pg_get_expr(conbin, conrelid)
    INTO actual_definition
    FROM pg_constraint
    WHERE conrelid = 'public.outbox_events'::regclass
      AND conname = 'outbox_events_aggregate_type_check'
      AND contype = 'c'
      AND convalidated;
    IF actual_definition IS DISTINCT FROM
        '(aggregate_type = ANY (ARRAY[''reservation''::text, ''ticket''::text, ''train_run''::text, ''hot_train_policy''::text]))'
    THEN
        RAISE EXCEPTION
            'version-6 aggregate constraint mismatch: got %',
            actual_definition;
    END IF;

    SELECT pg_get_expr(conbin, conrelid)
    INTO actual_definition
    FROM pg_constraint
    WHERE conrelid = 'public.outbox_events'::regclass
      AND conname = 'outbox_events_event_type_check'
      AND contype = 'c'
      AND convalidated;
    IF actual_definition IS DISTINCT FROM
        '(event_type = ANY (ARRAY[''reservation.held''::text, ''reservation.confirmed''::text, ''reservation.expired''::text, ''reservation.cancelled''::text, ''ticket.created''::text, ''trainrun.cancelled''::text, ''hot_train_policy.created''::text, ''hot_train_policy.updated''::text, ''hot_train_policy.disabled''::text]))'
    THEN
        RAISE EXCEPTION
            'version-6 event constraint mismatch: got %',
            actual_definition;
    END IF;
END;
$assert$;
