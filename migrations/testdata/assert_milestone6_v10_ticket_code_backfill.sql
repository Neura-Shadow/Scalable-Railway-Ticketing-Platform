DO $assert_populated_ticket_code_backfill$
DECLARE
    existing_code text;
    existing_id uuid;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.ticket_shard_locators AS locator
        LEFT JOIN public.ticket_code_directory AS directory
          ON directory.ticket_id=locator.ticket_id
        WHERE directory.ticket_id IS NULL
    ) THEN
        RAISE EXCEPTION 'a populated ticket locator was not globally claimed';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.ticket_shard_locators AS locator
        JOIN public.ticket_code_directory AS directory ON directory.ticket_id=locator.ticket_id
        LEFT JOIN public.tickets AS legacy
          ON locator.shard_id='legacy' AND legacy.id=locator.ticket_id
        LEFT JOIN booking_shard_0.tickets AS shard_zero
          ON locator.shard_id='shard-0' AND shard_zero.id=locator.ticket_id
        LEFT JOIN booking_shard_1.tickets AS shard_one
          ON locator.shard_id='shard-1' AND shard_one.id=locator.ticket_id
        WHERE directory.ticket_code IS DISTINCT FROM
              COALESCE(legacy.ticket_code,shard_zero.ticket_code,shard_one.ticket_code)
    ) THEN
        RAISE EXCEPTION 'a populated ticket code claim differs from its authoritative row';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.ticket_code_directory
        WHERE ticket_code='M4.legacy/ticket?0001'
          AND ticket_id='d0000000-0000-4000-8000-000000000003'
    ) THEN
        RAISE EXCEPTION 'a previously valid non-M6 ticket code did not survive the v9-to-v10 backfill';
    END IF;

    IF (SELECT count(*) FROM public.ticket_code_claim_readiness
        WHERE singleton AND state='ready' AND verified_at IS NOT NULL
          AND claimed_ticket_count=(SELECT count(*) FROM public.ticket_shard_locators)) <> 1 THEN
        RAISE EXCEPTION 'populated local ticket-code rollout did not become ready';
    END IF;

    SELECT ticket_code,ticket_id INTO existing_code,existing_id
    FROM public.ticket_code_directory ORDER BY ticket_code LIMIT 1;
    IF existing_id IS NULL THEN
        RAISE EXCEPTION 'populated fixture did not exercise ticket-code backfill';
    END IF;

    BEGIN
        INSERT INTO public.ticket_code_directory(ticket_code,ticket_id)
        VALUES(existing_code,'ffffffff-ffff-4fff-8fff-ffffffffffff');
        RAISE EXCEPTION 'duplicate code was accepted for another ticket';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO public.ticket_code_directory(ticket_code,ticket_id)
        VALUES('distinct_code_for_collision_probe_001',existing_id);
        RAISE EXCEPTION 'duplicate ticket ID was accepted under another code';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    BEGIN
        UPDATE public.ticket_code_directory SET created_at=created_at+interval '1 second'
        WHERE ticket_id=existing_id;
        RAISE EXCEPTION 'ticket-code tombstone was mutable';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$assert_populated_ticket_code_backfill$;

-- A planned M6 identity is claimed before it has a locator. This is an
-- intentional immutable tombstone and proves the directory does not rely on
-- post-issuance locator creation.
INSERT INTO public.ticket_code_directory(ticket_code,ticket_id)
VALUES('planned_ticket_identity_probe_001','eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee');

DO $assert_preclaim_tombstone$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.ticket_code_directory
        WHERE ticket_code='planned_ticket_identity_probe_001'
          AND ticket_id='eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'
    ) OR EXISTS (
        SELECT 1 FROM public.ticket_shard_locators
        WHERE ticket_id='eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'
    ) THEN
        RAISE EXCEPTION 'pre-issuance ticket identity claim was not retained as a tombstone';
    END IF;
END;
$assert_preclaim_tombstone$;

-- A preclaimed M6 locator must leave the rollout ready, while a locator minted
-- by either supported legacy confirmation path must synchronously revoke
-- readiness until an operator verifies and claims its authoritative code.
INSERT INTO public.ticket_shard_locators(
    ticket_id,ticket_order_id,reservation_id,train_run_id,shard_id,
    assignment_generation,owner_user_id,created_at,updated_at
)
SELECT 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',ticket_order_id,reservation_id,
       train_run_id,shard_id,assignment_generation,owner_user_id,
       clock_timestamp(),clock_timestamp()
FROM public.ticket_shard_locators
ORDER BY ticket_id
LIMIT 1;

DO $assert_preclaimed_locator_keeps_readiness$
BEGIN
    IF (SELECT count(*) FROM public.ticket_code_claim_readiness
        WHERE singleton AND state='ready' AND verified_at IS NOT NULL) <> 1 THEN
        RAISE EXCEPTION 'preclaimed M6 locator incorrectly revoked rollout readiness';
    END IF;
END;
$assert_preclaimed_locator_keeps_readiness$;

INSERT INTO public.ticket_shard_locators(
    ticket_id,ticket_order_id,reservation_id,train_run_id,shard_id,
    assignment_generation,owner_user_id,created_at,updated_at
)
SELECT 'ffffffff-ffff-4fff-8fff-fffffffffff0',ticket_order_id,reservation_id,
       train_run_id,shard_id,assignment_generation,owner_user_id,
       clock_timestamp(),clock_timestamp()
FROM public.ticket_shard_locators
ORDER BY ticket_id
LIMIT 1;

DO $assert_unclaimed_locator_revokes_readiness$
BEGIN
    IF (SELECT count(*) FROM public.ticket_code_claim_readiness
        WHERE singleton AND state='pending' AND verified_at IS NULL) <> 1 THEN
        RAISE EXCEPTION 'unclaimed locator did not synchronously revoke rollout readiness';
    END IF;
END;
$assert_unclaimed_locator_revokes_readiness$;

DELETE FROM public.ticket_shard_locators
WHERE ticket_id IN (
    'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',
    'ffffffff-ffff-4fff-8fff-fffffffffff0'
);
UPDATE public.ticket_code_claim_readiness
SET state='ready',
    claimed_ticket_count=(SELECT count(*) FROM public.ticket_code_directory),
    verified_at=clock_timestamp()
WHERE singleton;
