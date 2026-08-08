BEGIN;

SELECT pg_advisory_xact_lock(804230010);

-- Version 9 cannot represent any payment orchestration, provider operation,
-- signed webhook, reconciliation, or manual-review evidence. Refuse the
-- destructive downgrade when even a nonterminal row exists: an apparently
-- pending/uncertain row can still correspond to an external financial effect.
DO $m6_down_preflight$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.booking_shards
        WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
          AND (
              storage_kind <> 'postgres'
              OR schema_version <> 2
              OR enabled
              OR write_enabled
              OR state <> 'disabled'
          )
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while a physical shard is enabled or has an unexpected schema contract'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_intents) THEN
        RAISE EXCEPTION 'cannot downgrade while payment intent evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_sagas) THEN
        RAISE EXCEPTION 'cannot downgrade while payment saga evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_operations) THEN
        RAISE EXCEPTION 'cannot downgrade while provider operation evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_webhook_inbox) THEN
        RAISE EXCEPTION 'cannot downgrade while verified webhook evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_provider_event_conflicts) THEN
        RAISE EXCEPTION 'cannot downgrade while provider event conflict evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_reconciliation_checkpoints) THEN
        RAISE EXCEPTION 'cannot downgrade while payment reconciliation evidence is retained'
            USING ERRCODE = '55000';
    END IF;

    IF EXISTS (SELECT 1 FROM public.payment_manual_review_cases) THEN
        RAISE EXCEPTION 'cannot downgrade while payment manual-review evidence is retained'
            USING ERRCODE = '55000';
    END IF;
END
$m6_down_preflight$;

DROP TABLE public.payment_manual_review_cases;
DROP TABLE public.payment_reconciliation_checkpoints;
DROP TABLE public.payment_provider_event_conflicts;
DROP TABLE public.payment_webhook_inbox;
DROP TABLE public.payment_operations;
DROP TABLE public.payment_sagas;
DROP TABLE public.payment_intents;

DROP FUNCTION public.guard_payment_manual_review_case_row();
DROP FUNCTION public.guard_payment_reconciliation_checkpoint_row();
DROP FUNCTION public.guard_payment_provider_event_conflict_row();
DROP FUNCTION public.guard_payment_webhook_inbox_row();
DROP FUNCTION public.guard_payment_financial_settlement();
DROP FUNCTION public.guard_payment_operation_row();
DROP FUNCTION public.guard_payment_saga_row();
DROP FUNCTION public.guard_payment_intent_row();

UPDATE public.booking_shards
SET schema_version = 1
WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
  AND storage_kind = 'postgres'
  AND schema_version = 2;

COMMIT;
