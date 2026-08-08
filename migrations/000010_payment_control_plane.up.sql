BEGIN;

SELECT pg_advisory_xact_lock(804230010);

-- Both fixed physical shards must run booking-shard schema v2 before payment
-- routing is enabled. The catalog stores only the required schema contract;
-- connection details remain process configuration.
DO $m6_physical_schema_preflight$
BEGIN
    IF (
        SELECT count(*)
        FROM public.booking_shards
        WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
          AND storage_kind = 'postgres'
          AND schema_version = 1
    ) <> 2 THEN
        RAISE EXCEPTION 'fixed physical shard catalog is not at schema version 1'
            USING ERRCODE = '55000';
    END IF;
END
$m6_physical_schema_preflight$;

UPDATE public.booking_shards
SET schema_version = 2
WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
  AND storage_kind = 'postgres'
  AND schema_version = 1;

-- Payment orchestration is durable in the control database. The authoritative
-- reservation and ticket mutations remain shard-local and are represented here
-- only by globally unique identities and bounded fingerprints. No provider
-- credential, raw webhook body, card data, or customer-supplied amount is stored.
CREATE TABLE public.payment_intents (
    payment_intent_id uuid PRIMARY KEY,
    reservation_id uuid NOT NULL
        REFERENCES public.reservation_directory(reservation_id) ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    owner_user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    provider text NOT NULL
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_payment_id text
        CHECK (
            provider_payment_id IS NULL
            OR provider_payment_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        ),
    hosted_session_ref text
        CHECK (
            hosted_session_ref IS NULL
            OR hosted_session_ref ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        ),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    state text NOT NULL DEFAULT 'created' CHECK (
        state IN (
            'created', 'reservation_securing', 'checkout_pending',
            'awaiting_customer', 'authorization_pending', 'authorized',
            'capture_pending', 'captured', 'ticket_issue_pending',
            'completed', 'void_pending', 'voided', 'refund_pending',
            'refunded', 'cancelled', 'failed', 'manual_review', 'expired'
        )
    ),
    idempotency_key_hash bytea NOT NULL
        CHECK (octet_length(idempotency_key_hash) = 32),
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (payment_intent_id, reservation_id),
    UNIQUE (payment_intent_id, provider, amount_minor, currency),
    UNIQUE (owner_user_id, idempotency_key_hash),
    UNIQUE (provider, provider_payment_id),
    CONSTRAINT payment_intents_hosted_session_check CHECK (
        hosted_session_ref IS NULL OR provider_payment_id IS NOT NULL
    ),
    CONSTRAINT payment_intents_completion_check CHECK (
        state NOT IN (
            'completed', 'voided', 'refunded', 'cancelled', 'failed', 'expired'
        ) OR completed_at IS NOT NULL
    )
);

CREATE UNIQUE INDEX payment_intents_one_active_reservation_idx
    ON public.payment_intents (reservation_id)
    WHERE state NOT IN (
        'completed', 'voided', 'refunded', 'cancelled', 'failed', 'expired'
    );
CREATE INDEX payment_intents_owner_created_idx
    ON public.payment_intents (
        owner_user_id, created_at DESC, payment_intent_id DESC
    );
CREATE INDEX payment_intents_provider_payment_idx
    ON public.payment_intents (provider, provider_payment_id)
    WHERE provider_payment_id IS NOT NULL;
CREATE INDEX payment_intents_recovery_idx
    ON public.payment_intents (state, updated_at, payment_intent_id)
    WHERE state IN (
        'reservation_securing', 'checkout_pending', 'authorization_pending',
        'capture_pending', 'ticket_issue_pending', 'void_pending',
        'refund_pending', 'manual_review'
    );

CREATE FUNCTION public.guard_payment_intent_row()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_intent_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.reservation_directory AS directory
        WHERE directory.reservation_id = NEW.reservation_id
          AND directory.train_run_id = NEW.train_run_id
          AND directory.owner_user_id = NEW.owner_user_id
          AND directory.state IN ('active', 'moving')
    ) THEN
        RAISE EXCEPTION 'payment intent does not match an active reservation directory entry'
            USING ERRCODE = '23514';
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state NOT IN ('created', 'reservation_securing')
           OR NEW.provider_payment_id IS NOT NULL
           OR NEW.hosted_session_ref IS NOT NULL
           OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION 'new payment intent must begin before provider activity'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.payment_intent_id <> OLD.payment_intent_id
           OR NEW.reservation_id <> OLD.reservation_id
           OR NEW.train_run_id <> OLD.train_run_id
           OR NEW.owner_user_id <> OLD.owner_user_id
           OR NEW.provider <> OLD.provider
           OR NEW.amount_minor <> OLD.amount_minor
           OR NEW.currency <> OLD.currency
           OR NEW.idempotency_key_hash <> OLD.idempotency_key_hash
           OR NEW.request_fingerprint <> OLD.request_fingerprint THEN
            RAISE EXCEPTION 'payment intent financial identity is immutable'
                USING ERRCODE = '23514';
        END IF;

        IF OLD.provider_payment_id IS NOT NULL
           AND NEW.provider_payment_id IS DISTINCT FROM OLD.provider_payment_id THEN
            RAISE EXCEPTION 'provider payment identity is immutable once assigned'
                USING ERRCODE = '23514';
        END IF;

        IF OLD.hosted_session_ref IS NOT NULL
           AND NEW.hosted_session_ref IS DISTINCT FROM OLD.hosted_session_ref THEN
            RAISE EXCEPTION 'hosted payment session reference is immutable once assigned'
                USING ERRCODE = '23514';
        END IF;

        IF OLD.completed_at IS NOT NULL
           AND NEW.completed_at IS DISTINCT FROM OLD.completed_at THEN
            RAISE EXCEPTION 'payment intent completion evidence is immutable'
                USING ERRCODE = '23514';
        END IF;

        IF NEW.state <> OLD.state AND NOT (
            (OLD.state = 'created' AND NEW.state IN (
                'reservation_securing', 'cancelled', 'failed'
            ))
            OR (OLD.state = 'reservation_securing' AND NEW.state IN (
                'checkout_pending', 'cancelled', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'checkout_pending' AND NEW.state IN (
                'awaiting_customer', 'cancelled', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'awaiting_customer' AND NEW.state IN (
                'authorization_pending', 'void_pending', 'cancelled',
                'manual_review', 'expired'
            ))
            OR (OLD.state = 'authorization_pending' AND NEW.state IN (
                'authorized', 'cancelled', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'authorized' AND NEW.state IN (
                'capture_pending', 'void_pending', 'manual_review'
            ))
            OR (OLD.state = 'capture_pending' AND NEW.state IN (
                'captured', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'captured' AND NEW.state IN (
                'ticket_issue_pending', 'refund_pending', 'manual_review'
            ))
            OR (OLD.state = 'ticket_issue_pending' AND NEW.state IN (
                'completed', 'refund_pending', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'completed' AND NEW.state IN (
                'refund_pending', 'manual_review'
            ))
            OR (OLD.state = 'void_pending' AND NEW.state IN (
                'voided', 'manual_review'
            ))
            OR (OLD.state = 'voided' AND NEW.state = 'cancelled')
            OR (OLD.state = 'refund_pending' AND NEW.state IN (
                'refunded', 'manual_review'
            ))
            OR (OLD.state = 'refunded' AND NEW.state = 'cancelled')
            OR (OLD.state = 'manual_review' AND NEW.state IN (
                'authorization_pending', 'authorized', 'capture_pending',
                'captured', 'ticket_issue_pending', 'void_pending',
                'refund_pending', 'cancelled', 'failed'
            ))
        ) THEN
            RAISE EXCEPTION 'invalid payment intent state transition'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    IF NEW.completed_at IS NOT NULL
       AND (TG_OP = 'INSERT' OR OLD.completed_at IS NULL)
       AND NEW.state NOT IN (
            'completed', 'voided', 'refunded', 'cancelled', 'failed', 'expired'
       ) THEN
        RAISE EXCEPTION 'payment completion timestamp requires a terminal state'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$payment_intent_guard$;

CREATE TRIGGER payment_intents_guard
BEFORE INSERT OR UPDATE ON public.payment_intents
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_intent_row();
CREATE TRIGGER payment_intents_set_updated_at
BEFORE UPDATE ON public.payment_intents
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE public.payment_sagas (
    saga_id uuid PRIMARY KEY,
    payment_intent_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    current_step text NOT NULL DEFAULT 'secure_reservation' CHECK (
        current_step IN (
            'secure_reservation', 'create_checkout', 'await_provider',
            'authorize', 'capture', 'reconcile_provider', 'issue_tickets',
            'void', 'refund', 'compensate', 'complete'
        )
    ),
    state text NOT NULL DEFAULT 'created' CHECK (
        state IN (
            'created', 'reservation_secured', 'checkout_created',
            'awaiting_provider', 'authorized', 'capturing', 'captured',
            'issuing_tickets', 'completed', 'compensating', 'refunding',
            'compensated', 'failed', 'manual_review'
        )
    ),
    lease_owner text,
    lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 1000000),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (saga_id, payment_intent_id),
    CONSTRAINT payment_sagas_intent_fkey FOREIGN KEY (
        payment_intent_id, reservation_id
    ) REFERENCES public.payment_intents (
        payment_intent_id, reservation_id
    ) ON DELETE RESTRICT,
    CONSTRAINT payment_sagas_lease_check CHECK (
        (lease_owner IS NULL AND lease_until IS NULL)
        OR (
            lease_owner IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        )
    ),
    CONSTRAINT payment_sagas_completion_check CHECK (
        (
            state IN ('completed', 'compensated', 'failed')
            AND completed_at IS NOT NULL
            AND lease_owner IS NULL
            AND lease_until IS NULL
        )
        OR (
            state NOT IN ('completed', 'compensated', 'failed')
            AND completed_at IS NULL
        )
    )
);

CREATE UNIQUE INDEX payment_sagas_one_active_intent_idx
    ON public.payment_sagas (payment_intent_id)
    WHERE state NOT IN ('completed', 'compensated', 'failed');
CREATE UNIQUE INDEX payment_sagas_one_active_reservation_idx
    ON public.payment_sagas (reservation_id)
    WHERE state NOT IN ('completed', 'compensated', 'failed');
CREATE INDEX payment_sagas_claim_idx
    ON public.payment_sagas (
        state, next_attempt_at, lease_until, updated_at, saga_id
    )
    WHERE state NOT IN ('completed', 'compensated', 'failed');

CREATE FUNCTION public.guard_payment_saga_row()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_saga_guard$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NOT (
            (NEW.state = 'created' AND NEW.current_step = 'secure_reservation')
            OR (
                NEW.state = 'compensating'
                AND NEW.current_step IN ('void', 'refund', 'compensate')
            )
        )
           OR NEW.attempts <> 0
           OR NEW.lease_owner IS NOT NULL
           OR NEW.lease_until IS NOT NULL
           OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION 'new payment saga must begin at reservation security'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.saga_id <> OLD.saga_id
           OR NEW.payment_intent_id <> OLD.payment_intent_id
           OR NEW.reservation_id <> OLD.reservation_id THEN
            RAISE EXCEPTION 'payment saga identity is immutable'
                USING ERRCODE = '23514';
        END IF;

        IF NEW.state <> OLD.state AND NOT (
            (OLD.state = 'created' AND NEW.state IN (
                'reservation_secured', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'reservation_secured' AND NEW.state IN (
                'checkout_created', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'checkout_created' AND NEW.state IN (
                'awaiting_provider', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'awaiting_provider' AND NEW.state IN (
                'authorized', 'compensating', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'authorized' AND NEW.state IN (
                'capturing', 'compensating', 'manual_review'
            ))
            OR (OLD.state = 'capturing' AND NEW.state IN (
                'captured', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'captured' AND NEW.state IN (
                'issuing_tickets', 'compensating', 'manual_review'
            ))
            OR (OLD.state = 'issuing_tickets' AND NEW.state IN (
                'completed', 'compensating', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'compensating' AND NEW.state IN (
                'refunding', 'compensated', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'refunding' AND NEW.state IN (
                'compensated', 'failed', 'manual_review'
            ))
            OR (OLD.state = 'manual_review' AND NEW.state IN (
                'awaiting_provider', 'authorized', 'capturing', 'captured',
                'issuing_tickets',
                'completed', 'compensating', 'refunding', 'compensated', 'failed'
            ))
        ) THEN
            RAISE EXCEPTION 'invalid payment saga state transition'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    RETURN NEW;
END;
$payment_saga_guard$;

CREATE TRIGGER payment_sagas_guard
BEFORE INSERT OR UPDATE ON public.payment_sagas
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_saga_row();
CREATE TRIGGER payment_sagas_set_updated_at
BEFORE UPDATE ON public.payment_sagas
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- Every provider side effect has one durable, stable idempotency identity.
-- Successful capture/refund rows are the immutable financial ledger for M6;
-- the composite foreign key binds their amount and currency to the server-
-- derived payment intent rather than to request input.
CREATE TABLE public.payment_operations (
    operation_id uuid PRIMARY KEY,
    payment_intent_id uuid NOT NULL,
    provider text NOT NULL
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    operation_type text NOT NULL CHECK (
        operation_type IN (
            'create_checkout', 'query_status', 'authorize', 'capture',
            'void', 'refund'
        )
    ),
    provider_idempotency_key_hash bytea NOT NULL
        CHECK (octet_length(provider_idempotency_key_hash) = 32),
    provider_operation_id text CHECK (
        provider_operation_id IS NULL
        OR provider_operation_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    state text NOT NULL DEFAULT 'pending' CHECK (
        state IN (
            'pending', 'claimed', 'in_flight', 'succeeded',
            'failed_retryable', 'failed_permanent', 'uncertain', 'cancelled'
        )
    ),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 1000000),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text,
    lease_until timestamptz,
    normalized_provider_state text CHECK (
        normalized_provider_state IS NULL
        OR normalized_provider_state IN (
            'created', 'requires_customer_action', 'authorized', 'captured',
            'voided', 'refunded', 'failed', 'cancelled', 'unknown'
        )
    ),
    response_fingerprint bytea CHECK (
        response_fingerprint IS NULL OR octet_length(response_fingerprint) = 32
    ),
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (provider, provider_idempotency_key_hash),
    UNIQUE (provider, provider_operation_id),
    UNIQUE (operation_id, payment_intent_id),
    CONSTRAINT payment_operations_intent_amount_fkey FOREIGN KEY (
        payment_intent_id, provider, amount_minor, currency
    ) REFERENCES public.payment_intents (
        payment_intent_id, provider, amount_minor, currency
    ) ON DELETE RESTRICT,
    CONSTRAINT payment_operations_lease_check CHECK (
        (
            state IN ('claimed', 'in_flight')
            AND lease_owner IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        )
        OR (
            state NOT IN ('claimed', 'in_flight')
            AND lease_owner IS NULL
            AND lease_until IS NULL
        )
    ),
    CONSTRAINT payment_operations_completion_check CHECK (
        (
            state IN ('succeeded', 'failed_permanent', 'cancelled')
            AND completed_at IS NOT NULL
        )
        OR (
            state NOT IN ('succeeded', 'failed_permanent', 'cancelled')
            AND completed_at IS NULL
        )
    ),
    CONSTRAINT payment_operations_failure_check CHECK (
        state NOT IN ('failed_retryable', 'failed_permanent', 'uncertain')
        OR bounded_error_category IS NOT NULL
    ),
    CONSTRAINT payment_operations_success_check CHECK (
        state <> 'succeeded'
        OR (
            response_fingerprint IS NOT NULL
            AND normalized_provider_state IS NOT NULL
            AND (
                operation_type = 'query_status'
                OR provider_operation_id IS NOT NULL
            )
        )
    ),
    CONSTRAINT payment_operations_success_state_check CHECK (
        state <> 'succeeded'
        OR operation_type = 'query_status'
        OR (
            operation_type = 'create_checkout'
            AND normalized_provider_state IN (
                'created', 'requires_customer_action'
            )
        )
        OR (
            operation_type = 'authorize'
            AND normalized_provider_state = 'authorized'
        )
        OR (
            operation_type = 'capture'
            AND normalized_provider_state = 'captured'
        )
        OR (
            operation_type = 'void'
            AND normalized_provider_state = 'voided'
        )
        OR (
            operation_type = 'refund'
            AND normalized_provider_state = 'refunded'
        )
    )
);

CREATE UNIQUE INDEX payment_operations_one_financial_type_idx
    ON public.payment_operations (payment_intent_id, operation_type)
    WHERE operation_type IN (
        'create_checkout', 'authorize', 'capture', 'void', 'refund'
    );
CREATE INDEX payment_operations_claim_idx
    ON public.payment_operations (
        state, next_attempt_at, lease_until, updated_at, operation_id
    )
    WHERE state IN ('pending', 'claimed', 'failed_retryable', 'uncertain');
CREATE INDEX payment_operations_intent_created_idx
    ON public.payment_operations (
        payment_intent_id, created_at, operation_id
    );
CREATE INDEX payment_operations_uncertain_idx
    ON public.payment_operations (updated_at, operation_id)
    WHERE state = 'uncertain';

CREATE FUNCTION public.guard_payment_operation_row()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_operation_guard$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'pending'
           OR NEW.attempts <> 0
           OR NEW.provider_operation_id IS NOT NULL
           OR NEW.normalized_provider_state IS NOT NULL
           OR NEW.response_fingerprint IS NOT NULL
           OR NEW.bounded_error_category IS NOT NULL
           OR NEW.lease_owner IS NOT NULL
           OR NEW.lease_until IS NOT NULL
           OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION 'new payment operation must begin pending without provider evidence'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.operation_id <> OLD.operation_id
           OR NEW.payment_intent_id <> OLD.payment_intent_id
           OR NEW.provider <> OLD.provider
           OR NEW.operation_type <> OLD.operation_type
           OR NEW.provider_idempotency_key_hash <>
                OLD.provider_idempotency_key_hash
           OR NEW.amount_minor <> OLD.amount_minor
           OR NEW.currency <> OLD.currency THEN
            RAISE EXCEPTION 'payment operation financial identity is immutable'
                USING ERRCODE = '23514';
        END IF;

        IF OLD.provider_operation_id IS NOT NULL
           AND NEW.provider_operation_id IS DISTINCT FROM
                OLD.provider_operation_id THEN
            RAISE EXCEPTION 'provider operation identity is immutable once assigned'
                USING ERRCODE = '23514';
        END IF;

        IF OLD.state = 'succeeded' AND (
            NEW.state <> OLD.state
            OR NEW.provider_operation_id IS DISTINCT FROM
                OLD.provider_operation_id
            OR NEW.normalized_provider_state IS DISTINCT FROM
                OLD.normalized_provider_state
            OR NEW.response_fingerprint IS DISTINCT FROM OLD.response_fingerprint
            OR NEW.completed_at IS DISTINCT FROM OLD.completed_at
        ) THEN
            RAISE EXCEPTION 'successful payment operation evidence is immutable'
                USING ERRCODE = '23514';
        END IF;

        IF NEW.state <> OLD.state AND NOT (
            (OLD.state = 'pending' AND NEW.state IN ('claimed', 'cancelled'))
            OR (OLD.state = 'claimed' AND NEW.state IN (
                'pending', 'in_flight', 'cancelled'
            ))
            OR (OLD.state = 'in_flight' AND NEW.state IN (
                'succeeded', 'failed_retryable', 'failed_permanent', 'uncertain'
            ))
            OR (OLD.state = 'failed_retryable' AND NEW.state IN (
                'pending', 'cancelled'
            ))
            OR (OLD.state = 'uncertain' AND NEW.state IN (
                'succeeded', 'failed_retryable', 'failed_permanent'
            ))
        ) THEN
            RAISE EXCEPTION 'invalid payment operation state transition'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    RETURN NEW;
END;
$payment_operation_guard$;

-- Serialize capture/refund accounting on the immutable payment-intent row.
-- M6 permits one full capture and one full refund only. The composite FK has
-- already forced both rows to use the exact intent amount and currency.
CREATE FUNCTION public.guard_payment_financial_settlement()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_financial_guard$
BEGIN
    IF NEW.state = 'succeeded'
       AND NEW.operation_type IN ('capture', 'void', 'refund')
       AND (
            TG_OP = 'INSERT'
            OR OLD.state IS DISTINCT FROM NEW.state
       ) THEN
        PERFORM 1
        FROM public.payment_intents AS intent
        WHERE intent.payment_intent_id = NEW.payment_intent_id
        FOR UPDATE;

        IF NEW.operation_type = 'refund'
           AND NOT EXISTS (
                SELECT 1
                FROM public.payment_operations AS capture
                WHERE capture.payment_intent_id = NEW.payment_intent_id
                  AND capture.operation_type = 'capture'
                  AND capture.state = 'succeeded'
                  AND capture.amount_minor = NEW.amount_minor
                  AND capture.currency = NEW.currency
           ) THEN
            RAISE EXCEPTION 'full refund requires matching successful capture evidence'
                USING ERRCODE = '23514';
        END IF;

        IF NEW.operation_type = 'capture'
           AND EXISTS (
                SELECT 1
                FROM public.payment_operations AS void_operation
                WHERE void_operation.payment_intent_id = NEW.payment_intent_id
                  AND void_operation.operation_type = 'void'
                  AND void_operation.state = 'succeeded'
           ) THEN
            RAISE EXCEPTION 'capture cannot succeed after a successful void'
                USING ERRCODE = '23514';
        END IF;

        IF NEW.operation_type = 'void'
           AND EXISTS (
                SELECT 1
                FROM public.payment_operations AS capture
                WHERE capture.payment_intent_id = NEW.payment_intent_id
                  AND capture.operation_type = 'capture'
                  AND capture.state = 'succeeded'
           ) THEN
            RAISE EXCEPTION 'void cannot succeed after a successful capture'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    RETURN NEW;
END;
$payment_financial_guard$;

CREATE TRIGGER payment_operations_financial_guard
BEFORE INSERT OR UPDATE OF state ON public.payment_operations
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_financial_settlement();
CREATE TRIGGER payment_operations_guard
BEFORE INSERT OR UPDATE ON public.payment_operations
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_operation_row();
CREATE TRIGGER payment_operations_set_updated_at
BEFORE UPDATE ON public.payment_operations
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- The HTTP boundary stores only verified, bounded metadata. payload_hash is a
-- SHA-256 digest of the verified request body; the body and signature never
-- enter the database. Unknown signed event types may be stored then ignored.
CREATE TABLE public.payment_webhook_inbox (
    inbox_id uuid PRIMARY KEY,
    provider text NOT NULL
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_event_id text NOT NULL
        CHECK (provider_event_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    event_type text NOT NULL
        CHECK (event_type ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    provider_payment_id text CHECK (
        provider_payment_id IS NULL
        OR provider_payment_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    verified_key_id text NOT NULL
        CHECK (verified_key_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'),
    event_created_at timestamptz NOT NULL,
    signature_verified_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    body_size_bytes integer NOT NULL CHECK (body_size_bytes BETWEEN 1 AND 1048576),
    state text NOT NULL DEFAULT 'received' CHECK (
        state IN (
            'received', 'processing', 'processed', 'ignored',
            'failed_retryable', 'failed_permanent', 'security_conflict'
        )
    ),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 1000000),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text,
    lease_until timestamptz,
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    processed_at timestamptz,
    UNIQUE (provider, provider_event_id),
    UNIQUE (provider, provider_event_id, payload_hash),
    CONSTRAINT payment_webhook_inbox_verified_time_check CHECK (
        signature_verified_at <= received_at
    ),
    CONSTRAINT payment_webhook_inbox_lease_check CHECK (
        (
            state = 'processing'
            AND lease_owner IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        )
        OR (
            state <> 'processing'
            AND lease_owner IS NULL
            AND lease_until IS NULL
        )
    ),
    CONSTRAINT payment_webhook_inbox_completion_check CHECK (
        (
            state IN (
                'processed', 'ignored', 'failed_permanent', 'security_conflict'
            )
            AND processed_at IS NOT NULL
        )
        OR (
            state NOT IN (
                'processed', 'ignored', 'failed_permanent', 'security_conflict'
            )
            AND processed_at IS NULL
        )
    ),
    CONSTRAINT payment_webhook_inbox_failure_check CHECK (
        state NOT IN ('failed_retryable', 'failed_permanent', 'security_conflict')
        OR bounded_error_category IS NOT NULL
    )
);

CREATE INDEX payment_webhook_inbox_claim_idx
    ON public.payment_webhook_inbox (
        state, next_attempt_at, lease_until, received_at, inbox_id
    )
    WHERE state IN ('received', 'processing', 'failed_retryable');
CREATE INDEX payment_webhook_inbox_payment_idx
    ON public.payment_webhook_inbox (
        provider, provider_payment_id, event_created_at, inbox_id
    )
    WHERE provider_payment_id IS NOT NULL;
CREATE INDEX payment_webhook_inbox_lag_idx
    ON public.payment_webhook_inbox (received_at, inbox_id)
    WHERE state IN ('received', 'processing', 'failed_retryable');

CREATE FUNCTION public.guard_payment_webhook_inbox_row()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_webhook_guard$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'received'
           OR NEW.attempts <> 0
           OR NEW.lease_owner IS NOT NULL
           OR NEW.lease_until IS NOT NULL
           OR NEW.processed_at IS NOT NULL
           OR NEW.bounded_error_category IS NOT NULL THEN
            RAISE EXCEPTION 'verified webhook must enter the inbox in received state'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.inbox_id <> OLD.inbox_id
       OR NEW.provider <> OLD.provider
       OR NEW.provider_event_id <> OLD.provider_event_id
       OR NEW.event_type <> OLD.event_type
       OR NEW.provider_payment_id IS DISTINCT FROM OLD.provider_payment_id
       OR NEW.payload_hash <> OLD.payload_hash
       OR NEW.verified_key_id <> OLD.verified_key_id
       OR NEW.event_created_at <> OLD.event_created_at
       OR NEW.signature_verified_at <> OLD.signature_verified_at
       OR NEW.received_at <> OLD.received_at
       OR NEW.body_size_bytes <> OLD.body_size_bytes THEN
        RAISE EXCEPTION 'verified webhook envelope is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'received' AND NEW.state IN (
            'processing', 'ignored', 'security_conflict'
        ))
        OR (OLD.state = 'processing' AND NEW.state IN (
            'processed', 'ignored', 'failed_retryable',
            'failed_permanent', 'security_conflict'
        ))
        OR (OLD.state = 'failed_retryable' AND NEW.state IN (
            'processing', 'failed_permanent', 'security_conflict'
        ))
    ) THEN
        RAISE EXCEPTION 'invalid payment webhook state transition'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$payment_webhook_guard$;

CREATE TRIGGER payment_webhook_inbox_guard
BEFORE INSERT OR UPDATE ON public.payment_webhook_inbox
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_webhook_inbox_row();

-- A reused provider event identity with a different digest is never merged
-- into the canonical inbox row. It is durable security evidence, without the
-- conflicting body or signature.
CREATE TABLE public.payment_provider_event_conflicts (
    conflict_id uuid PRIMARY KEY,
    provider text NOT NULL,
    provider_event_id text NOT NULL,
    canonical_payload_hash bytea NOT NULL
        CHECK (octet_length(canonical_payload_hash) = 32),
    conflicting_payload_hash bytea NOT NULL
        CHECK (octet_length(conflicting_payload_hash) = 32),
    state text NOT NULL DEFAULT 'open' CHECK (
        state IN ('open', 'investigating', 'resolved', 'dismissed')
    ),
    occurrence_count integer NOT NULL DEFAULT 1
        CHECK (occurrence_count BETWEEN 1 AND 1000000),
    bounded_resolution_category text CHECK (
        bounded_resolution_category IS NULL
        OR bounded_resolution_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    first_detected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_detected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (provider, provider_event_id, conflicting_payload_hash),
    CONSTRAINT payment_provider_event_conflicts_inbox_fkey FOREIGN KEY (
        provider, provider_event_id, canonical_payload_hash
    ) REFERENCES public.payment_webhook_inbox (
        provider, provider_event_id, payload_hash
    ) ON DELETE RESTRICT,
    CONSTRAINT payment_provider_event_conflicts_distinct_hash_check CHECK (
        canonical_payload_hash <> conflicting_payload_hash
    ),
    CONSTRAINT payment_provider_event_conflicts_time_check CHECK (
        last_detected_at >= first_detected_at
    ),
    CONSTRAINT payment_provider_event_conflicts_resolution_check CHECK (
        (
            state IN ('resolved', 'dismissed')
            AND resolved_at IS NOT NULL
            AND bounded_resolution_category IS NOT NULL
        )
        OR (
            state NOT IN ('resolved', 'dismissed')
            AND resolved_at IS NULL
            AND bounded_resolution_category IS NULL
        )
    )
);

CREATE INDEX payment_provider_event_conflicts_open_idx
    ON public.payment_provider_event_conflicts (
        state, first_detected_at, conflict_id
    )
    WHERE state IN ('open', 'investigating');

CREATE FUNCTION public.guard_payment_provider_event_conflict_row()
RETURNS trigger
LANGUAGE plpgsql
AS $provider_event_conflict_guard$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'open'
           OR NEW.occurrence_count <> 1
           OR NEW.resolved_at IS NOT NULL
           OR NEW.bounded_resolution_category IS NOT NULL THEN
            RAISE EXCEPTION 'new provider event conflict must begin open'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.conflict_id <> OLD.conflict_id
       OR NEW.provider <> OLD.provider
       OR NEW.provider_event_id <> OLD.provider_event_id
       OR NEW.canonical_payload_hash <> OLD.canonical_payload_hash
       OR NEW.conflicting_payload_hash <> OLD.conflicting_payload_hash
       OR NEW.first_detected_at <> OLD.first_detected_at THEN
        RAISE EXCEPTION 'provider event conflict identity is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.occurrence_count < OLD.occurrence_count
       OR NEW.last_detected_at < OLD.last_detected_at THEN
        RAISE EXCEPTION 'provider event conflict evidence cannot decrease'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'open' AND NEW.state IN (
            'investigating', 'resolved', 'dismissed'
        ))
        OR (OLD.state = 'investigating' AND NEW.state IN (
            'resolved', 'dismissed'
        ))
    ) THEN
        RAISE EXCEPTION 'invalid provider event conflict state transition'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$provider_event_conflict_guard$;

CREATE TRIGGER payment_provider_event_conflicts_guard
BEFORE INSERT OR UPDATE ON public.payment_provider_event_conflicts
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_provider_event_conflict_row();
CREATE TRIGGER payment_provider_event_conflicts_set_updated_at
BEFORE UPDATE ON public.payment_provider_event_conflicts
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- Reconciliation is detect-only by default. A checkpoint records bounded
-- counts and a durable cursor; it cannot itself mutate seats or issue tickets.
CREATE TABLE public.payment_reconciliation_checkpoints (
    reconciliation_id uuid PRIMARY KEY,
    scope text NOT NULL CHECK (
        scope IN (
            'payment-intents', 'payment-operations', 'payment-webhooks',
            'payment-tickets', 'payment-provider', 'payment-all'
        )
    ),
    payment_intent_id uuid
        REFERENCES public.payment_intents(payment_intent_id) ON DELETE RESTRICT,
    mode text NOT NULL DEFAULT 'detect_only'
        CHECK (mode IN ('detect_only', 'safe_repair')),
    state text NOT NULL DEFAULT 'pending' CHECK (
        state IN ('pending', 'running', 'passed', 'mismatch', 'partial', 'failed')
    ),
    cursor_received_at timestamptz,
    cursor_inbox_id uuid
        REFERENCES public.payment_webhook_inbox(inbox_id) ON DELETE RESTRICT,
    rows_examined bigint NOT NULL DEFAULT 0 CHECK (rows_examined >= 0),
    mismatch_count bigint NOT NULL DEFAULT 0 CHECK (mismatch_count >= 0),
    repair_count bigint NOT NULL DEFAULT 0 CHECK (repair_count >= 0),
    truncated boolean NOT NULL DEFAULT false,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 1000000),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text,
    lease_until timestamptz,
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    started_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT payment_reconciliation_checkpoints_cursor_check CHECK (
        (cursor_received_at IS NULL AND cursor_inbox_id IS NULL)
        OR (cursor_received_at IS NOT NULL AND cursor_inbox_id IS NOT NULL)
    ),
    CONSTRAINT payment_reconciliation_checkpoints_repair_mode_check CHECK (
        mode = 'safe_repair' OR repair_count = 0
    ),
    CONSTRAINT payment_reconciliation_checkpoints_count_check CHECK (
        mismatch_count <= rows_examined
        AND repair_count <= mismatch_count
    ),
    CONSTRAINT payment_reconciliation_checkpoints_lease_check CHECK (
        (
            state = 'running'
            AND lease_owner IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND started_at IS NOT NULL
            AND completed_at IS NULL
        )
        OR (
            state <> 'running'
            AND lease_owner IS NULL
            AND lease_until IS NULL
        )
    ),
    CONSTRAINT payment_reconciliation_checkpoints_completion_check CHECK (
        (
            state IN ('passed', 'mismatch', 'partial', 'failed')
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND completed_at >= started_at
        )
        OR (
            state = 'pending'
            AND started_at IS NULL
            AND completed_at IS NULL
        )
        OR state = 'running'
    ),
    CONSTRAINT payment_reconciliation_checkpoints_error_check CHECK (
        state <> 'failed' OR bounded_error_category IS NOT NULL
    )
);

CREATE UNIQUE INDEX payment_reconciliation_one_active_global_idx
    ON public.payment_reconciliation_checkpoints (scope)
    WHERE payment_intent_id IS NULL AND state IN ('pending', 'running');
CREATE UNIQUE INDEX payment_reconciliation_one_active_intent_idx
    ON public.payment_reconciliation_checkpoints (scope, payment_intent_id)
    WHERE payment_intent_id IS NOT NULL AND state IN ('pending', 'running');
CREATE INDEX payment_reconciliation_claim_idx
    ON public.payment_reconciliation_checkpoints (
        state, next_attempt_at, lease_until, created_at, reconciliation_id
    )
    WHERE state IN ('pending', 'running');
CREATE INDEX payment_reconciliation_results_idx
    ON public.payment_reconciliation_checkpoints (
        state, completed_at DESC, reconciliation_id
    )
    WHERE state IN ('mismatch', 'partial', 'failed');

CREATE FUNCTION public.guard_payment_reconciliation_checkpoint_row()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_reconciliation_guard$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'pending'
           OR NEW.rows_examined <> 0
           OR NEW.mismatch_count <> 0
           OR NEW.repair_count <> 0
           OR NEW.attempts <> 0
           OR NEW.lease_owner IS NOT NULL
           OR NEW.lease_until IS NOT NULL
           OR NEW.started_at IS NOT NULL
           OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION 'new payment reconciliation checkpoint must begin pending'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.reconciliation_id <> OLD.reconciliation_id
       OR NEW.scope <> OLD.scope
       OR NEW.payment_intent_id IS DISTINCT FROM OLD.payment_intent_id
       OR NEW.mode <> OLD.mode
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'payment reconciliation identity is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state IN ('passed', 'mismatch', 'partial', 'failed') THEN
        IF NEW.state <> OLD.state
           OR NEW.cursor_received_at IS DISTINCT FROM OLD.cursor_received_at
           OR NEW.cursor_inbox_id IS DISTINCT FROM OLD.cursor_inbox_id
           OR NEW.rows_examined <> OLD.rows_examined
           OR NEW.mismatch_count <> OLD.mismatch_count
           OR NEW.repair_count <> OLD.repair_count
           OR NEW.truncated <> OLD.truncated
           OR NEW.attempts <> OLD.attempts
           OR NEW.next_attempt_at <> OLD.next_attempt_at
           OR NEW.bounded_error_category IS DISTINCT FROM
                OLD.bounded_error_category
           OR NEW.started_at IS DISTINCT FROM OLD.started_at
           OR NEW.completed_at IS DISTINCT FROM OLD.completed_at THEN
            RAISE EXCEPTION 'completed payment reconciliation evidence is immutable'
                USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'pending' AND NEW.state = 'running')
        OR (OLD.state = 'running' AND NEW.state IN (
            'pending', 'passed', 'mismatch', 'partial', 'failed'
        ))
    ) THEN
        RAISE EXCEPTION 'invalid payment reconciliation state transition'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$payment_reconciliation_guard$;

CREATE TRIGGER payment_reconciliation_checkpoints_guard
BEFORE INSERT OR UPDATE ON public.payment_reconciliation_checkpoints
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_reconciliation_checkpoint_row();
CREATE TRIGGER payment_reconciliation_checkpoints_set_updated_at
BEFORE UPDATE ON public.payment_reconciliation_checkpoints
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- Every unresolved uncertainty has a visible bounded review deadline. Source
-- references are durable identities only and never include provider bodies,
-- credentials, free-form notes, customer payment details, or connection data.
CREATE TABLE public.payment_manual_review_cases (
    review_case_id uuid PRIMARY KEY,
    payment_intent_id uuid
        REFERENCES public.payment_intents(payment_intent_id) ON DELETE RESTRICT,
    operation_id uuid,
    inbox_id uuid
        REFERENCES public.payment_webhook_inbox(inbox_id) ON DELETE RESTRICT,
    conflict_id uuid
        REFERENCES public.payment_provider_event_conflicts(conflict_id)
        ON DELETE RESTRICT,
    reconciliation_id uuid
        REFERENCES public.payment_reconciliation_checkpoints(reconciliation_id)
        ON DELETE RESTRICT,
    reason_category text NOT NULL
        CHECK (reason_category ~ '^[a-z][a-z0-9_]{0,63}$'),
    state text NOT NULL DEFAULT 'open' CHECK (
        state IN ('open', 'assigned', 'investigating', 'resolved', 'dismissed')
    ),
    assigned_operator_id uuid
        REFERENCES public.users(id) ON DELETE RESTRICT,
    review_due_at timestamptz NOT NULL,
    bounded_resolution_category text CHECK (
        bounded_resolution_category IS NULL
        OR bounded_resolution_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    CONSTRAINT payment_manual_review_cases_source_check CHECK (
        payment_intent_id IS NOT NULL
        OR operation_id IS NOT NULL
        OR inbox_id IS NOT NULL
        OR conflict_id IS NOT NULL
        OR reconciliation_id IS NOT NULL
    ),
    CONSTRAINT payment_manual_review_cases_operation_check CHECK (
        operation_id IS NULL OR payment_intent_id IS NOT NULL
    ),
    CONSTRAINT payment_manual_review_cases_operation_fkey FOREIGN KEY (
        operation_id, payment_intent_id
    ) REFERENCES public.payment_operations (
        operation_id, payment_intent_id
    ) ON DELETE RESTRICT,
    CONSTRAINT payment_manual_review_cases_assignment_check CHECK (
        (state = 'open' AND assigned_operator_id IS NULL)
        OR (
            state IN ('assigned', 'investigating')
            AND assigned_operator_id IS NOT NULL
        )
        OR state IN ('resolved', 'dismissed')
    ),
    CONSTRAINT payment_manual_review_cases_due_check CHECK (
        review_due_at >= created_at
    ),
    CONSTRAINT payment_manual_review_cases_resolution_check CHECK (
        (
            state IN ('resolved', 'dismissed')
            AND resolved_at IS NOT NULL
            AND bounded_resolution_category IS NOT NULL
        )
        OR (
            state NOT IN ('resolved', 'dismissed')
            AND resolved_at IS NULL
            AND bounded_resolution_category IS NULL
        )
    )
);

CREATE UNIQUE INDEX payment_manual_review_one_active_reason_idx
    ON public.payment_manual_review_cases (
        payment_intent_id, reason_category
    )
    WHERE payment_intent_id IS NOT NULL
      AND state IN ('open', 'assigned', 'investigating');
CREATE INDEX payment_manual_review_due_idx
    ON public.payment_manual_review_cases (
        review_due_at, created_at, review_case_id
    )
    WHERE state IN ('open', 'assigned', 'investigating');
CREATE INDEX payment_manual_review_operation_idx
    ON public.payment_manual_review_cases (operation_id, review_case_id)
    WHERE operation_id IS NOT NULL;

CREATE FUNCTION public.guard_payment_manual_review_case_row()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_review_guard$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'open'
           OR NEW.assigned_operator_id IS NOT NULL
           OR NEW.resolved_at IS NOT NULL
           OR NEW.bounded_resolution_category IS NOT NULL THEN
            RAISE EXCEPTION 'new payment manual-review case must begin open'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.review_case_id <> OLD.review_case_id
           OR NEW.payment_intent_id IS DISTINCT FROM OLD.payment_intent_id
           OR NEW.operation_id IS DISTINCT FROM OLD.operation_id
           OR NEW.inbox_id IS DISTINCT FROM OLD.inbox_id
           OR NEW.conflict_id IS DISTINCT FROM OLD.conflict_id
           OR NEW.reconciliation_id IS DISTINCT FROM OLD.reconciliation_id
           OR NEW.reason_category <> OLD.reason_category
           OR NEW.created_at <> OLD.created_at THEN
            RAISE EXCEPTION 'manual review evidence identity is immutable'
                USING ERRCODE = '23514';
        END IF;

        IF NEW.state <> OLD.state AND NOT (
            (OLD.state = 'open' AND NEW.state IN (
                'assigned', 'investigating', 'resolved', 'dismissed'
            ))
            OR (OLD.state = 'assigned' AND NEW.state IN (
                'investigating', 'resolved', 'dismissed'
            ))
            OR (OLD.state = 'investigating' AND NEW.state IN (
                'resolved', 'dismissed'
            ))
        ) THEN
            RAISE EXCEPTION 'invalid manual review state transition'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    RETURN NEW;
END;
$payment_review_guard$;

CREATE TRIGGER payment_manual_review_cases_guard
BEFORE INSERT OR UPDATE ON public.payment_manual_review_cases
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_manual_review_case_row();
CREATE TRIGGER payment_manual_review_cases_set_updated_at
BEFORE UPDATE ON public.payment_manual_review_cases
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

COMMIT;
