BEGIN;

-- Milestone 7 keeps shard-action execution budgets independent from provider
-- operations and from the payment saga's orchestration history. Existing shard
-- command and receipt identities remain unchanged.
CREATE TABLE public.payment_saga_actions (
    action_id uuid PRIMARY KEY,
    saga_id uuid NOT NULL,
    payment_intent_id uuid NOT NULL,
    action_type text NOT NULL CHECK (
        action_type IN (
            'issue_tickets', 'mark_refund_pending',
            'cancel_voided_reservation', 'compensate'
        )
    ),
    state text NOT NULL DEFAULT 'pending' CHECK (
        state IN (
            'pending', 'processing', 'succeeded',
            'failed_retryable', 'failed_permanent', 'cancelled'
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
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (saga_id, action_type),
    CONSTRAINT payment_saga_actions_saga_fkey FOREIGN KEY (
        saga_id, payment_intent_id
    ) REFERENCES public.payment_sagas (
        saga_id, payment_intent_id
    ) ON DELETE RESTRICT,
    CONSTRAINT payment_saga_actions_lease_check CHECK (
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
    CONSTRAINT payment_saga_actions_failure_check CHECK (
        state NOT IN ('failed_retryable', 'failed_permanent')
        OR bounded_error_category IS NOT NULL
    ),
    CONSTRAINT payment_saga_actions_completion_check CHECK (
        (
            state IN ('succeeded', 'failed_permanent', 'cancelled')
            AND completed_at IS NOT NULL
        )
        OR (
            state NOT IN ('succeeded', 'failed_permanent', 'cancelled')
            AND completed_at IS NULL
        )
    )
);

CREATE INDEX payment_saga_actions_claim_idx
    ON public.payment_saga_actions (
        state, next_attempt_at, lease_until, updated_at, action_id
    )
    WHERE state IN ('pending', 'processing', 'failed_retryable');

CREATE FUNCTION public.guard_payment_saga_action_row()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_saga_action_guard$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'pending'
           OR NEW.attempts <> 0
           OR NEW.lease_owner IS NOT NULL
           OR NEW.lease_until IS NOT NULL
           OR NEW.bounded_error_category IS NOT NULL
           OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION 'new payment saga action must begin pending'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.action_id <> OLD.action_id
       OR NEW.saga_id <> OLD.saga_id
       OR NEW.payment_intent_id <> OLD.payment_intent_id
       OR NEW.action_type <> OLD.action_type THEN
        RAISE EXCEPTION 'payment saga action identity is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'succeeded' AND (
        NEW.state <> OLD.state
        OR NEW.completed_at IS DISTINCT FROM OLD.completed_at
    ) THEN
        RAISE EXCEPTION 'successful payment saga action is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'pending' AND NEW.state IN ('processing', 'cancelled'))
        OR (OLD.state = 'processing' AND NEW.state IN (
            'succeeded', 'failed_retryable', 'failed_permanent'
        ))
        OR (OLD.state = 'failed_retryable' AND NEW.state IN (
            'processing', 'cancelled'
        ))
    ) THEN
        RAISE EXCEPTION 'invalid payment saga action state transition'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$payment_saga_action_guard$;

CREATE TRIGGER payment_saga_actions_guard
BEFORE INSERT OR UPDATE ON public.payment_saga_actions
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_saga_action_row();

CREATE TRIGGER payment_saga_actions_set_updated_at
BEFORE UPDATE ON public.payment_saga_actions
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- Upgrade is restart-safe for M6 sagas already waiting on a shard action.
INSERT INTO public.payment_saga_actions (
    action_id, saga_id, payment_intent_id, action_type
)
SELECT gen_random_uuid(), saga.saga_id, saga.payment_intent_id,
       CASE
           WHEN saga.state = 'issuing_tickets' THEN 'issue_tickets'
           WHEN saga.state = 'compensating' AND saga.current_step = 'refund'
               THEN 'mark_refund_pending'
           WHEN saga.state = 'compensating' AND saga.current_step = 'compensate'
               THEN 'cancel_voided_reservation'
           WHEN saga.state = 'refunding' THEN 'compensate'
       END
FROM public.payment_sagas AS saga
WHERE (saga.state = 'issuing_tickets' AND saga.current_step = 'issue_tickets')
   OR (saga.state = 'compensating' AND saga.current_step IN ('refund', 'compensate'))
   OR (saga.state = 'refunding' AND saga.current_step = 'compensate')
ON CONFLICT DO NOTHING;

-- Capability metadata is fixed-width and contains no provider credential.
CREATE TABLE public.payment_provider_capabilities (
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (
        provider_account_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    api_version text NOT NULL CHECK (
        api_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'
    ),
    hosted_checkout boolean NOT NULL,
    authorize boolean NOT NULL,
    capture boolean NOT NULL,
    void_payment boolean NOT NULL,
    full_refund boolean NOT NULL,
    partial_refund boolean NOT NULL,
    payment_status_query boolean NOT NULL,
    settlement_transactions boolean NOT NULL,
    payout_reports boolean NOT NULL,
    webhook_signatures boolean NOT NULL,
    webhook_key_rotation boolean NOT NULL,
    profile_hash bytea NOT NULL CHECK (octet_length(profile_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider, provider_account_id, api_version)
);

CREATE TRIGGER payment_provider_capabilities_set_updated_at
BEFORE UPDATE ON public.payment_provider_capabilities
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- Operational double-entry evidence. This is not statutory accounting.
CREATE TABLE public.financial_ledger_accounts (
    account_code text PRIMARY KEY CHECK (
        account_code IN (
            'customer_funds_pending', 'ticket_sales',
            'provider_receivable', 'provider_refund_receivable',
            'provider_fee_expense', 'settlement_cash',
            'reconciliation_suspense'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO public.financial_ledger_accounts(account_code)
VALUES
    ('customer_funds_pending'),
    ('ticket_sales'),
    ('provider_receivable'),
    ('provider_refund_receivable'),
    ('provider_fee_expense'),
    ('settlement_cash'),
    ('reconciliation_suspense');

CREATE TABLE public.financial_ledger_transactions (
    transaction_id uuid PRIMARY KEY,
    event_id text NOT NULL UNIQUE CHECK (length(event_id) BETWEEN 1 AND 200),
    correlation text NOT NULL CHECK (length(correlation) BETWEEN 1 AND 200),
    purpose text NOT NULL CHECK (
        purpose IN (
            'capture', 'ticket_issuance', 'refund', 'provider_fee',
            'settlement', 'payout', 'reversal'
        )
    ),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    created_at timestamptz NOT NULL
);

CREATE INDEX financial_ledger_transactions_correlation_idx
    ON public.financial_ledger_transactions(correlation, created_at, transaction_id);

CREATE TABLE public.financial_ledger_postings (
    transaction_id uuid NOT NULL
        REFERENCES public.financial_ledger_transactions(transaction_id)
        ON DELETE RESTRICT,
    posting_index smallint NOT NULL CHECK (posting_index >= 0),
    account_code text NOT NULL
        REFERENCES public.financial_ledger_accounts(account_code)
        ON DELETE RESTRICT,
    side text NOT NULL CHECK (side IN ('debit', 'credit')),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    eligibility_cutoff_at timestamptz NOT NULL,
    CONSTRAINT financial_ledger_postings_transaction_index_key
        PRIMARY KEY (transaction_id, posting_index)
);

CREATE INDEX financial_ledger_postings_account_idx
    ON public.financial_ledger_postings(account_code, currency, transaction_id);

CREATE TABLE public.financial_ledger_reversals (
    reversal_transaction_id uuid PRIMARY KEY
        REFERENCES public.financial_ledger_transactions(transaction_id)
        ON DELETE RESTRICT,
    original_transaction_id uuid NOT NULL UNIQUE
        REFERENCES public.financial_ledger_transactions(transaction_id)
        ON DELETE RESTRICT,
    created_at timestamptz NOT NULL,
    CHECK (reversal_transaction_id <> original_transaction_id)
);

CREATE FUNCTION public.guard_financial_ledger_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $financial_ledger_immutable$
BEGIN
    RAISE EXCEPTION 'committed financial ledger evidence is immutable'
        USING ERRCODE = '23514';
END;
$financial_ledger_immutable$;

CREATE TRIGGER financial_ledger_transactions_guard_immutable
BEFORE UPDATE OR DELETE ON public.financial_ledger_transactions
FOR EACH ROW EXECUTE FUNCTION public.guard_financial_ledger_immutable();
CREATE TRIGGER financial_ledger_postings_guard_immutable
BEFORE UPDATE OR DELETE ON public.financial_ledger_postings
FOR EACH ROW EXECUTE FUNCTION public.guard_financial_ledger_immutable();
CREATE TRIGGER financial_ledger_reversals_guard_immutable
BEFORE UPDATE OR DELETE ON public.financial_ledger_reversals
FOR EACH ROW EXECUTE FUNCTION public.guard_financial_ledger_immutable();
CREATE TRIGGER financial_ledger_accounts_guard_immutable
BEFORE UPDATE OR DELETE ON public.financial_ledger_accounts
FOR EACH ROW EXECUTE FUNCTION public.guard_financial_ledger_immutable();

CREATE FUNCTION public.check_financial_ledger_balance()
RETURNS trigger
LANGUAGE plpgsql
AS $financial_ledger_balance$
DECLARE
    checked_transaction_id uuid;
    posting_count bigint;
    posting_currency_count bigint;
    transaction_currency text;
    debit_total numeric;
    credit_total numeric;
BEGIN
    checked_transaction_id := COALESCE(NEW.transaction_id, OLD.transaction_id);
    SELECT tx.currency,
           count(posting.posting_index),
           count(DISTINCT posting.currency),
           COALESCE(sum(posting.amount_minor) FILTER (WHERE posting.side='debit'), 0),
           COALESCE(sum(posting.amount_minor) FILTER (WHERE posting.side='credit'), 0)
      INTO transaction_currency, posting_count, posting_currency_count,
           debit_total, credit_total
      FROM public.financial_ledger_transactions AS tx
      LEFT JOIN public.financial_ledger_postings AS posting
        ON posting.transaction_id = tx.transaction_id
     WHERE tx.transaction_id = checked_transaction_id
     GROUP BY tx.currency;
    IF NOT FOUND
       OR posting_count < 2
       OR posting_currency_count <> 1
       OR EXISTS (
            SELECT 1 FROM public.financial_ledger_postings
             WHERE transaction_id = checked_transaction_id
               AND currency <> transaction_currency
       )
       OR debit_total <> credit_total
       OR debit_total > 9223372036854775807::numeric THEN
        RAISE EXCEPTION 'financial ledger transaction is not balanced'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$financial_ledger_balance$;

CREATE CONSTRAINT TRIGGER financial_ledger_transactions_check_balance
AFTER INSERT ON public.financial_ledger_transactions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.check_financial_ledger_balance();

CREATE CONSTRAINT TRIGGER financial_ledger_postings_check_balance
AFTER INSERT OR UPDATE OR DELETE ON public.financial_ledger_postings
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.check_financial_ledger_balance();

-- Backfill immutable M6 provider effects into the M7 operational ledger.
-- Event and transaction identities are derived only from durable database
-- identities, so an interrupted/replayed upgrade cannot create duplicates.
-- A zero-value successful financial effect cannot be represented by the M7
-- posting invariant (amount_minor > 0), and ambiguous/missing terminal
-- evidence must stop the upgrade rather than manufacture accounting facts.
DO $m7_financial_ledger_backfill_guard$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM public.payment_operations AS operation
         WHERE operation.operation_type IN ('capture', 'refund')
           AND operation.state = 'succeeded'
           AND operation.amount_minor <= 0
    ) OR EXISTS (
        SELECT 1
          FROM public.payment_intents AS intent
         WHERE intent.state IN ('completed', 'refunded')
           AND intent.amount_minor <= 0
    ) THEN
        RAISE EXCEPTION 'cannot backfill zero-value historical payment evidence'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM public.payment_intents AS intent
         WHERE intent.state IN ('completed', 'refunded')
           AND intent.amount_minor > 0
           AND NOT EXISTS (
                SELECT 1
                  FROM public.payment_operations AS operation
                 WHERE operation.payment_intent_id = intent.payment_intent_id
                   AND operation.operation_type = 'capture'
                   AND operation.state = 'succeeded'
                   AND operation.amount_minor = intent.amount_minor
                   AND operation.currency = intent.currency
           )
    ) THEN
        RAISE EXCEPTION 'terminal historical payment lacks successful capture evidence'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM public.payment_intents AS intent
         WHERE intent.state = 'completed'
           AND intent.amount_minor > 0
           AND NOT EXISTS (
                SELECT 1
                  FROM public.ticket_order_shard_locators AS ticket_order
                 WHERE ticket_order.reservation_id = intent.reservation_id
                   AND ticket_order.total_amount_minor = intent.amount_minor
                   AND ticket_order.currency = intent.currency
           )
    ) THEN
        RAISE EXCEPTION 'completed historical payment lacks ticket issuance evidence'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM public.payment_intents AS intent
         WHERE intent.state = 'refunded'
           AND intent.amount_minor > 0
           AND NOT EXISTS (
                SELECT 1
                  FROM public.payment_operations AS operation
                 WHERE operation.payment_intent_id = intent.payment_intent_id
                   AND operation.operation_type = 'refund'
                   AND operation.state = 'succeeded'
                   AND operation.amount_minor = intent.amount_minor
                   AND operation.currency = intent.currency
           )
    ) THEN
        RAISE EXCEPTION 'refunded historical payment lacks successful refund evidence'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM public.ticket_order_shard_locators AS ticket_order
          JOIN public.payment_intents AS intent
            ON intent.reservation_id = ticket_order.reservation_id
           AND intent.amount_minor = ticket_order.total_amount_minor
           AND intent.currency = ticket_order.currency
          JOIN public.payment_operations AS capture
            ON capture.payment_intent_id = intent.payment_intent_id
           AND capture.operation_type = 'capture'
           AND capture.state = 'succeeded'
           AND capture.amount_minor = intent.amount_minor
           AND capture.currency = intent.currency
          LEFT JOIN public.payment_sagas AS saga
            ON saga.payment_intent_id = intent.payment_intent_id
           AND saga.reservation_id = intent.reservation_id
         GROUP BY ticket_order.ticket_order_id
        HAVING count(*) <> 1 OR count(saga.saga_id) <> 1
    ) THEN
        RAISE EXCEPTION 'historical ticket issuance lacks one canonical saga identity'
            USING ERRCODE = '23514';
    END IF;
END;
$m7_financial_ledger_backfill_guard$;

-- PostgreSQL core exposes SHA-2 but not SHA-1. This transaction-local helper
-- implements RFC 3174 solely to reproduce google/uuid.NewSHA1 exactly without
-- making uuid-ossp or pgcrypto a deployment dependency.
CREATE FUNCTION pg_temp.m7_sha1(input bytea)
RETURNS bytea
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $m7_sha1$
DECLARE
    message bytea := input || decode('80', 'hex');
    length_bytes bytea := decode('0000000000000000', 'hex');
    input_bits bigint := octet_length(input)::bigint * 8;
    words bigint[] := array_fill(0::bigint, ARRAY[80], ARRAY[0]);
    h0 bigint := 1732584193;
    h1 bigint := 4023233417;
    h2 bigint := 2562383102;
    h3 bigint := 271733878;
    h4 bigint := 3285377520;
    a bigint;
    b bigint;
    c bigint;
    d bigint;
    e bigint;
    f bigint;
    k bigint;
    temporary bigint;
    digest bytea := decode(repeat('00', 20), 'hex');
    block_index integer;
    word_index integer;
    byte_index integer;
BEGIN
    WHILE mod(octet_length(message), 64) <> 56 LOOP
        message := message || decode('00', 'hex');
    END LOOP;
    FOR byte_index IN 0..7 LOOP
        length_bytes := set_byte(
            length_bytes,
            7 - byte_index,
            ((input_bits >> (byte_index * 8)) & 255)::integer
        );
    END LOOP;
    message := message || length_bytes;

    FOR block_index IN 0..(octet_length(message) / 64 - 1) LOOP
        FOR word_index IN 0..15 LOOP
            words[word_index] :=
                (get_byte(message, block_index * 64 + word_index * 4)::bigint << 24)
              | (get_byte(message, block_index * 64 + word_index * 4 + 1)::bigint << 16)
              | (get_byte(message, block_index * 64 + word_index * 4 + 2)::bigint << 8)
              |  get_byte(message, block_index * 64 + word_index * 4 + 3)::bigint;
        END LOOP;
        FOR word_index IN 16..79 LOOP
            temporary := words[word_index - 3] # words[word_index - 8]
                       # words[word_index - 14] # words[word_index - 16];
            words[word_index] := ((temporary << 1) | (temporary >> 31)) & 4294967295;
        END LOOP;

        a := h0;
        b := h1;
        c := h2;
        d := h3;
        e := h4;
        FOR word_index IN 0..79 LOOP
            IF word_index < 20 THEN
                f := (b & c) | ((b # 4294967295) & d);
                k := 1518500249;
            ELSIF word_index < 40 THEN
                f := b # c # d;
                k := 1859775393;
            ELSIF word_index < 60 THEN
                f := (b & c) | (b & d) | (c & d);
                k := 2400959708;
            ELSE
                f := b # c # d;
                k := 3395469782;
            END IF;
            temporary := (
                ((a << 5) | (a >> 27)) + f + e + k + words[word_index]
            ) & 4294967295;
            e := d;
            d := c;
            c := ((b << 30) | (b >> 2)) & 4294967295;
            b := a;
            a := temporary;
        END LOOP;
        h0 := (h0 + a) & 4294967295;
        h1 := (h1 + b) & 4294967295;
        h2 := (h2 + c) & 4294967295;
        h3 := (h3 + d) & 4294967295;
        h4 := (h4 + e) & 4294967295;
    END LOOP;

    FOR word_index IN 0..4 LOOP
        temporary := CASE word_index
            WHEN 0 THEN h0 WHEN 1 THEN h1 WHEN 2 THEN h2
            WHEN 3 THEN h3 ELSE h4 END;
        digest := set_byte(digest, word_index * 4, ((temporary >> 24) & 255)::integer);
        digest := set_byte(digest, word_index * 4 + 1, ((temporary >> 16) & 255)::integer);
        digest := set_byte(digest, word_index * 4 + 2, ((temporary >> 8) & 255)::integer);
        digest := set_byte(digest, word_index * 4 + 3, (temporary & 255)::integer);
    END LOOP;
    RETURN digest;
END;
$m7_sha1$;

CREATE FUNCTION pg_temp.m7_uuid_new_sha1(namespace_id uuid, value text)
RETURNS uuid
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $m7_uuid_new_sha1$
DECLARE
    digest bytea;
    encoded text;
BEGIN
    digest := pg_temp.m7_sha1(
        decode(replace(namespace_id::text, '-', ''), 'hex')
        || convert_to(value, 'UTF8')
    );
    digest := set_byte(digest, 6, (get_byte(digest, 6) & 15) | 80);
    digest := set_byte(digest, 8, (get_byte(digest, 8) & 63) | 128);
    encoded := encode(substring(digest FROM 1 FOR 16), 'hex');
    RETURN (
        substring(encoded, 1, 8) || '-' || substring(encoded, 9, 4)
        || '-' || substring(encoded, 13, 4) || '-' || substring(encoded, 17, 4)
        || '-' || substring(encoded, 21, 12)
    )::uuid;
END;
$m7_uuid_new_sha1$;

WITH historical_events (
    event_id, correlation, purpose, amount_minor, currency, created_at,
    debit_account, credit_account
) AS (
    SELECT 'capture:' || operation.operation_id::text,
           'payment:' || operation.payment_intent_id::text,
           'capture', operation.amount_minor, operation.currency,
           operation.completed_at,
           'provider_receivable', 'customer_funds_pending'
      FROM public.payment_operations AS operation
     WHERE operation.operation_type = 'capture'
       AND operation.state = 'succeeded'
       AND operation.amount_minor > 0
    UNION ALL
    SELECT 'ticket_issuance:' || pg_temp.m7_uuid_new_sha1(
               '8fd46050-e41f-5b3c-876c-77d4f4fa2570'::uuid,
               saga.saga_id::text || ':ticket_issuance'
           )::text,
           'payment:' || intent.payment_intent_id::text,
           'ticket_issuance', ticket_order.total_amount_minor,
           ticket_order.currency, ticket_order.created_at,
           'customer_funds_pending', 'ticket_sales'
      FROM public.ticket_order_shard_locators AS ticket_order
      JOIN public.payment_intents AS intent
        ON intent.reservation_id = ticket_order.reservation_id
       AND intent.amount_minor = ticket_order.total_amount_minor
       AND intent.currency = ticket_order.currency
      JOIN public.payment_operations AS capture
        ON capture.payment_intent_id = intent.payment_intent_id
       AND capture.operation_type = 'capture'
       AND capture.state = 'succeeded'
        AND capture.amount_minor = intent.amount_minor
        AND capture.currency = intent.currency
      JOIN public.payment_sagas AS saga
        ON saga.payment_intent_id = intent.payment_intent_id
       AND saga.reservation_id = intent.reservation_id
     WHERE ticket_order.total_amount_minor > 0
    UNION ALL
    SELECT 'refund:' || operation.operation_id::text,
           'payment:' || operation.payment_intent_id::text,
           'refund', operation.amount_minor, operation.currency,
           operation.completed_at,
           CASE WHEN EXISTS (
                SELECT 1
                  FROM public.payment_intents AS intent
                  JOIN public.ticket_order_shard_locators AS ticket_order
                    ON ticket_order.reservation_id = intent.reservation_id
                   AND ticket_order.total_amount_minor = operation.amount_minor
                   AND ticket_order.currency = operation.currency
                 WHERE intent.payment_intent_id = operation.payment_intent_id
           ) THEN 'ticket_sales' ELSE 'customer_funds_pending' END,
           'provider_refund_receivable'
      FROM public.payment_operations AS operation
     WHERE operation.operation_type = 'refund'
       AND operation.state = 'succeeded'
       AND operation.amount_minor > 0
), normalized_events AS (
    SELECT event.*,
           pg_temp.m7_uuid_new_sha1(
               '6ba7b812-9dad-11d1-80b4-00c04fd430c8'::uuid,
               'railway-ledger-v1:' || event.event_id
           ) AS transaction_id
      FROM historical_events AS event
)
INSERT INTO public.financial_ledger_transactions (
    transaction_id, event_id, correlation, purpose,
    currency, fingerprint, created_at
)
SELECT event.transaction_id, event.event_id, event.correlation, event.purpose,
       event.currency,
       sha256(convert_to(
            '{"kind":"append","original":"00000000-0000-0000-0000-000000000000"'
            || ',"event_id":"' || event.event_id
           || '","correlation":"' || event.correlation
           || '","purpose":"' || event.purpose
           || '","currency":"' || event.currency
           || '","postings":[{"Account":"' || event.debit_account
           || '","Side":"debit","AmountMinor":' || event.amount_minor::text
           || ',"Currency":"' || event.currency
           || '"},{"Account":"' || event.credit_account
           || '","Side":"credit","AmountMinor":' || event.amount_minor::text
           || ',"Currency":"' || event.currency || '"}]}',
           'UTF8'
       )),
       event.created_at
  FROM normalized_events AS event
ON CONFLICT (event_id) DO NOTHING;

WITH historical_events (
    event_id, amount_minor, currency, debit_account, credit_account
) AS (
    SELECT 'capture:' || operation.operation_id::text,
           operation.amount_minor, operation.currency,
           'provider_receivable', 'customer_funds_pending'
      FROM public.payment_operations AS operation
     WHERE operation.operation_type = 'capture'
       AND operation.state = 'succeeded'
       AND operation.amount_minor > 0
    UNION ALL
    SELECT 'ticket_issuance:' || pg_temp.m7_uuid_new_sha1(
               '8fd46050-e41f-5b3c-876c-77d4f4fa2570'::uuid,
               saga.saga_id::text || ':ticket_issuance'
           )::text,
           ticket_order.total_amount_minor, ticket_order.currency,
           'customer_funds_pending', 'ticket_sales'
      FROM public.ticket_order_shard_locators AS ticket_order
      JOIN public.payment_intents AS intent
        ON intent.reservation_id = ticket_order.reservation_id
       AND intent.amount_minor = ticket_order.total_amount_minor
       AND intent.currency = ticket_order.currency
      JOIN public.payment_operations AS capture
        ON capture.payment_intent_id = intent.payment_intent_id
       AND capture.operation_type = 'capture'
       AND capture.state = 'succeeded'
        AND capture.amount_minor = intent.amount_minor
        AND capture.currency = intent.currency
      JOIN public.payment_sagas AS saga
        ON saga.payment_intent_id = intent.payment_intent_id
       AND saga.reservation_id = intent.reservation_id
     WHERE ticket_order.total_amount_minor > 0
    UNION ALL
    SELECT 'refund:' || operation.operation_id::text,
           operation.amount_minor, operation.currency,
           CASE WHEN EXISTS (
                SELECT 1
                  FROM public.payment_intents AS intent
                  JOIN public.ticket_order_shard_locators AS ticket_order
                    ON ticket_order.reservation_id = intent.reservation_id
                   AND ticket_order.total_amount_minor = operation.amount_minor
                   AND ticket_order.currency = operation.currency
                 WHERE intent.payment_intent_id = operation.payment_intent_id
           ) THEN 'ticket_sales' ELSE 'customer_funds_pending' END,
           'provider_refund_receivable'
      FROM public.payment_operations AS operation
     WHERE operation.operation_type = 'refund'
       AND operation.state = 'succeeded'
       AND operation.amount_minor > 0
)
INSERT INTO public.financial_ledger_postings (
    transaction_id, posting_index, account_code, side,
    amount_minor, currency
)
SELECT transaction.transaction_id, posting.posting_index,
       posting.account_code, posting.side,
       event.amount_minor, event.currency
  FROM historical_events AS event
  JOIN public.financial_ledger_transactions AS transaction
    ON transaction.event_id = event.event_id
 CROSS JOIN LATERAL (VALUES
    (0::smallint, event.debit_account, 'debit'),
    (1::smallint, event.credit_account, 'credit')
 ) AS posting(posting_index, account_code, side)
ON CONFLICT (transaction_id, posting_index) DO NOTHING;

-- Normalized provider settlement and payout evidence. Gross amounts are signed
-- provider effects, fees are non-negative, and net=gross-fee.
CREATE TABLE public.provider_balance_transactions (
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    provider_record_id text NOT NULL CHECK (length(provider_record_id) BETWEEN 1 AND 200),
    payment_correlation text CHECK (payment_correlation IS NULL OR length(payment_correlation) BETWEEN 1 AND 200),
    operation_type text NOT NULL CHECK (operation_type IN ('capture','refund','fee','settlement','payout')),
    gross_minor bigint NOT NULL,
    fee_minor bigint NOT NULL CHECK (fee_minor >= 0),
    net_minor bigint NOT NULL,
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    available_at timestamptz,
    provider_created_at timestamptz NOT NULL,
    provider_settlement_id text CHECK (provider_settlement_id IS NULL OR length(provider_settlement_id) BETWEEN 1 AND 200),
    provider_payout_id text CHECK (provider_payout_id IS NULL OR length(provider_payout_id) BETWEEN 1 AND 200),
    payout_status text CHECK (payout_status IS NULL OR payout_status ~ '^[a-z][a-z0-9_]{0,63}$'),
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    imported_at timestamptz NOT NULL,
    PRIMARY KEY (provider, provider_account_id, provider_record_id),
    CHECK (gross_minor <> '-9223372036854775808'::bigint),
    CHECK (net_minor <> '-9223372036854775808'::bigint),
    CHECK ((gross_minor::numeric - fee_minor::numeric) = net_minor)
);

CREATE TABLE public.provider_settlement_batches (
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    provider_record_id text NOT NULL CHECK (length(provider_record_id) BETWEEN 1 AND 200),
    payment_correlation text CHECK (payment_correlation IS NULL OR length(payment_correlation) BETWEEN 1 AND 200),
    operation_type text NOT NULL CHECK (operation_type IN ('capture','refund','fee','settlement','payout')),
    gross_minor bigint NOT NULL,
    fee_minor bigint NOT NULL CHECK (fee_minor >= 0),
    net_minor bigint NOT NULL,
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    available_at timestamptz,
    provider_created_at timestamptz NOT NULL,
    provider_settlement_id text CHECK (provider_settlement_id IS NULL OR length(provider_settlement_id) BETWEEN 1 AND 200),
    provider_payout_id text CHECK (provider_payout_id IS NULL OR length(provider_payout_id) BETWEEN 1 AND 200),
    payout_status text CHECK (payout_status IS NULL OR payout_status ~ '^[a-z][a-z0-9_]{0,63}$'),
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    imported_at timestamptz NOT NULL,
    PRIMARY KEY (provider, provider_account_id, provider_record_id),
    CHECK (gross_minor <> '-9223372036854775808'::bigint),
    CHECK (net_minor <> '-9223372036854775808'::bigint),
    CHECK ((gross_minor::numeric - fee_minor::numeric) = net_minor)
);

CREATE TABLE public.provider_settlement_lines (
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    provider_record_id text NOT NULL CHECK (length(provider_record_id) BETWEEN 1 AND 200),
    payment_correlation text CHECK (payment_correlation IS NULL OR length(payment_correlation) BETWEEN 1 AND 200),
    operation_type text NOT NULL CHECK (operation_type IN ('capture','refund','fee','settlement','payout')),
    gross_minor bigint NOT NULL,
    fee_minor bigint NOT NULL CHECK (fee_minor >= 0),
    net_minor bigint NOT NULL,
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    available_at timestamptz,
    provider_created_at timestamptz NOT NULL,
    provider_settlement_id text CHECK (provider_settlement_id IS NULL OR length(provider_settlement_id) BETWEEN 1 AND 200),
    provider_payout_id text CHECK (provider_payout_id IS NULL OR length(provider_payout_id) BETWEEN 1 AND 200),
    payout_status text CHECK (payout_status IS NULL OR payout_status ~ '^[a-z][a-z0-9_]{0,63}$'),
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    imported_at timestamptz NOT NULL,
    PRIMARY KEY (provider, provider_account_id, provider_record_id),
    CHECK (gross_minor <> '-9223372036854775808'::bigint),
    CHECK (net_minor <> '-9223372036854775808'::bigint),
    CHECK ((gross_minor::numeric - fee_minor::numeric) = net_minor)
);

CREATE TABLE public.provider_payouts (
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    provider_record_id text NOT NULL CHECK (length(provider_record_id) BETWEEN 1 AND 200),
    payment_correlation text CHECK (payment_correlation IS NULL OR length(payment_correlation) BETWEEN 1 AND 200),
    operation_type text NOT NULL CHECK (operation_type IN ('capture','refund','fee','settlement','payout')),
    gross_minor bigint NOT NULL,
    fee_minor bigint NOT NULL CHECK (fee_minor >= 0),
    net_minor bigint NOT NULL,
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    available_at timestamptz,
    provider_created_at timestamptz NOT NULL,
    provider_settlement_id text CHECK (provider_settlement_id IS NULL OR length(provider_settlement_id) BETWEEN 1 AND 200),
    provider_payout_id text CHECK (provider_payout_id IS NULL OR length(provider_payout_id) BETWEEN 1 AND 200),
    payout_status text NOT NULL CHECK (payout_status ~ '^[a-z][a-z0-9_]{0,63}$'),
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    imported_at timestamptz NOT NULL,
    PRIMARY KEY (provider, provider_account_id, provider_record_id, payout_status),
    CHECK (gross_minor <> '-9223372036854775808'::bigint),
    CHECK (net_minor <> '-9223372036854775808'::bigint),
    CHECK ((gross_minor::numeric - fee_minor::numeric) = net_minor)
);

CREATE TABLE public.provider_payout_lines (
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    provider_record_id text NOT NULL CHECK (length(provider_record_id) BETWEEN 1 AND 200),
    payment_correlation text CHECK (payment_correlation IS NULL OR length(payment_correlation) BETWEEN 1 AND 200),
    operation_type text NOT NULL CHECK (operation_type IN ('capture','refund','fee','settlement','payout')),
    gross_minor bigint NOT NULL,
    fee_minor bigint NOT NULL CHECK (fee_minor >= 0),
    net_minor bigint NOT NULL,
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    available_at timestamptz,
    provider_created_at timestamptz NOT NULL,
    provider_settlement_id text CHECK (provider_settlement_id IS NULL OR length(provider_settlement_id) BETWEEN 1 AND 200),
    provider_payout_id text CHECK (provider_payout_id IS NULL OR length(provider_payout_id) BETWEEN 1 AND 200),
    payout_status text CHECK (payout_status IS NULL OR payout_status ~ '^[a-z][a-z0-9_]{0,63}$'),
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    imported_at timestamptz NOT NULL,
    PRIMARY KEY (provider, provider_account_id, provider_record_id),
    CHECK (gross_minor <> '-9223372036854775808'::bigint),
    CHECK (net_minor <> '-9223372036854775808'::bigint),
    CHECK ((gross_minor::numeric - fee_minor::numeric) = net_minor)
);

CREATE TABLE public.provider_settlement_import_checkpoints (
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    cursor text NOT NULL DEFAULT '' CHECK (octet_length(cursor) <= 1024),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text CHECK (
        lease_owner IS NULL OR lease_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    lease_token uuid,
    lease_until timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider, provider_account_id),
    CHECK (
        (lease_owner IS NULL AND lease_token IS NULL AND lease_until IS NULL)
        OR
        (lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
    )
);

CREATE INDEX provider_settlement_import_due_idx
    ON public.provider_settlement_import_checkpoints(next_attempt_at, lease_until, provider, provider_account_id);

CREATE TABLE public.provider_settlement_import_conflicts (
    conflict_id uuid PRIMARY KEY,
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    record_kind text NOT NULL CHECK (
        record_kind IN (
            'balance_transaction', 'settlement_batch', 'settlement_line',
            'payout', 'payout_line'
        )
    ),
    provider_record_id text NOT NULL CHECK (length(provider_record_id) BETWEEN 1 AND 200),
    stored_hash bytea NOT NULL CHECK (octet_length(stored_hash) = 32),
    incoming_hash bytea NOT NULL CHECK (octet_length(incoming_hash) = 32),
    detected_at timestamptz NOT NULL,
    UNIQUE (
        provider, provider_account_id, record_kind,
        provider_record_id, incoming_hash
    ),
    CHECK (stored_hash <> incoming_hash)
);

CREATE TABLE public.settlement_reconciliation_runs (
    run_id uuid PRIMARY KEY,
    scope_type text NOT NULL CHECK (
        scope_type IN ('payment', 'period', 'settlement', 'payout')
    ),
    scope_value text NOT NULL CHECK (length(scope_value) BETWEEN 1 AND 200),
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    pages integer NOT NULL CHECK (pages >= 0),
    examined integer NOT NULL CHECK (examined >= 0),
    completed boolean NOT NULL,
    bounded boolean NOT NULL,
    finding_count integer NOT NULL CHECK (finding_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (completed_at >= started_at),
    CHECK (NOT (completed AND bounded))
);

CREATE TABLE public.settlement_reconciliation_mismatches (
    run_id uuid NOT NULL
        REFERENCES public.settlement_reconciliation_runs(run_id)
        ON DELETE RESTRICT,
    finding_index integer NOT NULL CHECK (finding_index >= 0),
    correlation text NOT NULL CHECK (length(correlation) BETWEEN 1 AND 200),
    evidence_kind text NOT NULL CHECK (
        evidence_kind IN (
            'provider', 'payment_operation', 'ledger',
            'settlement', 'payout'
        )
    ),
    reason text NOT NULL CHECK (
        reason IN (
            'missing', 'unexpected', 'amount', 'currency', 'fee',
            'duplicate', 'age', 'event_conflict', 'ledger_imbalance',
            'payout_lifecycle'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, finding_index)
);

-- Operator acknowledgement is append-only evidence. It never mutates the
-- observed reconciliation run or any financial/provider fact.
CREATE TABLE public.settlement_reconciliation_reviews (
    review_id uuid PRIMARY KEY,
    run_id uuid NOT NULL
        REFERENCES public.settlement_reconciliation_runs(run_id)
        ON DELETE RESTRICT,
    reviewer_id text NOT NULL CHECK (
        reviewer_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$'
    ),
    disposition text NOT NULL CHECK (
        disposition IN (
            'acknowledged', 'investigating',
            'accepted_exception', 'resolved_external'
        )
    ),
    evidence_hash bytea NOT NULL CHECK (octet_length(evidence_hash) = 32),
    reviewed_at timestamptz NOT NULL,
    UNIQUE (run_id, review_id)
);

CREATE FUNCTION public.guard_m7_evidence_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $m7_evidence_immutable$
BEGIN
    RAISE EXCEPTION 'Milestone 7 evidence is immutable'
        USING ERRCODE = '23514';
END;
$m7_evidence_immutable$;

CREATE TRIGGER provider_balance_transactions_guard_immutable
BEFORE UPDATE OR DELETE ON public.provider_balance_transactions
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();
CREATE TRIGGER provider_settlement_batches_guard_immutable
BEFORE UPDATE OR DELETE ON public.provider_settlement_batches
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();
CREATE TRIGGER provider_settlement_lines_guard_immutable
BEFORE UPDATE OR DELETE ON public.provider_settlement_lines
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();
CREATE TRIGGER provider_payouts_guard_immutable
BEFORE UPDATE OR DELETE ON public.provider_payouts
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();
CREATE TRIGGER provider_payout_lines_guard_immutable
BEFORE UPDATE OR DELETE ON public.provider_payout_lines
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();
CREATE TRIGGER provider_settlement_import_conflicts_guard_immutable
BEFORE UPDATE OR DELETE ON public.provider_settlement_import_conflicts
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();
CREATE TRIGGER settlement_reconciliation_runs_guard_immutable
BEFORE UPDATE OR DELETE ON public.settlement_reconciliation_runs
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();
CREATE TRIGGER settlement_reconciliation_mismatches_guard_immutable
BEFORE UPDATE OR DELETE ON public.settlement_reconciliation_mismatches
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();
CREATE TRIGGER settlement_reconciliation_reviews_guard_immutable
BEFORE UPDATE OR DELETE ON public.settlement_reconciliation_reviews
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();

-- Whole-ticket partial refund control authority. Physical ticket/seat changes
-- are represented only by stable command and receipt evidence.
CREATE TABLE public.ticket_refund_requests (
    refund_request_id uuid PRIMARY KEY,
    payment_intent_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    ticket_order_id uuid NOT NULL
        REFERENCES public.ticket_order_shard_locators(ticket_order_id)
        ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    owner_user_id uuid NOT NULL
        REFERENCES public.users(id) ON DELETE RESTRICT,
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    idempotency_key_hash bytea NOT NULL
        CHECK (octet_length(idempotency_key_hash) = 32),
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    eligibility_cutoff_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'created' CHECK (
        state IN (
            'created', 'validating', 'refund_pending',
            'provider_uncertain', 'refund_succeeded',
            'shard_compensating', 'completed', 'manual_review', 'failed'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (owner_user_id, idempotency_key_hash),
    UNIQUE (refund_request_id, amount_minor, currency),
    FOREIGN KEY (payment_intent_id, reservation_id)
        REFERENCES public.payment_intents(payment_intent_id, reservation_id)
        ON DELETE RESTRICT,
    CHECK (created_at < eligibility_cutoff_at),
    CHECK (
        (state IN ('completed', 'failed') AND completed_at IS NOT NULL)
        OR
        (state NOT IN ('completed', 'failed') AND completed_at IS NULL)
    )
);

CREATE INDEX ticket_refund_requests_owner_created_idx
    ON public.ticket_refund_requests(
        owner_user_id, created_at DESC, refund_request_id
    );
CREATE INDEX ticket_refund_requests_work_idx
    ON public.ticket_refund_requests(state, updated_at, refund_request_id)
    WHERE state IN (
        'validating', 'refund_pending', 'provider_uncertain',
        'refund_succeeded', 'shard_compensating', 'manual_review'
    );

CREATE TABLE public.ticket_refund_request_items (
    refund_request_id uuid NOT NULL
        REFERENCES public.ticket_refund_requests(refund_request_id)
        ON DELETE RESTRICT,
    ticket_id uuid NOT NULL
        REFERENCES public.ticket_shard_locators(ticket_id)
        ON DELETE RESTRICT,
    fare_amount_minor bigint NOT NULL CHECK (fare_amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    state text NOT NULL DEFAULT 'selected' CHECK (
        state IN ('selected', 'refund_pending', 'refunded', 'failed')
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ticket_refund_request_items_request_ticket_key
        PRIMARY KEY (refund_request_id, ticket_id)
);

CREATE INDEX ticket_refund_request_items_request_idx
    ON public.ticket_refund_request_items(refund_request_id, ticket_id);
CREATE UNIQUE INDEX ticket_refund_request_items_active_ticket_idx
    ON public.ticket_refund_request_items(ticket_id)
    WHERE state <> 'failed';

CREATE TABLE public.ticket_refund_sagas (
    refund_saga_id uuid PRIMARY KEY,
    refund_request_id uuid NOT NULL UNIQUE
        REFERENCES public.ticket_refund_requests(refund_request_id)
        ON DELETE RESTRICT,
    current_step text NOT NULL DEFAULT 'validate' CHECK (
        current_step IN (
            'validate', 'refund_provider', 'query_provider', 'release_prepared',
            'compensate_shard', 'finalize', 'complete'
        )
    ),
    state text NOT NULL DEFAULT 'created' CHECK (
        state IN (
            'created', 'validating', 'refund_pending',
            'provider_uncertain', 'refund_succeeded',
            'shard_compensating', 'completed', 'manual_review', 'failed'
        )
    ),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 1000000),
    prepare_attempts integer NOT NULL DEFAULT 0 CHECK (prepare_attempts BETWEEN 0 AND 1000000),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text CHECK (
        lease_owner IS NULL OR lease_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    lease_until timestamptz,
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL)),
    CHECK (
        (state = 'completed' AND completed_at IS NOT NULL)
        OR (state <> 'completed' AND completed_at IS NULL)
    )
);

CREATE INDEX ticket_refund_sagas_claim_idx
    ON public.ticket_refund_sagas(
        state, next_attempt_at, lease_until, updated_at, refund_saga_id
    )
    WHERE state IN (
        'created', 'validating', 'refund_pending',
        'provider_uncertain', 'refund_succeeded', 'shard_compensating'
    );

CREATE TABLE public.ticket_refund_operations (
    refund_operation_id uuid PRIMARY KEY,
    refund_request_id uuid NOT NULL UNIQUE,
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_payment_id text NOT NULL CHECK (
        provider_payment_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    provider_refund_id text CHECK (
        provider_refund_id IS NULL
        OR provider_refund_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    provider_idempotency_key_hash bytea NOT NULL
        CHECK (octet_length(provider_idempotency_key_hash) = 32),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    state text NOT NULL DEFAULT 'pending' CHECK (
        state IN (
            'pending', 'processing', 'uncertain', 'succeeded',
            'failed_retryable', 'failed_permanent', 'manual_review'
        )
    ),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 1000000),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text CHECK (
        lease_owner IS NULL OR lease_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    lease_until timestamptz,
    captured_total_minor bigint CHECK (captured_total_minor >= 0),
    refunded_total_minor bigint CHECK (refunded_total_minor >= 0),
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
    UNIQUE (provider, provider_refund_id),
    UNIQUE (provider, provider_idempotency_key_hash),
    FOREIGN KEY (refund_request_id, amount_minor, currency)
        REFERENCES public.ticket_refund_requests(
            refund_request_id, amount_minor, currency
        ) ON DELETE RESTRICT,
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL)),
    CHECK (
        refunded_total_minor IS NULL
        OR captured_total_minor IS NOT NULL
           AND refunded_total_minor <= captured_total_minor
    ),
    CHECK (
        (state IN ('succeeded', 'failed_permanent') AND completed_at IS NOT NULL)
        OR (state NOT IN ('succeeded', 'failed_permanent') AND completed_at IS NULL)
    )
);

CREATE INDEX ticket_refund_operations_claim_idx
    ON public.ticket_refund_operations(
        state, next_attempt_at, lease_until, updated_at, refund_operation_id
    )
    WHERE state IN ('pending', 'processing', 'uncertain', 'failed_retryable');

CREATE TABLE public.ticket_refund_prepare_bindings (
    receipt_id uuid PRIMARY KEY,
    refund_request_id uuid NOT NULL UNIQUE
        REFERENCES public.ticket_refund_requests(refund_request_id) ON DELETE RESTRICT,
    refund_operation_id uuid NOT NULL UNIQUE
        REFERENCES public.ticket_refund_operations(refund_operation_id) ON DELETE RESTRICT,
    command_id uuid NOT NULL UNIQUE,
    train_run_id uuid NOT NULL REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    selected_ticket_count integer NOT NULL CHECK (selected_ticket_count BETWEEN 1 AND 1000),
    prepared_at timestamptz NOT NULL
);

CREATE TABLE public.ticket_refund_manual_reviews (
    review_id uuid PRIMARY KEY,
    refund_request_id uuid NOT NULL
        REFERENCES public.ticket_refund_requests(refund_request_id)
        ON DELETE RESTRICT,
    reason_category text NOT NULL CHECK (
        reason_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    state text NOT NULL DEFAULT 'open' CHECK (
        state IN ('open', 'resolved', 'dismissed')
    ),
    evidence_fingerprint bytea NOT NULL
        CHECK (octet_length(evidence_fingerprint) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    UNIQUE (refund_request_id, reason_category, evidence_fingerprint),
    CHECK ((state = 'open') = (resolved_at IS NULL))
);

CREATE FUNCTION public.guard_ticket_refund_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $ticket_refund_identity$
BEGIN
    IF TG_TABLE_NAME = 'ticket_refund_requests' AND (
        NEW.refund_request_id <> OLD.refund_request_id
        OR NEW.payment_intent_id <> OLD.payment_intent_id
        OR NEW.reservation_id <> OLD.reservation_id
        OR NEW.ticket_order_id <> OLD.ticket_order_id
        OR NEW.train_run_id <> OLD.train_run_id
        OR NEW.owner_user_id <> OLD.owner_user_id
        OR NEW.provider <> OLD.provider
        OR NEW.idempotency_key_hash <> OLD.idempotency_key_hash
        OR NEW.request_fingerprint <> OLD.request_fingerprint
        OR NEW.amount_minor <> OLD.amount_minor
        OR NEW.currency <> OLD.currency
        OR NEW.eligibility_cutoff_at <> OLD.eligibility_cutoff_at
    ) THEN
        RAISE EXCEPTION 'ticket refund request identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF TG_TABLE_NAME = 'ticket_refund_request_items' AND (
        NEW.refund_request_id <> OLD.refund_request_id
        OR NEW.ticket_id <> OLD.ticket_id
        OR NEW.fare_amount_minor <> OLD.fare_amount_minor
        OR NEW.currency <> OLD.currency
    ) THEN
        RAISE EXCEPTION 'ticket refund item identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$ticket_refund_identity$;

CREATE TRIGGER ticket_refund_requests_guard_identity
BEFORE UPDATE ON public.ticket_refund_requests
FOR EACH ROW EXECUTE FUNCTION public.guard_ticket_refund_identity();
CREATE TRIGGER ticket_refund_request_items_guard_identity
BEFORE UPDATE ON public.ticket_refund_request_items
FOR EACH ROW EXECUTE FUNCTION public.guard_ticket_refund_identity();
CREATE TRIGGER ticket_refund_requests_set_updated_at
BEFORE UPDATE ON public.ticket_refund_requests
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE FUNCTION public.guard_ticket_refund_prepare_binding_immutable()
RETURNS trigger LANGUAGE plpgsql AS $ticket_refund_prepare_binding_immutable$
BEGIN
    RAISE EXCEPTION 'ticket refund prepare binding is immutable' USING ERRCODE = '23514';
END;
$ticket_refund_prepare_binding_immutable$;
CREATE TRIGGER ticket_refund_prepare_bindings_guard_immutable
BEFORE UPDATE OR DELETE ON public.ticket_refund_prepare_bindings
FOR EACH ROW EXECUTE FUNCTION public.guard_ticket_refund_prepare_binding_immutable();
CREATE TRIGGER ticket_refund_request_items_set_updated_at
BEFORE UPDATE ON public.ticket_refund_request_items
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER ticket_refund_sagas_set_updated_at
BEFORE UPDATE ON public.ticket_refund_sagas
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER ticket_refund_operations_set_updated_at
BEFORE UPDATE ON public.ticket_refund_operations
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE FUNCTION public.guard_ticket_refund_request_state()
RETURNS trigger LANGUAGE plpgsql AS $ticket_refund_request_state$
BEGIN
    IF OLD.state IN ('completed','failed') AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal ticket refund request is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state='created' AND NEW.state IN ('refund_pending','manual_review')) OR
        (OLD.state='refund_pending' AND NEW.state IN ('provider_uncertain','refund_succeeded','failed','manual_review')) OR
        (OLD.state='provider_uncertain' AND NEW.state IN ('refund_pending','refund_succeeded','manual_review')) OR
        (OLD.state='refund_succeeded' AND NEW.state IN ('shard_compensating','manual_review')) OR
        (OLD.state='shard_compensating' AND NEW.state IN ('completed','manual_review'))
    ) THEN
        RAISE EXCEPTION 'illegal ticket refund request state transition' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$ticket_refund_request_state$;
CREATE TRIGGER ticket_refund_requests_guard_state
BEFORE UPDATE ON public.ticket_refund_requests
FOR EACH ROW EXECUTE FUNCTION public.guard_ticket_refund_request_state();

CREATE FUNCTION public.guard_ticket_refund_operation_state()
RETURNS trigger LANGUAGE plpgsql AS $ticket_refund_operation_state$
BEGIN
    IF NEW.refund_operation_id <> OLD.refund_operation_id
       OR NEW.refund_request_id <> OLD.refund_request_id
       OR NEW.provider <> OLD.provider
       OR NEW.provider_payment_id <> OLD.provider_payment_id
       OR NEW.provider_idempotency_key_hash <> OLD.provider_idempotency_key_hash
       OR NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency THEN
        RAISE EXCEPTION 'ticket refund operation identity is immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.state IN ('succeeded','failed_permanent') AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal ticket refund operation is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state='pending' AND NEW.state IN ('processing','failed_permanent','manual_review')) OR
        (OLD.state='processing' AND NEW.state IN ('pending','uncertain','succeeded','failed_retryable','failed_permanent','manual_review')) OR
        (OLD.state='uncertain' AND NEW.state IN ('processing','manual_review')) OR
        (OLD.state='failed_retryable' AND NEW.state IN ('processing','failed_permanent','manual_review'))
    ) THEN
        RAISE EXCEPTION 'illegal ticket refund operation state transition' USING ERRCODE='23514';
    END IF;
    IF NEW.state='succeeded' AND (
        NEW.provider_refund_id IS NULL OR NEW.response_fingerprint IS NULL
        OR NEW.captured_total_minor IS NULL OR NEW.refunded_total_minor IS NULL
        OR NEW.refunded_total_minor < NEW.amount_minor
        OR NEW.refunded_total_minor > NEW.captured_total_minor
        OR NEW.completed_at IS NULL
    ) THEN
        RAISE EXCEPTION 'succeeded ticket refund operation lacks exact provider proof' USING ERRCODE='23514';
    END IF;
    IF NEW.state='failed_permanent' AND (NEW.provider_refund_id IS NOT NULL OR NEW.completed_at IS NULL) THEN
        RAISE EXCEPTION 'failed ticket refund operation has provider effect evidence' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$ticket_refund_operation_state$;
CREATE TRIGGER ticket_refund_operations_guard_state
BEFORE UPDATE ON public.ticket_refund_operations
FOR EACH ROW EXECUTE FUNCTION public.guard_ticket_refund_operation_state();

-- Full-intent refunds and whole-ticket subset refunds share one provider
-- capture. Serialize both insertion paths on the intent row so concurrent API
-- requests cannot schedule cumulative refunds above the captured amount.
CREATE FUNCTION public.guard_refund_lane_exclusive()
RETURNS trigger
LANGUAGE plpgsql
AS $refund_lane_exclusive$
DECLARE
    target_intent_id uuid;
BEGIN
    IF TG_TABLE_NAME = 'payment_operations'
       AND NEW.operation_type <> 'refund' THEN
        RETURN NEW;
    END IF;
    target_intent_id := NEW.payment_intent_id;
    PERFORM 1 FROM public.payment_intents
     WHERE payment_intent_id = target_intent_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'payment intent does not exist'
            USING ERRCODE = '23503';
    END IF;
    IF TG_TABLE_NAME = 'payment_operations' THEN
        IF NEW.operation_type = 'refund' AND EXISTS (
            SELECT 1 FROM public.ticket_refund_requests
             WHERE payment_intent_id = target_intent_id
               AND state <> 'failed'
        ) THEN
            RAISE EXCEPTION 'partial refund evidence already exists'
                USING ERRCODE = '23514';
        END IF;
    ELSIF EXISTS (
        SELECT 1 FROM public.payment_operations
         WHERE payment_intent_id = target_intent_id
           AND operation_type = 'refund'
    ) THEN
        RAISE EXCEPTION 'full refund evidence already exists'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$refund_lane_exclusive$;

CREATE TRIGGER payment_operations_guard_refund_lane
BEFORE INSERT ON public.payment_operations
FOR EACH ROW EXECUTE FUNCTION public.guard_refund_lane_exclusive();
CREATE TRIGGER ticket_refund_requests_guard_refund_lane
BEFORE INSERT ON public.ticket_refund_requests
FOR EACH ROW EXECUTE FUNCTION public.guard_refund_lane_exclusive();

-- The control database retains three logical booking layouts for physical
-- migration and reverse-migration compatibility. Keep their Milestone 7
-- ticket/refund contract identical to a booking-shard v3 source, while
-- leaving assignment_generation owned only by physical shards.
ALTER TABLE public.reservations
    DROP CONSTRAINT reservations_status_check,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    ADD CONSTRAINT reservations_status_check CHECK (
        status IN (
            'held', 'payment_pending', 'payment_review', 'confirmed',
            'partially_refund_pending', 'partially_refunded',
            'refund_pending', 'expired', 'cancelled'
        )
    ),
    ADD CONSTRAINT reservations_payment_snapshot_check CHECK (
        (
            payment_intent_id IS NULL
            AND payment_amount_minor IS NULL
            AND payment_currency IS NULL
            AND payment_grace_expires_at IS NULL
            AND status NOT IN (
                'payment_pending', 'payment_review',
                'partially_refund_pending', 'partially_refunded',
                'refund_pending'
            )
        )
        OR
        (
            payment_intent_id IS NOT NULL
            AND status IN (
                'payment_pending', 'payment_review', 'confirmed',
                'partially_refund_pending', 'partially_refunded',
                'refund_pending', 'cancelled'
            )
            AND payment_amount_minor = total_amount_minor
            AND payment_amount_minor >= 0
            AND payment_currency = currency
            AND payment_currency ~ '^[A-Z]{3}$'
            AND payment_grace_expires_at > created_at
        )
    );

ALTER TABLE booking_shard_0.reservations
    DROP CONSTRAINT reservations_status_check,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    ADD CONSTRAINT reservations_status_check CHECK (
        status IN (
            'held', 'payment_pending', 'payment_review', 'confirmed',
            'partially_refund_pending', 'partially_refunded',
            'refund_pending', 'expired', 'cancelled'
        )
    ),
    ADD CONSTRAINT reservations_payment_snapshot_check CHECK (
        (
            payment_intent_id IS NULL
            AND payment_amount_minor IS NULL
            AND payment_currency IS NULL
            AND payment_grace_expires_at IS NULL
            AND status NOT IN (
                'payment_pending', 'payment_review',
                'partially_refund_pending', 'partially_refunded',
                'refund_pending'
            )
        )
        OR
        (
            payment_intent_id IS NOT NULL
            AND status IN (
                'payment_pending', 'payment_review', 'confirmed',
                'partially_refund_pending', 'partially_refunded',
                'refund_pending', 'cancelled'
            )
            AND payment_amount_minor = total_amount_minor
            AND payment_amount_minor >= 0
            AND payment_currency = currency
            AND payment_currency ~ '^[A-Z]{3}$'
            AND payment_grace_expires_at > created_at
        )
    );

ALTER TABLE booking_shard_1.reservations
    DROP CONSTRAINT reservations_status_check,
    DROP CONSTRAINT reservations_payment_snapshot_check,
    ADD CONSTRAINT reservations_status_check CHECK (
        status IN (
            'held', 'payment_pending', 'payment_review', 'confirmed',
            'partially_refund_pending', 'partially_refunded',
            'refund_pending', 'expired', 'cancelled'
        )
    ),
    ADD CONSTRAINT reservations_payment_snapshot_check CHECK (
        (
            payment_intent_id IS NULL
            AND payment_amount_minor IS NULL
            AND payment_currency IS NULL
            AND payment_grace_expires_at IS NULL
            AND status NOT IN (
                'payment_pending', 'payment_review',
                'partially_refund_pending', 'partially_refunded',
                'refund_pending'
            )
        )
        OR
        (
            payment_intent_id IS NOT NULL
            AND status IN (
                'payment_pending', 'payment_review', 'confirmed',
                'partially_refund_pending', 'partially_refunded',
                'refund_pending', 'cancelled'
            )
            AND payment_amount_minor = total_amount_minor
            AND payment_amount_minor >= 0
            AND payment_currency = currency
            AND payment_currency ~ '^[A-Z]{3}$'
            AND payment_grace_expires_at > created_at
        )
    );

ALTER TABLE public.ticket_orders
    DROP CONSTRAINT ticket_orders_status_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    ADD CONSTRAINT ticket_orders_status_check CHECK (
        status IN (
            'confirmed', 'payment_pending', 'payment_authorized',
            'payment_captured', 'issuance_pending', 'issued',
            'partial_refund_pending', 'partially_refunded',
            'refund_pending', 'refunded', 'cancelled', 'manual_review'
        )
    ),
    ADD CONSTRAINT ticket_orders_payment_amounts_check CHECK (
        authorized_amount_minor IN (0, total_amount_minor)
        AND captured_amount_minor IN (0, authorized_amount_minor)
        AND refunded_amount_minor >= 0
        AND refunded_amount_minor <= captured_amount_minor
        AND captured_amount_minor <= authorized_amount_minor
        AND authorized_amount_minor <= total_amount_minor
    ),
    ADD CONSTRAINT ticket_orders_payment_state_check CHECK (
        (status <> 'payment_authorized'
         OR authorized_amount_minor = total_amount_minor)
        AND (status NOT IN (
            'payment_captured', 'issuance_pending', 'issued',
            'partial_refund_pending', 'partially_refunded',
            'refund_pending', 'refunded'
        ) OR captured_amount_minor = total_amount_minor)
        AND (status <> 'partially_refunded'
             OR (refunded_amount_minor > 0
                 AND refunded_amount_minor < captured_amount_minor))
        AND (status <> 'refunded'
             OR refunded_amount_minor = captured_amount_minor)
        AND (status <> 'cancelled' OR captured_amount_minor = 0
             OR refunded_amount_minor = captured_amount_minor)
    );

ALTER TABLE booking_shard_0.ticket_orders
    DROP CONSTRAINT ticket_orders_status_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    ADD CONSTRAINT ticket_orders_status_check CHECK (
        status IN (
            'confirmed', 'payment_pending', 'payment_authorized',
            'payment_captured', 'issuance_pending', 'issued',
            'partial_refund_pending', 'partially_refunded',
            'refund_pending', 'refunded', 'cancelled', 'manual_review'
        )
    ),
    ADD CONSTRAINT ticket_orders_payment_amounts_check CHECK (
        authorized_amount_minor IN (0, total_amount_minor)
        AND captured_amount_minor IN (0, authorized_amount_minor)
        AND refunded_amount_minor >= 0
        AND refunded_amount_minor <= captured_amount_minor
        AND captured_amount_minor <= authorized_amount_minor
        AND authorized_amount_minor <= total_amount_minor
    ),
    ADD CONSTRAINT ticket_orders_payment_state_check CHECK (
        (status <> 'payment_authorized'
         OR authorized_amount_minor = total_amount_minor)
        AND (status NOT IN (
            'payment_captured', 'issuance_pending', 'issued',
            'partial_refund_pending', 'partially_refunded',
            'refund_pending', 'refunded'
        ) OR captured_amount_minor = total_amount_minor)
        AND (status <> 'partially_refunded'
             OR (refunded_amount_minor > 0
                 AND refunded_amount_minor < captured_amount_minor))
        AND (status <> 'refunded'
             OR refunded_amount_minor = captured_amount_minor)
        AND (status <> 'cancelled' OR captured_amount_minor = 0
             OR refunded_amount_minor = captured_amount_minor)
    );

ALTER TABLE booking_shard_1.ticket_orders
    DROP CONSTRAINT ticket_orders_status_check,
    DROP CONSTRAINT ticket_orders_payment_amounts_check,
    DROP CONSTRAINT ticket_orders_payment_state_check,
    ADD CONSTRAINT ticket_orders_status_check CHECK (
        status IN (
            'confirmed', 'payment_pending', 'payment_authorized',
            'payment_captured', 'issuance_pending', 'issued',
            'partial_refund_pending', 'partially_refunded',
            'refund_pending', 'refunded', 'cancelled', 'manual_review'
        )
    ),
    ADD CONSTRAINT ticket_orders_payment_amounts_check CHECK (
        authorized_amount_minor IN (0, total_amount_minor)
        AND captured_amount_minor IN (0, authorized_amount_minor)
        AND refunded_amount_minor >= 0
        AND refunded_amount_minor <= captured_amount_minor
        AND captured_amount_minor <= authorized_amount_minor
        AND authorized_amount_minor <= total_amount_minor
    ),
    ADD CONSTRAINT ticket_orders_payment_state_check CHECK (
        (status <> 'payment_authorized'
         OR authorized_amount_minor = total_amount_minor)
        AND (status NOT IN (
            'payment_captured', 'issuance_pending', 'issued',
            'partial_refund_pending', 'partially_refunded',
            'refund_pending', 'refunded'
        ) OR captured_amount_minor = total_amount_minor)
        AND (status <> 'partially_refunded'
             OR (refunded_amount_minor > 0
                 AND refunded_amount_minor < captured_amount_minor))
        AND (status <> 'refunded'
             OR refunded_amount_minor = captured_amount_minor)
        AND (status <> 'cancelled' OR captured_amount_minor = 0
             OR refunded_amount_minor = captured_amount_minor)
    );

ALTER TABLE public.tickets
    DROP CONSTRAINT tickets_status_check,
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN ('pending', 'active', 'refund_pending', 'refunded', 'cancelled')
    );
ALTER TABLE booking_shard_0.tickets
    DROP CONSTRAINT tickets_status_check,
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN ('pending', 'active', 'refund_pending', 'refunded', 'cancelled')
    );
ALTER TABLE booking_shard_1.tickets
    DROP CONSTRAINT tickets_status_check,
    ADD CONSTRAINT tickets_status_check CHECK (
        status IN ('pending', 'active', 'refund_pending', 'refunded', 'cancelled')
    );

CREATE FUNCTION public.guard_control_ticket_refund_transition()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $control_ticket_refund_transition$
BEGIN
    IF OLD.status IN ('refunded', 'cancelled')
       AND NEW.status IS DISTINCT FROM OLD.status THEN
        RAISE EXCEPTION 'terminal ticket state is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.status = 'refund_pending'
       AND NEW.status NOT IN (
           'refund_pending', 'active', 'refunded', 'cancelled'
       ) THEN
        RAISE EXCEPTION 'invalid ticket refund transition'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$control_ticket_refund_transition$;

CREATE TRIGGER tickets_guard_refund_transition
BEFORE UPDATE OF status ON public.tickets
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_transition();
CREATE TRIGGER tickets_guard_refund_transition
BEFORE UPDATE OF status ON booking_shard_0.tickets
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_transition();
CREATE TRIGGER tickets_guard_refund_transition
BEFORE UPDATE OF status ON booking_shard_1.tickets
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_transition();

CREATE TABLE public.ticket_refund_prepare_receipts_physical (
    id uuid PRIMARY KEY,
    command_id uuid NOT NULL UNIQUE,
    refund_request_id uuid NOT NULL UNIQUE,
    refund_operation_id uuid NOT NULL UNIQUE,
    payment_intent_id uuid NOT NULL,
    reservation_id uuid NOT NULL REFERENCES public.reservations(id) ON DELETE RESTRICT,
    ticket_order_id uuid NOT NULL REFERENCES public.ticket_orders(id) ON DELETE RESTRICT,
    train_run_id uuid NOT NULL REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    ticket_ids uuid[] NOT NULL CHECK (cardinality(ticket_ids) BETWEEN 1 AND 1000),
    prior_order_state text NOT NULL CHECK (prior_order_state IN ('issued','partially_refunded')),
    prior_reservation_state text NOT NULL CHECK (prior_reservation_state IN ('confirmed','partially_refunded')),
    state text NOT NULL CHECK (state IN ('prepared','released','applied')),
    requested_at timestamptz NOT NULL,
    eligibility_cutoff_at timestamptz NOT NULL,
    prepared_at timestamptz NOT NULL,
    resolved_at timestamptz,
    CHECK (requested_at < eligibility_cutoff_at),
    CHECK ((state='prepared') = (resolved_at IS NULL))
);

CREATE TABLE booking_shard_0.ticket_refund_prepare_receipts
    (LIKE public.ticket_refund_prepare_receipts_physical INCLUDING ALL,
     FOREIGN KEY (reservation_id) REFERENCES booking_shard_0.reservations(id) ON DELETE RESTRICT,
     FOREIGN KEY (ticket_order_id) REFERENCES booking_shard_0.ticket_orders(id) ON DELETE RESTRICT,
     FOREIGN KEY (train_run_id) REFERENCES public.train_runs(id) ON DELETE RESTRICT);
CREATE TABLE booking_shard_1.ticket_refund_prepare_receipts
    (LIKE public.ticket_refund_prepare_receipts_physical INCLUDING ALL,
     FOREIGN KEY (reservation_id) REFERENCES booking_shard_1.reservations(id) ON DELETE RESTRICT,
     FOREIGN KEY (ticket_order_id) REFERENCES booking_shard_1.ticket_orders(id) ON DELETE RESTRICT,
     FOREIGN KEY (train_run_id) REFERENCES public.train_runs(id) ON DELETE RESTRICT);
ALTER TABLE public.ticket_refund_prepare_receipts_physical RENAME TO ticket_refund_prepare_receipts;

CREATE FUNCTION public.guard_ticket_refund_prepare_receipt_transition()
RETURNS trigger LANGUAGE plpgsql AS $ticket_refund_prepare_receipt_transition$
BEGIN
    IF NEW.id <> OLD.id OR NEW.command_id <> OLD.command_id
       OR NEW.refund_request_id <> OLD.refund_request_id
       OR NEW.refund_operation_id <> OLD.refund_operation_id
       OR NEW.payment_intent_id <> OLD.payment_intent_id
       OR NEW.reservation_id <> OLD.reservation_id OR NEW.ticket_order_id <> OLD.ticket_order_id
       OR NEW.train_run_id <> OLD.train_run_id OR NEW.assignment_generation <> OLD.assignment_generation
       OR NEW.request_fingerprint <> OLD.request_fingerprint OR NEW.amount_minor <> OLD.amount_minor
       OR NEW.currency <> OLD.currency OR NEW.ticket_ids <> OLD.ticket_ids
       OR NEW.prior_order_state <> OLD.prior_order_state
       OR NEW.prior_reservation_state <> OLD.prior_reservation_state
       OR NEW.requested_at <> OLD.requested_at OR NEW.eligibility_cutoff_at <> OLD.eligibility_cutoff_at
       OR NEW.prepared_at <> OLD.prepared_at THEN
        RAISE EXCEPTION 'ticket refund prepare receipt identity is immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.state <> 'prepared' OR NEW.state NOT IN ('released','applied')
       OR OLD.resolved_at IS NOT NULL OR NEW.resolved_at IS NULL THEN
        RAISE EXCEPTION 'illegal ticket refund prepare receipt transition' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$ticket_refund_prepare_receipt_transition$;
CREATE TRIGGER ticket_refund_prepare_receipts_guard_transition
BEFORE UPDATE ON public.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_ticket_refund_prepare_receipt_transition();
CREATE TRIGGER ticket_refund_prepare_receipts_guard_transition
BEFORE UPDATE ON booking_shard_0.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_ticket_refund_prepare_receipt_transition();
CREATE TRIGGER ticket_refund_prepare_receipts_guard_transition
BEFORE UPDATE ON booking_shard_1.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_ticket_refund_prepare_receipt_transition();

CREATE TABLE public.ticket_refund_compensation_receipts (
    id uuid PRIMARY KEY,
    command_id uuid NOT NULL UNIQUE,
    refund_request_id uuid NOT NULL UNIQUE,
    refund_operation_id uuid NOT NULL UNIQUE,
    payment_intent_id uuid NOT NULL,
    reservation_id uuid NOT NULL
        REFERENCES public.reservations(id) ON DELETE RESTRICT,
    ticket_order_id uuid NOT NULL
        REFERENCES public.ticket_orders(id) ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),
    provider_proof_hash bytea NOT NULL
        CHECK (octet_length(provider_proof_hash) = 32),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    selected_ticket_count integer NOT NULL CHECK (selected_ticket_count > 0),
    released_seat_count integer NOT NULL CHECK (released_seat_count > 0),
    resulting_active_ticket_count integer NOT NULL
        CHECK (resulting_active_ticket_count >= 0),
    resulting_order_state text NOT NULL CHECK (
        resulting_order_state IN ('partially_refunded', 'refunded')
    ),
    committed_at timestamptz NOT NULL,
    CHECK (selected_ticket_count = released_seat_count),
    CHECK (
        (resulting_order_state = 'refunded'
         AND resulting_active_ticket_count = 0)
        OR
        (resulting_order_state = 'partially_refunded'
         AND resulting_active_ticket_count > 0)
    )
);

CREATE TABLE booking_shard_0.ticket_refund_compensation_receipts
    (LIKE public.ticket_refund_compensation_receipts INCLUDING ALL,
     FOREIGN KEY (reservation_id)
       REFERENCES booking_shard_0.reservations(id) ON DELETE RESTRICT,
     FOREIGN KEY (ticket_order_id)
       REFERENCES booking_shard_0.ticket_orders(id) ON DELETE RESTRICT,
     FOREIGN KEY (train_run_id)
       REFERENCES public.train_runs(id) ON DELETE RESTRICT);
CREATE TABLE booking_shard_1.ticket_refund_compensation_receipts
    (LIKE public.ticket_refund_compensation_receipts INCLUDING ALL,
     FOREIGN KEY (reservation_id)
       REFERENCES booking_shard_1.reservations(id) ON DELETE RESTRICT,
     FOREIGN KEY (ticket_order_id)
       REFERENCES booking_shard_1.ticket_orders(id) ON DELETE RESTRICT,
     FOREIGN KEY (train_run_id)
       REFERENCES public.train_runs(id) ON DELETE RESTRICT);

CREATE TABLE public.selected_ticket_refund_receipts (
    id uuid PRIMARY KEY,
    compensation_receipt_id uuid NOT NULL
        REFERENCES public.ticket_refund_compensation_receipts(id)
        ON DELETE RESTRICT,
    refund_request_id uuid NOT NULL,
    ticket_id uuid NOT NULL UNIQUE
        REFERENCES public.tickets(id) ON DELETE RESTRICT,
    reservation_seat_id uuid NOT NULL UNIQUE
        REFERENCES public.reservation_seats(id) ON DELETE RESTRICT,
    train_run_id uuid NOT NULL
        REFERENCES public.train_runs(id) ON DELETE RESTRICT,
    fare_amount_minor bigint NOT NULL CHECK (fare_amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    segment_mask_hash bytea NOT NULL
        CHECK (octet_length(segment_mask_hash) = 32),
    released_at timestamptz NOT NULL,
    UNIQUE (compensation_receipt_id, ticket_id)
);

CREATE TABLE booking_shard_0.selected_ticket_refund_receipts
    (LIKE public.selected_ticket_refund_receipts INCLUDING ALL,
     FOREIGN KEY (compensation_receipt_id)
       REFERENCES booking_shard_0.ticket_refund_compensation_receipts(id)
       ON DELETE RESTRICT,
     FOREIGN KEY (ticket_id)
       REFERENCES booking_shard_0.tickets(id) ON DELETE RESTRICT,
     FOREIGN KEY (reservation_seat_id)
       REFERENCES booking_shard_0.reservation_seats(id) ON DELETE RESTRICT,
     FOREIGN KEY (train_run_id)
       REFERENCES public.train_runs(id) ON DELETE RESTRICT);
CREATE TABLE booking_shard_1.selected_ticket_refund_receipts
    (LIKE public.selected_ticket_refund_receipts INCLUDING ALL,
     FOREIGN KEY (compensation_receipt_id)
       REFERENCES booking_shard_1.ticket_refund_compensation_receipts(id)
       ON DELETE RESTRICT,
     FOREIGN KEY (ticket_id)
       REFERENCES booking_shard_1.tickets(id) ON DELETE RESTRICT,
     FOREIGN KEY (reservation_seat_id)
       REFERENCES booking_shard_1.reservation_seats(id) ON DELETE RESTRICT,
     FOREIGN KEY (train_run_id)
       REFERENCES public.train_runs(id) ON DELETE RESTRICT);

CREATE VIEW public.physical_source_ticket_refund_prepare_receipt_rows AS
SELECT 'legacy'::text AS source_shard_id, receipt.*
FROM public.ticket_refund_prepare_receipts AS receipt
UNION ALL
SELECT 'shard-0'::text, receipt.*
FROM booking_shard_0.ticket_refund_prepare_receipts AS receipt
UNION ALL
SELECT 'shard-1'::text, receipt.*
FROM booking_shard_1.ticket_refund_prepare_receipts AS receipt;

CREATE VIEW public.physical_source_ticket_refund_compensation_receipt_rows AS
SELECT 'legacy'::text AS source_shard_id, receipt.*
FROM public.ticket_refund_compensation_receipts AS receipt
UNION ALL
SELECT 'shard-0'::text, receipt.*
FROM booking_shard_0.ticket_refund_compensation_receipts AS receipt
UNION ALL
SELECT 'shard-1'::text, receipt.*
FROM booking_shard_1.ticket_refund_compensation_receipts AS receipt;

CREATE VIEW public.physical_source_selected_ticket_refund_receipt_rows AS
SELECT 'legacy'::text AS source_shard_id, receipt.*
FROM public.selected_ticket_refund_receipts AS receipt
UNION ALL
SELECT 'shard-0'::text, receipt.*
FROM booking_shard_0.selected_ticket_refund_receipts AS receipt
UNION ALL
SELECT 'shard-1'::text, receipt.*
FROM booking_shard_1.selected_ticket_refund_receipts AS receipt;

ALTER TABLE public.physical_source_train_run_mutation_journal
    DROP CONSTRAINT physical_source_train_run_mutation_journal_table_name_check,
    ADD CONSTRAINT physical_source_train_run_mutation_journal_table_name_check
    CHECK (table_name IN (
        'train_run_booking_snapshots', 'booking_seat_catalog',
        'booking_fare_snapshots', 'seat_inventory', 'reservations',
        'reservation_seats', 'ticket_orders', 'tickets',
        'idempotency_records', 'booking_command_receipts',
        'payment_command_receipts', 'ticket_issuance_receipts',
        'payment_refund_receipts', 'payment_compensation_receipts',
        'ticket_refund_prepare_receipts',
        'ticket_refund_compensation_receipts',
        'selected_ticket_refund_receipts', 'outbox_events'
    ));

CREATE OR REPLACE FUNCTION public.append_physical_source_mutation(
    selected_train_run_id uuid,
    selected_source_shard_id text,
    target_table_name text,
    mutation_operation text,
    target_entity_id uuid,
    bounded_primary_key jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $m7_append_physical_source_mutation$
DECLARE
    capture_migration_id uuid;
    capture_generation bigint;
    allocated_sequence bigint;
BEGIN
    IF selected_train_run_id IS NULL
       OR selected_source_shard_id NOT IN ('legacy', 'shard-0', 'shard-1')
       OR target_table_name NOT IN (
           'train_run_booking_snapshots', 'booking_seat_catalog',
           'booking_fare_snapshots', 'seat_inventory', 'reservations',
           'reservation_seats', 'ticket_orders', 'tickets',
           'idempotency_records', 'booking_command_receipts',
           'payment_command_receipts', 'ticket_issuance_receipts',
           'payment_refund_receipts', 'payment_compensation_receipts',
           'ticket_refund_prepare_receipts',
           'ticket_refund_compensation_receipts',
           'selected_ticket_refund_receipts', 'outbox_events'
       )
       OR mutation_operation NOT IN ('INSERT', 'UPDATE', 'DELETE')
       OR target_entity_id IS NULL
       OR jsonb_typeof(bounded_primary_key) <> 'object'
       OR octet_length(bounded_primary_key::text) > 512 THEN
        RAISE EXCEPTION 'invalid physical source mutation capture input'
            USING ERRCODE = '22023';
    END IF;

    UPDATE public.physical_source_migration_capture_state AS capture
    SET next_sequence = capture.next_sequence + 1
    FROM public.train_run_shard_assignments AS assignment
    WHERE capture.train_run_id = selected_train_run_id
      AND capture.source_shard_id = selected_source_shard_id
      AND capture.capture_enabled
      AND assignment.train_run_id = capture.train_run_id
      AND assignment.shard_id = capture.source_shard_id
      AND assignment.assignment_generation = capture.source_generation
      AND assignment.assignment_state IN ('stable', 'draining', 'migrating')
    RETURNING capture.migration_id, capture.source_generation,
              capture.next_sequence
    INTO capture_migration_id, capture_generation, allocated_sequence;

    IF capture_migration_id IS NULL THEN
        RETURN;
    END IF;

    INSERT INTO public.physical_source_train_run_mutation_journal (
        migration_id, train_run_id, source_shard_id, source_generation,
        mutation_sequence, table_name, operation, entity_id,
        primary_key, metadata
    ) VALUES (
        capture_migration_id, selected_train_run_id,
        selected_source_shard_id, capture_generation, allocated_sequence,
        target_table_name, mutation_operation, target_entity_id,
        bounded_primary_key,
        jsonb_build_object('source_shard_id', selected_source_shard_id)
    );
END;
$m7_append_physical_source_mutation$;

CREATE OR REPLACE FUNCTION public.capture_physical_source_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $m7_capture_physical_source_receipt_mutation$
DECLARE
    source_shard_id text;
    affected_train_run_id uuid;
    affected_id uuid;
BEGIN
    source_shard_id := CASE TG_TABLE_SCHEMA
        WHEN 'public' THEN 'legacy'
        WHEN 'booking_shard_0' THEN 'shard-0'
        WHEN 'booking_shard_1' THEN 'shard-1'
        ELSE NULL
    END;
    IF source_shard_id IS NULL OR TG_TABLE_NAME NOT IN (
        'booking_command_receipts', 'payment_command_receipts',
        'ticket_issuance_receipts', 'payment_refund_receipts',
        'payment_compensation_receipts',
        'ticket_refund_prepare_receipts',
        'ticket_refund_compensation_receipts',
        'selected_ticket_refund_receipts'
    ) THEN
        RAISE EXCEPTION 'unapproved physical source receipt relation'
            USING ERRCODE = '22023';
    END IF;
    affected_train_run_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.train_run_id ELSE NEW.train_run_id
    END;
    affected_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END;
    PERFORM public.append_physical_source_mutation(
        affected_train_run_id, source_shard_id, TG_TABLE_NAME, TG_OP,
        affected_id, jsonb_build_object('source_id', affected_id)
    );
    RETURN COALESCE(NEW, OLD);
END;
$m7_capture_physical_source_receipt_mutation$;

-- Tighten the pre-v11 receipt guard so reverse-migration authorization is
-- bound to this transaction, the exact migration/train run/target generation,
-- and the currently active physical source assignment. A row for another
-- generation or train run must never authorize a shadow-table write.
CREATE OR REPLACE FUNCTION public.guard_control_booking_receipt_write()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $m7_guard_control_booking_receipt_write$
DECLARE
    selected_train_run_id uuid;
    selected_shard_id text;
    assignment_generation bigint;
    assignment_state text;
    active_physical_migration_id uuid;
    catalog_enabled boolean;
    catalog_write_enabled boolean;
    fence_generation bigint;
    fence_write_enabled boolean;
    migration_source_generation bigint;
    migration_state text;
BEGIN
    selected_train_run_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.train_run_id ELSE NEW.train_run_id
    END;
    selected_shard_id := CASE TG_TABLE_SCHEMA
        WHEN 'public' THEN 'legacy'
        WHEN 'booking_shard_0' THEN 'shard-0'
        WHEN 'booking_shard_1' THEN 'shard-1'
        ELSE NULL
    END;
    IF selected_shard_id IS NULL THEN
        RAISE EXCEPTION 'unapproved booking receipt schema'
            USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.physical_control_target_apply_authorizations AS apply_auth
          JOIN public.physical_shard_migrations AS migration
            ON migration.migration_id = apply_auth.migration_id
           AND migration.train_run_id = apply_auth.train_run_id
           AND migration.target_shard_id = apply_auth.target_shard_id
           AND migration.target_generation = apply_auth.target_generation
          JOIN public.train_run_shard_assignments AS assignment
            ON assignment.train_run_id = migration.train_run_id
           AND assignment.shard_id = migration.source_shard_id
           AND assignment.assignment_generation = migration.source_generation
           AND assignment.active_physical_migration_id = migration.migration_id
           AND assignment.assignment_state IN ('migrating', 'draining')
         WHERE apply_auth.transaction_id = txid_current()
           AND apply_auth.train_run_id = selected_train_run_id
           AND apply_auth.target_shard_id = selected_shard_id
           AND migration.reverse_migration
           AND migration.source_shard_id IN (
               'physical-shard-0', 'physical-shard-1'
           )
           AND migration.state IN (
               'preparing_target', 'capture_enabled', 'base_copying',
               'catching_up', 'validating_online', 'draining',
               'source_fenced', 'final_catchup', 'final_validating'
           )
    ) THEN
        RETURN COALESCE(NEW, OLD);
    END IF;
    SELECT assignment.assignment_generation, assignment.assignment_state,
           assignment.active_physical_migration_id,
           shard.enabled, shard.write_enabled
      INTO assignment_generation, assignment_state,
           active_physical_migration_id,
           catalog_enabled, catalog_write_enabled
      FROM public.train_run_shard_assignments AS assignment
      JOIN public.booking_shards AS shard
        ON shard.shard_id = assignment.shard_id
     WHERE assignment.train_run_id = selected_train_run_id
       AND assignment.shard_id = selected_shard_id
     FOR UPDATE OF assignment;
    IF selected_shard_id = 'legacy' THEN
        SELECT fence.assignment_generation, fence.write_enabled
          INTO fence_generation, fence_write_enabled
          FROM public.train_run_write_fences AS fence
         WHERE fence.train_run_id = selected_train_run_id
         FOR UPDATE;
    ELSIF selected_shard_id = 'shard-0' THEN
        SELECT fence.assignment_generation, fence.write_enabled
          INTO fence_generation, fence_write_enabled
          FROM booking_shard_0.train_run_write_fences AS fence
         WHERE fence.train_run_id = selected_train_run_id
         FOR UPDATE;
    ELSE
        SELECT fence.assignment_generation, fence.write_enabled
          INTO fence_generation, fence_write_enabled
          FROM booking_shard_1.train_run_write_fences AS fence
         WHERE fence.train_run_id = selected_train_run_id
         FOR UPDATE;
    END IF;
    IF assignment_generation IS NULL
       OR NOT catalog_enabled OR NOT catalog_write_enabled
       OR fence_generation IS DISTINCT FROM assignment_generation
       OR fence_write_enabled IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'booking receipt write is fenced'
            USING ERRCODE = '55000';
    END IF;
    IF assignment_state = 'stable'
       AND active_physical_migration_id IS NULL THEN
        RETURN COALESCE(NEW, OLD);
    END IF;
    IF assignment_state IN ('migrating', 'draining')
       AND active_physical_migration_id IS NOT NULL THEN
        SELECT migration.source_generation, migration.state
          INTO migration_source_generation, migration_state
          FROM public.physical_shard_migrations AS migration
         WHERE migration.migration_id = active_physical_migration_id
           AND migration.train_run_id = selected_train_run_id
           AND migration.source_shard_id = selected_shard_id
           AND migration.source_generation = assignment_generation;
        IF FOUND
           AND migration_state IN (
               'planned', 'preparing_target', 'capture_enabled',
               'base_copying', 'catching_up', 'validating_online', 'draining'
           )
           AND migration_source_generation = assignment_generation THEN
            RETURN COALESCE(NEW, OLD);
        END IF;
    END IF;
    RAISE EXCEPTION 'booking receipt write is fenced'
        USING ERRCODE = '55000';
END;
$m7_guard_control_booking_receipt_write$;

CREATE FUNCTION public.guard_physical_control_target_apply_authorization()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $physical_control_target_apply_authorization_guard$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'physical target authorization is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF OLD.transaction_id <> txid_current() THEN
            RAISE EXCEPTION 'physical target authorization is transaction-bound'
                USING ERRCODE = '55000';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.transaction_id <> txid_current() OR NOT EXISTS (
        SELECT 1
          FROM public.physical_shard_migrations AS migration
          JOIN public.train_run_shard_assignments AS assignment
            ON assignment.train_run_id = migration.train_run_id
           AND assignment.shard_id = migration.source_shard_id
           AND assignment.assignment_generation = migration.source_generation
           AND assignment.active_physical_migration_id = migration.migration_id
           AND assignment.assignment_state IN ('migrating', 'draining')
         WHERE migration.migration_id = NEW.migration_id
           AND migration.train_run_id = NEW.train_run_id
           AND migration.target_shard_id = NEW.target_shard_id
           AND migration.target_generation = NEW.target_generation
           AND migration.reverse_migration
           AND migration.source_shard_id IN (
               'physical-shard-0', 'physical-shard-1'
           )
           AND migration.state IN (
               'preparing_target', 'capture_enabled', 'base_copying',
               'catching_up', 'validating_online', 'draining',
               'source_fenced', 'final_catchup', 'final_validating'
           )
    ) THEN
        RAISE EXCEPTION 'physical target authorization is not exact'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$physical_control_target_apply_authorization_guard$;

CREATE TRIGGER physical_control_target_apply_authorizations_guard
BEFORE INSERT OR UPDATE OR DELETE
ON public.physical_control_target_apply_authorizations
FOR EACH ROW EXECUTE FUNCTION
    public.guard_physical_control_target_apply_authorization();

CREATE FUNCTION public.require_physical_target_authorization_release()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $physical_target_authorization_release$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM public.physical_control_target_apply_authorizations
         WHERE migration_id = NEW.migration_id
           AND transaction_id = NEW.transaction_id
    ) THEN
        RAISE EXCEPTION 'physical target authorization must be released before commit'
            USING ERRCODE = '55000';
    END IF;
    RETURN NULL;
END;
$physical_target_authorization_release$;

CREATE CONSTRAINT TRIGGER physical_control_target_authorization_release
AFTER INSERT ON public.physical_control_target_apply_authorizations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION
    public.require_physical_target_authorization_release();

CREATE TRIGGER physical_target_write_guard
BEFORE INSERT OR UPDATE OR DELETE
ON public.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_booking_receipt_write();
CREATE TRIGGER physical_target_write_guard
BEFORE INSERT OR UPDATE OR DELETE
ON public.ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_booking_receipt_write();
CREATE TRIGGER physical_target_write_guard
BEFORE INSERT OR UPDATE OR DELETE
ON public.selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_booking_receipt_write();
CREATE TRIGGER physical_target_write_guard
BEFORE INSERT OR UPDATE OR DELETE
ON booking_shard_0.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_booking_receipt_write();
CREATE TRIGGER physical_target_write_guard
BEFORE INSERT OR UPDATE OR DELETE
ON booking_shard_0.ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_booking_receipt_write();
CREATE TRIGGER physical_target_write_guard
BEFORE INSERT OR UPDATE OR DELETE
ON booking_shard_0.selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_booking_receipt_write();
CREATE TRIGGER physical_target_write_guard
BEFORE INSERT OR UPDATE OR DELETE
ON booking_shard_1.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_booking_receipt_write();
CREATE TRIGGER physical_target_write_guard
BEFORE INSERT OR UPDATE OR DELETE
ON booking_shard_1.ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_booking_receipt_write();
CREATE TRIGGER physical_target_write_guard
BEFORE INSERT OR UPDATE OR DELETE
ON booking_shard_1.selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_booking_receipt_write();

-- Runtime receipt evidence is immutable. The only delete exception is the
-- exact transaction-bound authorization used to clean a retained logical
-- predecessor while a physical source is being reverse-migrated.
CREATE FUNCTION public.guard_control_ticket_refund_evidence_mutation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $control_ticket_refund_evidence_mutation_guard$
DECLARE
    source_row jsonb;
    selected_shard_id text;
BEGIN
    IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'ticket_refund_prepare_receipts' THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'ticket refund receipt evidence is immutable'
            USING ERRCODE = '23514';
    END IF;
    source_row := to_jsonb(OLD);
    selected_shard_id := CASE TG_TABLE_SCHEMA
        WHEN 'public' THEN 'legacy'
        WHEN 'booking_shard_0' THEN 'shard-0'
        WHEN 'booking_shard_1' THEN 'shard-1'
        ELSE NULL
    END;
    IF selected_shard_id IS NOT NULL AND EXISTS (
        SELECT 1
          FROM public.physical_control_target_apply_authorizations AS apply_auth
          JOIN public.physical_shard_migrations AS migration
            ON migration.migration_id = apply_auth.migration_id
           AND migration.train_run_id = apply_auth.train_run_id
           AND migration.target_shard_id = apply_auth.target_shard_id
           AND apply_auth.target_generation = migration.target_generation
          JOIN public.train_run_shard_assignments AS assignment
            ON assignment.train_run_id = migration.train_run_id
           AND assignment.shard_id = migration.source_shard_id
           AND assignment.assignment_generation = migration.source_generation
           AND assignment.active_physical_migration_id = migration.migration_id
           AND assignment.assignment_state IN ('migrating', 'draining')
         WHERE apply_auth.transaction_id = txid_current()
           AND apply_auth.train_run_id = (source_row ->> 'train_run_id')::uuid
           AND apply_auth.target_shard_id = selected_shard_id
           AND migration.reverse_migration
           AND migration.source_shard_id IN ('physical-shard-0', 'physical-shard-1')
           AND migration.state IN (
               'preparing_target', 'capture_enabled', 'base_copying',
               'catching_up', 'validating_online', 'draining',
               'source_fenced', 'final_catchup', 'final_validating'
           )
    ) THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'ticket refund receipt evidence is immutable'
        USING ERRCODE = '23514';
END;
$control_ticket_refund_evidence_mutation_guard$;

CREATE TRIGGER ticket_refund_prepare_receipts_guard_evidence
BEFORE DELETE ON public.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_evidence_mutation();
CREATE TRIGGER ticket_refund_compensation_receipts_guard_evidence
BEFORE UPDATE OR DELETE ON public.ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_evidence_mutation();
CREATE TRIGGER selected_ticket_refund_receipts_guard_evidence
BEFORE UPDATE OR DELETE ON public.selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_evidence_mutation();
CREATE TRIGGER ticket_refund_prepare_receipts_guard_evidence
BEFORE DELETE ON booking_shard_0.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_evidence_mutation();
CREATE TRIGGER ticket_refund_compensation_receipts_guard_evidence
BEFORE UPDATE OR DELETE ON booking_shard_0.ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_evidence_mutation();
CREATE TRIGGER selected_ticket_refund_receipts_guard_evidence
BEFORE UPDATE OR DELETE ON booking_shard_0.selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_evidence_mutation();
CREATE TRIGGER ticket_refund_prepare_receipts_guard_evidence
BEFORE DELETE ON booking_shard_1.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_evidence_mutation();
CREATE TRIGGER ticket_refund_compensation_receipts_guard_evidence
BEFORE UPDATE OR DELETE ON booking_shard_1.ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_evidence_mutation();
CREATE TRIGGER selected_ticket_refund_receipts_guard_evidence
BEFORE UPDATE OR DELETE ON booking_shard_1.selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_control_ticket_refund_evidence_mutation();

CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE
ON public.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_receipt_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE
ON public.ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_receipt_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE
ON public.selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_receipt_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE
ON booking_shard_0.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_receipt_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE
ON booking_shard_0.ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_receipt_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE
ON booking_shard_0.selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_receipt_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE
ON booking_shard_1.ticket_refund_prepare_receipts
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_receipt_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE
ON booking_shard_1.ticket_refund_compensation_receipts
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_receipt_mutation();
CREATE TRIGGER physical_source_capture
AFTER INSERT OR UPDATE OR DELETE
ON booking_shard_1.selected_ticket_refund_receipts
FOR EACH ROW EXECUTE FUNCTION public.capture_physical_source_receipt_mutation();

-- Bind newly verified provider events to bounded account/environment evidence.
-- Both columns remain nullable as a pair so pre-v11 rows and the synthetic
-- sandbox protocol remain readable during rolling deployment.
ALTER TABLE public.payment_webhook_inbox
    ADD COLUMN provider_account_id text,
    ADD COLUMN provider_environment text,
    ADD CONSTRAINT payment_webhook_inbox_provider_binding_check CHECK (
        (provider_account_id IS NULL AND provider_environment IS NULL)
        OR (
            provider_account_id IS NOT NULL
            AND provider_environment IS NOT NULL
            AND provider_account_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND provider_environment IN ('test', 'live')
        )
    );

CREATE FUNCTION public.guard_payment_webhook_provider_binding()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_webhook_provider_binding_guard$
BEGIN
    IF NEW.provider_account_id IS DISTINCT FROM OLD.provider_account_id
       OR NEW.provider_environment IS DISTINCT FROM OLD.provider_environment THEN
        RAISE EXCEPTION 'verified webhook provider binding is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$payment_webhook_provider_binding_guard$;

CREATE TRIGGER payment_webhook_inbox_provider_binding_guard
BEFORE UPDATE ON public.payment_webhook_inbox
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_webhook_provider_binding();

-- Key-version lifecycle metadata only. Verification secret material remains in
-- process-specific secret provisioning.
CREATE TABLE public.payment_webhook_key_versions (
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    key_id text NOT NULL CHECK (
        key_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    state text NOT NULL CHECK (state IN ('accepted', 'primary', 'retired')),
    activated_at timestamptz NOT NULL,
    material_proof bytea NOT NULL CHECK (octet_length(material_proof) = 32),
    retirement_not_before timestamptz,
    retired_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider, provider_account_id, key_id),
    CHECK (retirement_not_before IS NULL OR retirement_not_before >= activated_at),
    CHECK (
        (state = 'retired' AND retired_at IS NOT NULL)
        OR (state <> 'retired' AND retired_at IS NULL)
    ),
    CHECK (state <> 'primary' OR retirement_not_before IS NULL)
);

CREATE UNIQUE INDEX payment_webhook_key_versions_one_primary_idx
    ON public.payment_webhook_key_versions(provider, provider_account_id)
    WHERE state = 'primary';
CREATE UNIQUE INDEX payment_webhook_key_versions_material_identity_idx
    ON public.payment_webhook_key_versions(
        provider, provider_account_id, material_proof
    );
CREATE TRIGGER payment_webhook_key_versions_set_updated_at
BEFORE UPDATE ON public.payment_webhook_key_versions
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- Retired versions age out of the hot keyring without destroying forensic
-- identity. Runtime queries never scan this append-only archive.
CREATE TABLE public.payment_webhook_key_version_archive (
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    key_id text NOT NULL CHECK (
        key_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    state text NOT NULL CHECK (state = 'retired'),
    activated_at timestamptz NOT NULL,
    material_proof bytea NOT NULL CHECK (octet_length(material_proof) = 32),
    retirement_not_before timestamptz NOT NULL,
    retired_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    archived_at timestamptz NOT NULL,
    archived_by text NOT NULL CHECK (
        archived_by ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$'
    ),
    PRIMARY KEY (provider, provider_account_id, key_id),
    CHECK (retirement_not_before >= activated_at),
    CHECK (retired_at >= retirement_not_before),
    CHECK (archived_at >= retired_at)
);
CREATE UNIQUE INDEX payment_webhook_key_version_archive_material_identity_idx
    ON public.payment_webhook_key_version_archive(
        provider, provider_account_id, material_proof
    );
CREATE TRIGGER payment_webhook_key_version_archive_guard_immutable
BEFORE UPDATE OR DELETE ON public.payment_webhook_key_version_archive
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();

CREATE FUNCTION public.guard_payment_webhook_key_version()
RETURNS trigger LANGUAGE plpgsql AS $payment_webhook_key_version_guard$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF EXISTS (
            SELECT 1
              FROM public.payment_webhook_key_version_archive AS archive
             WHERE archive.provider = NEW.provider
               AND archive.provider_account_id = NEW.provider_account_id
               AND (archive.key_id = NEW.key_id
                    OR archive.material_proof = NEW.material_proof)
        ) THEN
            RAISE EXCEPTION 'archived webhook key identity or proof cannot be reused'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF OLD.state <> 'retired' OR NOT EXISTS (
            SELECT 1
              FROM public.payment_webhook_key_version_archive AS archive
             WHERE archive.provider = OLD.provider
               AND archive.provider_account_id = OLD.provider_account_id
               AND archive.key_id = OLD.key_id
               AND archive.state = OLD.state
               AND archive.activated_at = OLD.activated_at
               AND archive.material_proof = OLD.material_proof
               AND archive.retirement_not_before IS NOT DISTINCT FROM OLD.retirement_not_before
               AND archive.retired_at = OLD.retired_at
        ) THEN
            RAISE EXCEPTION 'webhook key metadata deletion requires immutable archive evidence'
                USING ERRCODE='23514';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.provider <> OLD.provider
       OR NEW.provider_account_id <> OLD.provider_account_id
       OR NEW.key_id <> OLD.key_id
       OR NEW.activated_at <> OLD.activated_at
       OR NEW.material_proof <> OLD.material_proof THEN
        RAISE EXCEPTION 'webhook key identity and proof are immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.state = 'retired' AND ROW(
        NEW.state, NEW.retirement_not_before, NEW.retired_at
    ) IS DISTINCT FROM ROW(
        OLD.state, OLD.retirement_not_before, OLD.retired_at
    ) THEN
        RAISE EXCEPTION 'retired webhook key metadata is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$payment_webhook_key_version_guard$;
CREATE TRIGGER payment_webhook_key_versions_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.payment_webhook_key_versions
FOR EACH ROW EXECUTE FUNCTION public.guard_payment_webhook_key_version();

CREATE TABLE public.payment_webhook_key_rotation_audit (
    audit_id uuid PRIMARY KEY,
    provider text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    provider_account_id text NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 128),
    key_id text NOT NULL CHECK (key_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    from_state text CHECK (from_state IS NULL OR from_state IN ('accepted', 'primary', 'retired')),
    to_state text NOT NULL CHECK (to_state IN ('accepted', 'primary', 'retired')),
    actor text NOT NULL CHECK (actor ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$'),
    reason text NOT NULL CHECK (reason ~ '^[a-z][a-z0-9_]{0,63}$'),
    result text NOT NULL DEFAULT 'committed' CHECK (result = 'committed'),
    occurred_at timestamptz NOT NULL
);
CREATE TRIGGER payment_webhook_key_rotation_audit_guard_immutable
BEFORE UPDATE OR DELETE ON public.payment_webhook_key_rotation_audit
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();

-- Database-local regional authority is checked in the same transaction as
-- every control mutation. External fencing is still required before promotion.
CREATE TABLE public.regional_write_authority (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    region text NOT NULL CHECK (region IN ('region-a', 'region-b')),
    epoch bigint NOT NULL CHECK (epoch > 0),
    state text NOT NULL CHECK (
        state IN (
            'active', 'draining', 'fenced',
            'promoting', 'recovery', 'failed'
        )
    ),
    writes_enabled boolean NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (NOT writes_enabled OR state = 'active')
);

INSERT INTO public.regional_write_authority(
    singleton, region, epoch, state, writes_enabled
) VALUES (true, 'region-a', 1, 'active', true);

CREATE FUNCTION public.guard_regional_write_authority()
RETURNS trigger
LANGUAGE plpgsql
AS $regional_write_authority_guard$
BEGIN
    IF TG_OP = 'DELETE' OR NEW.singleton IS DISTINCT FROM OLD.singleton THEN
        RAISE EXCEPTION 'regional authority identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.epoch < OLD.epoch
       OR (NEW.epoch = OLD.epoch AND NEW.region IS DISTINCT FROM OLD.region) THEN
        RAISE EXCEPTION 'regional authority epoch cannot move backwards or change owner'
            USING ERRCODE = '23514';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$regional_write_authority_guard$;

CREATE TRIGGER regional_write_authority_guard_transition
BEFORE UPDATE OR DELETE ON public.regional_write_authority
FOR EACH ROW EXECUTE FUNCTION public.guard_regional_write_authority();

CREATE TABLE public.regional_failover_operations (
    operation_id uuid PRIMARY KEY,
    operation_kind text NOT NULL CHECK (operation_kind IN ('failover', 'failback')),
    source_region text NOT NULL CHECK (source_region IN ('region-a', 'region-b')),
    target_region text NOT NULL CHECK (target_region IN ('region-a', 'region-b')),
    source_epoch bigint NOT NULL CHECK (source_epoch > 0),
    target_epoch bigint CHECK (target_epoch > source_epoch),
    incident_id uuid NOT NULL,
    operator_id text NOT NULL CHECK (
        operator_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$'
    ),
    reason_category text NOT NULL CHECK (
        reason_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    stage text NOT NULL CHECK (
        stage IN (
            'planned', 'external_fencing_verified', 'positions_recorded',
            'passive_readiness_removed', 'control_promoted',
            'shard_0_promoted', 'shard_1_promoted',
            'roles_and_timelines_verified', 'epoch_allocated',
            'control_recovery_installed', 'shard_authorities_installed',
            'recovery_apis_started', 'reconciled',
            'payment_workers_enabled', 'settlement_workers_enabled',
            'ingress_switched', 'customer_writes_configured',
            'rto_recorded', 'rpo_recorded', 'target_active',
            'source_retained_fenced'
        )
    ),
    checkpoint_version bigint NOT NULL DEFAULT 1 CHECK (checkpoint_version > 0),
    checkpoint jsonb NOT NULL CHECK (
        jsonb_typeof(checkpoint) = 'object'
        AND octet_length(checkpoint::text) <= 65536
    ),
    phase_timestamps jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (
        jsonb_typeof(phase_timestamps) = 'object'
        AND octet_length(phase_timestamps::text) <= 32768
    ),
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    declared_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (incident_id, operation_kind),
    CHECK (source_region <> target_region),
    CHECK (
        (stage = 'source_retained_fenced' AND completed_at IS NOT NULL)
        OR (stage <> 'source_retained_fenced' AND completed_at IS NULL)
    )
);

CREATE INDEX regional_failover_operations_stage_idx
    ON public.regional_failover_operations(stage, updated_at, operation_id);

CREATE FUNCTION public.guard_regional_failover_operation()
RETURNS trigger
LANGUAGE plpgsql
AS $regional_failover_operation_guard$
DECLARE
    stage_order text[] := ARRAY[
        'planned', 'external_fencing_verified', 'positions_recorded',
        'passive_readiness_removed', 'control_promoted',
        'shard_0_promoted', 'shard_1_promoted',
        'roles_and_timelines_verified', 'epoch_allocated',
        'control_recovery_installed', 'shard_authorities_installed',
        'recovery_apis_started', 'reconciled',
        'payment_workers_enabled', 'settlement_workers_enabled',
        'ingress_switched', 'customer_writes_configured',
        'rto_recorded', 'rpo_recorded', 'target_active',
        'source_retained_fenced'
    ];
    old_index integer;
    new_index integer;
BEGIN
    IF NEW.operation_id <> OLD.operation_id
       OR NEW.operation_kind <> OLD.operation_kind
       OR NEW.source_region <> OLD.source_region
       OR NEW.target_region <> OLD.target_region
       OR NEW.source_epoch <> OLD.source_epoch
       OR NEW.incident_id <> OLD.incident_id
       OR NEW.operator_id <> OLD.operator_id
       OR NEW.reason_category <> OLD.reason_category
       OR NEW.declared_at <> OLD.declared_at THEN
        RAISE EXCEPTION 'regional failover identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.checkpoint_version <= OLD.checkpoint_version THEN
        RAISE EXCEPTION 'regional failover checkpoint version must advance'
            USING ERRCODE = '40001';
    END IF;
    old_index := array_position(stage_order, OLD.stage);
    new_index := array_position(stage_order, NEW.stage);
    IF new_index < old_index OR new_index > old_index + 1 THEN
        RAISE EXCEPTION 'regional failover stage must advance exactly once'
            USING ERRCODE = '23514';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$regional_failover_operation_guard$;

CREATE TRIGGER regional_failover_operations_guard
BEFORE UPDATE ON public.regional_failover_operations
FOR EACH ROW EXECUTE FUNCTION public.guard_regional_failover_operation();

CREATE TABLE public.backup_artifacts (
    backup_id uuid PRIMARY KEY,
    database_id text NOT NULL CHECK (database_id IN ('control', 'shard-0', 'shard-1')),
    repository_id text NOT NULL CHECK (repository_id ~ '^[a-z][a-z0-9-]{0,31}$'),
    backup_set text NOT NULL CHECK (
        backup_set ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    checksum bytea NOT NULL CHECK (octet_length(checksum) = 32),
    encrypted boolean NOT NULL CHECK (encrypted),
    source_timeline integer NOT NULL CHECK (source_timeline > 0),
    source_wal bigint NOT NULL CHECK (source_wal > 0),
    retention_state text NOT NULL DEFAULT 'retained' CHECK (
        retention_state IN ('retained', 'expiration_planned', 'expired')
    ),
    created_at timestamptz NOT NULL,
    expired_at timestamptz,
    UNIQUE (repository_id, database_id, backup_set),
    CHECK ((retention_state = 'expired') = (expired_at IS NOT NULL))
);

CREATE TABLE public.backup_operations (
    operation_id uuid PRIMARY KEY,
    operation_kind text NOT NULL CHECK (operation_kind = 'backup'),
    database_id text NOT NULL CHECK (database_id IN ('control', 'shard-0', 'shard-1')),
    repository_id text NOT NULL CHECK (repository_id ~ '^[a-z][a-z0-9-]{0,31}$'),
    state text NOT NULL CHECK (state IN ('planned', 'completed', 'failed')),
    backup_id uuid REFERENCES public.backup_artifacts(backup_id) ON DELETE RESTRICT,
    requested_at timestamptz NOT NULL,
    completed_at timestamptz,
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    CHECK ((state = 'completed') = (backup_id IS NOT NULL AND completed_at IS NOT NULL)),
    CHECK (state <> 'failed' OR bounded_error_category IS NOT NULL)
);

CREATE TABLE public.backup_verifications (
    verification_id uuid PRIMARY KEY,
    backup_id uuid NOT NULL
        REFERENCES public.backup_artifacts(backup_id) ON DELETE RESTRICT,
    state text NOT NULL CHECK (state IN ('passed', 'failed')),
    checksum bytea NOT NULL CHECK (octet_length(checksum) = 32),
    verifier_kind text NOT NULL CHECK (verifier_kind = 'pgbackrest_verify'),
    verified_at timestamptz NOT NULL,
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    CHECK ((state = 'passed') = (bounded_error_category IS NULL))
);

CREATE TABLE public.restore_validations (
    restore_validation_id uuid PRIMARY KEY,
    backup_id uuid NOT NULL
        REFERENCES public.backup_artifacts(backup_id) ON DELETE RESTRICT,
    target_id text NOT NULL CHECK (target_id ~ '^[a-z][a-z0-9-]{0,63}$'),
    database_id text NOT NULL CHECK (database_id IN ('control', 'shard-0', 'shard-1')),
    state text NOT NULL CHECK (state IN ('running', 'passed', 'failed')),
    point_in_time timestamptz NOT NULL,
    schema_version integer CHECK (schema_version > 0),
    timeline integer CHECK (timeline > 0),
    reconciled boolean NOT NULL DEFAULT false,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    UNIQUE (backup_id, target_id, point_in_time),
    CHECK (
        state <> 'passed'
        OR (
            completed_at IS NOT NULL AND reconciled
            AND schema_version IS NOT NULL AND timeline IS NOT NULL
        )
    )
);

CREATE TABLE public.backup_expiration_operations (
    expiration_id uuid PRIMARY KEY,
    backup_id uuid NOT NULL
        REFERENCES public.backup_artifacts(backup_id) ON DELETE RESTRICT,
    dry_run_digest bytea NOT NULL CHECK (octet_length(dry_run_digest) = 32),
    dry_run_at timestamptz NOT NULL,
    confirmed_by text CHECK (
        confirmed_by IS NULL
        OR confirmed_by ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$'
    ),
    confirmed_at timestamptz,
    state text NOT NULL CHECK (state IN ('dry_run', 'confirmed', 'executing', 'expired', 'failed')),
    execution_started_at timestamptz,
    completed_at timestamptz,
    bounded_error_category text CHECK (
        bounded_error_category IS NULL
        OR bounded_error_category ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    CHECK (
        state = 'dry_run'
        OR (confirmed_by IS NOT NULL AND confirmed_at IS NOT NULL)
    ),
    CHECK (state NOT IN ('executing', 'expired') OR execution_started_at IS NOT NULL)
);

CREATE TRIGGER backup_verifications_guard_immutable
BEFORE UPDATE OR DELETE ON public.backup_verifications
FOR EACH ROW EXECUTE FUNCTION public.guard_m7_evidence_immutable();

CREATE FUNCTION public.guard_backup_artifact_transition()
RETURNS trigger LANGUAGE plpgsql AS $guard_backup_artifact_transition$
BEGIN
    IF TG_OP = 'DELETE'
       OR NEW.backup_id <> OLD.backup_id
       OR NEW.database_id <> OLD.database_id
       OR NEW.repository_id <> OLD.repository_id
       OR NEW.backup_set <> OLD.backup_set
       OR NEW.checksum <> OLD.checksum
       OR NEW.encrypted <> OLD.encrypted
       OR NEW.source_timeline <> OLD.source_timeline
       OR NEW.source_wal <> OLD.source_wal
       OR NEW.created_at <> OLD.created_at
       OR (OLD.retention_state = 'retained' AND NEW.retention_state NOT IN ('retained', 'expiration_planned'))
       OR (OLD.retention_state = 'expiration_planned' AND NEW.retention_state NOT IN ('expiration_planned', 'expired'))
       OR (OLD.retention_state = 'expired' AND NEW.retention_state <> 'expired') THEN
        RAISE EXCEPTION 'backup artifact identity or retention history is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$guard_backup_artifact_transition$;

CREATE TRIGGER backup_artifacts_guard_transition
BEFORE UPDATE OR DELETE ON public.backup_artifacts
FOR EACH ROW EXECUTE FUNCTION public.guard_backup_artifact_transition();

CREATE FUNCTION public.guard_backup_operation_transition()
RETURNS trigger LANGUAGE plpgsql AS $guard_backup_operation_transition$
BEGIN
    IF TG_OP = 'DELETE'
       OR NEW.operation_id <> OLD.operation_id
       OR NEW.operation_kind <> OLD.operation_kind
       OR NEW.database_id <> OLD.database_id
       OR NEW.repository_id <> OLD.repository_id
       OR NEW.requested_at <> OLD.requested_at
       OR OLD.state <> 'planned'
       OR NEW.state NOT IN ('completed', 'failed') THEN
        RAISE EXCEPTION 'backup operation identity is immutable and state is monotonic' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$guard_backup_operation_transition$;

CREATE TRIGGER backup_operations_guard_transition
BEFORE UPDATE OR DELETE ON public.backup_operations
FOR EACH ROW EXECUTE FUNCTION public.guard_backup_operation_transition();

CREATE FUNCTION public.guard_restore_validation_transition()
RETURNS trigger LANGUAGE plpgsql AS $guard_restore_validation_transition$
BEGIN
    IF TG_OP = 'DELETE'
       OR NEW.restore_validation_id <> OLD.restore_validation_id
       OR NEW.backup_id <> OLD.backup_id
       OR NEW.target_id <> OLD.target_id
       OR NEW.database_id <> OLD.database_id
       OR NEW.point_in_time <> OLD.point_in_time
       OR NEW.started_at <> OLD.started_at
       OR OLD.state <> 'running'
       OR NEW.state NOT IN ('passed', 'failed') THEN
        RAISE EXCEPTION 'restore validation identity is immutable and state is monotonic' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$guard_restore_validation_transition$;

CREATE TRIGGER restore_validations_guard_transition
BEFORE UPDATE OR DELETE ON public.restore_validations
FOR EACH ROW EXECUTE FUNCTION public.guard_restore_validation_transition();

CREATE FUNCTION public.guard_backup_expiration_transition()
RETURNS trigger LANGUAGE plpgsql AS $guard_backup_expiration_transition$
BEGIN
    IF TG_OP = 'DELETE'
       OR NEW.expiration_id <> OLD.expiration_id
       OR NEW.backup_id <> OLD.backup_id
       OR NEW.dry_run_digest <> OLD.dry_run_digest
       OR NEW.dry_run_at <> OLD.dry_run_at
       OR (OLD.state <> 'dry_run' AND (
            NEW.confirmed_by IS DISTINCT FROM OLD.confirmed_by
            OR NEW.confirmed_at IS DISTINCT FROM OLD.confirmed_at
       ))
       OR (OLD.execution_started_at IS NOT NULL
           AND NEW.execution_started_at IS DISTINCT FROM OLD.execution_started_at)
       OR (OLD.state = 'dry_run' AND NEW.state NOT IN ('confirmed', 'failed'))
       OR (OLD.state = 'confirmed' AND NEW.state NOT IN ('executing', 'failed'))
       OR (OLD.state = 'executing' AND NEW.state NOT IN ('expired', 'failed'))
       OR OLD.state IN ('expired', 'failed') THEN
        RAISE EXCEPTION 'backup expiration identity is immutable and state is monotonic' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$guard_backup_expiration_transition$;

CREATE TRIGGER backup_expiration_operations_guard_transition
BEFORE UPDATE OR DELETE ON public.backup_expiration_operations
FOR EACH ROW EXECUTE FUNCTION public.guard_backup_expiration_transition();

-- Runtime physical routing moves from booking-shard schema 2 to schema 3.
DO $booking_shard_v3_catalog$
DECLARE
    changed integer;
BEGIN
    UPDATE public.booking_shards
       SET schema_version = 3,
           updated_at = clock_timestamp()
     WHERE shard_id IN ('physical-shard-0', 'physical-shard-1')
       AND storage_kind = 'postgres'
       AND schema_version = 2;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 2 THEN
        RAISE EXCEPTION 'expected exactly two physical shard catalog rows at schema 2'
            USING ERRCODE = '23514';
    END IF;
END;
$booking_shard_v3_catalog$;

-- Defense in depth for every control-database DML path. Application writers
-- must set these four bounded values with SET LOCAL before the first mutation
-- in a transaction. Merely selecting the authority row in application code is
-- insufficient because an older or overlooked write path could otherwise
-- bypass regional fencing.
CREATE FUNCTION public.guard_regional_application_write()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $regional_application_write_guard$
DECLARE
    deployment_region text;
    deployment_role text;
    deployment_epoch_text text;
    deployment_epoch bigint;
    deployment_writes_enabled text;
    authority_region text;
    authority_epoch bigint;
    authority_state text;
    authority_writes_enabled boolean;
BEGIN
    deployment_region := current_setting(
        'railway.deployment_region', true
    );
    deployment_role := current_setting(
        'railway.deployment_role', true
    );
    deployment_epoch_text := current_setting(
        'railway.region_epoch', true
    );
    deployment_writes_enabled := current_setting(
        'railway.regional_writes_enabled', true
    );

    IF deployment_region NOT IN ('region-a', 'region-b')
       OR deployment_role <> 'active'
       OR deployment_writes_enabled <> 'true'
       OR deployment_epoch_text IS NULL
       OR deployment_epoch_text !~ '^[1-9][0-9]{0,18}$' THEN
        RAISE EXCEPTION 'regional application write context is absent or disabled'
            USING ERRCODE = '55000';
    END IF;
    BEGIN
        deployment_epoch := deployment_epoch_text::bigint;
    EXCEPTION WHEN numeric_value_out_of_range THEN
        RAISE EXCEPTION 'regional application epoch is out of range'
            USING ERRCODE = '55000';
    END;

    SELECT authority.region, authority.epoch, authority.state,
           authority.writes_enabled
      INTO authority_region, authority_epoch, authority_state,
           authority_writes_enabled
      FROM public.regional_write_authority AS authority
     WHERE authority.singleton
     FOR SHARE;
    IF NOT FOUND
       OR authority_state <> 'active'
       OR NOT authority_writes_enabled
       OR deployment_region <> authority_region
       OR deployment_epoch <> authority_epoch THEN
        RAISE EXCEPTION 'regional application write is fenced'
            USING ERRCODE = '55000';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$regional_application_write_guard$;

-- Failover journals may be written by the active deployment or by a bounded
-- recovery deployment whose candidate epoch is not older than the durable
-- authority. This exception is attached only to the failover journal; it is
-- not a generic bypass for customer, payment, ledger, or routing tables.
CREATE FUNCTION public.guard_regional_operational_write()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $regional_operational_write_guard$
DECLARE
    deployment_region text;
    deployment_role text;
    deployment_epoch_text text;
    deployment_epoch bigint;
    deployment_writes_enabled text;
    authority_region text;
    authority_epoch bigint;
    authority_state text;
    authority_writes_enabled boolean;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'regional failover journal cannot be deleted'
            USING ERRCODE = '23514';
    END IF;
    deployment_region := current_setting(
        'railway.deployment_region', true
    );
    deployment_role := current_setting(
        'railway.deployment_role', true
    );
    deployment_epoch_text := current_setting(
        'railway.region_epoch', true
    );
    deployment_writes_enabled := current_setting(
        'railway.regional_writes_enabled', true
    );
    IF deployment_region NOT IN ('region-a', 'region-b')
       OR deployment_role NOT IN ('active', 'recovery')
       OR deployment_epoch_text IS NULL
       OR deployment_epoch_text !~ '^[1-9][0-9]{0,18}$'
       OR (
           deployment_role = 'active'
           AND deployment_writes_enabled <> 'true'
       )
       OR (
           deployment_role = 'recovery'
           AND deployment_writes_enabled <> 'false'
       ) THEN
        RAISE EXCEPTION 'regional operational write context is invalid'
            USING ERRCODE = '55000';
    END IF;
    BEGIN
        deployment_epoch := deployment_epoch_text::bigint;
    EXCEPTION WHEN numeric_value_out_of_range THEN
        RAISE EXCEPTION 'regional operational epoch is out of range'
            USING ERRCODE = '55000';
    END;

    SELECT authority.region, authority.epoch, authority.state,
           authority.writes_enabled
      INTO authority_region, authority_epoch, authority_state,
           authority_writes_enabled
      FROM public.regional_write_authority AS authority
     WHERE authority.singleton
     FOR SHARE;
    IF NOT FOUND OR (
        deployment_role = 'active'
        AND (
            authority_state <> 'active'
            OR NOT authority_writes_enabled
            OR deployment_region <> authority_region
            OR deployment_epoch <> authority_epoch
        )
    ) OR (
        deployment_role = 'recovery'
        AND deployment_epoch < authority_epoch
    ) THEN
        RAISE EXCEPTION 'regional operational write is fenced'
            USING ERRCODE = '55000';
    END IF;
    IF (
        (
            NEW.source_region = deployment_region
            AND NEW.source_epoch = deployment_epoch
        ) OR (
            NEW.target_region = deployment_region
            AND NEW.target_epoch = deployment_epoch
        )
    ) IS NOT TRUE THEN
        RAISE EXCEPTION 'regional failover operation is not bound to deployment'
            USING ERRCODE = '55000';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$regional_operational_write_guard$;

CREATE FUNCTION public.guard_regional_authority_command()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $regional_authority_command_guard$
DECLARE
    deployment_region text;
    deployment_role text;
    deployment_epoch_text text;
    deployment_epoch bigint;
    deployment_writes_enabled text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'regional authority cannot be deleted'
            USING ERRCODE = '23514';
    END IF;
    deployment_region := current_setting(
        'railway.deployment_region', true
    );
    deployment_role := current_setting(
        'railway.deployment_role', true
    );
    deployment_epoch_text := current_setting(
        'railway.region_epoch', true
    );
    deployment_writes_enabled := current_setting(
        'railway.regional_writes_enabled', true
    );
    IF deployment_region NOT IN ('region-a', 'region-b')
       OR deployment_role <> 'recovery'
       OR deployment_writes_enabled <> 'false'
       OR deployment_epoch_text IS NULL
       OR deployment_epoch_text !~ '^[1-9][0-9]{0,18}$' THEN
        RAISE EXCEPTION 'regional authority command requires recovery context'
            USING ERRCODE = '55000';
    END IF;
    BEGIN
        deployment_epoch := deployment_epoch_text::bigint;
    EXCEPTION WHEN numeric_value_out_of_range THEN
        RAISE EXCEPTION 'regional authority command epoch is out of range'
            USING ERRCODE = '55000';
    END;
    IF NEW.region <> deployment_region OR NEW.epoch <> deployment_epoch THEN
        RAISE EXCEPTION 'regional authority command does not match deployment'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$regional_authority_command_guard$;

CREATE TRIGGER regional_write_context_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.regional_failover_operations
FOR EACH ROW EXECUTE FUNCTION public.guard_regional_operational_write();

CREATE TRIGGER regional_write_context_guard
BEFORE UPDATE OR DELETE ON public.regional_write_authority
FOR EACH ROW EXECUTE FUNCTION public.guard_regional_authority_command();

-- The relation allowlist is derived once from the fixed v11 schema. The two
-- operational relations above and schema_migrations have explicit handling;
-- every other ordinary or partitioned table in the three control layouts gets
-- the same fail-closed trigger.
DO $install_regional_application_guards$
DECLARE
    guarded_relation record;
BEGIN
    FOR guarded_relation IN
        SELECT schema_row.nspname AS schema_name,
               table_row.relname AS table_name
          FROM pg_catalog.pg_class AS table_row
          JOIN pg_catalog.pg_namespace AS schema_row
            ON schema_row.oid = table_row.relnamespace
         WHERE schema_row.nspname IN (
                   'public', 'booking_shard_0', 'booking_shard_1'
               )
           AND table_row.relkind IN ('r', 'p')
           AND NOT (
               schema_row.nspname = 'public'
               AND table_row.relname IN (
                   'schema_migrations', 'regional_write_authority',
                   'regional_failover_operations'
               )
           )
         ORDER BY schema_row.nspname, table_row.relname
    LOOP
        EXECUTE format(
            'CREATE TRIGGER regional_write_context_guard '
            'BEFORE INSERT OR UPDATE OR DELETE ON %I.%I '
            'FOR EACH ROW EXECUTE FUNCTION '
            'public.guard_regional_application_write()',
            guarded_relation.schema_name, guarded_relation.table_name
        );
    END LOOP;
END;
$install_regional_application_guards$;

-- Runtime and migration roles may own the disposable database. Row triggers do
-- not fire for TRUNCATE, so reject it explicitly on every v11 base table rather
-- than allowing a table owner to bypass regional or immutable evidence guards.
CREATE FUNCTION public.reject_regional_truncate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $reject_regional_truncate$
BEGIN
    RAISE EXCEPTION 'regional tables cannot be truncated'
        USING ERRCODE = '23514';
END;
$reject_regional_truncate$;

DO $install_regional_truncate_guards$
DECLARE
    guarded_relation record;
BEGIN
    FOR guarded_relation IN
        SELECT schema_row.nspname AS schema_name,
               table_row.relname AS table_name
          FROM pg_catalog.pg_class AS table_row
          JOIN pg_catalog.pg_namespace AS schema_row
            ON schema_row.oid = table_row.relnamespace
         WHERE schema_row.nspname IN (
                   'public', 'booking_shard_0', 'booking_shard_1'
               )
           AND table_row.relkind IN ('r', 'p')
           AND NOT (
               schema_row.nspname = 'public'
               AND table_row.relname = 'schema_migrations'
           )
         ORDER BY schema_row.nspname, table_row.relname
    LOOP
        EXECUTE format(
            'CREATE TRIGGER regional_truncate_guard '
            'BEFORE TRUNCATE ON %I.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION '
            'public.reject_regional_truncate()',
            guarded_relation.schema_name, guarded_relation.table_name
        );
    END LOOP;
END;
$install_regional_truncate_guards$;

COMMIT;
