DO $$
DECLARE
    expected jsonb := jsonb_build_object(
        '73000000-0000-4000-8000-000000000001', 'issue_tickets',
        '73000000-0000-4000-8000-000000000002', 'mark_refund_pending',
        '73000000-0000-4000-8000-000000000003', 'cancel_voided_reservation',
        '73000000-0000-4000-8000-000000000004', 'compensate'
    );
    actual jsonb;
BEGIN
    SELECT jsonb_object_agg(action.saga_id::text, action.action_type)
      INTO actual
      FROM public.payment_saga_actions AS action
     WHERE action.saga_id IN (
        '73000000-0000-4000-8000-000000000001',
        '73000000-0000-4000-8000-000000000002',
        '73000000-0000-4000-8000-000000000003',
        '73000000-0000-4000-8000-000000000004'
     );
    IF actual IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'Milestone 7 saga-action backfill mismatch: %', actual;
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.payment_saga_actions AS action
         WHERE action.saga_id IN (
            '73000000-0000-4000-8000-000000000001',
            '73000000-0000-4000-8000-000000000002',
            '73000000-0000-4000-8000-000000000003',
            '73000000-0000-4000-8000-000000000004'
         )
           AND (
               action.state <> 'pending'
               OR action.attempts <> 0
               OR action.payment_intent_id::text <> replace(
                   action.saga_id::text, '73000000', '72000000'
               )
           )
    ) THEN
        RAISE EXCEPTION 'Milestone 7 backfill did not preserve pending identity';
    END IF;
    IF (
        SELECT count(*)
          FROM public.reservation_directory
         WHERE reservation_id::text LIKE '71000000-0000-4000-8000-%'
           AND state = 'active'
    ) <> 4 THEN
        RAISE EXCEPTION 'version-10 reservation directory fixture was not preserved';
    END IF;

    IF (
        SELECT count(*)
          FROM public.financial_ledger_transactions
         WHERE correlation LIKE 'payment:75000000-0000-4000-8000-%'
    ) <> 7 THEN
        RAISE EXCEPTION 'Milestone 7 historical ledger backfill count mismatch';
    END IF;

	-- Exercise the same event identity used by the runtime settlement detector
	-- against populated version-10 operations. An upgrade that invents a
	-- migration-only prefix would make every historical provider record appear
	-- to have missing local ledger evidence.
	IF EXISTS (
		SELECT 1
		  FROM public.payment_operations AS operation
		  LEFT JOIN public.financial_ledger_transactions AS transaction
			ON transaction.event_id = operation.operation_type || ':' || operation.operation_id::text
		   AND transaction.correlation = 'payment:' || operation.payment_intent_id::text
		 WHERE operation.payment_intent_id::text LIKE '75000000-0000-4000-8000-%'
		   AND operation.operation_type IN ('capture', 'refund')
		   AND operation.state = 'succeeded'
		   AND transaction.transaction_id IS NULL
	) THEN
		RAISE EXCEPTION 'Milestone 7 populated detector ledger identity mismatch';
	END IF;

    IF EXISTS (
        WITH expected (
            event_id, correlation, purpose, posting_index,
            account_code, side, amount_minor, currency
        ) AS (VALUES
            ('capture:76000000-0000-4000-8000-000000000001',
             'payment:75000000-0000-4000-8000-000000000001', 'capture', 0::smallint,
             'provider_receivable', 'debit', 12500::bigint, 'TWD'),
            ('capture:76000000-0000-4000-8000-000000000001',
             'payment:75000000-0000-4000-8000-000000000001', 'capture', 1::smallint,
             'customer_funds_pending', 'credit', 12500::bigint, 'TWD'),
            ('capture:76000000-0000-4000-8000-000000000002',
             'payment:75000000-0000-4000-8000-000000000002', 'capture', 0::smallint,
             'provider_receivable', 'debit', 12500::bigint, 'TWD'),
            ('capture:76000000-0000-4000-8000-000000000002',
             'payment:75000000-0000-4000-8000-000000000002', 'capture', 1::smallint,
             'customer_funds_pending', 'credit', 12500::bigint, 'TWD'),
            ('capture:76000000-0000-4000-8000-000000000003',
             'payment:75000000-0000-4000-8000-000000000003', 'capture', 0::smallint,
             'provider_receivable', 'debit', 12500::bigint, 'TWD'),
            ('capture:76000000-0000-4000-8000-000000000003',
             'payment:75000000-0000-4000-8000-000000000003', 'capture', 1::smallint,
             'customer_funds_pending', 'credit', 12500::bigint, 'TWD'),
            ('ticket_issuance:ddb62b09-9c50-526a-adb4-e32a16aa7c66',
             'payment:75000000-0000-4000-8000-000000000001', 'ticket_issuance', 0::smallint,
             'customer_funds_pending', 'debit', 12500::bigint, 'TWD'),
            ('ticket_issuance:ddb62b09-9c50-526a-adb4-e32a16aa7c66',
             'payment:75000000-0000-4000-8000-000000000001', 'ticket_issuance', 1::smallint,
             'ticket_sales', 'credit', 12500::bigint, 'TWD'),
            ('ticket_issuance:133c9281-f9d4-5450-a93c-f78f12837673',
             'payment:75000000-0000-4000-8000-000000000002', 'ticket_issuance', 0::smallint,
             'customer_funds_pending', 'debit', 12500::bigint, 'TWD'),
            ('ticket_issuance:133c9281-f9d4-5450-a93c-f78f12837673',
             'payment:75000000-0000-4000-8000-000000000002', 'ticket_issuance', 1::smallint,
             'ticket_sales', 'credit', 12500::bigint, 'TWD'),
            ('refund:77000000-0000-4000-8000-000000000002',
             'payment:75000000-0000-4000-8000-000000000002', 'refund', 0::smallint,
             'ticket_sales', 'debit', 12500::bigint, 'TWD'),
            ('refund:77000000-0000-4000-8000-000000000002',
             'payment:75000000-0000-4000-8000-000000000002', 'refund', 1::smallint,
             'provider_refund_receivable', 'credit', 12500::bigint, 'TWD'),
            ('refund:77000000-0000-4000-8000-000000000003',
             'payment:75000000-0000-4000-8000-000000000003', 'refund', 0::smallint,
             'customer_funds_pending', 'debit', 12500::bigint, 'TWD'),
            ('refund:77000000-0000-4000-8000-000000000003',
             'payment:75000000-0000-4000-8000-000000000003', 'refund', 1::smallint,
             'provider_refund_receivable', 'credit', 12500::bigint, 'TWD')
        ), actual AS (
            SELECT transaction.event_id, transaction.correlation,
                   transaction.purpose, posting.posting_index,
                   posting.account_code, posting.side,
                   posting.amount_minor, posting.currency
              FROM public.financial_ledger_transactions AS transaction
              JOIN public.financial_ledger_postings AS posting
                ON posting.transaction_id = transaction.transaction_id
             WHERE transaction.correlation LIKE 'payment:75000000-0000-4000-8000-%'
        )
        SELECT 1 FROM (
            (SELECT * FROM expected EXCEPT SELECT * FROM actual)
            UNION ALL
            (SELECT * FROM actual EXCEPT SELECT * FROM expected)
        ) AS difference
    ) THEN
        RAISE EXCEPTION 'Milestone 7 historical ledger posting semantics mismatch';
    END IF;

    -- These values are generated by the Go production constructors. They bind
    -- the SQL implementation to google/uuid.NewSHA1, the zero original UUID,
    -- encoding/json field order, and canonical issuance identity.
    IF EXISTS (
        WITH expected(event_id, transaction_id, fingerprint) AS (VALUES
            (
                'capture:76000000-0000-4000-8000-000000000001',
                '19df6ea8-c426-5a9d-bed6-20c0e7d70fa9'::uuid,
                decode('962e3259a0b8bb0e4a85aed712498395b37719c82778d8dcb81748e4a767d626', 'hex')
            ),
            (
                'ticket_issuance:ddb62b09-9c50-526a-adb4-e32a16aa7c66',
                'f020d39d-d1cb-5f81-955e-e84dc3fa6244'::uuid,
                decode('d9e0ae58551a9829103246f613d371b754af2a68fec8bf8c011b36d1ce459227', 'hex')
            )
        )
        SELECT 1
          FROM expected
          LEFT JOIN public.financial_ledger_transactions AS transaction
            ON transaction.event_id = expected.event_id
           AND transaction.transaction_id = expected.transaction_id
           AND transaction.fingerprint = expected.fingerprint
         WHERE transaction.transaction_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Milestone 7 Go/SQL ledger golden vector mismatch';
    END IF;

    IF EXISTS (
        WITH ledger_summary AS (
            SELECT transaction.transaction_id, transaction.event_id,
                   transaction.correlation, transaction.purpose,
                   transaction.currency, transaction.fingerprint,
                   count(*) AS posting_count,
                   count(DISTINCT posting.currency) AS currency_count,
                   bool_or(posting.currency <> transaction.currency) AS currency_mismatch,
                   sum(posting.amount_minor) FILTER (WHERE posting.side = 'debit') AS debit_total,
                   sum(posting.amount_minor) FILTER (WHERE posting.side = 'credit') AS credit_total,
                   string_agg(
                       '{"Account":"' || posting.account_code
                       || '","Side":"' || posting.side
                       || '","AmountMinor":' || posting.amount_minor::text
                       || ',"Currency":"' || posting.currency || '"}',
                       ',' ORDER BY posting.posting_index
                   ) AS postings_json
              FROM public.financial_ledger_transactions AS transaction
              JOIN public.financial_ledger_postings AS posting
                ON posting.transaction_id = transaction.transaction_id
             WHERE transaction.correlation LIKE 'payment:75000000-0000-4000-8000-%'
             GROUP BY transaction.transaction_id, transaction.event_id,
                      transaction.correlation, transaction.purpose,
                      transaction.currency, transaction.fingerprint
        )
        SELECT 1
          FROM ledger_summary AS transaction
         WHERE transaction.posting_count <> 2
            OR transaction.currency_count <> 1
            OR transaction.currency_mismatch
            OR transaction.debit_total <> transaction.credit_total
            OR (get_byte(uuid_send(transaction.transaction_id), 6) >> 4) <> 5
            OR (get_byte(uuid_send(transaction.transaction_id), 8) & 192) <> 128
            OR transaction.fingerprint <> sha256(convert_to(
                '{"kind":"append","original":"00000000-0000-0000-0000-000000000000"'
                || ',"event_id":"' || transaction.event_id
                || '","correlation":"' || transaction.correlation
                || '","purpose":"' || transaction.purpose
                || '","currency":"' || transaction.currency
                || '","postings":[' || transaction.postings_json || ']}',
                'UTF8'
            ))
    ) THEN
        RAISE EXCEPTION 'Milestone 7 historical ledger identity or balance mismatch';
    END IF;
END;
$$;
