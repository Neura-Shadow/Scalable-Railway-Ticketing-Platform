// Package postgres persists the immutable operational ledger in PostgreSQL.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"math"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrInvalidStore = errors.New("invalid ledger postgres store")

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store struct {
	db     DB
	writer *authoritypostgres.ControlWriter
}

type storeOptions struct {
	deployment *authority.Deployment
}

type Option func(*storeOptions)

func WithRegionalAuthority(deployment authority.Deployment) Option {
	return func(options *storeOptions) {
		options.deployment = &deployment
	}
}

func New(db DB, options ...Option) (*Store, error) {
	if db == nil {
		return nil, ErrInvalidStore
	}
	configured := storeOptions{}
	for _, apply := range options {
		if apply == nil {
			return nil, ErrInvalidStore
		}
		apply(&configured)
	}
	store := &Store{db: db}
	if configured.deployment == nil {
		return store, nil
	}
	writer, err := authoritypostgres.NewControlWriter(
		db,
		*configured.deployment,
		pgx.TxOptions{IsoLevel: pgx.Serializable},
	)
	if err != nil {
		return nil, ErrInvalidStore
	}
	store.writer = writer
	return store, nil
}

const transactionByIDSQL = `
SELECT ledger_tx.transaction_id,ledger_tx.event_id,ledger_tx.correlation,
       ledger_tx.purpose,ledger_tx.currency,ledger_tx.fingerprint,ledger_tx.created_at,
       reversal.original_transaction_id,
       COALESCE(jsonb_agg(jsonb_build_object(
           'account',posting.account_code,'side',posting.side,
           'amount_minor',posting.amount_minor,'currency',posting.currency
       ) ORDER BY posting.posting_index) FILTER (WHERE posting.transaction_id IS NOT NULL),'[]'::jsonb)
FROM public.financial_ledger_transactions AS ledger_tx
LEFT JOIN public.financial_ledger_postings AS posting
  ON posting.transaction_id=ledger_tx.transaction_id
LEFT JOIN public.financial_ledger_reversals AS reversal
  ON reversal.reversal_transaction_id=ledger_tx.transaction_id
WHERE ledger_tx.transaction_id=$1
GROUP BY ledger_tx.transaction_id,reversal.original_transaction_id`

const transactionByEventSQL = `
SELECT ledger_tx.transaction_id,ledger_tx.event_id,ledger_tx.correlation,
       ledger_tx.purpose,ledger_tx.currency,ledger_tx.fingerprint,
       ledger_tx.created_at,reversal.original_transaction_id,
       COALESCE(jsonb_agg(jsonb_build_object(
           'account',posting.account_code,'side',posting.side,
           'amount_minor',posting.amount_minor,'currency',posting.currency
       ) ORDER BY posting.posting_index) FILTER (WHERE posting.transaction_id IS NOT NULL),'[]'::jsonb)
FROM public.financial_ledger_transactions AS ledger_tx
LEFT JOIN public.financial_ledger_postings AS posting
  ON posting.transaction_id=ledger_tx.transaction_id
LEFT JOIN public.financial_ledger_reversals AS reversal
  ON reversal.reversal_transaction_id=ledger_tx.transaction_id
WHERE ledger_tx.event_id=$1
GROUP BY ledger_tx.transaction_id,reversal.original_transaction_id`

const insertTransactionSQL = `
INSERT INTO public.financial_ledger_transactions(
 transaction_id,event_id,correlation,purpose,currency,fingerprint,created_at
) VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT(event_id) DO NOTHING`

const insertPostingSQL = `
INSERT INTO public.financial_ledger_postings(
 transaction_id,posting_index,account_code,side,amount_minor,currency
) VALUES($1,$2,$3,$4,$5,$6)`

const lockOriginalSQL = `
SELECT transaction_id FROM public.financial_ledger_transactions
WHERE transaction_id=$1 FOR UPDATE`

const existingReversalSQL = `
SELECT reversal_transaction_id FROM public.financial_ledger_reversals
WHERE original_transaction_id=$1`

const insertReversalSQL = `
INSERT INTO public.financial_ledger_reversals(
 reversal_transaction_id,original_transaction_id,created_at
) VALUES($1,$2,$3)`

func (store *Store) Append(ctx context.Context, candidate ledger.Transaction) (ledger.Transaction, bool, error) {
	if store == nil || store.db == nil || store.writer == nil || !validCandidate(candidate, false) {
		return ledger.Transaction{}, false, ErrInvalidStore
	}
	var stored ledger.Transaction
	var inserted bool
	err := store.writer.Write(ctx, func(tx pgx.Tx) error {
		var err error
		stored, inserted, err = appendTransaction(ctx, tx, candidate)
		return err
	})
	return stored, inserted, err
}

// AppendInTx appends one already validated ledger transaction to an existing
// control-plane transaction. It is the only supported seam for committing a
// financial domain fact and its operational ledger evidence atomically.
func AppendInTx(ctx context.Context, tx pgx.Tx, candidate ledger.Transaction) (ledger.Transaction, bool, error) {
	if tx == nil || !validCandidate(candidate, false) {
		return ledger.Transaction{}, false, ErrInvalidStore
	}
	return appendTransaction(ctx, tx, candidate)
}

func appendTransaction(ctx context.Context, tx pgx.Tx, candidate ledger.Transaction) (ledger.Transaction, bool, error) {
	existing, found, err := findByEvent(ctx, tx, candidate.EventID)
	if err != nil {
		return ledger.Transaction{}, false, err
	}
	if found {
		if !sameCanonicalIdentity(existing, candidate) {
			return ledger.Transaction{}, false, ledger.ErrEventConflict
		}
		return existing, false, nil
	}

	tag, err := tx.Exec(ctx, insertTransactionSQL,
		candidate.ID, candidate.EventID, candidate.Correlation, candidate.Purpose,
		candidate.Currency, candidate.Fingerprint[:], candidate.CreatedAt.UTC(),
	)
	if err != nil {
		return ledger.Transaction{}, false, mapWriteError(err)
	}
	if tag.RowsAffected() != 1 {
		existing, found, err = findByEvent(ctx, tx, candidate.EventID)
		if err != nil {
			return ledger.Transaction{}, false, err
		}
		if !found || !sameCanonicalIdentity(existing, candidate) {
			return ledger.Transaction{}, false, ledger.ErrEventConflict
		}
		return existing, false, nil
	}
	if err := insertPostings(ctx, tx, candidate); err != nil {
		return ledger.Transaction{}, false, err
	}
	return clone(candidate), true, nil
}

func (store *Store) AppendReversal(ctx context.Context, originalID uuid.UUID, candidate ledger.Transaction) (ledger.Transaction, bool, error) {
	if store == nil || store.db == nil || store.writer == nil || originalID == uuid.Nil || !validCandidate(candidate, true) ||
		candidate.ReversalOf == nil || *candidate.ReversalOf != originalID {
		return ledger.Transaction{}, false, ErrInvalidStore
	}
	var stored ledger.Transaction
	var inserted bool
	err := store.writer.Write(ctx, func(tx pgx.Tx) error {
		var err error
		stored, inserted, err = appendReversal(ctx, tx, originalID, candidate)
		return err
	})
	return stored, inserted, err
}

func appendReversal(ctx context.Context, tx pgx.Tx, originalID uuid.UUID, candidate ledger.Transaction) (ledger.Transaction, bool, error) {
	existing, found, err := findByEvent(ctx, tx, candidate.EventID)
	if err != nil {
		return ledger.Transaction{}, false, err
	}
	if found {
		if !sameCanonicalIdentity(existing, candidate) {
			return ledger.Transaction{}, false, ledger.ErrEventConflict
		}
		return existing, false, nil
	}
	var lockedID uuid.UUID
	if err := tx.QueryRow(ctx, lockOriginalSQL, originalID).Scan(&lockedID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.Transaction{}, false, ledger.ErrNotFound
		}
		return ledger.Transaction{}, false, err
	}
	var reversalID uuid.UUID
	err = tx.QueryRow(ctx, existingReversalSQL, originalID).Scan(&reversalID)
	if err == nil {
		return ledger.Transaction{}, false, ledger.ErrAlreadyReversed
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ledger.Transaction{}, false, err
	}
	tag, err := tx.Exec(ctx, insertTransactionSQL,
		candidate.ID, candidate.EventID, candidate.Correlation, candidate.Purpose,
		candidate.Currency, candidate.Fingerprint[:], candidate.CreatedAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		if err != nil {
			return ledger.Transaction{}, false, mapWriteError(err)
		}
		return ledger.Transaction{}, false, ledger.ErrEventConflict
	}
	if err := insertPostings(ctx, tx, candidate); err != nil {
		return ledger.Transaction{}, false, err
	}
	if _, err := tx.Exec(ctx, insertReversalSQL, candidate.ID, originalID, candidate.CreatedAt.UTC()); err != nil {
		if isUniqueViolation(err) {
			return ledger.Transaction{}, false, ledger.ErrAlreadyReversed
		}
		return ledger.Transaction{}, false, err
	}
	return clone(candidate), true, nil
}

func sameCanonicalIdentity(existing, candidate ledger.Transaction) bool {
	if existing.ID != candidate.ID || existing.Fingerprint != candidate.Fingerprint {
		return false
	}
	if existing.ReversalOf == nil || candidate.ReversalOf == nil {
		return existing.ReversalOf == nil && candidate.ReversalOf == nil
	}
	return *existing.ReversalOf == *candidate.ReversalOf
}

func (store *Store) Get(ctx context.Context, id uuid.UUID) (ledger.Transaction, bool, error) {
	if store == nil || store.db == nil || id == uuid.Nil {
		return ledger.Transaction{}, false, nil
	}
	transaction, err := scanTransaction(store.db.QueryRow(ctx, transactionByIDSQL, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Transaction{}, false, nil
	}
	return transaction, err == nil, err
}

func findByEvent(ctx context.Context, tx pgx.Tx, eventID string) (ledger.Transaction, bool, error) {
	transaction, err := scanTransaction(tx.QueryRow(ctx, transactionByEventSQL, eventID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Transaction{}, false, nil
	}
	return transaction, err == nil, err
}

type postingJSON struct {
	Account     ledger.Account `json:"account"`
	Side        ledger.Side    `json:"side"`
	AmountMinor int64          `json:"amount_minor"`
	Currency    string         `json:"currency"`
}

func scanTransaction(row pgx.Row) (ledger.Transaction, error) {
	var transaction ledger.Transaction
	var purpose string
	var fingerprint []byte
	var originalID *uuid.UUID
	var encoded []byte
	if err := row.Scan(
		&transaction.ID, &transaction.EventID, &transaction.Correlation, &purpose,
		&transaction.Currency, &fingerprint, &transaction.CreatedAt, &originalID, &encoded,
	); err != nil {
		return ledger.Transaction{}, err
	}
	if len(fingerprint) != len(transaction.Fingerprint) {
		return ledger.Transaction{}, ErrInvalidStore
	}
	copy(transaction.Fingerprint[:], fingerprint)
	transaction.Purpose = ledger.Purpose(purpose)
	transaction.ReversalOf = originalID
	var postings []postingJSON
	if err := json.Unmarshal(encoded, &postings); err != nil {
		return ledger.Transaction{}, ErrInvalidStore
	}
	transaction.Postings = make([]ledger.Posting, len(postings))
	for index, posting := range postings {
		transaction.Postings[index] = ledger.Posting(posting)
	}
	if !validCandidate(transaction, originalID != nil) {
		return ledger.Transaction{}, ErrInvalidStore
	}
	return clone(transaction), nil
}

func insertPostings(ctx context.Context, tx pgx.Tx, transaction ledger.Transaction) error {
	for index, posting := range transaction.Postings {
		if _, err := tx.Exec(ctx, insertPostingSQL,
			transaction.ID, index, posting.Account, posting.Side, posting.AmountMinor, posting.Currency,
		); err != nil {
			return mapWriteError(err)
		}
	}
	return nil
}

func validCandidate(transaction ledger.Transaction, reversal bool) bool {
	if transaction.ID == uuid.Nil || transaction.EventID == "" || len(transaction.EventID) > 200 ||
		transaction.Correlation == "" || len(transaction.Correlation) > 200 ||
		!transaction.Purpose.Valid() || !validCurrency(transaction.Currency) || transaction.CreatedAt.IsZero() ||
		len(transaction.Postings) < 2 || (reversal != (transaction.Purpose == ledger.PurposeReversal)) ||
		(reversal != (transaction.ReversalOf != nil)) {
		return false
	}
	debits, credits := int64(0), int64(0)
	for _, posting := range transaction.Postings {
		if !posting.Account.Valid() || !posting.Side.Valid() || posting.AmountMinor <= 0 || posting.Currency != transaction.Currency {
			return false
		}
		if posting.Side == ledger.Debit {
			if debits > math.MaxInt64-posting.AmountMinor {
				return false
			}
			debits += posting.AmountMinor
		} else {
			if credits > math.MaxInt64-posting.AmountMinor {
				return false
			}
			credits += posting.AmountMinor
		}
	}
	return debits == credits
}

func validCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for _, value := range currency {
		if value < 'A' || value > 'Z' {
			return false
		}
	}
	return true
}

func mapWriteError(err error) error {
	if isUniqueViolation(err) {
		return ledger.ErrStoreConflict
	}
	return err
}

func isUniqueViolation(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == "23505"
}

func clone(transaction ledger.Transaction) ledger.Transaction {
	transaction.Postings = append([]ledger.Posting(nil), transaction.Postings...)
	if transaction.ReversalOf != nil {
		original := *transaction.ReversalOf
		transaction.ReversalOf = &original
	}
	return transaction
}

var _ ledger.Store = (*Store)(nil)
