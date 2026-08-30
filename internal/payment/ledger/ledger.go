// Package ledger owns the immutable operational financial journal.
//
// The journal is deliberately not a general accounting system. It accepts only
// a bounded account vocabulary, checked minor-unit postings, and append-only
// reversals. Persistence adapters must preserve the atomic Store contract.
package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

const maxIdentityBytes = 200

var (
	ErrInvalidJournal   = errors.New("invalid ledger journal")
	ErrInvalidEntry     = errors.New("invalid ledger entry")
	ErrUnknownAccount   = errors.New("unknown ledger account")
	ErrInvalidPosting   = errors.New("invalid ledger posting")
	ErrCurrencyMismatch = errors.New("ledger currency mismatch")
	ErrAmountOverflow   = errors.New("ledger amount overflow")
	ErrUnbalanced       = errors.New("unbalanced ledger entry")
	ErrEventConflict    = errors.New("ledger event conflict")
	ErrNotFound         = errors.New("ledger transaction not found")
	ErrAlreadyReversed  = errors.New("ledger transaction already reversed")
	ErrStoreConflict    = errors.New("ledger store conflict")
)

// Account is a closed operational account vocabulary. It does not assert a
// statutory, GAAP, IFRS, tax, or merchant-of-record meaning.
type Account string

const (
	AccountCustomerFundsPending     Account = "customer_funds_pending"
	AccountTicketSales              Account = "ticket_sales"
	AccountProviderReceivable       Account = "provider_receivable"
	AccountProviderRefundReceivable Account = "provider_refund_receivable"
	AccountProviderFeeExpense       Account = "provider_fee_expense"
	AccountSettlementCash           Account = "settlement_cash"
	AccountReconciliationSuspense   Account = "reconciliation_suspense"
)

var allowedAccounts = map[Account]struct{}{
	AccountCustomerFundsPending:     {},
	AccountTicketSales:              {},
	AccountProviderReceivable:       {},
	AccountProviderRefundReceivable: {},
	AccountProviderFeeExpense:       {},
	AccountSettlementCash:           {},
	AccountReconciliationSuspense:   {},
}

func (account Account) Valid() bool {
	_, ok := allowedAccounts[account]
	return ok
}

type Side string

const (
	Debit  Side = "debit"
	Credit Side = "credit"
)

func (side Side) Valid() bool { return side == Debit || side == Credit }

type Purpose string

const (
	PurposeCapture        Purpose = "capture"
	PurposeTicketIssuance Purpose = "ticket_issuance"
	PurposeRefund         Purpose = "refund"
	PurposeProviderFee    Purpose = "provider_fee"
	PurposeSettlement     Purpose = "settlement"
	PurposePayout         Purpose = "payout"
	PurposeReversal       Purpose = "reversal"
)

func (purpose Purpose) Valid() bool {
	switch purpose {
	case PurposeCapture, PurposeTicketIssuance, PurposeRefund, PurposeProviderFee, PurposeSettlement, PurposePayout, PurposeReversal:
		return true
	default:
		return false
	}
}

// Posting always carries a positive amount. Credits and debits are represented
// by Side rather than by signed money.
type Posting struct {
	Account     Account
	Side        Side
	AmountMinor int64
	Currency    string
}

type AppendRequest struct {
	EventID     string
	Correlation string
	Purpose     Purpose
	Currency    string
	Postings    []Posting
}

// TicketIssuanceAppendRequest builds the canonical journal entry shared by
// ordinary issuance, repair, and historical migration replay.
func TicketIssuanceAppendRequest(intentID, issuanceID uuid.UUID, amountMinor int64, currency string) AppendRequest {
	return AppendRequest{
		EventID:     "ticket_issuance:" + issuanceID.String(),
		Correlation: "payment:" + intentID.String(),
		Purpose:     PurposeTicketIssuance,
		Currency:    currency,
		Postings: []Posting{
			{Account: AccountCustomerFundsPending, Side: Debit, AmountMinor: amountMinor, Currency: currency},
			{Account: AccountTicketSales, Side: Credit, AmountMinor: amountMinor, Currency: currency},
		},
	}
}

type ReverseRequest struct {
	EventID               string
	Correlation           string
	OriginalTransactionID uuid.UUID
}

type Fingerprint [sha256.Size]byte

// Transaction is an immutable snapshot. Journal and Store implementations
// return defensive posting copies so caller mutation cannot rewrite history.
type Transaction struct {
	ID          uuid.UUID
	EventID     string
	Correlation string
	Purpose     Purpose
	Currency    string
	Postings    []Posting
	ReversalOf  *uuid.UUID
	Fingerprint Fingerprint
	CreatedAt   time.Time
}

type Clock interface {
	Now() time.Time
}

// Store atomically enforces event uniqueness and one reversal per transaction.
// Append returns the previously stored transaction with created=false for an
// exact replay, and ErrEventConflict for a changed replay.
type Store interface {
	Append(context.Context, Transaction) (stored Transaction, created bool, err error)
	AppendReversal(context.Context, uuid.UUID, Transaction) (stored Transaction, created bool, err error)
	Get(context.Context, uuid.UUID) (Transaction, bool, error)
}

type Journal struct {
	store Store
	clock Clock
}

func NewJournal(store Store, clock Clock) (*Journal, error) {
	if store == nil || clock == nil {
		return nil, ErrInvalidJournal
	}
	return &Journal{store: store, clock: clock}, nil
}

func (journal *Journal) Append(ctx context.Context, request AppendRequest) (Transaction, error) {
	if journal == nil || journal.store == nil || journal.clock == nil {
		return Transaction{}, ErrInvalidJournal
	}
	if request.Purpose == PurposeReversal {
		return Transaction{}, ErrInvalidEntry
	}
	candidate, err := PrepareAppend(request, journal.clock.Now())
	if err != nil {
		return Transaction{}, err
	}
	stored, _, err := journal.store.Append(ctx, candidate)
	if err != nil {
		return Transaction{}, err
	}
	return cloneTransaction(stored), nil
}

// PrepareAppend validates and deterministically identifies one operational
// ledger event without performing persistence. Control-plane services use it
// with a transaction-scoped PostgreSQL append so the domain fact and its
// ledger evidence commit atomically.
func PrepareAppend(request AppendRequest, createdAt time.Time) (Transaction, error) {
	if createdAt.IsZero() {
		return Transaction{}, ErrInvalidEntry
	}
	normalized, err := normalizeAppend(request)
	if err != nil {
		return Transaction{}, err
	}
	fingerprint, err := transactionFingerprint("append", uuid.Nil, normalized)
	if err != nil {
		return Transaction{}, errors.Join(ErrInvalidEntry, err)
	}
	return Transaction{
		ID:          transactionID(normalized.EventID),
		EventID:     normalized.EventID,
		Correlation: normalized.Correlation,
		Purpose:     normalized.Purpose,
		Currency:    normalized.Currency,
		Postings:    clonePostings(normalized.Postings),
		Fingerprint: fingerprint,
		CreatedAt:   createdAt.UTC(),
	}, nil
}

func (journal *Journal) Reverse(ctx context.Context, request ReverseRequest) (Transaction, error) {
	if journal == nil || journal.store == nil || journal.clock == nil {
		return Transaction{}, ErrInvalidJournal
	}
	eventID, err := normalizeIdentity(request.EventID)
	if err != nil || request.OriginalTransactionID == uuid.Nil {
		return Transaction{}, ErrInvalidEntry
	}
	correlation, err := normalizeIdentity(request.Correlation)
	if err != nil {
		return Transaction{}, ErrInvalidEntry
	}
	original, found, err := journal.store.Get(ctx, request.OriginalTransactionID)
	if err != nil {
		return Transaction{}, err
	}
	if !found {
		return Transaction{}, ErrNotFound
	}
	if original.ID != request.OriginalTransactionID || !original.Purpose.Valid() {
		return Transaction{}, ErrStoreConflict
	}
	if _, err := normalizeAppend(AppendRequest{
		EventID: original.EventID, Correlation: original.Correlation, Purpose: original.Purpose,
		Currency: original.Currency, Postings: original.Postings,
	}); err != nil {
		return Transaction{}, errors.Join(ErrStoreConflict, err)
	}
	postings := make([]Posting, len(original.Postings))
	for index, posting := range original.Postings {
		posting.Side = opposite(posting.Side)
		postings[index] = posting
	}
	normalized, err := normalizeAppend(AppendRequest{
		EventID: eventID, Correlation: correlation, Purpose: PurposeReversal, Currency: original.Currency, Postings: postings,
	})
	if err != nil {
		return Transaction{}, err
	}
	fingerprint, err := transactionFingerprint("reversal", original.ID, normalized)
	if err != nil {
		return Transaction{}, errors.Join(ErrInvalidEntry, err)
	}
	originalID := original.ID
	candidate := Transaction{
		ID:          transactionID(normalized.EventID),
		EventID:     normalized.EventID,
		Correlation: normalized.Correlation,
		Purpose:     normalized.Purpose,
		Currency:    normalized.Currency,
		Postings:    clonePostings(normalized.Postings),
		ReversalOf:  &originalID,
		Fingerprint: fingerprint,
		CreatedAt:   journal.clock.Now().UTC(),
	}
	stored, _, err := journal.store.AppendReversal(ctx, original.ID, candidate)
	if err != nil {
		return Transaction{}, err
	}
	return cloneTransaction(stored), nil
}

func transactionID(eventID string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("railway-ledger-v1:"+eventID))
}

func (journal *Journal) Find(ctx context.Context, id uuid.UUID) (Transaction, bool, error) {
	if journal == nil || journal.store == nil {
		return Transaction{}, false, ErrInvalidJournal
	}
	if id == uuid.Nil {
		return Transaction{}, false, nil
	}
	transaction, found, err := journal.store.Get(ctx, id)
	return cloneTransaction(transaction), found, err
}

func normalizeAppend(request AppendRequest) (AppendRequest, error) {
	eventID, err := normalizeIdentity(request.EventID)
	if err != nil {
		return AppendRequest{}, ErrInvalidEntry
	}
	correlation, err := normalizeIdentity(request.Correlation)
	if err != nil || !request.Purpose.Valid() || !validCurrency(request.Currency) || len(request.Postings) < 2 {
		return AppendRequest{}, ErrInvalidEntry
	}
	debits, credits := int64(0), int64(0)
	postings := clonePostings(request.Postings)
	for _, posting := range postings {
		if !posting.Account.Valid() {
			return AppendRequest{}, ErrUnknownAccount
		}
		if !posting.Side.Valid() || posting.AmountMinor <= 0 {
			return AppendRequest{}, ErrInvalidPosting
		}
		if posting.Currency != request.Currency {
			return AppendRequest{}, ErrCurrencyMismatch
		}
		if posting.Side == Debit {
			debits, err = checkedAdd(debits, posting.AmountMinor)
		} else {
			credits, err = checkedAdd(credits, posting.AmountMinor)
		}
		if err != nil {
			return AppendRequest{}, err
		}
	}
	if debits != credits {
		return AppendRequest{}, ErrUnbalanced
	}
	return AppendRequest{EventID: eventID, Correlation: correlation, Purpose: request.Purpose, Currency: request.Currency, Postings: postings}, nil
}

func checkedAdd(total, amount int64) (int64, error) {
	if amount < 0 || total > math.MaxInt64-amount {
		return 0, ErrAmountOverflow
	}
	return total + amount, nil
}

func normalizeIdentity(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxIdentityBytes {
		return "", ErrInvalidEntry
	}
	return value, nil
}

func validCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for index := range currency {
		if currency[index] < 'A' || currency[index] > 'Z' {
			return false
		}
	}
	return true
}

func opposite(side Side) Side {
	if side == Debit {
		return Credit
	}
	return Debit
}

func transactionFingerprint(kind string, original uuid.UUID, request AppendRequest) (Fingerprint, error) {
	payload := struct {
		Kind        string    `json:"kind"`
		Original    string    `json:"original,omitempty"`
		EventID     string    `json:"event_id"`
		Correlation string    `json:"correlation"`
		Purpose     Purpose   `json:"purpose"`
		Currency    string    `json:"currency"`
		Postings    []Posting `json:"postings"`
	}{kind, original.String(), request.EventID, request.Correlation, request.Purpose, request.Currency, request.Postings}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return Fingerprint{}, err
	}
	return sha256.Sum256(encoded), nil
}

func clonePostings(postings []Posting) []Posting {
	if postings == nil {
		return nil
	}
	return append([]Posting(nil), postings...)
}

func cloneTransaction(transaction Transaction) Transaction {
	transaction.Postings = clonePostings(transaction.Postings)
	if transaction.ReversalOf != nil {
		value := *transaction.ReversalOf
		transaction.ReversalOf = &value
	}
	return transaction
}
