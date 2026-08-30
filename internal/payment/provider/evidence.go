package provider

import (
	"context"
	"time"
)

// EvidenceListOptions is an explicit provider cursor page. Hidden automatic
// pagination is forbidden because the settlement importer durably owns cursor
// advancement.
type EvidenceListOptions struct {
	Limit         int
	StartingAfter string
}

// BalanceTransaction is normalized provider evidence. Gross and net are
// signed, fee is non-negative, and PaymentCorrelation is an opaque provider
// payment identity when the provider can prove one.
type BalanceTransaction struct {
	ID                 string
	GrossMinor         int64
	FeeMinor           int64
	NetMinor           int64
	Currency           string
	Type               string
	ReportingCategory  string
	Status             string
	SourceID           string
	PaymentCorrelation string
	CreatedAt          time.Time
	AvailableAt        time.Time
}

type BalanceTransactionPage struct {
	Items             []BalanceTransaction
	HasMore           bool
	NextStartingAfter string
}

type BalanceTransactionReader interface {
	ListBalanceTransactions(context.Context, EvidenceListOptions) (BalanceTransactionPage, error)
}

type Payout struct {
	ID                   string
	AmountMinor          int64
	Currency             string
	Status               string
	BalanceTransactionID string
	Automatic            bool
	CreatedAt            time.Time
	ArrivalAt            time.Time
}

type PayoutPage struct {
	Items             []Payout
	HasMore           bool
	NextStartingAfter string
}

type PayoutReader interface {
	ListPayouts(context.Context, EvidenceListOptions) (PayoutPage, error)
}

// RefundLookupRequest binds a read-only provider lookup to the same durable
// merchant identity used for the original refund mutation. Providers must not
// implement this by replaying the mutation. Limit bounds any provider-side
// search needed to resolve metadata-backed identities.
type RefundLookupRequest struct {
	PaymentIntentID   string
	ProviderPaymentID string
	AmountMinor       int64
	Currency          string
	IdempotencyKey    string
	Metadata          Metadata
	Limit             int
}

// RefundLookupResult is authoritative only when Definitive is true. Found
// identifies the one exact refund operation; aggregate payment totals are not
// sufficient evidence. A bounded search may return Definitive=false when more
// provider records remain outside the inspected page.
type RefundLookupResult struct {
	Found      bool
	Definitive bool
	Refund     OperationResult
}

type RefundLookupReader interface {
	LookupRefund(context.Context, RefundLookupRequest) (RefundLookupResult, error)
}
