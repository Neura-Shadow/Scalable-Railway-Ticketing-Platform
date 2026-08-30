package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	stripeprovider "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
)

const stripeAPIVersion = stripeprovider.APIVersion

var errSettlementSource = errors.New("settlement provider source failed")

type stripeSourceConfig struct {
	BaseURL              string
	APIKey               string
	APIVersion           string
	AccountID            string
	ConnectTimeout       time.Duration
	RequestTimeout       time.Duration
	MaxResponseBytes     int64
	Now                  func() time.Time
	AllowInsecureForTest bool
}

type stripeEvidenceReader interface {
	stripeprovider.BalanceTransactionReader
	stripeprovider.PayoutReader
	Ready(context.Context) error
	Descriptor() provider.Descriptor
	CloseIdleConnections()
}

// stripeSource adapts the selected provider's normalized optional evidence
// readers to the checkpointed settlement importer. HTTP, authentication,
// response bounds, API versioning, and error classification stay inside the
// provider adapter rather than being reimplemented by the worker.
type stripeSource struct {
	accountID string
	reader    stripeEvidenceReader
	now       func() time.Time
}

func newStripeSource(config stripeSourceConfig) (*stripeSource, error) {
	if config.APIVersion != stripeprovider.APIVersion || !safeIdentity(config.AccountID) {
		return nil, errSettlementSource
	}
	client, err := stripeprovider.NewStatusClient(stripeprovider.Config{
		SecretKey: config.APIKey, AccountID: config.AccountID, APIOrigin: config.BaseURL,
		ConnectTimeout: config.ConnectTimeout, RequestTimeout: config.RequestTimeout,
		MaxResponseBodyBytes: config.MaxResponseBytes, Now: config.Now,
		AllowInsecureForTest: config.AllowInsecureForTest,
	})
	if err != nil {
		return nil, errSettlementSource
	}
	if err := client.Descriptor().Require(provider.CapabilitySet{SettlementTransactions: true, PayoutReports: true}); err != nil {
		client.CloseIdleConnections()
		return nil, errSettlementSource
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &stripeSource{accountID: config.AccountID, reader: client, now: now}, nil
}

func (source *stripeSource) ListPage(ctx context.Context, scope settlement.AccountScope, cursor string, limit int) (settlement.Page, error) {
	if source == nil || source.reader == nil || ctx == nil || scope.Provider != "stripe" || scope.AccountID != source.accountID || limit < 1 || limit > 100 {
		return settlement.Page{}, errSettlementSource
	}
	phase, after, err := parseProviderCursor(cursor)
	if err != nil {
		return settlement.Page{}, errSettlementSource
	}
	if phase == "balance" {
		return source.listBalanceTransactions(ctx, after, limit)
	}
	return source.listPayouts(ctx, after, limit)
}

func (source *stripeSource) listBalanceTransactions(ctx context.Context, after string, limit int) (settlement.Page, error) {
	page, err := source.reader.ListBalanceTransactions(ctx, stripeprovider.ListOptions{Limit: limit, StartingAfter: after})
	if err != nil {
		return settlement.Page{}, err
	}
	records := make([]settlement.Record, len(page.Items))
	for index, item := range page.Items {
		operation := balanceOperation(item.ReportingCategory, item.Type)
		correlation := item.PaymentCorrelation
		if operation == settlement.OperationRefund {
			// A payment intent can have multiple partial refunds. Preserve the
			// provider refund identity so reconciliation never guesses which
			// local refund was represented by the balance transaction.
			correlation = item.SourceID
		}
		record := settlement.Record{
			Kind: settlement.RecordBalanceTransaction, ProviderID: item.ID,
			PaymentCorrelation: correlation, Operation: operation,
			GrossMinor: item.GrossMinor, FeeMinor: item.FeeMinor, NetMinor: item.NetMinor,
			Currency: item.Currency, AvailableAt: item.AvailableAt, CreatedAt: item.CreatedAt,
			PayoutStatus: item.Status,
		}
		if operation == settlement.OperationPayout {
			record.PayoutID = item.SourceID
		}
		records[index] = record
	}
	if page.HasMore {
		if page.NextStartingAfter == "" {
			return settlement.Page{}, errSettlementSource
		}
		return settlement.Page{Records: records, NextCursor: "b:" + page.NextStartingAfter}, nil
	}
	return settlement.Page{Records: records, NextCursor: "p:"}, nil
}

func (source *stripeSource) listPayouts(ctx context.Context, after string, limit int) (settlement.Page, error) {
	page, err := source.reader.ListPayouts(ctx, stripeprovider.ListOptions{Limit: limit, StartingAfter: after})
	if err != nil {
		return settlement.Page{}, err
	}
	records := make([]settlement.Record, len(page.Items))
	for index, item := range page.Items {
		records[index] = settlement.Record{
			Kind: settlement.RecordPayout, ProviderID: item.ID, Operation: settlement.OperationPayout,
			GrossMinor: item.AmountMinor, NetMinor: item.AmountMinor, Currency: item.Currency,
			AvailableAt: item.ArrivalAt, CreatedAt: item.CreatedAt,
			SettlementID: item.BalanceTransactionID, PayoutID: item.ID, PayoutStatus: item.Status,
		}
	}
	if page.HasMore {
		if page.NextStartingAfter == "" {
			return settlement.Page{}, errSettlementSource
		}
		return settlement.Page{Records: records, NextCursor: "p:" + page.NextStartingAfter}, nil
	}
	return settlement.Page{
		Records: records, NextCursor: "c:" + strconv.FormatInt(source.now().UTC().UnixNano(), 10), Done: true,
	}, nil
}

func (source *stripeSource) CloseIdleConnections() {
	if source != nil && source.reader != nil {
		source.reader.CloseIdleConnections()
	}
}

func parseProviderCursor(cursor string) (string, string, error) {
	switch {
	case cursor == "", strings.HasPrefix(cursor, "c:"):
		return "balance", "", nil
	case strings.HasPrefix(cursor, "b:"):
		return "balance", strings.TrimPrefix(cursor, "b:"), nil
	case strings.HasPrefix(cursor, "p:"):
		return "payout", strings.TrimPrefix(cursor, "p:"), nil
	default:
		return "", "", errSettlementSource
	}
}

func balanceOperation(category, kind string) settlement.Operation {
	value := strings.ToLower(strings.TrimSpace(category))
	if value == "" {
		value = strings.ToLower(strings.TrimSpace(kind))
	}
	switch {
	case strings.Contains(value, "refund"):
		return settlement.OperationRefund
	case strings.Contains(value, "fee") || strings.Contains(value, "cost"):
		return settlement.OperationFee
	case strings.Contains(value, "payout"):
		return settlement.OperationPayout
	case strings.Contains(value, "charge") || strings.Contains(value, "payment"):
		return settlement.OperationCapture
	default:
		return settlement.OperationSettlement
	}
}

var _ settlement.Source = (*stripeSource)(nil)
