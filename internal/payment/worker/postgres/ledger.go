package postgres

import (
	"context"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	ledgerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func appendCaptureLedger(ctx context.Context, tx pgx.Tx, intentID, operationID uuid.UUID, amount int64, currency string) error {
	return appendLedger(ctx, tx, ledger.AppendRequest{
		EventID: "capture:" + operationID.String(), Correlation: "payment:" + intentID.String(),
		Purpose: ledger.PurposeCapture, Currency: currency,
		Postings: []ledger.Posting{
			{Account: ledger.AccountProviderReceivable, Side: ledger.Debit, AmountMinor: amount, Currency: currency},
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Credit, AmountMinor: amount, Currency: currency},
		},
	})
}

func appendIssuanceLedger(ctx context.Context, tx pgx.Tx, intentID, issuanceID uuid.UUID, amount int64, currency string) error {
	return appendLedger(ctx, tx, ledger.TicketIssuanceAppendRequest(intentID, issuanceID, amount, currency))
}

func appendRefundLedger(ctx context.Context, tx pgx.Tx, intentID, operationID uuid.UUID, amount int64, currency string, issued bool) error {
	source := ledger.AccountCustomerFundsPending
	if issued {
		source = ledger.AccountTicketSales
	}
	return appendLedger(ctx, tx, ledger.AppendRequest{
		EventID: "refund:" + operationID.String(), Correlation: "payment:" + intentID.String(),
		Purpose: ledger.PurposeRefund, Currency: currency,
		Postings: []ledger.Posting{
			{Account: source, Side: ledger.Debit, AmountMinor: amount, Currency: currency},
			{Account: ledger.AccountProviderRefundReceivable, Side: ledger.Credit, AmountMinor: amount, Currency: currency},
		},
	})
}

func appendLedger(ctx context.Context, tx pgx.Tx, request ledger.AppendRequest) error {
	candidate, err := ledger.PrepareAppend(request, time.Now().UTC())
	if err != nil {
		return worker.ErrStoreUnavailable
	}
	if _, _, err := ledgerpostgres.AppendInTx(ctx, tx, candidate); err != nil {
		return worker.ErrStoreUnavailable
	}
	return nil
}
