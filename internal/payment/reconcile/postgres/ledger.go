package postgres

import (
	"context"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	ledgerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func appendRepairCaptureLedger(ctx context.Context, tx pgx.Tx, e repairEvidence) error {
	return appendRepairLedger(ctx, tx, ledger.AppendRequest{
		EventID: "capture:" + e.OperationID.String(), Correlation: "payment:" + e.IntentID.String(),
		Purpose: ledger.PurposeCapture, Currency: e.Currency,
		Postings: []ledger.Posting{
			{Account: ledger.AccountProviderReceivable, Side: ledger.Debit, AmountMinor: e.AmountMinor, Currency: e.Currency},
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Credit, AmountMinor: e.AmountMinor, Currency: e.Currency},
		},
	}, e.CompletedAt)
}

func appendRepairIssuanceLedger(ctx context.Context, tx pgx.Tx, e repairEvidence, issuanceID uuid.UUID, at time.Time) error {
	return appendRepairLedger(ctx, tx, ledger.TicketIssuanceAppendRequest(e.IntentID, issuanceID, e.AmountMinor, e.Currency), at)
}

func appendRepairRefundLedger(ctx context.Context, tx pgx.Tx, e repairEvidence, issued bool) error {
	source := ledger.AccountCustomerFundsPending
	if issued {
		source = ledger.AccountTicketSales
	}
	return appendRepairLedger(ctx, tx, ledger.AppendRequest{
		EventID: "refund:" + e.OperationID.String(), Correlation: "payment:" + e.IntentID.String(),
		Purpose: ledger.PurposeRefund, Currency: e.Currency,
		Postings: []ledger.Posting{
			{Account: source, Side: ledger.Debit, AmountMinor: e.AmountMinor, Currency: e.Currency},
			{Account: ledger.AccountProviderRefundReceivable, Side: ledger.Credit, AmountMinor: e.AmountMinor, Currency: e.Currency},
		},
	}, e.CompletedAt)
}

func appendRepairLedger(ctx context.Context, tx pgx.Tx, request ledger.AppendRequest, at time.Time) error {
	entry, err := ledger.PrepareAppend(request, at.UTC())
	if err != nil {
		return reconcile.ErrRepairUnavailable
	}
	if _, _, err := ledgerpostgres.AppendInTx(ctx, tx, entry); err != nil {
		return reconcile.ErrRepairUnavailable
	}
	return nil
}
