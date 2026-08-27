package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	ledgerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	"github.com/jackc/pgx/v5"
)

func appendSettlementLedger(ctx context.Context, tx pgx.Tx, scope settlement.AccountScope, record settlement.ImportedRecord) error {
	evidenceID := settlementEvidenceID(record)
	identity := sha256.Sum256([]byte(string(record.Kind) + "\x00" + scope.Provider + "\x00" + scope.AccountID + "\x00" + evidenceID))
	appendEntry := func(suffix string, purpose ledger.Purpose, postings []ledger.Posting) error {
		entry, err := ledger.PrepareAppend(ledger.AppendRequest{
			EventID:     "settlement:" + suffix + ":" + hex.EncodeToString(identity[:]),
			Correlation: record.ProviderID,
			Purpose:     purpose, Currency: record.Currency, Postings: postings,
		}, record.ImportedAt.UTC())
		if err != nil {
			return settlement.ErrInvalidRecord
		}
		_, _, err = ledgerpostgres.AppendInTx(ctx, tx, entry)
		return err
	}
	// Balance transactions are the provider's actual clearing movements. Other
	// report shapes may describe the same money and therefore remain evidence
	// only rather than creating duplicate journal effects.
	if record.Kind == settlement.RecordBalanceTransaction && record.Operation == settlement.OperationCapture && record.GrossMinor > 0 {
		if err := appendEntry("capture", ledger.PurposeSettlement, []ledger.Posting{
			{Account: ledger.AccountReconciliationSuspense, Side: ledger.Debit, AmountMinor: record.GrossMinor, Currency: record.Currency},
			{Account: ledger.AccountProviderReceivable, Side: ledger.Credit, AmountMinor: record.GrossMinor, Currency: record.Currency},
		}); err != nil {
			return err
		}
	}
	if record.Kind == settlement.RecordBalanceTransaction && record.Operation == settlement.OperationRefund && record.GrossMinor < 0 {
		amount := -record.GrossMinor
		if err := appendEntry("refund", ledger.PurposeSettlement, []ledger.Posting{
			{Account: ledger.AccountProviderRefundReceivable, Side: ledger.Debit, AmountMinor: amount, Currency: record.Currency},
			{Account: ledger.AccountReconciliationSuspense, Side: ledger.Credit, AmountMinor: amount, Currency: record.Currency},
		}); err != nil {
			return err
		}
	}
	if record.Kind == settlement.RecordBalanceTransaction && record.FeeMinor > 0 {
		if err := appendEntry("fee", ledger.PurposeProviderFee, []ledger.Posting{
			{Account: ledger.AccountProviderFeeExpense, Side: ledger.Debit, AmountMinor: record.FeeMinor, Currency: record.Currency},
			{Account: ledger.AccountReconciliationSuspense, Side: ledger.Credit, AmountMinor: record.FeeMinor, Currency: record.Currency},
		}); err != nil {
			return err
		}
	}
	if record.Kind == settlement.RecordPayout && record.Operation == settlement.OperationPayout && record.PayoutStatus == "paid" && record.NetMinor > 0 {
		if err := appendEntry("payout", ledger.PurposePayout, []ledger.Posting{
			{Account: ledger.AccountSettlementCash, Side: ledger.Debit, AmountMinor: record.NetMinor, Currency: record.Currency},
			{Account: ledger.AccountReconciliationSuspense, Side: ledger.Credit, AmountMinor: record.NetMinor, Currency: record.Currency},
		}); err != nil {
			return err
		}
	}
	return nil
}
