package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	settlementpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestImporterRunOnceCommitsRecordAndCheckpointInOneTransaction(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_ops"}
	tx := &fakeTx{rows: []pgx.Row{fakeRow{values: []any{""}}}}
	db := &fakeDB{rows: []pgx.Row{fakeRow{values: []any{""}}}, transactions: []pgx.Tx{tx}}
	store := newSettlementStore(t, db)
	source := settlement.NewMemorySource(map[string]settlement.Page{
		"": {
			Records: []settlement.Record{{
				Kind: settlement.RecordBalanceTransaction, ProviderID: "txn_1", PaymentCorrelation: "operation-1",
				Operation: settlement.OperationCapture, GrossMinor: 1_000, FeeMinor: 30, NetMinor: 970,
				Currency: "TWD", CreatedAt: time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC),
			}},
			NextCursor: "cursor-1", Done: true,
		},
	})
	importer, err := settlement.NewImporter(source, store, settlement.ImporterConfig{PageSize: 10, MaxPages: 1})
	if err != nil {
		t.Fatal(err)
	}

	report, err := importer.RunOnce(context.Background(), scope)
	if err != nil || report.Inserted != 1 || report.EndCursor != "cursor-1" || !tx.committed {
		t.Fatalf("RunOnce() = (%+v, %v), committed=%v", report, err, tx.committed)
	}
	if len(tx.execs) != 9 ||
		!strings.Contains(tx.execs[0].query, "provider_settlement_import_checkpoints") ||
		!strings.Contains(tx.execs[1].query, "provider_balance_transactions") ||
		!strings.Contains(tx.execs[2].query, "financial_ledger_transactions") ||
		!strings.Contains(tx.execs[8].query, "provider_settlement_import_checkpoints") {
		t.Fatalf("unexpected SQL sequence: %+v", tx.execs)
	}
}

func TestCommitPagePostsBalancedCaptureFeeRefundAndPaidPayoutClearing(t *testing.T) {
	t.Parallel()
	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_financial"}
	at := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	tx := &fakeTx{rows: []pgx.Row{fakeRow{values: []any{""}}}}
	store := newSettlementStore(t, &fakeDB{transactions: []pgx.Tx{tx}})
	result, err := store.CommitPage(context.Background(), settlement.PageCommit{
		Scope: scope, NextCursor: "cursor-1", Records: []settlement.ImportedRecord{
			{Record: settlement.Record{
				Kind: settlement.RecordBalanceTransaction, ProviderID: "txn_fee", PaymentCorrelation: "operation-1",
				Operation: settlement.OperationCapture, GrossMinor: 1_000, FeeMinor: 30, NetMinor: 970,
				Currency: "TWD", CreatedAt: at,
			}, PayloadHash: settlement.PayloadHash{1}, ImportedAt: at.Add(time.Minute)},
			{Record: settlement.Record{
				Kind: settlement.RecordBalanceTransaction, ProviderID: "txn_refund", PaymentCorrelation: "refund-1",
				Operation: settlement.OperationRefund, GrossMinor: -200, NetMinor: -200,
				Currency: "TWD", CreatedAt: at.Add(30 * time.Minute),
			}, PayloadHash: settlement.PayloadHash{2}, ImportedAt: at.Add(31 * time.Minute)},
			{Record: settlement.Record{
				Kind: settlement.RecordPayout, ProviderID: "po_1", Operation: settlement.OperationPayout,
				GrossMinor: 770, NetMinor: 770, Currency: "TWD", CreatedAt: at.Add(time.Hour), PayoutStatus: "paid",
			}, PayloadHash: settlement.PayloadHash{3}, ImportedAt: at.Add(time.Hour + time.Minute)},
			{Record: settlement.Record{
				Kind: settlement.RecordPayout, ProviderID: "po_1", Operation: settlement.OperationPayout,
				GrossMinor: 770, NetMinor: 770, Currency: "TWD", CreatedAt: at.Add(time.Hour), PayoutStatus: "pending",
			}, PayloadHash: settlement.PayloadHash{4}, ImportedAt: at.Add(time.Hour + 2*time.Minute)},
		},
	})
	if err != nil || result.Inserted != 4 || !tx.committed {
		t.Fatalf("CommitPage() = (%+v, %v), committed=%v", result, err, tx.committed)
	}
	if got := countExecs(tx.execs, "INSERT INTO public.financial_ledger_transactions"); got != 4 {
		t.Fatalf("ledger transaction inserts = %d, want capture+fee+refund+paid-payout: %+v", got, tx.execs)
	}
	if got := countExecs(tx.execs, "INSERT INTO public.financial_ledger_postings"); got != 8 {
		t.Fatalf("ledger posting inserts = %d, want 8: %+v", got, tx.execs)
	}
	assertSettlementPosting(t, tx.execs, ledger.AccountReconciliationSuspense, ledger.Debit, 1_000)
	assertSettlementPosting(t, tx.execs, ledger.AccountProviderReceivable, ledger.Credit, 1_000)
	assertSettlementPosting(t, tx.execs, ledger.AccountProviderFeeExpense, ledger.Debit, 30)
	assertSettlementPosting(t, tx.execs, ledger.AccountProviderRefundReceivable, ledger.Debit, 200)
	assertSettlementPosting(t, tx.execs, ledger.AccountReconciliationSuspense, ledger.Credit, 200)
	assertSettlementPosting(t, tx.execs, ledger.AccountSettlementCash, ledger.Debit, 770)
	if got := countSettlementPosting(tx.execs, ledger.AccountSettlementCash, ledger.Debit); got != 1 {
		t.Fatalf("cash postings = %d, want only terminal paid payout: %+v", got, tx.execs)
	}
	assertEvidenceLifecycle(t, tx.execs, "po_1", "paid")
	assertEvidenceLifecycle(t, tx.execs, "po_1", "pending")
}

func TestCommitPageReplaysSameHashAndRetainsChangedHashConflict(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_replay"}
	createdAt := time.Date(2026, 8, 11, 9, 30, 0, 0, time.UTC)
	imported := settlement.ImportedRecord{
		Record: settlement.Record{
			Kind: settlement.RecordPayout, ProviderID: "po_1", Operation: settlement.OperationPayout,
			GrossMinor: 500, NetMinor: 500, Currency: "TWD", CreatedAt: createdAt, PayoutStatus: "paid",
		},
		PayloadHash: settlement.PayloadHash{1}, ImportedAt: createdAt.Add(time.Minute),
	}

	replayTx := &fakeTx{
		rows: []pgx.Row{fakeRow{values: []any{""}}, fakeRow{values: []any{append([]byte(nil), imported.PayloadHash[:]...)}}},
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("INSERT 0 0"), pgconn.NewCommandTag("UPDATE 1"),
		},
	}
	replayStore := newSettlementStore(t, &fakeDB{transactions: []pgx.Tx{replayTx}})
	result, err := replayStore.CommitPage(context.Background(), settlement.PageCommit{
		Scope: scope, NextCursor: "cursor-1", Records: []settlement.ImportedRecord{imported},
	})
	if err != nil || result.Replayed != 1 || result.Conflicts != 0 || !replayTx.committed {
		t.Fatalf("same hash CommitPage() = (%+v, %v), committed=%v", result, err, replayTx.committed)
	}
	if countExecs(replayTx.execs, "financial_ledger_transactions") != 0 || countExecs(replayTx.execs, "financial_ledger_postings") != 0 {
		t.Fatalf("same-hash replay posted ledger entries: %+v", replayTx.execs)
	}

	changedHash := bytes.Repeat([]byte{9}, 32)
	conflictTx := &fakeTx{
		rows: []pgx.Row{fakeRow{values: []any{""}}, fakeRow{values: []any{changedHash}}},
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("INSERT 0 0"),
			pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("UPDATE 1"),
		},
	}
	conflictStore := newSettlementStore(t, &fakeDB{transactions: []pgx.Tx{conflictTx}})
	result, err = conflictStore.CommitPage(context.Background(), settlement.PageCommit{
		Scope: scope, NextCursor: "cursor-1", Records: []settlement.ImportedRecord{imported},
	})
	if err != nil || result.Conflicts != 1 || !conflictTx.committed ||
		len(conflictTx.execs) != 4 || !strings.Contains(conflictTx.execs[2].query, "provider_settlement_import_conflicts") {
		t.Fatalf("changed hash CommitPage() = (%+v, %v), committed=%v execs=%+v", result, err, conflictTx.committed, conflictTx.execs)
	}
	if countExecs(conflictTx.execs, "financial_ledger_transactions") != 0 || countExecs(conflictTx.execs, "financial_ledger_postings") != 0 {
		t.Fatalf("hash conflict posted ledger entries: %+v", conflictTx.execs)
	}
	if got := conflictTx.execs[2].args[4]; got != "po_1" {
		t.Fatalf("payout conflict identity = %v, want canonical payout identity without lifecycle suffix", got)
	}
}

func TestCommitPageRejectsInvalidProviderArithmeticBeforeTransaction(t *testing.T) {
	t.Parallel()

	store, _ := settlementpostgres.New(&fakeDB{})
	_, err := store.CommitPage(context.Background(), settlement.PageCommit{
		Scope:      settlement.AccountScope{Provider: "stripe", AccountID: "acct_invalid"},
		NextCursor: "next",
		Records: []settlement.ImportedRecord{{
			Record: settlement.Record{
				Kind: settlement.RecordBalanceTransaction, ProviderID: "txn_invalid", Operation: settlement.OperationCapture,
				GrossMinor: 100, FeeMinor: 10, NetMinor: 100, Currency: "TWD", CreatedAt: time.Now(),
			},
			PayloadHash: settlement.PayloadHash{1}, ImportedAt: time.Now(),
		}},
	})
	if !errors.Is(err, settlement.ErrInvalidRecord) {
		t.Fatalf("CommitPage() error = %v, want ErrInvalidRecord", err)
	}
}

func TestCommitPageRejectsUnboundedPayoutStatusBeforeTransaction(t *testing.T) {
	t.Parallel()

	store, _ := settlementpostgres.New(&fakeDB{})
	_, err := store.CommitPage(context.Background(), settlement.PageCommit{
		Scope:      settlement.AccountScope{Provider: "stripe", AccountID: "acct_invalid"},
		NextCursor: "next",
		Records: []settlement.ImportedRecord{{
			Record: settlement.Record{
				Kind: settlement.RecordPayout, ProviderID: "po_invalid", Operation: settlement.OperationPayout,
				GrossMinor: 100, NetMinor: 100, Currency: "TWD", CreatedAt: time.Now(), PayoutStatus: "PAID",
			},
			PayloadHash: settlement.PayloadHash{1}, ImportedAt: time.Now(),
		}},
	})
	if !errors.Is(err, settlement.ErrInvalidRecord) {
		t.Fatalf("CommitPage() error = %v, want ErrInvalidRecord", err)
	}
}

func TestDetectorRunOnceReadsBoundedComparisonsAndAppendsFindings(t *testing.T) {
	t.Parallel()

	rows := &fakeRows{data: [][]any{{
		"payout:po_1", "payout", true, int64(500), int64(0), "TWD",
		false, int64(0), int64(0), "", 1, false, false, false, true, false,
	}}}
	runTx := &fakeTx{}
	db := &fakeDB{queryRows: []pgx.Rows{rows}, transactions: []pgx.Tx{runTx}}
	store := newSettlementStore(t, db)
	detector, err := settlement.NewDetector(store, settlement.DetectorConfig{PageSize: 10, MaxPages: 1})
	if err != nil {
		t.Fatal(err)
	}
	report, err := detector.RunOnce(context.Background(), settlement.DetectionScope{Kind: settlement.ScopePayout, Value: "po_1"})
	if err != nil || report.Examined != 1 || len(report.Findings) != 1 || report.Findings[0].Reason != settlement.FindingMissing || !runTx.committed {
		t.Fatalf("detector RunOnce() = (%+v, %v), committed=%v", report, err, runTx.committed)
	}
	if len(runTx.execs) != 2 || !strings.Contains(runTx.execs[0].query, "settlement_reconciliation_runs") ||
		!strings.Contains(runTx.execs[1].query, "settlement_reconciliation_mismatches") {
		t.Fatalf("detect-only append SQL = %+v", runTx.execs)
	}
	if len(db.queries) != 1 || !strings.Contains(db.queries[0], "public.payment_intents") ||
		!strings.Contains(db.queries[0], "candidate.operation_type=scoped.operation_type") ||
		!strings.Contains(db.queries[0], "public.ticket_refund_operations") ||
		!strings.Contains(db.queries[0], "ledger_tx.event_id=operation.ledger_event_id") {
		t.Fatalf("provider-to-payment correlation SQL = %q", db.queries)
	}
}

func TestDetectionQueryCoversLocalOnlySuccessExactRefundAndPayoutTotals(t *testing.T) {
	t.Parallel()

	rows := &fakeRows{data: [][]any{
		{"re_missing", "provider", true, int64(300), int64(0), "TWD", false, int64(0), int64(0), "", 0, true, false, false, true, false},
		{"payout:po_mismatch", "payout", true, int64(970), int64(0), "TWD", true, int64(960), int64(0), "TWD", 1, false, false, false, true, false},
	}}
	runTx := &fakeTx{}
	db := &fakeDB{queryRows: []pgx.Rows{rows}, transactions: []pgx.Tx{runTx}}
	store := newSettlementStore(t, db)
	detector, err := settlement.NewDetector(store, settlement.DetectorConfig{PageSize: 10, MaxPages: 1})
	if err != nil {
		t.Fatal(err)
	}
	report, err := detector.RunOnce(context.Background(), settlement.DetectionScope{Kind: settlement.ScopePeriod, Value: "2026-08-01/2026-08-12"})
	if err != nil {
		t.Fatal(err)
	}
	reasons := map[settlement.FindingReason]bool{}
	for _, finding := range report.Findings {
		reasons[finding.Reason] = true
	}
	if !reasons[settlement.FindingMissing] || !reasons[settlement.FindingAge] || !reasons[settlement.FindingAmount] {
		t.Fatalf("findings = %+v", report.Findings)
	}
	query := db.queries[0]
	for _, required := range []string{
		"local_operations AS", "local_missing AS", "operation.provider_operation_id=evidence.payment_correlation",
		"operation_type='capture' AND operation.provider_payment_id=evidence.payment_correlation",
		"payout_totals AS", "record_kind IN ('balance_transaction','settlement_line','payout_line')",
		"WHEN 'payout_line' THEN 1", "WHEN 'settlement_line' THEN 2",
		"payout_lifecycle AS", "COALESCE(payout.provider_payout_id,payout.provider_record_id)",
		"COALESCE(payout_status,'')<>'paid'",
		"COALESCE(payout_status,'')", "payout_lifecycle_checks AS",
		"payout.payout_status NOT IN ('pending','in_transit','paid')",
		"ORDER BY imported_at DESC,provider_created_at DESC",
		"conflict.detected_at <= $7", "imported_at <= $7",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("detection SQL missing %q", required)
		}
	}
	if strings.Contains(query, "operation.provider_payment_id=scoped.payment_correlation") {
		t.Fatalf("refund correlation must not fall back to a shared payment intent: %q", query)
	}
}

func TestDetectionPaginationPinsOneAsOfBoundaryAcrossPages(t *testing.T) {
	t.Parallel()
	comparison := func(correlation string) []any {
		return []any{correlation, "ledger", true, int64(100), int64(0), "TWD", true, int64(100), int64(0), "TWD", 1, false, false, true, true, false}
	}
	db := &fakeDB{
		queryRows: []pgx.Rows{
			&fakeRows{data: [][]any{comparison("txn_1"), comparison("txn_2")}},
			&fakeRows{data: [][]any{comparison("txn_2")}},
		},
		transactions: []pgx.Tx{&fakeTx{}},
	}
	store := newSettlementStore(t, db)
	detector, err := settlement.NewDetector(store, settlement.DetectorConfig{PageSize: 1, MaxPages: 2})
	if err != nil {
		t.Fatal(err)
	}
	report, err := detector.RunOnce(context.Background(), settlement.DetectionScope{Kind: settlement.ScopePayment, Value: "pi_snapshot"})
	if err != nil || !report.Completed || report.Examined != 2 || len(db.queryArgs) != 2 {
		t.Fatalf("RunOnce() report=%+v err=%v query args=%+v", report, err, db.queryArgs)
	}
	first, firstOK := db.queryArgs[0][6].(time.Time)
	second, secondOK := db.queryArgs[1][6].(time.Time)
	if !firstOK || !secondOK || !first.Equal(second) {
		t.Fatalf("as-of boundaries = (%v, %v), want one stable snapshot", db.queryArgs[0][6], db.queryArgs[1][6])
	}
}

func TestAppendReviewPersistsImmutableEvidenceOnly(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{}
	store := newSettlementStore(t, &fakeDB{transactions: []pgx.Tx{tx}})
	review := settlementpostgres.Review{
		ID:           uuid.MustParse("0198a9d3-c042-7145-b691-8a3b31ba7aac"),
		RunID:        uuid.MustParse("0198a9d3-c042-7145-b691-8a3b31ba7aad"),
		ReviewerID:   "operator:payments-oncall",
		Disposition:  settlementpostgres.ReviewInvestigating,
		EvidenceHash: [32]byte{1, 2, 3},
		ReviewedAt:   time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
	}
	if err := store.AppendReview(context.Background(), review); err != nil {
		t.Fatal(err)
	}
	if !tx.committed || len(tx.execs) != 1 ||
		!strings.Contains(tx.execs[0].query, "INSERT INTO public.settlement_reconciliation_reviews") ||
		strings.Contains(strings.ToUpper(tx.execs[0].query), "UPDATE") ||
		strings.Contains(strings.ToUpper(tx.execs[0].query), "DELETE") {
		t.Fatalf("append-only review SQL = %+v, committed=%v", tx.execs, tx.committed)
	}
}

func TestAppendReviewRejectsInvalidEvidenceBeforeTransaction(t *testing.T) {
	t.Parallel()

	store, _ := settlementpostgres.New(&fakeDB{})
	err := store.AppendReview(context.Background(), settlementpostgres.Review{
		ID:           uuid.New(),
		RunID:        uuid.New(),
		ReviewerID:   "operator with spaces",
		Disposition:  settlementpostgres.ReviewAcknowledged,
		EvidenceHash: [32]byte{1},
		ReviewedAt:   time.Now(),
	})
	if !errors.Is(err, settlementpostgres.ErrInvalidReview) {
		t.Fatalf("AppendReview() error = %v, want ErrInvalidReview", err)
	}
}

func TestReadyChecksCompleteFinancialPersistenceContract(t *testing.T) {
	t.Parallel()

	db := &fakeDB{rows: []pgx.Row{fakeRow{values: []any{true}}}}
	store, _ := settlementpostgres.New(db)
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.rowQueries) != 1 {
		t.Fatalf("readiness queries = %d", len(db.rowQueries))
	}
	for _, table := range []string{
		"financial_ledger_accounts", "financial_ledger_transactions", "financial_ledger_postings", "financial_ledger_reversals",
		"provider_balance_transactions", "provider_settlement_batches", "provider_settlement_lines", "provider_payouts", "provider_payout_lines",
		"provider_settlement_import_checkpoints", "provider_settlement_import_conflicts",
		"settlement_reconciliation_runs", "settlement_reconciliation_mismatches", "settlement_reconciliation_reviews",
	} {
		if !strings.Contains(db.rowQueries[0], table) {
			t.Fatalf("readiness query missing %s", table)
		}
	}
}

type fakeDB struct {
	rows         []pgx.Row
	queryRows    []pgx.Rows
	transactions []pgx.Tx
	queries      []string
	queryArgs    [][]any
	rowQueries   []string
}

func newSettlementStore(t *testing.T, db *fakeDB) *settlementpostgres.Store {
	t.Helper()
	region, _ := authority.ParseRegion("region-a")
	epoch, _ := authority.NewEpoch(7)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	store, err := settlementpostgres.New(db, settlementpostgres.WithRegionalAuthority(deployment))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store
}

func (db *fakeDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	if len(db.transactions) == 0 {
		return nil, errors.New("unexpected BeginTx")
	}
	tx := db.transactions[0]
	db.transactions = db.transactions[1:]
	if regional, ok := tx.(*fakeTx); ok {
		regional.rows = append([]pgx.Row{fakeRow{values: []any{"region-a", int64(7), "active", true}}}, regional.rows...)
	}
	return tx, nil
}

func (db *fakeDB) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	db.rowQueries = append(db.rowQueries, query)
	if len(db.rows) == 0 {
		return fakeRow{err: errors.New("unexpected QueryRow")}
	}
	row := db.rows[0]
	db.rows = db.rows[1:]
	return row
}

func (db *fakeDB) Query(_ context.Context, query string, args ...any) (pgx.Rows, error) {
	db.queries = append(db.queries, query)
	db.queryArgs = append(db.queryArgs, append([]any(nil), args...))
	if len(db.queryRows) == 0 {
		return nil, errors.New("unexpected Query")
	}
	rows := db.queryRows[0]
	db.queryRows = db.queryRows[1:]
	return rows, nil
}

type execCall struct {
	query string
	args  []any
}

type fakeTx struct {
	pgx.Tx
	rows      []pgx.Row
	execs     []execCall
	tags      []pgconn.CommandTag
	committed bool
}

func (tx *fakeTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	if strings.Contains(query, "financial_ledger_transactions") {
		return fakeRow{err: pgx.ErrNoRows}
	}
	if len(tx.rows) == 0 {
		return fakeRow{err: errors.New("unexpected QueryRow")}
	}
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}

func (tx *fakeTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, execCall{query: query, args: args})
	if len(tx.tags) > 0 {
		tag := tx.tags[0]
		tx.tags = tx.tags[1:]
		return tag, nil
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *fakeTx) Commit(context.Context) error {
	tx.committed = true
	return nil
}

func (*fakeTx) Rollback(context.Context) error { return nil }

type fakeRow struct {
	values []any
	err    error
}

type fakeRows struct {
	pgx.Rows
	data  [][]any
	index int
	err   error
}

func (rows *fakeRows) Next() bool {
	if rows.index >= len(rows.data) {
		return false
	}
	rows.index++
	return true
}

func (rows *fakeRows) Scan(dest ...any) error {
	if rows.index == 0 || rows.index > len(rows.data) {
		return errors.New("Scan without Next")
	}
	return scanValues(rows.data[rows.index-1], dest)
}

func (rows *fakeRows) Close()     {}
func (rows *fakeRows) Err() error { return rows.err }

func (row fakeRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	return scanValues(row.values, dest)
}

func scanValues(values []any, dest []any) error {
	if len(dest) != len(values) {
		return errors.New("unexpected scan width")
	}
	for index := range dest {
		target := reflect.ValueOf(dest[index])
		value := reflect.ValueOf(values[index])
		if !value.IsValid() {
			target.Elem().SetZero()
			continue
		}
		if !value.Type().AssignableTo(target.Elem().Type()) {
			return errors.New("scan value type mismatch")
		}
		target.Elem().Set(value)
	}
	return nil
}

func countExecs(calls []execCall, fragment string) int {
	count := 0
	for _, call := range calls {
		if strings.Contains(call.query, fragment) {
			count++
		}
	}
	return count
}

func countSettlementPosting(calls []execCall, account ledger.Account, side ledger.Side) int {
	count := 0
	for _, call := range calls {
		if !strings.Contains(call.query, "INSERT INTO public.financial_ledger_postings") || len(call.args) != 6 {
			continue
		}
		if call.args[2] == account && call.args[3] == side {
			count++
		}
	}
	return count
}

func assertSettlementPosting(t *testing.T, calls []execCall, account ledger.Account, side ledger.Side, amount int64) {
	t.Helper()
	for _, call := range calls {
		if strings.Contains(call.query, "INSERT INTO public.financial_ledger_postings") && len(call.args) == 6 &&
			call.args[2] == account && call.args[3] == side && call.args[4] == amount {
			return
		}
	}
	t.Fatalf("missing posting account=%s side=%s amount=%d: %+v", account, side, amount, calls)
}

func assertEvidenceLifecycle(t *testing.T, calls []execCall, identity, status string) {
	t.Helper()
	for _, call := range calls {
		if strings.Contains(call.query, "INSERT INTO public.provider_payouts") && len(call.args) > 13 &&
			call.args[2] == identity && call.args[13] == status {
			return
		}
	}
	t.Fatalf("missing immutable payout lifecycle identity=%q status=%q: %+v", identity, status, calls)
}
