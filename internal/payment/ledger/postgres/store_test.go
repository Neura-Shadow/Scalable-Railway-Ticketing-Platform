package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	ledgerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

func TestJournalAppendCommitsTransactionAndPostingsAtomically(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{rows: []pgx.Row{fakeRow{err: pgx.ErrNoRows}}}
	store := newLedgerStore(t, &fakeDB{transactions: []pgx.Tx{tx}})
	journal, err := ledger.NewJournal(store, fixedClock{now: time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}

	created, err := journal.Append(context.Background(), ledger.AppendRequest{
		EventID: "capture:op-1", Correlation: "payment:op-1", Purpose: ledger.PurposeCapture, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountProviderReceivable, Side: ledger.Debit, AmountMinor: 1_000, Currency: "TWD"},
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Credit, AmountMinor: 1_000, Currency: "TWD"},
		},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if created.ID.String() == "" || !tx.committed || len(tx.execs) != 3 {
		t.Fatalf("created=%+v committed=%v execs=%+v", created, tx.committed, tx.execs)
	}
	if !strings.Contains(tx.execs[0].query, "financial_ledger_transactions") ||
		!strings.Contains(tx.execs[1].query, "financial_ledger_postings") ||
		!strings.Contains(tx.execs[2].query, "financial_ledger_postings") {
		t.Fatalf("unexpected SQL sequence: %+v", tx.execs)
	}
}

func TestJournalExactReplayReadsOriginalAndChangedReplayConflicts(t *testing.T) {
	t.Parallel()

	request := ledger.AppendRequest{
		EventID: "capture:op-replay", Correlation: "payment:op-replay", Purpose: ledger.PurposeCapture, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountProviderReceivable, Side: ledger.Debit, AmountMinor: 700, Currency: "TWD"},
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Credit, AmountMinor: 700, Currency: "TWD"},
		},
	}
	firstTx := &fakeTx{rows: []pgx.Row{fakeRow{err: pgx.ErrNoRows}}}
	firstStore := newLedgerStore(t, &fakeDB{transactions: []pgx.Tx{firstTx}})
	firstJournal, _ := ledger.NewJournal(firstStore, fixedClock{now: time.Date(2026, 8, 11, 8, 30, 0, 0, time.UTC)})
	stored, err := firstJournal.Append(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	replayTx := &fakeTx{rows: []pgx.Row{transactionRow(t, stored)}}
	replayStore := newLedgerStore(t, &fakeDB{transactions: []pgx.Tx{replayTx}})
	replayJournal, _ := ledger.NewJournal(replayStore, fixedClock{now: stored.CreatedAt.Add(time.Hour)})
	replayed, err := replayJournal.Append(context.Background(), request)
	if err != nil || replayed.ID != stored.ID || !replayTx.committed || len(replayTx.execs) != 0 {
		t.Fatalf("exact replay = (%+v, %v), committed=%v execs=%d", replayed, err, replayTx.committed, len(replayTx.execs))
	}

	changed := request
	changed.Postings = append([]ledger.Posting(nil), request.Postings...)
	changed.Postings[0].AmountMinor = 701
	changed.Postings[1].AmountMinor = 701
	conflictTx := &fakeTx{rows: []pgx.Row{transactionRow(t, stored)}}
	conflictStore := newLedgerStore(t, &fakeDB{transactions: []pgx.Tx{conflictTx}})
	conflictJournal, _ := ledger.NewJournal(conflictStore, fixedClock{now: stored.CreatedAt})
	if _, err := conflictJournal.Append(context.Background(), changed); !errors.Is(err, ledger.ErrEventConflict) || conflictTx.committed {
		t.Fatalf("changed replay error=%v committed=%v", err, conflictTx.committed)
	}
}

func TestAppendInTxRejectsMatchingFingerprintStoredUnderWrongTransactionID(t *testing.T) {
	t.Parallel()

	candidate, err := ledger.PrepareAppend(ledger.AppendRequest{
		EventID: "capture:canonical-id", Correlation: "payment:canonical-id", Purpose: ledger.PurposeCapture, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountProviderReceivable, Side: ledger.Debit, AmountMinor: 700, Currency: "TWD"},
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Credit, AmountMinor: 700, Currency: "TWD"},
		},
	}, time.Date(2026, 8, 11, 8, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	wrongIdentity := candidate
	wrongIdentity.ID = uuid.New()
	tx := &fakeTx{rows: []pgx.Row{transactionRow(t, wrongIdentity)}}
	if _, _, err := ledgerpostgres.AppendInTx(context.Background(), tx, candidate); !errors.Is(err, ledger.ErrEventConflict) {
		t.Fatalf("AppendInTx() error = %v, want ErrEventConflict", err)
	}
	if len(tx.execs) != 0 {
		t.Fatalf("conflicting canonical identity attempted writes: %+v", tx.execs)
	}
}

func TestJournalReversalRejectsMatchingFingerprintStoredUnderWrongTransactionID(t *testing.T) {
	t.Parallel()

	clock := fixedClock{now: time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC)}
	memoryJournal, err := ledger.NewJournal(ledger.NewMemoryStore(), clock)
	if err != nil {
		t.Fatal(err)
	}
	original, err := memoryJournal.Append(context.Background(), ledger.AppendRequest{
		EventID: "capture:reversal-canonical-id", Correlation: "payment:reversal-canonical-id", Purpose: ledger.PurposeCapture, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountProviderReceivable, Side: ledger.Debit, AmountMinor: 700, Currency: "TWD"},
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Credit, AmountMinor: 700, Currency: "TWD"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reverseRequest := ledger.ReverseRequest{
		EventID: "reversal:canonical-id", Correlation: "review:canonical-id", OriginalTransactionID: original.ID,
	}
	canonicalReversal, err := memoryJournal.Reverse(context.Background(), reverseRequest)
	if err != nil {
		t.Fatal(err)
	}
	wrongIdentity := canonicalReversal
	wrongIdentity.ID = uuid.New()
	tx := &fakeTx{rows: []pgx.Row{transactionRow(t, wrongIdentity)}}
	store := newLedgerStore(t, &fakeDB{
		rows: []pgx.Row{transactionRow(t, original)}, transactions: []pgx.Tx{tx},
	})
	journal, err := ledger.NewJournal(store, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Reverse(context.Background(), reverseRequest); !errors.Is(err, ledger.ErrEventConflict) {
		t.Fatalf("Reverse() error = %v, want ErrEventConflict", err)
	}
	if tx.committed || len(tx.execs) != 0 {
		t.Fatalf("conflicting reversal identity committed=%v writes=%+v", tx.committed, tx.execs)
	}
}

func TestJournalReversalLocksOriginalAndAppendsOneReversal(t *testing.T) {
	t.Parallel()

	originalRequest := ledger.AppendRequest{
		EventID: "issue:order-1", Correlation: "payment:order-1", Purpose: ledger.PurposeTicketIssuance, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Debit, AmountMinor: 900, Currency: "TWD"},
			{Account: ledger.AccountTicketSales, Side: ledger.Credit, AmountMinor: 900, Currency: "TWD"},
		},
	}
	seedTx := &fakeTx{rows: []pgx.Row{fakeRow{err: pgx.ErrNoRows}}}
	seedStore := newLedgerStore(t, &fakeDB{transactions: []pgx.Tx{seedTx}})
	seedJournal, _ := ledger.NewJournal(seedStore, fixedClock{now: time.Now()})
	original, err := seedJournal.Append(context.Background(), originalRequest)
	if err != nil {
		t.Fatal(err)
	}

	reversalTx := &fakeTx{rows: []pgx.Row{
		fakeRow{err: pgx.ErrNoRows},
		fakeRow{values: []any{original.ID}},
		fakeRow{err: pgx.ErrNoRows},
	}}
	reversalDB := &fakeDB{rows: []pgx.Row{transactionRow(t, original)}, transactions: []pgx.Tx{reversalTx}}
	store := newLedgerStore(t, reversalDB)
	journal, _ := ledger.NewJournal(store, fixedClock{now: original.CreatedAt.Add(time.Minute)})
	reversal, err := journal.Reverse(context.Background(), ledger.ReverseRequest{
		EventID: "reverse:order-1", Correlation: "review:1", OriginalTransactionID: original.ID,
	})
	if err != nil || !reversalTx.committed || reversal.ReversalOf == nil || *reversal.ReversalOf != original.ID {
		t.Fatalf("Reverse() = (%+v, %v), committed=%v", reversal, err, reversalTx.committed)
	}
	if len(reversalTx.execs) != 4 || !strings.Contains(reversalTx.execs[3].query, "financial_ledger_reversals") {
		t.Fatalf("reversal SQL = %+v", reversalTx.execs)
	}
}

func transactionRow(t *testing.T, transaction ledger.Transaction) pgx.Row {
	t.Helper()
	postings := make([]map[string]any, len(transaction.Postings))
	for index, posting := range transaction.Postings {
		postings[index] = map[string]any{
			"account": posting.Account, "side": posting.Side,
			"amount_minor": posting.AmountMinor, "currency": posting.Currency,
		}
	}
	encoded, err := json.Marshal(postings)
	if err != nil {
		t.Fatal(err)
	}
	return fakeRow{values: []any{
		transaction.ID, transaction.EventID, transaction.Correlation, string(transaction.Purpose), transaction.Currency,
		transaction.Fingerprint[:], transaction.CreatedAt, transaction.ReversalOf, encoded,
	}}
}

func newLedgerStore(t *testing.T, db *fakeDB) *ledgerpostgres.Store {
	t.Helper()
	region, _ := authority.ParseRegion("region-a")
	epoch, _ := authority.NewEpoch(7)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	store, err := ledgerpostgres.New(db, ledgerpostgres.WithRegionalAuthority(deployment))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store
}

type fakeDB struct {
	rows         []pgx.Row
	transactions []pgx.Tx
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

func (db *fakeDB) QueryRow(context.Context, string, ...any) pgx.Row {
	if len(db.rows) == 0 {
		return fakeRow{err: errors.New("unexpected QueryRow")}
	}
	row := db.rows[0]
	db.rows = db.rows[1:]
	return row
}

type execCall struct {
	query string
	args  []any
}

type fakeTx struct {
	pgx.Tx
	rows      []pgx.Row
	execs     []execCall
	committed bool
}

func (tx *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row {
	if len(tx.rows) == 0 {
		return fakeRow{err: errors.New("unexpected QueryRow")}
	}
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}

func (tx *fakeTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, execCall{query: query, args: args})
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

func (row fakeRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(dest) != len(row.values) {
		return errors.New("unexpected scan width")
	}
	for index := range dest {
		target := reflect.ValueOf(dest[index])
		value := reflect.ValueOf(row.values[index])
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
