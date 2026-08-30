package ledger_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	"github.com/google/uuid"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

func TestJournalAppendsBalancedEntryAndReplaysExactEvent(t *testing.T) {
	t.Parallel()

	store := ledger.NewMemoryStore()
	journal, err := ledger.NewJournal(store, fixedClock{now: time.Date(2026, 8, 11, 1, 2, 3, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	request := ledger.AppendRequest{
		EventID:     "capture:operation-1",
		Correlation: "payment:intent-1",
		Purpose:     ledger.PurposeCapture,
		Currency:    "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Debit, AmountMinor: 12_500, Currency: "TWD"},
			{Account: ledger.AccountProviderReceivable, Side: ledger.Credit, AmountMinor: 12_500, Currency: "TWD"},
		},
	}

	first, err := journal.Append(context.Background(), request)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	second, err := journal.Append(context.Background(), request)
	if err != nil {
		t.Fatalf("replayed Append() error = %v", err)
	}
	if first.ID == second.ID && first.EventID == request.EventID && first.CreatedAt.Equal(second.CreatedAt) {
		return
	}
	t.Fatalf("exact replay changed transaction: first=%+v second=%+v", first, second)
}

func TestPrepareAppendUsesStableIdentityForOneEvent(t *testing.T) {
	t.Parallel()
	request := ledger.AppendRequest{
		EventID: "capture:operation-stable", Correlation: "payment:intent-stable",
		Purpose: ledger.PurposeCapture, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountProviderReceivable, Side: ledger.Debit, AmountMinor: 700, Currency: "TWD"},
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Credit, AmountMinor: 700, Currency: "TWD"},
		},
	}
	first, err := ledger.PrepareAppend(request, time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("PrepareAppend() error = %v", err)
	}
	second, err := ledger.PrepareAppend(request, time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("PrepareAppend() replay error = %v", err)
	}
	if first.ID != second.ID || first.Fingerprint != second.Fingerprint {
		t.Fatalf("stable event identity mismatch: first=%v second=%v", first.ID, second.ID)
	}
}

func TestPrepareAppendMatchesMigrationCaptureGoldenVector(t *testing.T) {
	t.Parallel()

	transaction, err := ledger.PrepareAppend(ledger.AppendRequest{
		EventID:     "capture:76000000-0000-4000-8000-000000000001",
		Correlation: "payment:75000000-0000-4000-8000-000000000001",
		Purpose:     ledger.PurposeCapture,
		Currency:    "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountProviderReceivable, Side: ledger.Debit, AmountMinor: 12_500, Currency: "TWD"},
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Credit, AmountMinor: 12_500, Currency: "TWD"},
		},
	}, time.Date(2026, 1, 2, 0, 0, 11, 0, time.UTC))
	if err != nil {
		t.Fatalf("PrepareAppend() error = %v", err)
	}
	if want := uuid.MustParse("19df6ea8-c426-5a9d-bed6-20c0e7d70fa9"); transaction.ID != want {
		t.Fatalf("transaction ID = %s, want %s", transaction.ID, want)
	}
	if got, want := transaction.Fingerprint, "962e3259a0b8bb0e4a85aed712498395b37719c82778d8dcb81748e4a767d626"; stringHex(got[:]) != want {
		t.Fatalf("fingerprint = %x, want %s", got, want)
	}
}

func TestPrepareAppendMatchesMigrationIssuanceGoldenVector(t *testing.T) {
	t.Parallel()

	transaction, err := ledger.PrepareAppend(ledger.TicketIssuanceAppendRequest(
		uuid.MustParse("75000000-0000-4000-8000-000000000001"),
		uuid.MustParse("ddb62b09-9c50-526a-adb4-e32a16aa7c66"),
		12_500,
		"TWD",
	), time.Date(2026, 1, 2, 0, 1, 1, 0, time.UTC))
	if err != nil {
		t.Fatalf("PrepareAppend() error = %v", err)
	}
	if want := uuid.MustParse("f020d39d-d1cb-5f81-955e-e84dc3fa6244"); transaction.ID != want {
		t.Fatalf("transaction ID = %s, want %s", transaction.ID, want)
	}
	if got, want := transaction.Fingerprint, "d9e0ae58551a9829103246f613d371b754af2a68fec8bf8c011b36d1ce459227"; stringHex(got[:]) != want {
		t.Fatalf("fingerprint = %x, want %s", got, want)
	}
}

func stringHex(value []byte) string {
	const digits = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, item := range value {
		encoded[index*2] = digits[item>>4]
		encoded[index*2+1] = digits[item&0x0f]
	}
	return string(encoded)
}

func TestJournalRejectsInvalidOrUnbalancedPostings(t *testing.T) {
	t.Parallel()

	valid := ledger.AppendRequest{
		EventID: "event", Correlation: "correlation", Purpose: ledger.PurposeCapture, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Debit, AmountMinor: 100, Currency: "TWD"},
			{Account: ledger.AccountProviderReceivable, Side: ledger.Credit, AmountMinor: 100, Currency: "TWD"},
		},
	}
	tests := []struct {
		name   string
		mutate func(*ledger.AppendRequest)
		want   error
	}{
		{name: "unknown account", mutate: func(request *ledger.AppendRequest) { request.Postings[0].Account = "cash" }, want: ledger.ErrUnknownAccount},
		{name: "unknown purpose", mutate: func(request *ledger.AppendRequest) { request.Purpose = "cash_move" }, want: ledger.ErrInvalidEntry},
		{name: "caller reversal", mutate: func(request *ledger.AppendRequest) { request.Purpose = ledger.PurposeReversal }, want: ledger.ErrInvalidEntry},
		{name: "zero amount", mutate: func(request *ledger.AppendRequest) { request.Postings[0].AmountMinor = 0 }, want: ledger.ErrInvalidPosting},
		{name: "negative amount", mutate: func(request *ledger.AppendRequest) { request.Postings[0].AmountMinor = -1 }, want: ledger.ErrInvalidPosting},
		{name: "mixed currency", mutate: func(request *ledger.AppendRequest) { request.Postings[1].Currency = "USD" }, want: ledger.ErrCurrencyMismatch},
		{name: "lowercase currency", mutate: func(request *ledger.AppendRequest) { request.Currency = "twd" }, want: ledger.ErrInvalidEntry},
		{name: "one posting", mutate: func(request *ledger.AppendRequest) { request.Postings = request.Postings[:1] }, want: ledger.ErrInvalidEntry},
		{name: "unbalanced", mutate: func(request *ledger.AppendRequest) { request.Postings[1].AmountMinor = 99 }, want: ledger.ErrUnbalanced},
		{name: "overflow", mutate: func(request *ledger.AppendRequest) {
			request.Postings = []ledger.Posting{
				{Account: ledger.AccountCustomerFundsPending, Side: ledger.Debit, AmountMinor: math.MaxInt64, Currency: "TWD"},
				{Account: ledger.AccountTicketSales, Side: ledger.Debit, AmountMinor: 1, Currency: "TWD"},
				{Account: ledger.AccountProviderReceivable, Side: ledger.Credit, AmountMinor: math.MaxInt64, Currency: "TWD"},
			}
		}, want: ledger.ErrAmountOverflow},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := valid
			request.Postings = append([]ledger.Posting(nil), valid.Postings...)
			test.mutate(&request)
			journal, err := ledger.NewJournal(ledger.NewMemoryStore(), fixedClock{now: time.Now()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := journal.Append(context.Background(), request); !errors.Is(err, test.want) {
				t.Fatalf("Append() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestJournalRejectsChangedReplayAndKeepsCommittedPostingsImmutable(t *testing.T) {
	t.Parallel()

	journal, err := ledger.NewJournal(ledger.NewMemoryStore(), fixedClock{now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	request := ledger.AppendRequest{
		EventID: "capture:operation-2", Correlation: "payment:intent-2", Purpose: ledger.PurposeCapture, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Debit, AmountMinor: 500, Currency: "TWD"},
			{Account: ledger.AccountProviderReceivable, Side: ledger.Credit, AmountMinor: 500, Currency: "TWD"},
		},
	}
	stored, err := journal.Append(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	request.Postings[0].AmountMinor = 600
	request.Postings[1].AmountMinor = 600
	if _, err := journal.Append(context.Background(), request); !errors.Is(err, ledger.ErrEventConflict) {
		t.Fatalf("changed replay error = %v, want ErrEventConflict", err)
	}
	stored.Postings[0].AmountMinor = 1
	found, ok, err := journal.Find(context.Background(), stored.ID)
	if err != nil || !ok {
		t.Fatalf("Find() = (%+v, %v, %v)", found, ok, err)
	}
	if got := found.Postings[0].AmountMinor; got != 500 {
		t.Fatalf("committed posting mutated to %d", got)
	}
}

func TestJournalReversalSwapsSidesAndPermitsAtMostOne(t *testing.T) {
	t.Parallel()

	journal, err := ledger.NewJournal(ledger.NewMemoryStore(), fixedClock{now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	original, err := journal.Append(context.Background(), ledger.AppendRequest{
		EventID: "issued:order-1", Correlation: "payment:intent-3", Purpose: ledger.PurposeTicketIssuance, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Debit, AmountMinor: 800, Currency: "TWD"},
			{Account: ledger.AccountTicketSales, Side: ledger.Credit, AmountMinor: 800, Currency: "TWD"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := ledger.ReverseRequest{EventID: "reverse:order-1", Correlation: "manual-review:1", OriginalTransactionID: original.ID}
	reversal, err := journal.Reverse(context.Background(), request)
	if err != nil {
		t.Fatalf("Reverse() error = %v", err)
	}
	if reversal.ReversalOf == nil || *reversal.ReversalOf != original.ID || reversal.Purpose != ledger.PurposeReversal || reversal.Postings[0].Side != ledger.Credit || reversal.Postings[1].Side != ledger.Debit {
		t.Fatalf("unexpected reversal = %+v", reversal)
	}
	replayed, err := journal.Reverse(context.Background(), request)
	if err != nil || replayed.ID != reversal.ID {
		t.Fatalf("replayed Reverse() = (%+v, %v), want ID %s", replayed, err, reversal.ID)
	}
	if _, err := journal.Reverse(context.Background(), ledger.ReverseRequest{
		EventID: "reverse:order-1-again", Correlation: "manual-review:2", OriginalTransactionID: original.ID,
	}); !errors.Is(err, ledger.ErrAlreadyReversed) {
		t.Fatalf("second reversal error = %v, want ErrAlreadyReversed", err)
	}
	if _, err := journal.Reverse(context.Background(), ledger.ReverseRequest{
		EventID: "reverse:missing", Correlation: "manual-review:3", OriginalTransactionID: uuid.New(),
	}); !errors.Is(err, ledger.ErrNotFound) {
		t.Fatalf("missing reversal error = %v, want ErrNotFound", err)
	}
}

func TestJournalConcurrentExactReplayConvergesOnOneTransaction(t *testing.T) {
	t.Parallel()

	journal, err := ledger.NewJournal(ledger.NewMemoryStore(), fixedClock{now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	request := ledger.AppendRequest{
		EventID: "settlement:line-1", Correlation: "payout:1", Purpose: ledger.PurposeSettlement, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountSettlementCash, Side: ledger.Debit, AmountMinor: 900, Currency: "TWD"},
			{Account: ledger.AccountProviderReceivable, Side: ledger.Credit, AmountMinor: 900, Currency: "TWD"},
		},
	}
	const callers = 32
	ids := make(chan uuid.UUID, callers)
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			transaction, err := journal.Append(context.Background(), request)
			if err != nil {
				errs <- err
				return
			}
			ids <- transaction.ID
		}()
	}
	group.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Append() error = %v", err)
	}
	var expected uuid.UUID
	for id := range ids {
		if expected == uuid.Nil {
			expected = id
		}
		if id != expected {
			t.Fatalf("concurrent replay IDs differ: %s and %s", expected, id)
		}
	}
}
