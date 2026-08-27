package settlement_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
)

func TestImporterRunOnceCommitsBoundedPagesAndCursor(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_test_ops"}
	source := settlement.NewMemorySource(map[string]settlement.Page{
		"": {
			Records:    []settlement.Record{{Kind: settlement.RecordBalanceTransaction, ProviderID: "txn_1", Operation: settlement.OperationCapture, GrossMinor: 1_000, FeeMinor: 30, NetMinor: 970, Currency: "TWD", CreatedAt: time.Date(2026, 8, 11, 1, 0, 0, 0, time.UTC)}},
			NextCursor: "cursor-1",
		},
		"cursor-1": {
			Records:    []settlement.Record{{Kind: settlement.RecordPayout, ProviderID: "po_1", Operation: settlement.OperationPayout, GrossMinor: 970, NetMinor: 970, Currency: "TWD", CreatedAt: time.Date(2026, 8, 11, 2, 0, 0, 0, time.UTC), PayoutStatus: "paid"}},
			NextCursor: "cursor-2",
			Done:       true,
		},
	})
	store := settlement.NewMemoryImportStore()
	importer, err := settlement.NewImporter(source, store, settlement.ImporterConfig{PageSize: 10, MaxPages: 2})
	if err != nil {
		t.Fatal(err)
	}

	report, err := importer.RunOnce(context.Background(), scope)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if report.Pages != 2 || report.Inserted != 2 || !report.Completed || report.EndCursor != "cursor-2" {
		t.Fatalf("RunOnce() report = %+v", report)
	}
	if got, err := store.Checkpoint(context.Background(), scope); err != nil || got != "cursor-2" {
		t.Fatalf("Checkpoint() = (%q, %v), want cursor-2", got, err)
	}
}

func TestImporterReplayIsHarmlessAndChangedIdentityIsConflictEvidence(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_test_conflict"}
	createdAt := time.Date(2026, 8, 11, 3, 0, 0, 0, time.UTC)
	original := settlement.Record{
		Kind: settlement.RecordBalanceTransaction, ProviderID: "txn_conflict", Operation: settlement.OperationCapture,
		GrossMinor: 1_000, FeeMinor: 30, NetMinor: 970, Currency: "TWD", CreatedAt: createdAt,
	}
	source := settlement.NewMemorySource(map[string]settlement.Page{
		"": {Records: []settlement.Record{original}, NextCursor: "cursor-1", Done: true},
	})
	store := settlement.NewMemoryImportStore()
	importer, err := settlement.NewImporter(source, store, settlement.ImporterConfig{PageSize: 10, MaxPages: 2})
	if err != nil {
		t.Fatal(err)
	}
	if report, err := importer.RunOnce(context.Background(), scope); err != nil || report.Inserted != 1 {
		t.Fatalf("first RunOnce() = (%+v, %v)", report, err)
	}

	source.SetPage("cursor-1", settlement.Page{Records: []settlement.Record{original}, NextCursor: "cursor-2", Done: true})
	if report, err := importer.RunOnce(context.Background(), scope); err != nil || report.Replayed != 1 || report.Conflicts != 0 {
		t.Fatalf("exact replay RunOnce() = (%+v, %v)", report, err)
	}

	changed := original
	changed.FeeMinor = 31
	changed.NetMinor = 969
	source.SetPage("cursor-2", settlement.Page{Records: []settlement.Record{changed}, NextCursor: "cursor-3", Done: true})
	report, err := importer.RunOnce(context.Background(), scope)
	if !errors.Is(err, settlement.ErrPayloadConflict) || report.Conflicts != 1 || report.EndCursor != "cursor-3" {
		t.Fatalf("changed RunOnce() = (%+v, %v), want committed conflict", report, err)
	}
	stored, found, err := store.Record(context.Background(), scope, settlement.RecordBalanceTransaction, original.ProviderID)
	if err != nil || !found || stored.NetMinor != original.NetMinor {
		t.Fatalf("stored original = (%+v, %v, %v)", stored, found, err)
	}
	conflicts, err := store.PayloadConflicts(context.Background(), scope)
	if err != nil || len(conflicts) != 1 || conflicts[0].StoredHash == conflicts[0].IncomingHash {
		t.Fatalf("PayloadConflicts() = (%+v, %v)", conflicts, err)
	}
}

func TestImporterRejectsUncheckedProviderArithmetic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		gross int64
		fee   int64
		net   int64
	}{
		{name: "mismatched net", gross: 100, fee: 3, net: 98},
		{name: "negative fee", gross: 100, fee: -1, net: 101},
		{name: "absolute value overflow", gross: math.MinInt64, fee: 0, net: math.MinInt64},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := settlement.NewMemorySource(map[string]settlement.Page{"": {
				Records: []settlement.Record{{
					Kind: settlement.RecordBalanceTransaction, ProviderID: "txn_invalid",
					Operation: settlement.OperationCapture, GrossMinor: test.gross,
					FeeMinor: test.fee, NetMinor: test.net, Currency: "TWD", CreatedAt: time.Now(),
				}},
				NextCursor: "next", Done: true,
			}})
			importer, err := settlement.NewImporter(source, settlement.NewMemoryImportStore(), settlement.ImporterConfig{PageSize: 1, MaxPages: 1})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := importer.RunOnce(context.Background(), settlement.AccountScope{Provider: "stripe", AccountID: "acct_test"}); !errors.Is(err, settlement.ErrInvalidRecord) {
				t.Fatalf("RunOnce() error = %v, want ErrInvalidRecord", err)
			}
		})
	}
}

func TestImporterRejectsUnboundedPayoutStatus(t *testing.T) {
	t.Parallel()

	source := settlement.NewMemorySource(map[string]settlement.Page{"": {
		Records: []settlement.Record{{
			Kind: settlement.RecordPayout, ProviderID: "po_invalid", Operation: settlement.OperationPayout,
			GrossMinor: 100, NetMinor: 100, Currency: "TWD", CreatedAt: time.Now(), PayoutStatus: "PAID",
		}},
		NextCursor: "next", Done: true,
	}})
	importer, err := settlement.NewImporter(source, settlement.NewMemoryImportStore(), settlement.ImporterConfig{PageSize: 1, MaxPages: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.RunOnce(context.Background(), settlement.AccountScope{Provider: "stripe", AccountID: "acct_test"}); !errors.Is(err, settlement.ErrInvalidRecord) {
		t.Fatalf("RunOnce() error = %v, want ErrInvalidRecord", err)
	}
}

func TestImporterStopsAtConfiguredPageBound(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_test_bounded"}
	record := func(id string, minute int) settlement.Record {
		return settlement.Record{
			Kind: settlement.RecordSettlementLine, ProviderID: id, Operation: settlement.OperationSettlement,
			GrossMinor: 100, NetMinor: 100, Currency: "TWD", CreatedAt: time.Date(2026, 8, 11, 4, minute, 0, 0, time.UTC),
		}
	}
	source := settlement.NewMemorySource(map[string]settlement.Page{
		"":         {Records: []settlement.Record{record("line_1", 1)}, NextCursor: "cursor-1"},
		"cursor-1": {Records: []settlement.Record{record("line_2", 2)}, NextCursor: "cursor-2"},
		"cursor-2": {Records: []settlement.Record{record("line_3", 3)}, NextCursor: "cursor-3", Done: true},
	})
	store := settlement.NewMemoryImportStore()
	importer, err := settlement.NewImporter(source, store, settlement.ImporterConfig{PageSize: 1, MaxPages: 2})
	if err != nil {
		t.Fatal(err)
	}
	report, err := importer.RunOnce(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if report.Pages != 2 || report.Inserted != 2 || !report.Bounded || report.Completed || report.EndCursor != "cursor-2" {
		t.Fatalf("bounded report = %+v", report)
	}
	if calls := source.Calls(); len(calls) != 2 {
		t.Fatalf("provider calls = %v, want 2", calls)
	}
}

func TestImporterRejectsOversizedOrStalledPagesAndHonorsCancellation(t *testing.T) {
	t.Parallel()

	validRecord := settlement.Record{
		Kind: settlement.RecordPayout, ProviderID: "po_limit", Operation: settlement.OperationPayout,
		GrossMinor: 100, NetMinor: 100, Currency: "TWD", CreatedAt: time.Now(), PayoutStatus: "paid",
	}
	tests := []struct {
		name string
		page settlement.Page
		want error
	}{
		{name: "oversized", page: settlement.Page{Records: []settlement.Record{validRecord, validRecord}, NextCursor: "next"}, want: settlement.ErrPageLimit},
		{name: "stalled", page: settlement.Page{Records: []settlement.Record{validRecord}, NextCursor: ""}, want: settlement.ErrCursorStalled},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			importer, err := settlement.NewImporter(
				settlement.NewMemorySource(map[string]settlement.Page{"": test.page}),
				settlement.NewMemoryImportStore(),
				settlement.ImporterConfig{PageSize: 1, MaxPages: 1},
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := importer.RunOnce(context.Background(), settlement.AccountScope{Provider: "stripe", AccountID: "acct_test"}); !errors.Is(err, test.want) {
				t.Fatalf("RunOnce() error = %v, want %v", err, test.want)
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	importer, err := settlement.NewImporter(
		settlement.NewMemorySource(nil), settlement.NewMemoryImportStore(), settlement.ImporterConfig{PageSize: 1, MaxPages: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.RunOnce(ctx, settlement.AccountScope{Provider: "stripe", AccountID: "acct_test"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled RunOnce() error = %v", err)
	}
}

func TestImporterResumesAfterBoundedPartialPageFailureWithoutSkippingCommittedCursor(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_partial"}
	at := time.Date(2026, 8, 17, 5, 0, 0, 0, time.UTC)
	record := func(id string) settlement.Record {
		return settlement.Record{
			Kind: settlement.RecordBalanceTransaction, ProviderID: id, Operation: settlement.OperationCapture,
			GrossMinor: 100, NetMinor: 100, Currency: "TWD", CreatedAt: at,
		}
	}
	sourceFailure := errors.New("bounded second page failure")
	source := &partialFailureSource{pages: map[string]settlement.Page{
		"":         {Records: []settlement.Record{record("txn_partial_1")}, NextCursor: "cursor-1"},
		"cursor-1": {Records: []settlement.Record{record("txn_partial_2")}, NextCursor: "cursor-2", Done: true},
	}, failCursor: "cursor-1", failErr: sourceFailure}
	store := settlement.NewMemoryImportStore()
	importer, err := settlement.NewImporter(source, store, settlement.ImporterConfig{PageSize: 1, MaxPages: 2})
	if err != nil {
		t.Fatal(err)
	}

	report, err := importer.RunOnce(context.Background(), scope)
	if !errors.Is(err, sourceFailure) || report.Inserted != 1 || report.EndCursor != "cursor-1" {
		t.Fatalf("partial pass report=%+v err=%v", report, err)
	}
	if cursor, err := store.Checkpoint(context.Background(), scope); err != nil || cursor != "cursor-1" {
		t.Fatalf("checkpoint after partial failure = %q, %v", cursor, err)
	}

	report, err = importer.RunOnce(context.Background(), scope)
	if err != nil || report.Inserted != 1 || !report.Completed || report.StartCursor != "cursor-1" || report.EndCursor != "cursor-2" {
		t.Fatalf("resumed pass report=%+v err=%v", report, err)
	}
}

func TestImporterTreatsReorderedDuplicatePagesAsReplay(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_reordered"}
	at := time.Date(2026, 8, 17, 6, 0, 0, 0, time.UTC)
	first := settlement.Record{Kind: settlement.RecordSettlementLine, ProviderID: "line_a", Operation: settlement.OperationSettlement, GrossMinor: 70, NetMinor: 70, Currency: "TWD", CreatedAt: at}
	second := settlement.Record{Kind: settlement.RecordSettlementLine, ProviderID: "line_b", Operation: settlement.OperationSettlement, GrossMinor: 30, NetMinor: 30, Currency: "TWD", CreatedAt: at.Add(time.Second)}
	source := settlement.NewMemorySource(map[string]settlement.Page{
		"":         {Records: []settlement.Record{first, second}, NextCursor: "cursor-1"},
		"cursor-1": {Records: []settlement.Record{second, first}, NextCursor: "cursor-2", Done: true},
	})
	importer, err := settlement.NewImporter(source, settlement.NewMemoryImportStore(), settlement.ImporterConfig{PageSize: 2, MaxPages: 2})
	if err != nil {
		t.Fatal(err)
	}
	report, err := importer.RunOnce(context.Background(), scope)
	if err != nil || report.Inserted != 2 || report.Replayed != 2 || report.Conflicts != 0 || !report.Completed {
		t.Fatalf("reordered duplicate report=%+v err=%v", report, err)
	}
}

type partialFailureSource struct {
	pages      map[string]settlement.Page
	failCursor string
	failErr    error
	failed     bool
}

func (source *partialFailureSource) ListPage(_ context.Context, _ settlement.AccountScope, cursor string, _ int) (settlement.Page, error) {
	if cursor == source.failCursor && !source.failed {
		source.failed = true
		return settlement.Page{}, source.failErr
	}
	return source.pages[cursor], nil
}
