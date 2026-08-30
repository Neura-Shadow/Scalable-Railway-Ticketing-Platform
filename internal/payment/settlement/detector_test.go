package settlement_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
)

func TestDetectorRunOnceRecordsMismatchesWithoutRepairSurface(t *testing.T) {
	t.Parallel()

	scope := settlement.DetectionScope{Kind: settlement.ScopePayout, Value: "po_test_1"}
	store := settlement.NewMemoryDetectionStore()
	store.SetComparisons(scope, []settlement.Comparison{
		{
			Correlation: "payment:1", Kind: settlement.EvidencePayout,
			Expected:       settlement.Evidence{Present: true, AmountMinor: 970, FeeMinor: 30, Currency: "TWD"},
			Observed:       settlement.Evidence{Present: true, AmountMinor: 960, FeeMinor: 40, Currency: "USD"},
			DuplicateCount: 2, Aged: true, EventConflict: true, BalanceChecked: true, LedgerBalanced: false,
			PayoutLifecycleInvalid: true,
		},
		{
			Correlation: "payment:2", Kind: settlement.EvidenceSettlement,
			Expected: settlement.Evidence{Present: true, AmountMinor: 500, Currency: "TWD"},
		},
	})
	detector, err := settlement.NewDetector(store, settlement.DetectorConfig{PageSize: 1, MaxPages: 2})
	if err != nil {
		t.Fatal(err)
	}

	report, err := detector.RunOnce(context.Background(), scope)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	wantReasons := map[settlement.FindingReason]bool{
		settlement.FindingMissing: true, settlement.FindingAmount: true,
		settlement.FindingCurrency: true, settlement.FindingFee: true,
		settlement.FindingDuplicate: true, settlement.FindingAge: true,
		settlement.FindingEventConflict: true, settlement.FindingImbalance: true,
		settlement.FindingPayoutLifecycle: true,
	}
	for _, finding := range report.Findings {
		delete(wantReasons, finding.Reason)
	}
	if len(wantReasons) != 0 {
		t.Fatalf("missing finding reasons %v in %+v", wantReasons, report.Findings)
	}
	if report.Examined != 2 || report.Pages != 2 || !report.Completed {
		t.Fatalf("report = %+v", report)
	}
	runs := store.Runs()
	if len(runs) != 1 || runs[0].ID != report.RunID || len(runs[0].Findings) != len(report.Findings) {
		t.Fatalf("immutable runs = %+v", runs)
	}

	methodSet := reflect.TypeOf(detector)
	if methodSet.NumMethod() != 1 || methodSet.Method(0).Name != "RunOnce" {
		t.Fatalf("Detector public methods = %v; detect-only surface must expose only RunOnce", methodSet.NumMethod())
	}
}
