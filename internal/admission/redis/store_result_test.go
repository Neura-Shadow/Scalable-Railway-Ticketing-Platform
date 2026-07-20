package admissionredis

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestJoinResultMapsSingleElementDomainErrors(t *testing.T) {
	for _, testCase := range []struct {
		name string
		code string
		want error
	}{
		{name: "join conflict", code: "join_conflict", want: ErrJoinConflict},
		{name: "queue full", code: "queue_full", want: ErrQueueFull},
		{name: "policy mismatch", code: "policy_mismatch", want: ErrPolicyMismatch},
		{name: "continuity lost", code: "continuity_lost", want: ErrContinuityLost},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, _, err := parseJoinResult([]any{testCase.code})
			if !errors.Is(err, testCase.want) {
				t.Fatalf("parseJoinResult(%q) error = %v, want %v", testCase.code, err, testCase.want)
			}
		})
	}
}

func TestResultErrorMapsProcessingCancellation(t *testing.T) {
	if err := resultError("in_progress"); !errors.Is(err, ErrInProgress) {
		t.Fatalf("resultError(in_progress) = %v, want %v", err, ErrInProgress)
	}
}

func TestIssueResultSeparatesBoundedMaintenanceCountsFromIssuedEntries(t *testing.T) {
	first := uuid.NewString()
	second := uuid.NewString()
	result, err := parseIssueResult([]any{"ok", int64(2), int64(3), int64(5), first, second})
	if err != nil {
		t.Fatalf("parseIssueResult() error = %v", err)
	}
	if result.RecoveredLeases != 2 || result.ExpiredTokens != 3 || result.ExpiredEntries != 5 ||
		len(result.IssuedEntryIDs) != 2 || result.IssuedEntryIDs[0] != first || result.IssuedEntryIDs[1] != second {
		t.Fatalf("parseIssueResult() = %+v", result)
	}
}

func TestMaintenanceResultRejectsLegacyOrNegativeCounts(t *testing.T) {
	for _, value := range [][]any{
		{"ok", int64(1), int64(2)},
		{"ok", int64(1), int64(2), int64(-1)},
		{"ok", "not-a-number", int64(2), int64(3)},
	} {
		if _, err := parseMaintenanceResult(value); !errors.Is(err, ErrBackendUnavailable) {
			t.Fatalf("parseMaintenanceResult(%v) error = %v, want %v", value, err, ErrBackendUnavailable)
		}
	}
}
