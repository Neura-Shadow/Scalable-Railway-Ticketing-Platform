package admissionredis

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func TestParseGenerationContinuityKeyIsStrictAndBounded(t *testing.T) {
	runID := uuid.New()
	key := fmt.Sprintf("railway:wr:{%s|business}:v17:continuity", runID)
	scope, err := parseGenerationContinuityKey("railway", key)
	if err != nil {
		t.Fatal(err)
	}
	if scope.TrainRunID != runID.String() || scope.SeatClass != "business" || scope.Version != 17 {
		t.Fatalf("scope = %+v", scope)
	}

	for _, malformed := range []string{
		"railway:wr:{" + runID.String() + "|premium}:v17:continuity",
		"railway:wr:{" + runID.String() + "|business}:v0:continuity",
		"railway:wr:{../../secret|business}:v17:continuity",
		"railway:wr:{" + runID.String() + "|business}:v17:tokens",
	} {
		if _, err := parseGenerationContinuityKey("railway", malformed); !errors.Is(err, ErrBackendUnavailable) {
			t.Fatalf("parseGenerationContinuityKey(%q) error = %v", malformed, err)
		}
	}
}
