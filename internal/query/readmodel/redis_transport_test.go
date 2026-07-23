package readmodel

import (
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestSafeStreamMessagesBoundsUntrustedFields(t *testing.T) {
	messages := safeStreamMessages([]redis.XMessage{{
		ID: "1-0",
		Values: map[string]any{
			"event_id":       strings.Repeat("a", maxSafeStreamFieldLength+1),
			"event_type":     "trainrun.updated",
			"aggregate_type": "train_run",
			"aggregate_id":   strings.Repeat("b", maxSafeStreamFieldLength+1),
		},
	}})

	if len(messages) != 1 {
		t.Fatalf("safeStreamMessages() length = %d, want 1", len(messages))
	}
	if messages[0].Values["event_id"] != invalidSafeStreamField ||
		messages[0].Values["aggregate_id"] != invalidSafeStreamField {
		t.Fatalf("oversized fields were retained: %+v", messages[0].Values)
	}
	if messages[0].Values["event_type"] != "trainrun.updated" {
		t.Fatalf("bounded field changed: %+v", messages[0].Values)
	}
}
