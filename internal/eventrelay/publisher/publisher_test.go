package publisher_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/publisher"
	"github.com/google/uuid"
)

func TestLogPublisherEmitsMetadataWithoutEventPayload(t *testing.T) {
	t.Parallel()

	const payloadSecret = "sentinel-m11-event-payload-secret"
	var output bytes.Buffer
	adapter, err := publisher.NewLog(slog.New(slog.NewJSONHandler(&output, nil)))
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID:            uuid.New(),
		AggregateType: "reservation",
		AggregateID:   uuid.New(),
		EventType:     "reservation.held",
		EventVersion:  1,
		Payload:       json.RawMessage(`{"secret":"` + payloadSecret + `"}`),
	}

	if err := adapter.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	logged := output.String()
	if strings.Contains(logged, payloadSecret) || strings.Contains(logged, string(event.Payload)) {
		t.Fatalf("log publisher exposed event payload: %s", logged)
	}
	for _, want := range []string{"reservation.held", `"event_version":1`} {
		if !strings.Contains(logged, want) {
			t.Fatalf("log publisher output %q does not contain %q", logged, want)
		}
	}
}
