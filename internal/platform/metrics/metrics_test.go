package metrics_test

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestCallerValuesNeverEnterMetricsExpositionOrLabels(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	recorder, err := metrics.New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	sentinelID := "reservation-019f661d-e56c-7f93-8618-942adeed5565"
	sentinelSecret := "jwt-super-secret-sentinel-value"
	recorder.ObserveHTTP("SENTINEL", "/api/v1/reservations/"+sentinelID+"?token="+sentinelSecret, 799, time.Second)
	recorder.RecordReservation(sentinelSecret, sentinelID, sentinelSecret)
	recorder.RecordSeatInventory(sentinelID, sentinelSecret, sentinelID)
	recorder.RecordOutbox(sentinelSecret, sentinelID, sentinelSecret, sentinelID)

	response := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	exposition := response.Body.String()
	for _, forbidden := range []string{sentinelID, sentinelSecret} {
		if strings.Contains(exposition, forbidden) {
			t.Fatalf("metrics exposition contains forbidden caller value %q", forbidden)
		}
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				for _, forbidden := range []string{sentinelID, sentinelSecret} {
					if strings.Contains(label.GetValue(), forbidden) {
						t.Fatalf("label %s contains forbidden caller value %q", label.GetName(), forbidden)
					}
				}
			}
		}
	}
}

func TestNormalizersRetainOnlyBoundedValues(t *testing.T) {
	t.Parallel()

	if got := metrics.NormalizePath("/api/v1/reservations/:id"); got != "/api/v1/reservations/:reservation_id" {
		t.Fatalf("NormalizePath(known pattern) = %q", got)
	}
	if got := metrics.NormalizePath("/api/v1/reservations/customer-provided-id"); got != metrics.UnknownPath {
		t.Fatalf("NormalizePath(raw path) = %q, want %q", got, metrics.UnknownPath)
	}
	if got := metrics.NormalizeEventType(" Reservation.Held "); got != "reservation.held" {
		t.Fatalf("NormalizeEventType(known) = %q", got)
	}
	if got := metrics.NormalizeEventType("reservation.customer-provided-id"); got != metrics.UnknownEventType {
		t.Fatalf("NormalizeEventType(unknown) = %q", got)
	}
	if got := metrics.NormalizeReason(""); got != "none" {
		t.Fatalf("NormalizeReason(empty) = %q", got)
	}
	if got := metrics.NormalizeReason("customer-provided-secret"); got != metrics.UnknownReason {
		t.Fatalf("NormalizeReason(unknown) = %q", got)
	}
}
