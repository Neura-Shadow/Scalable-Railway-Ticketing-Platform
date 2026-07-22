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

func TestEveryHTTPRouteFamilyUsesABoundedCanonicalPattern(t *testing.T) {
	t.Parallel()

	patterns := []string{
		"/api/v1/auth/login",
		"/api/v1/passengers/:id",
		"/api/v1/ticket-orders/:id",
		"/api/v1/reservations/:id/confirm",
		"/api/v1/train-runs/:id/availability",
		"/api/v1/admin/fares",
		"/api/v1/operator/train-runs/:id/status",
		"/api/v1/operator/hot-train-policies/:id",
		"/api/v1/waiting-room/entries/:id",
	}
	for _, pattern := range patterns {
		if got := metrics.NormalizePath(pattern); got == metrics.UnknownPath {
			t.Fatalf("NormalizePath(%q) = unknown", pattern)
		}
	}
}

func TestAdmissionMetricsExposeExactFamiliesWithBoundedLabels(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	recorder, err := metrics.New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	sentinel := "customer-secret-019f661d"
	recorder.RecordWaitingRoomJoin("failure", sentinel, sentinel)
	recorder.RecordWaitingRoomCancel("success", "", "standard")
	recorder.RecordWaitingRoomExpired("business")
	recorder.RecordAdmissionExpirations(3, 2, "standard")
	recorder.RecordAdmissionAttempt("success", "", "first", 2*time.Second)
	recorder.RecordAdmissionToken("acquire", "conflict", sentinel)
	recorder.RecordReservationQuotaRejected(sentinel, sentinel)
	recorder.RecordReservationBackpressureRejected(sentinel, sentinel)
	recorder.RecordHotTrainReservation("conflict", "no_inventory", "standard", time.Second)
	recorder.RecordAdmissionWorkerPass(true, 250*time.Millisecond, time.Unix(1_784_380_800, 0))
	recorder.RecordAdmissionWorkerPass(false, -time.Second, time.Unix(1_784_380_900, 0))
	recorder.RecordAdmissionWorkerState(17, 9)

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	names := make(map[string]struct{}, len(families))
	var expiredTokens float64
	var expiredEntries float64
	for _, family := range families {
		names[family.GetName()] = struct{}{}
		for _, sample := range family.GetMetric() {
			labels := make(map[string]string, len(sample.GetLabel()))
			for _, label := range sample.GetLabel() {
				labels[label.GetName()] = label.GetValue()
				if strings.Contains(label.GetValue(), sentinel) {
					t.Fatalf("metric %s label %s contains caller value", family.GetName(), label.GetName())
				}
			}
			switch family.GetName() {
			case "admission_token_expired_total":
				if labels["reason"] == "ttl" {
					expiredTokens += sample.GetCounter().GetValue()
				}
			case "waiting_room_expired_total":
				if labels["seat_class"] == "standard" {
					expiredEntries += sample.GetCounter().GetValue()
				}
			}
		}
	}
	if expiredTokens != 3 || expiredEntries != 2 {
		t.Fatalf("independent expiry counters = (tokens=%v, entries=%v), want (3, 2)", expiredTokens, expiredEntries)
	}

	for _, name := range []string{
		"waiting_room_join_total",
		"waiting_room_join_failure_total",
		"waiting_room_cancel_total",
		"waiting_room_expired_total",
		"waiting_room_admission_attempt_total",
		"waiting_room_admission_issued_total",
		"waiting_room_wait_duration_seconds",
		"admission_token_acquire_total",
		"admission_token_conflict_total",
		"reservation_quota_rejected_total",
		"reservation_backpressure_rejected_total",
		"hot_train_reservation_total",
		"hot_train_reservation_conflict_total",
		"hot_train_reservation_duration_seconds",
		"admission_worker_pass_total",
		"admission_worker_pass_duration_seconds",
		"admission_worker_last_success_timestamp_seconds",
		"waiting_room_queue_depth",
		"waiting_room_inflight_admissions",
	} {
		if _, ok := names[name]; !ok {
			t.Errorf("metric family %s was not registered or observed", name)
		}
	}
}

func TestAdmissionWorkerLastSuccessAdvancesOnlyOnSuccessfulPass(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	recorder, err := metrics.New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	successAt := time.Unix(1_784_380_800, 0)
	recorder.RecordAdmissionWorkerPass(true, time.Second, successAt)
	recorder.RecordAdmissionWorkerPass(false, time.Second, successAt.Add(time.Minute))

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() != "admission_worker_last_success_timestamp_seconds" {
			continue
		}
		samples := family.GetMetric()
		if len(samples) != 1 || samples[0].GetGauge().GetValue() != float64(successAt.Unix()) {
			t.Fatalf("last-success sample = %+v, want %d", samples, successAt.Unix())
		}
		return
	}
	t.Fatal("admission_worker_last_success_timestamp_seconds was not gathered")
}
