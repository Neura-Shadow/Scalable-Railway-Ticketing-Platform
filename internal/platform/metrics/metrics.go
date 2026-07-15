// Package metrics exposes bounded Prometheus recorders. All caller-controlled
// values pass through finite allowlists before they become labels.
package metrics

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	UnknownEventType = "unknown"
	UnknownReason    = "unknown"
	UnknownPath      = "/unknown"
)

var allowedPaths = map[string]string{
	"/livez":                              "/livez",
	"/readyz":                             "/readyz",
	"/metrics":                            "/metrics",
	"/api/v1/stations":                    "/api/v1/stations",
	"/api/v1/routes":                      "/api/v1/routes",
	"/api/v1/train-runs/search":           "/api/v1/train-runs/search",
	"/api/v1/train-runs/:id/availability": "/api/v1/train-runs/:train_run_id/availability",
	"/api/v1/train-runs/:train_run_id/availability": "/api/v1/train-runs/:train_run_id/availability",
	"/api/v1/reservations":                          "/api/v1/reservations",
	"/api/v1/reservations/:id":                      "/api/v1/reservations/:reservation_id",
	"/api/v1/reservations/:reservation_id":          "/api/v1/reservations/:reservation_id",
	"/api/v1/reservations/:id/confirm":              "/api/v1/reservations/:reservation_id/confirm",
	"/api/v1/reservations/:reservation_id/confirm":  "/api/v1/reservations/:reservation_id/confirm",
	"/api/v1/reservations/:id/cancel":               "/api/v1/reservations/:reservation_id/cancel",
	"/api/v1/reservations/:reservation_id/cancel":   "/api/v1/reservations/:reservation_id/cancel",
}

var allowedEventTypes = set(
	"reservation.held",
	"reservation.confirmed",
	"reservation.cancelled",
	"reservation.expired",
	"ticket.issued",
	"train_run.cancelled",
	"outbox.dead_lettered",
)

var allowedReasons = set(
	"none",
	"invalid_request",
	"unauthenticated",
	"forbidden",
	"not_found",
	"conflict",
	"expired",
	"cancelled",
	"timeout",
	"unavailable",
	"database",
	"internal",
	"duplicate",
	"rate_limited",
	"stale_lock",
)

var (
	allowedMethods               = set("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
	allowedResults               = set("success", "failure", "conflict", "duplicate", "skipped")
	allowedReservationOperations = set("hold", "confirm", "cancel", "expire")
	allowedSeatOperations        = set("allocate", "release")
	allowedOutboxOperations      = set("create", "claim", "publish", "finalize", "dead_letter")
)

// Metrics records application activity without exposing raw Prometheus
// vectors to callers.
type Metrics struct {
	httpRequests        *prometheus.CounterVec
	httpDuration        *prometheus.HistogramVec
	reservationEvents   *prometheus.CounterVec
	seatInventoryEvents *prometheus.CounterVec
	outboxEvents        *prometheus.CounterVec
}

// New registers the platform's bounded metric families.
func New(registerer prometheus.Registerer) (*Metrics, error) {
	if registerer == nil {
		return nil, errors.New("metrics: nil Prometheus registerer")
	}

	m := &Metrics{
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests by bounded method, route pattern, and status.",
		}, []string{"method", "path", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration by bounded method and route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
		reservationEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservation_operations_total",
			Help: "Reservation operations by bounded operation, result, and reason.",
		}, []string{"operation", "result", "reason"}),
		seatInventoryEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "seat_inventory_operations_total",
			Help: "Seat inventory operations by bounded operation, result, and reason.",
		}, []string{"operation", "result", "reason"}),
		outboxEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_operations_total",
			Help: "Outbox operations by bounded operation, event type, result, and reason.",
		}, []string{"operation", "event_type", "result", "reason"}),
	}

	for _, collector := range []prometheus.Collector{
		m.httpRequests,
		m.httpDuration,
		m.reservationEvents,
		m.seatInventoryEvents,
		m.outboxEvents,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("metrics: register collector: %w", err)
		}
	}
	return m, nil
}

// ObserveHTTP records one HTTP request. path must be a normalized router
// pattern; raw or unknown paths collapse to /unknown.
func (m *Metrics) ObserveHTTP(method, path string, status int, duration time.Duration) {
	normalizedMethod := normalizeMethod(method)
	normalizedPath := NormalizePath(path)
	normalizedStatus := normalizeStatus(status)
	seconds := duration.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	m.httpRequests.WithLabelValues(normalizedMethod, normalizedPath, normalizedStatus).Inc()
	m.httpDuration.WithLabelValues(normalizedMethod, normalizedPath).Observe(seconds)
}

// RecordReservation records a reservation lifecycle operation.
func (m *Metrics) RecordReservation(operation, result, reason string) {
	m.reservationEvents.WithLabelValues(
		normalize(operation, allowedReservationOperations, "unknown"),
		normalize(result, allowedResults, "unknown"),
		NormalizeReason(reason),
	).Inc()
}

// RecordSeatInventory records an allocation or release operation.
func (m *Metrics) RecordSeatInventory(operation, result, reason string) {
	m.seatInventoryEvents.WithLabelValues(
		normalize(operation, allowedSeatOperations, "unknown"),
		normalize(result, allowedResults, "unknown"),
		NormalizeReason(reason),
	).Inc()
}

// RecordOutbox records an outbox lifecycle operation.
func (m *Metrics) RecordOutbox(operation, eventType, result, reason string) {
	m.outboxEvents.WithLabelValues(
		normalize(operation, allowedOutboxOperations, "unknown"),
		NormalizeEventType(eventType),
		normalize(result, allowedResults, "unknown"),
		NormalizeReason(reason),
	).Inc()
}

// NormalizePath converts known router patterns to canonical bounded labels.
func NormalizePath(path string) string {
	if canonical, ok := allowedPaths[strings.TrimSpace(path)]; ok {
		return canonical
	}
	return UnknownPath
}

// NormalizeEventType collapses unknown event types to unknown.
func NormalizeEventType(eventType string) string {
	return normalize(eventType, allowedEventTypes, UnknownEventType)
}

// NormalizeReason collapses unknown reasons to unknown. An omitted reason is
// represented by the bounded none value.
func NormalizeReason(reason string) string {
	if strings.TrimSpace(reason) == "" {
		return "none"
	}
	return normalize(reason, allowedReasons, UnknownReason)
}

func normalizeMethod(method string) string {
	method = strings.ToUpper(strings.TrimSpace(method))
	if _, ok := allowedMethods[method]; ok {
		return method
	}
	return "OTHER"
}

func normalizeStatus(status int) string {
	if status < 100 || status > 599 {
		return "unknown"
	}
	return strconv.Itoa(status)
}

func normalize(value string, allowed map[string]struct{}, fallback string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if _, ok := allowed[value]; ok {
		return value
	}
	return fallback
}

func set(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
