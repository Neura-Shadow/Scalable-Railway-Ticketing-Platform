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
	"/api/v1/auth/register":               "/api/v1/auth/register",
	"/api/v1/auth/login":                  "/api/v1/auth/login",
	"/api/v1/auth/refresh":                "/api/v1/auth/refresh",
	"/api/v1/auth/logout":                 "/api/v1/auth/logout",
	"/api/v1/stations":                    "/api/v1/stations",
	"/api/v1/routes":                      "/api/v1/routes",
	"/api/v1/train-runs/search":           "/api/v1/train-runs/search",
	"/api/v1/train-runs/:id/availability": "/api/v1/train-runs/:train_run_id/availability",
	"/api/v1/train-runs/:train_run_id/availability":  "/api/v1/train-runs/:train_run_id/availability",
	"/api/v1/reservations":                           "/api/v1/reservations",
	"/api/v1/reservations/:id":                       "/api/v1/reservations/:reservation_id",
	"/api/v1/reservations/:reservation_id":           "/api/v1/reservations/:reservation_id",
	"/api/v1/reservations/:id/confirm":               "/api/v1/reservations/:reservation_id/confirm",
	"/api/v1/reservations/:reservation_id/confirm":   "/api/v1/reservations/:reservation_id/confirm",
	"/api/v1/reservations/:id/cancel":                "/api/v1/reservations/:reservation_id/cancel",
	"/api/v1/reservations/:reservation_id/cancel":    "/api/v1/reservations/:reservation_id/cancel",
	"/api/v1/passengers":                             "/api/v1/passengers",
	"/api/v1/passengers/:id":                         "/api/v1/passengers/:passenger_id",
	"/api/v1/ticket-orders":                          "/api/v1/ticket-orders",
	"/api/v1/ticket-orders/:id":                      "/api/v1/ticket-orders/:ticket_order_id",
	"/api/v1/tickets/:id":                            "/api/v1/tickets/:ticket_id",
	"/api/v1/payment-intents/:id":                    "/api/v1/payment-intents/:payment_intent_id",
	"/api/v1/payment-intents/:id/cancel":             "/api/v1/payment-intents/:payment_intent_id/cancel",
	"/api/v1/reservations/:id/payment-intents":       "/api/v1/reservations/:reservation_id/payment-intents",
	"/webhooks/payments/:provider":                   "/webhooks/payments/:provider",
	"/api/v1/admin/stations":                         "/api/v1/admin/stations",
	"/api/v1/admin/routes":                           "/api/v1/admin/routes",
	"/api/v1/admin/trains":                           "/api/v1/admin/trains",
	"/api/v1/admin/coaches":                          "/api/v1/admin/coaches",
	"/api/v1/admin/seats":                            "/api/v1/admin/seats",
	"/api/v1/admin/fares":                            "/api/v1/admin/fares",
	"/api/v1/operator/train-runs":                    "/api/v1/operator/train-runs",
	"/api/v1/operator/train-runs/:id/inventory":      "/api/v1/operator/train-runs/:train_run_id/inventory",
	"/api/v1/operator/train-runs/:id/status":         "/api/v1/operator/train-runs/:train_run_id/status",
	"/api/v1/operator/hot-train-policies":            "/api/v1/operator/hot-train-policies",
	"/api/v1/operator/hot-train-policies/:id":        "/api/v1/operator/hot-train-policies/:policy_id",
	"/api/v1/operator/hot-train-policies/:policy_id": "/api/v1/operator/hot-train-policies/:policy_id",
	"/api/v1/waiting-room/entries":                   "/api/v1/waiting-room/entries",
	"/api/v1/waiting-room/entries/:id":               "/api/v1/waiting-room/entries/:entry_id",
	"/api/v1/waiting-room/entries/:entry_id":         "/api/v1/waiting-room/entries/:entry_id",
}

var allowedEventTypes = set(
	"reservation.held",
	"reservation.confirmed",
	"reservation.cancelled",
	"reservation.expired",
	"ticket.created",
	"trainrun.created",
	"trainrun.updated",
	"trainrun.cancelled",
	"outbox.dead_lettered",
	"hot_train_policy.created",
	"hot_train_policy.updated",
	"hot_train_policy.disabled",
	"station.created",
	"station.updated",
	"station.disabled",
	"route.created",
	"route.updated",
	"route.disabled",
	"train.updated",
	"coach.updated",
	"seat.updated",
	"fare.created",
	"fare.updated",
	"fare.disabled",
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
	"queue_full",
	"quota",
	"backpressure",
	"redis",
	"processing",
	"policy_version",
	"continuity_lost",
	"owner_mismatch",
	"mac_invalid",
	"binding_conflict",
	"no_inventory",
	"admission_required",
	"token_invalid",
	"token_expired",
	"capacity",
	"continuity",
	"maintenance",
	"queue_read",
	"redis_time",
	"token_generation",
	"issue",
	"locator",
	"state_counts",
	"ttl",
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
	admission           *admissionMetrics
	sharding            *shardingMetrics
	physical            *physicalMetrics
	payment             *paymentMetrics
}

// New registers the platform's bounded metric families.
func New(registerer prometheus.Registerer) (*Metrics, error) {
	if registerer == nil {
		return nil, errors.New("metrics: nil Prometheus registerer")
	}

	admission := newAdmissionMetrics()
	sharding := newShardingMetrics()
	physical := newPhysicalMetrics()
	payment := newPaymentMetrics()
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
		admission: admission,
		sharding:  sharding,
		physical:  physical,
		payment:   payment,
	}

	for _, collector := range []prometheus.Collector{
		m.httpRequests,
		m.httpDuration,
		m.reservationEvents,
		m.seatInventoryEvents,
		m.outboxEvents,
		m.admission.waitingRoomJoin,
		m.admission.waitingRoomJoinFailure,
		m.admission.waitingRoomDuplicateJoin,
		m.admission.waitingRoomQueueFull,
		m.admission.waitingRoomCancel,
		m.admission.waitingRoomExpired,
		m.admission.admissionAttempt,
		m.admission.admissionIssued,
		m.admission.admissionFailure,
		m.admission.waitDuration,
		m.admission.tokenAcquire,
		m.admission.tokenConsume,
		m.admission.tokenRelease,
		m.admission.tokenExpired,
		m.admission.tokenConflict,
		m.admission.quotaRejected,
		m.admission.backpressureRejected,
		m.admission.hotReservation,
		m.admission.hotReservationConflict,
		m.admission.hotReservationDuration,
		m.admission.admissionWorkerPass,
		m.admission.admissionWorkerDuration,
		m.admission.admissionWorkerLastSuccess,
		m.admission.waitingRoomQueueDepth,
		m.admission.waitingRoomInflight,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("metrics: register collector: %w", err)
		}
	}
	for _, collector := range m.sharding.collectors() {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("metrics: register sharding collector: %w", err)
		}
	}
	for _, collector := range m.physical.collectors() {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("metrics: register physical collector: %w", err)
		}
	}
	for _, collector := range m.payment.collectors() {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("metrics: register payment collector: %w", err)
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
