package metrics

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	allowedSeatClasses      = set("standard", "business", "first")
	allowedAdmissionResults = set(
		"success",
		"failure",
		"conflict",
		"duplicate",
		"acquired",
		"retry_allowed",
		"replay_allowed",
		"in_progress",
		"consumed",
		"released",
		"expired",
		"cancelled",
	)
	allowedTokenOperations = set("acquire", "consume", "release", "expire")
)

const maxAdmissionExpiryMetricIncrement = int64(1_000)

type admissionMetrics struct {
	waitingRoomJoin            *prometheus.CounterVec
	waitingRoomJoinFailure     *prometheus.CounterVec
	waitingRoomDuplicateJoin   *prometheus.CounterVec
	waitingRoomQueueFull       *prometheus.CounterVec
	waitingRoomCancel          *prometheus.CounterVec
	waitingRoomExpired         *prometheus.CounterVec
	admissionAttempt           *prometheus.CounterVec
	admissionIssued            *prometheus.CounterVec
	admissionFailure           *prometheus.CounterVec
	waitDuration               *prometheus.HistogramVec
	tokenAcquire               *prometheus.CounterVec
	tokenConsume               *prometheus.CounterVec
	tokenRelease               *prometheus.CounterVec
	tokenExpired               *prometheus.CounterVec
	tokenConflict              *prometheus.CounterVec
	quotaRejected              *prometheus.CounterVec
	backpressureRejected       *prometheus.CounterVec
	hotReservation             *prometheus.CounterVec
	hotReservationConflict     *prometheus.CounterVec
	hotReservationDuration     *prometheus.HistogramVec
	admissionWorkerPass        *prometheus.CounterVec
	admissionWorkerDuration    *prometheus.HistogramVec
	admissionWorkerLastSuccess prometheus.Gauge
	waitingRoomQueueDepth      prometheus.Gauge
	waitingRoomInflight        prometheus.Gauge
}

func newAdmissionMetrics() *admissionMetrics {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    name,
			Help:    help,
			Buckets: prometheus.DefBuckets,
		}, labels)
	}
	return &admissionMetrics{
		waitingRoomJoin:          counter("waiting_room_join_total", "Waiting-room join attempts.", "result", "reason", "seat_class"),
		waitingRoomJoinFailure:   counter("waiting_room_join_failure_total", "Failed waiting-room joins.", "reason", "seat_class"),
		waitingRoomDuplicateJoin: counter("waiting_room_duplicate_join_total", "Duplicate waiting-room joins.", "seat_class"),
		waitingRoomQueueFull:     counter("waiting_room_queue_full_total", "Waiting-room joins rejected at capacity.", "seat_class"),
		waitingRoomCancel:        counter("waiting_room_cancel_total", "Waiting-room cancellation attempts.", "result", "reason", "seat_class"),
		waitingRoomExpired:       counter("waiting_room_expired_total", "Expired waiting-room entries.", "seat_class"),
		admissionAttempt:         counter("waiting_room_admission_attempt_total", "Waiting-room admission attempts.", "result", "reason", "seat_class"),
		admissionIssued:          counter("waiting_room_admission_issued_total", "Issued waiting-room admissions.", "seat_class"),
		admissionFailure:         counter("waiting_room_admission_failure_total", "Failed waiting-room admissions.", "reason", "seat_class"),
		waitDuration:             histogram("waiting_room_wait_duration_seconds", "Time from queue join to admission.", "seat_class"),
		tokenAcquire:             counter("admission_token_acquire_total", "Admission-token acquire decisions.", "result", "reason"),
		tokenConsume:             counter("admission_token_consume_total", "Admission-token consume decisions.", "result", "reason"),
		tokenRelease:             counter("admission_token_release_total", "Admission-token release decisions.", "result", "reason"),
		tokenExpired:             counter("admission_token_expired_total", "Expired admission tokens.", "reason"),
		tokenConflict:            counter("admission_token_conflict_total", "Admission-token identity conflicts.", "reason"),
		quotaRejected:            counter("reservation_quota_rejected_total", "Reservation creates rejected by durable quota.", "reason", "seat_class"),
		backpressureRejected:     counter("reservation_backpressure_rejected_total", "Reservation creates rejected by local backpressure.", "reason", "seat_class"),
		hotReservation:           counter("hot_train_reservation_total", "Hot-train reservation attempts.", "result", "reason", "seat_class"),
		hotReservationConflict:   counter("hot_train_reservation_conflict_total", "Hot-train reservation conflicts.", "reason", "seat_class"),
		hotReservationDuration:   histogram("hot_train_reservation_duration_seconds", "Hot-train reservation duration.", "result", "seat_class"),
		admissionWorkerPass:      counter("admission_worker_pass_total", "Admission-worker passes by bounded result.", "result"),
		admissionWorkerDuration:  histogram("admission_worker_pass_duration_seconds", "Admission-worker pass duration by bounded result.", "result"),
		admissionWorkerLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "admission_worker_last_success_timestamp_seconds",
			Help: "Unix timestamp of the most recent successful admission-worker pass.",
		}),
		waitingRoomQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "waiting_room_queue_depth",
			Help: "Sum of queued entries observed across the most recent bounded admission-worker policy page.",
		}),
		waitingRoomInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "waiting_room_inflight_admissions",
			Help: "Sum of inflight admissions observed across the most recent bounded admission-worker policy page.",
		}),
	}
}

func (m *Metrics) RecordWaitingRoomJoin(result, reason, seatClass string) {
	result = normalize(result, allowedAdmissionResults, "unknown")
	reason = NormalizeReason(reason)
	seatClass = NormalizeSeatClass(seatClass)
	m.admission.waitingRoomJoin.WithLabelValues(result, reason, seatClass).Inc()
	if result == "failure" || result == "conflict" {
		m.admission.waitingRoomJoinFailure.WithLabelValues(reason, seatClass).Inc()
	}
	if result == "duplicate" {
		m.admission.waitingRoomDuplicateJoin.WithLabelValues(seatClass).Inc()
	}
	if reason == "queue_full" {
		m.admission.waitingRoomQueueFull.WithLabelValues(seatClass).Inc()
	}
}

func (m *Metrics) RecordWaitingRoomCancel(result, reason, seatClass string) {
	m.admission.waitingRoomCancel.WithLabelValues(
		normalize(result, allowedAdmissionResults, "unknown"),
		NormalizeReason(reason),
		NormalizeSeatClass(seatClass),
	).Inc()
}

func (m *Metrics) RecordWaitingRoomExpired(seatClass string) {
	m.admission.waitingRoomExpired.WithLabelValues(NormalizeSeatClass(seatClass)).Inc()
}

// RecordAdmissionExpirations adds the two independent bounded maintenance
// results without introducing one metric call per expired record.
func (m *Metrics) RecordAdmissionExpirations(expiredTokens, expiredEntries int64, seatClass string) {
	if expiredTokens > maxAdmissionExpiryMetricIncrement {
		expiredTokens = maxAdmissionExpiryMetricIncrement
	}
	if expiredEntries > maxAdmissionExpiryMetricIncrement {
		expiredEntries = maxAdmissionExpiryMetricIncrement
	}
	if expiredTokens > 0 {
		m.admission.tokenExpired.WithLabelValues("ttl").Add(float64(expiredTokens))
	}
	if expiredEntries > 0 {
		m.admission.waitingRoomExpired.WithLabelValues(NormalizeSeatClass(seatClass)).Add(float64(expiredEntries))
	}
}

func (m *Metrics) RecordAdmissionAttempt(result, reason, seatClass string, wait time.Duration) {
	result = normalize(result, allowedAdmissionResults, "unknown")
	reason = NormalizeReason(reason)
	seatClass = NormalizeSeatClass(seatClass)
	m.admission.admissionAttempt.WithLabelValues(result, reason, seatClass).Inc()
	if result == "success" {
		m.admission.admissionIssued.WithLabelValues(seatClass).Inc()
		m.admission.waitDuration.WithLabelValues(seatClass).Observe(nonNegativeSeconds(wait))
		return
	}
	m.admission.admissionFailure.WithLabelValues(reason, seatClass).Inc()
}

func (m *Metrics) RecordAdmissionToken(operation, result, reason string) {
	result = normalize(result, allowedAdmissionResults, "unknown")
	reason = NormalizeReason(reason)
	switch normalize(operation, allowedTokenOperations, "unknown") {
	case "acquire":
		m.admission.tokenAcquire.WithLabelValues(result, reason).Inc()
	case "consume":
		m.admission.tokenConsume.WithLabelValues(result, reason).Inc()
	case "release":
		m.admission.tokenRelease.WithLabelValues(result, reason).Inc()
	case "expire":
		m.admission.tokenExpired.WithLabelValues(reason).Inc()
	}
	if result == "conflict" {
		m.admission.tokenConflict.WithLabelValues(reason).Inc()
	}
}

func (m *Metrics) RecordReservationQuotaRejected(reason, seatClass string) {
	m.admission.quotaRejected.WithLabelValues(NormalizeReason(reason), NormalizeSeatClass(seatClass)).Inc()
}

func (m *Metrics) RecordReservationBackpressureRejected(reason, seatClass string) {
	m.admission.backpressureRejected.WithLabelValues(NormalizeReason(reason), NormalizeSeatClass(seatClass)).Inc()
}

func (m *Metrics) RecordHotTrainReservation(result, reason, seatClass string, duration time.Duration) {
	result = normalize(result, allowedAdmissionResults, "unknown")
	reason = NormalizeReason(reason)
	seatClass = NormalizeSeatClass(seatClass)
	m.admission.hotReservation.WithLabelValues(result, reason, seatClass).Inc()
	m.admission.hotReservationDuration.WithLabelValues(result, seatClass).Observe(nonNegativeSeconds(duration))
	if result == "conflict" {
		m.admission.hotReservationConflict.WithLabelValues(reason, seatClass).Inc()
	}
}

// RecordAdmissionWorkerPass records lifecycle health without policy, train,
// user, or token labels. The result is derived from a boolean so callers
// cannot introduce unbounded cardinality.
func (m *Metrics) RecordAdmissionWorkerPass(succeeded bool, duration time.Duration, completedAt time.Time) {
	result := "failure"
	if succeeded {
		result = "success"
	}
	m.admission.admissionWorkerPass.WithLabelValues(result).Inc()
	m.admission.admissionWorkerDuration.WithLabelValues(result).Observe(nonNegativeSeconds(duration))
	if succeeded && !completedAt.IsZero() {
		m.admission.admissionWorkerLastSuccess.Set(float64(completedAt.Unix()))
	}
}

// RecordAdmissionWorkerState exposes only aggregate counts from the worker's
// bounded policy page. It has no policy, train, user, entry, or token labels.
func (m *Metrics) RecordAdmissionWorkerState(queueDepth, inflightAdmissions int64) {
	if queueDepth < 0 {
		queueDepth = 0
	}
	if inflightAdmissions < 0 {
		inflightAdmissions = 0
	}
	m.admission.waitingRoomQueueDepth.Set(float64(queueDepth))
	m.admission.waitingRoomInflight.Set(float64(inflightAdmissions))
}

func NormalizeSeatClass(value string) string {
	return normalize(strings.TrimSpace(value), allowedSeatClasses, "unknown")
}

func nonNegativeSeconds(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}
