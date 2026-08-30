package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const maximumPaymentMetricDuration = 30 * 24 * time.Hour

var (
	allowedPaymentProviders  = set("disabled", "sandbox", "stripe")
	allowedPaymentOperations = set("create_checkout", "query_status", "authorize", "capture", "void", "refund", "issue_tickets", "compensate", "process_webhook")
	allowedPaymentResults    = set("success", "failure", "conflict", "duplicate", "ignored", "retry", "uncertain", "manual_review", "replay", "skipped", "superseded")
	allowedPaymentStates     = set(
		"created", "reservation_securing", "checkout_pending", "awaiting_customer",
		"authorization_pending", "authorized", "capture_pending", "captured",
		"ticket_issue_pending", "completed", "void_pending", "voided",
		"refund_pending", "refunded", "cancelled", "failed", "manual_review", "expired",
		"reservation_secured", "checkout_created", "awaiting_provider", "capturing",
		"issuing_tickets", "compensating", "refunding", "compensated",
	)
	allowedPaymentErrors = set(
		"none", "transport_retryable", "timeout_unknown", "validation_permanent",
		"authentication", "provider_unavailable", "rate_limited", "conflict",
		"inconsistent_response", "database", "shard_unavailable", "receipt_conflict",
		"lease_expired", "attempts_exhausted", "invariant_mismatch",
		"provider_outcome_unknown", "provider_not_applied", "provider_state_conflict",
		"database_finalize_failed", "invalid_claim", "invalid_action",
		"shard_command_failed", "shard_receipt_conflict",
		"uncertainty_window_exceeded",
	)
	allowedPaymentEvents = set(
		"payment.checkout_created", "payment.authorized", "payment.captured",
		"payment.voided", "payment.refunded", "unknown",
	)
	allowedPaymentWorkerFailureLanes = set(
		"claim_operations", "process_operation", "claim_webhooks",
		"process_webhook", "claim_actions", "process_action",
	)
	allowedPaymentWorkerFailureReasons = set(
		"store_unavailable", "lease_lost", "invalid_claim", "provider_unavailable",
		"provider_outcome_unknown", "database_finalize_failed", "shard_unavailable",
		"regional_authority_unavailable", "constraint_rejected", "timeout", "unknown",
	)
	allowedReconciliationTypes = set("payment_intents", "payment_operations", "payment_webhooks", "payment_tickets", "payment_provider", "payment_all")
)

type paymentMetrics struct {
	intentTotal, sagaTransition, sagaFailure, sagaManualReview *prometheus.CounterVec
	operationTotal, operationUncertain                         *prometheus.CounterVec
	captureTotal, voidTotal, refundTotal                       *prometheus.CounterVec
	webhookTotal, webhookDuplicate, webhookInvalid             *prometheus.CounterVec
	webhookConflict                                            *prometheus.CounterVec
	ticketIssuanceTotal, ticketIssuanceFailure, ticketReplay   *prometheus.CounterVec
	reconciliationTotal, reconciliationMismatch                *prometheus.CounterVec
	reconciliationRepair, workerLaneFailure                    *prometheus.CounterVec
	intentDuration, operationDuration, webhookDuration         *prometheus.HistogramVec
	webhookLag, ticketIssuanceDuration                         *prometheus.HistogramVec
}

func newPaymentMetrics() *paymentMetrics {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: prometheus.DefBuckets}, labels)
	}
	return &paymentMetrics{
		intentTotal:            counter("payment_intent_total", "Payment intent outcomes.", "state", "result"),
		intentDuration:         histogram("payment_intent_duration_seconds", "Payment intent processing duration.", "result"),
		sagaTransition:         counter("payment_saga_transition_total", "Payment saga transitions.", "from_state", "to_state"),
		sagaFailure:            counter("payment_saga_failure_total", "Payment saga failures.", "error_category"),
		sagaManualReview:       counter("payment_saga_manual_review_total", "Payment sagas escalated to manual review.", "error_category"),
		operationTotal:         counter("payment_operation_total", "Provider-neutral payment operation outcomes.", "provider", "operation", "result"),
		operationDuration:      histogram("payment_operation_duration_seconds", "Provider-neutral payment operation duration.", "provider", "operation", "result"),
		operationUncertain:     counter("payment_operation_uncertain_total", "Payment operations with uncertain provider outcomes.", "provider", "operation"),
		captureTotal:           counter("payment_capture_total", "Payment capture outcomes.", "provider", "result"),
		voidTotal:              counter("payment_void_total", "Payment void outcomes.", "provider", "result"),
		refundTotal:            counter("payment_refund_total", "Payment refund outcomes.", "provider", "result"),
		webhookTotal:           counter("payment_webhook_total", "Verified payment webhook outcomes.", "provider", "event_type", "result"),
		webhookDuplicate:       counter("payment_webhook_duplicate_total", "Duplicate verified payment webhooks.", "provider"),
		webhookInvalid:         counter("payment_webhook_invalid_signature_total", "Rejected payment webhook signatures.", "provider"),
		webhookConflict:        counter("payment_webhook_conflict_total", "Provider event identity conflicts.", "provider"),
		webhookDuration:        histogram("payment_webhook_processing_duration_seconds", "Durable payment webhook processing duration.", "provider", "result"),
		webhookLag:             histogram("payment_webhook_lag_seconds", "Lag between provider event creation and processing.", "provider", "event_type"),
		ticketIssuanceTotal:    counter("ticket_issuance_total", "Ticket issuance outcomes.", "result"),
		ticketIssuanceFailure:  counter("ticket_issuance_failure_total", "Ticket issuance failures.", "error_category"),
		ticketIssuanceDuration: histogram("ticket_issuance_duration_seconds", "Ticket issuance duration.", "result"),
		ticketReplay:           counter("ticket_issuance_replay_total", "Ticket issuance receipt replays.", "result"),
		reconciliationTotal:    counter("payment_reconciliation_total", "Payment reconciliation outcomes.", "reconciliation_type", "result"),
		reconciliationMismatch: counter("payment_reconciliation_mismatch_total", "Payment reconciliation mismatches.", "reconciliation_type"),
		reconciliationRepair:   counter("payment_reconciliation_repair_total", "Explicit payment reconciliation repairs.", "reconciliation_type", "result"),
		workerLaneFailure:      counter("payment_worker_lane_failure_total", "Payment worker failures by bounded lane and reason.", "lane", "reason"),
	}
}

func (m *paymentMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.intentTotal, m.intentDuration, m.sagaTransition, m.sagaFailure, m.sagaManualReview,
		m.operationTotal, m.operationDuration, m.operationUncertain, m.captureTotal, m.voidTotal, m.refundTotal,
		m.webhookTotal, m.webhookDuplicate, m.webhookInvalid, m.webhookConflict, m.webhookDuration, m.webhookLag,
		m.ticketIssuanceTotal, m.ticketIssuanceFailure, m.ticketIssuanceDuration, m.ticketReplay,
		m.reconciliationTotal, m.reconciliationMismatch, m.reconciliationRepair, m.workerLaneFailure,
	}
}

func (m *Metrics) RecordPaymentWorkerLaneFailure(lane, reason string) {
	m.payment.workerLaneFailure.WithLabelValues(
		normalize(lane, allowedPaymentWorkerFailureLanes, "unknown"),
		normalize(reason, allowedPaymentWorkerFailureReasons, "unknown"),
	).Inc()
}

func (m *Metrics) RecordPaymentIntent(state, result string, duration time.Duration) {
	state = normalize(state, allowedPaymentStates, "unknown")
	result = normalize(result, allowedPaymentResults, "unknown")
	m.payment.intentTotal.WithLabelValues(state, result).Inc()
	m.payment.intentDuration.WithLabelValues(result).Observe(boundedPaymentSeconds(duration))
}

func (m *Metrics) RecordPaymentSagaTransition(from, to string) {
	m.payment.sagaTransition.WithLabelValues(
		normalize(from, allowedPaymentStates, "unknown"), normalize(to, allowedPaymentStates, "unknown"),
	).Inc()
}

func (m *Metrics) RecordPaymentSagaFailure(category string, manualReview bool) {
	category = normalize(category, allowedPaymentErrors, "unknown")
	m.payment.sagaFailure.WithLabelValues(category).Inc()
	if manualReview {
		m.payment.sagaManualReview.WithLabelValues(category).Inc()
	}
}

func (m *Metrics) RecordPaymentOperation(provider, operation, result string, duration time.Duration, uncertain bool) {
	reason := "none"
	if uncertain {
		reason = "uncertain"
	} else if result != "success" {
		reason = result
	}
	m.RecordPaymentOperationWithReason(provider, operation, result, reason, duration, uncertain)
}

// RecordPaymentOperationWithReason preserves the bounded provider failure
// category observed at the worker boundary instead of deriving it from the
// coarser result label.
func (m *Metrics) RecordPaymentOperationWithReason(provider, operation, result, reason string, duration time.Duration, uncertain bool) {
	provider = normalize(provider, allowedPaymentProviders, "unknown")
	operation = normalize(operation, allowedPaymentOperations, "unknown")
	result = normalize(result, allowedPaymentResults, "unknown")
	m.RecordProviderAdapter(provider, operation, result, normalizePaymentProviderReason(reason), duration)
	m.payment.operationTotal.WithLabelValues(provider, operation, result).Inc()
	m.payment.operationDuration.WithLabelValues(provider, operation, result).Observe(boundedPaymentSeconds(duration))
	if uncertain {
		m.payment.operationUncertain.WithLabelValues(provider, operation).Inc()
	}
	switch operation {
	case "capture":
		m.payment.captureTotal.WithLabelValues(provider, result).Inc()
	case "void":
		m.payment.voidTotal.WithLabelValues(provider, result).Inc()
	case "refund":
		m.payment.refundTotal.WithLabelValues(provider, result).Inc()
	}
}

func normalizePaymentProviderReason(reason string) string {
	switch reason {
	case "none":
		return "none"
	case "transport_retryable":
		return "transport"
	case "timeout_unknown", "provider_outcome_unknown":
		return "timeout"
	case "authentication":
		return "authentication"
	case "provider_unavailable":
		return "provider_unavailable"
	case "rate_limited":
		return "rate_limited"
	case "validation_permanent", "invalid_claim", "invalid_action":
		return "validation"
	case "conflict", "provider_state_conflict", "receipt_conflict", "shard_receipt_conflict":
		return "conflict"
	case "database", "database_finalize_failed":
		return "database"
	case "invariant_mismatch", "inconsistent_response":
		return "invariant_mismatch"
	case "uncertain":
		return "uncertain"
	default:
		return "unknown"
	}
}

func (m *Metrics) RecordPaymentWebhook(provider, eventType, result string, duration, lag time.Duration) {
	provider = normalize(provider, allowedPaymentProviders, "unknown")
	eventType = normalize(eventType, allowedPaymentEvents, "unknown")
	result = normalize(result, allowedPaymentResults, "unknown")
	m.payment.webhookTotal.WithLabelValues(provider, eventType, result).Inc()
	m.payment.webhookDuration.WithLabelValues(provider, result).Observe(boundedPaymentSeconds(duration))
	m.payment.webhookLag.WithLabelValues(provider, eventType).Observe(boundedPaymentSeconds(lag))
	switch result {
	case "duplicate":
		m.payment.webhookDuplicate.WithLabelValues(provider).Inc()
	case "conflict":
		m.payment.webhookConflict.WithLabelValues(provider).Inc()
	}
}

func (m *Metrics) RecordPaymentWebhookInvalid(provider string) {
	m.payment.webhookInvalid.WithLabelValues(normalize(provider, allowedPaymentProviders, "unknown")).Inc()
}

func (m *Metrics) RecordTicketIssuance(result, category string, duration time.Duration, replay bool) {
	result = normalize(result, allowedPaymentResults, "unknown")
	m.payment.ticketIssuanceTotal.WithLabelValues(result).Inc()
	m.payment.ticketIssuanceDuration.WithLabelValues(result).Observe(boundedPaymentSeconds(duration))
	if result == "failure" {
		m.payment.ticketIssuanceFailure.WithLabelValues(normalize(category, allowedPaymentErrors, "unknown")).Inc()
	}
	if replay {
		m.payment.ticketReplay.WithLabelValues(result).Inc()
	}
}

func boundedPaymentSeconds(duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	if duration > maximumPaymentMetricDuration {
		duration = maximumPaymentMetricDuration
	}
	return duration.Seconds()
}

func (m *Metrics) RecordPaymentReconciliation(kind, result string, mismatch, repair bool) {
	kind = normalize(kind, allowedReconciliationTypes, "unknown")
	result = normalize(result, allowedPaymentResults, "unknown")
	m.payment.reconciliationTotal.WithLabelValues(kind, result).Inc()
	if mismatch {
		m.payment.reconciliationMismatch.WithLabelValues(kind).Inc()
	}
	if repair {
		m.payment.reconciliationRepair.WithLabelValues(kind, result).Inc()
	}
}
