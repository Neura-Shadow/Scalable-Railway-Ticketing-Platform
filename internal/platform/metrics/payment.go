package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	allowedPaymentProviders  = set("disabled", "sandbox")
	allowedPaymentOperations = set("create_checkout", "query_status", "authorize", "capture", "void", "refund", "issue_tickets", "compensate", "process_webhook")
	allowedPaymentResults    = set("success", "failure", "conflict", "duplicate", "ignored", "retry", "uncertain", "manual_review", "replay", "skipped")
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
	)
	allowedPaymentEvents = set(
		"payment.checkout_created", "payment.authorized", "payment.captured",
		"payment.voided", "payment.refunded", "unknown",
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
	reconciliationRepair                                       *prometheus.CounterVec
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
		webhookDuration:        histogram("payment_webhook_processing_duration_seconds", "Payment webhook ingress duration.", "provider", "result"),
		webhookLag:             histogram("payment_webhook_lag_seconds", "Lag between provider event creation and processing.", "provider", "event_type"),
		ticketIssuanceTotal:    counter("ticket_issuance_total", "Ticket issuance outcomes.", "result"),
		ticketIssuanceFailure:  counter("ticket_issuance_failure_total", "Ticket issuance failures.", "error_category"),
		ticketIssuanceDuration: histogram("ticket_issuance_duration_seconds", "Ticket issuance duration.", "result"),
		ticketReplay:           counter("ticket_issuance_replay_total", "Ticket issuance receipt replays.", "result"),
		reconciliationTotal:    counter("payment_reconciliation_total", "Payment reconciliation outcomes.", "reconciliation_type", "result"),
		reconciliationMismatch: counter("payment_reconciliation_mismatch_total", "Payment reconciliation mismatches.", "reconciliation_type"),
		reconciliationRepair:   counter("payment_reconciliation_repair_total", "Explicit payment reconciliation repairs.", "reconciliation_type", "result"),
	}
}

func (m *paymentMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.intentTotal, m.intentDuration, m.sagaTransition, m.sagaFailure, m.sagaManualReview,
		m.operationTotal, m.operationDuration, m.operationUncertain, m.captureTotal, m.voidTotal, m.refundTotal,
		m.webhookTotal, m.webhookDuplicate, m.webhookInvalid, m.webhookConflict, m.webhookDuration, m.webhookLag,
		m.ticketIssuanceTotal, m.ticketIssuanceFailure, m.ticketIssuanceDuration, m.ticketReplay,
		m.reconciliationTotal, m.reconciliationMismatch, m.reconciliationRepair,
	}
}

func (m *Metrics) RecordPaymentIntent(state, result string, duration time.Duration) {
	state = normalize(state, allowedPaymentStates, "unknown")
	result = normalize(result, allowedPaymentResults, "unknown")
	m.payment.intentTotal.WithLabelValues(state, result).Inc()
	m.payment.intentDuration.WithLabelValues(result).Observe(nonNegativeSeconds(duration))
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
	provider = normalize(provider, allowedPaymentProviders, "unknown")
	operation = normalize(operation, allowedPaymentOperations, "unknown")
	result = normalize(result, allowedPaymentResults, "unknown")
	m.payment.operationTotal.WithLabelValues(provider, operation, result).Inc()
	m.payment.operationDuration.WithLabelValues(provider, operation, result).Observe(nonNegativeSeconds(duration))
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

func (m *Metrics) RecordPaymentWebhook(provider, eventType, result string, duration, lag time.Duration) {
	provider = normalize(provider, allowedPaymentProviders, "unknown")
	eventType = normalize(eventType, allowedPaymentEvents, "unknown")
	result = normalize(result, allowedPaymentResults, "unknown")
	m.payment.webhookTotal.WithLabelValues(provider, eventType, result).Inc()
	m.payment.webhookDuration.WithLabelValues(provider, result).Observe(nonNegativeSeconds(duration))
	m.payment.webhookLag.WithLabelValues(provider, eventType).Observe(nonNegativeSeconds(lag))
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
	m.payment.ticketIssuanceDuration.WithLabelValues(result).Observe(nonNegativeSeconds(duration))
	if result == "failure" {
		m.payment.ticketIssuanceFailure.WithLabelValues(normalize(category, allowedPaymentErrors, "unknown")).Inc()
	}
	if replay {
		m.payment.ticketReplay.WithLabelValues(result).Inc()
	}
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
