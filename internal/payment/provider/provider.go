// Package provider defines the provider-neutral payment boundary. It contains
// no provider endpoint, credential, database topology, or customer payment
// instrument fields.
package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Status is the normalized provider payment state.
type Status string

const (
	StatusCreated                Status = "created"
	StatusRequiresCustomerAction Status = "requires_customer_action"
	StatusAuthorized             Status = "authorized"
	StatusCaptured               Status = "captured"
	StatusVoided                 Status = "voided"
	StatusRefunded               Status = "refunded"
	StatusFailed                 Status = "failed"
	StatusCancelled              Status = "cancelled"
	StatusUnknown                Status = "unknown"
)

// ErrorCategory is a bounded, provider-independent failure category.
type ErrorCategory string

const (
	ErrorTransport            ErrorCategory = "transport_retryable"
	ErrorTimeoutUnknown       ErrorCategory = "timeout_unknown"
	ErrorPermanentValidation  ErrorCategory = "validation_permanent"
	ErrorAuthentication       ErrorCategory = "authentication"
	ErrorUnavailable          ErrorCategory = "provider_unavailable"
	ErrorRateLimited          ErrorCategory = "rate_limited"
	ErrorConflict             ErrorCategory = "conflict"
	ErrorInconsistentResponse ErrorCategory = "inconsistent_response"
)

// Error is safe for callers to classify. Message must never contain request
// payloads, secrets, signatures, or provider response bodies.
type Error struct {
	Category  ErrorCategory
	Operation string
	Retryable bool
	Uncertain bool
	// RetryAfter is bounded provider guidance for read-only callers. Financial
	// mutation callers must still use their durable query-before-retry policy.
	RetryAfter time.Duration
	Message    string
}

func (e *Error) Error() string {
	if e == nil {
		return "payment provider error"
	}
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("payment provider %s failed", e.Operation)
}

// Metadata is deliberately small and must contain only non-sensitive,
// provider-facing correlation values.
type Metadata map[string]string

type CreateCheckoutRequest struct {
	PaymentIntentID   string   `json:"payment_intent_id"`
	MerchantReference string   `json:"merchant_reference"`
	AmountMinor       int64    `json:"amount_minor"`
	Currency          string   `json:"currency"`
	IdempotencyKey    string   `json:"idempotency_key"`
	Metadata          Metadata `json:"metadata,omitempty"`
}

type Checkout struct {
	ProviderPaymentID string `json:"provider_payment_id"`
	HostedReference   string `json:"hosted_reference"`
	SyntheticToken    string `json:"synthetic_token"`
	Status            Status `json:"status"`
	AmountMinor       int64  `json:"amount_minor"`
	Currency          string `json:"currency"`
}

type Payment struct {
	ProviderPaymentID string    `json:"provider_payment_id"`
	Status            Status    `json:"status"`
	AmountMinor       int64     `json:"amount_minor"`
	Currency          string    `json:"currency"`
	CapturedMinor     int64     `json:"captured_minor"`
	RefundedMinor     int64     `json:"refunded_minor"`
	ProviderUpdatedAt time.Time `json:"provider_updated_at"`
}

// FinancialExpectation is the server-owned monetary identity against which a
// provider observation is evaluated. Callers must source it from durable
// merchant state, never from the provider response being authorized to cause
// a financial or inventory transition.
type FinancialExpectation struct {
	AmountMinor int64
	Currency    string
}

// FinancialObservation is the smallest provider-neutral monetary snapshot
// shared by synchronous operations, status queries, webhooks, and
// reconciliation.
type FinancialObservation struct {
	Status        Status
	AmountMinor   int64
	Currency      string
	CapturedMinor int64
	RefundedMinor int64
}

var ErrInconsistentFinancialObservation = errors.New("payment provider financial observation is inconsistent")

// EvaluateFinancialObservation is a pure, fail-closed evaluator. It performs
// no I/O and returns the same bounded error for all inconsistencies so provider
// payloads can never escape through an error message.
func EvaluateFinancialObservation(expectation FinancialExpectation, observation FinancialObservation) error {
	if expectation.AmountMinor <= 0 || !validFinancialCurrency(expectation.Currency) ||
		observation.AmountMinor != expectation.AmountMinor || observation.Currency != expectation.Currency ||
		observation.CapturedMinor < 0 || observation.RefundedMinor < 0 ||
		observation.CapturedMinor > observation.AmountMinor || observation.RefundedMinor > observation.CapturedMinor {
		return ErrInconsistentFinancialObservation
	}
	switch observation.Status {
	case StatusCreated, StatusRequiresCustomerAction, StatusAuthorized, StatusVoided, StatusFailed, StatusCancelled:
		if observation.CapturedMinor != 0 || observation.RefundedMinor != 0 {
			return ErrInconsistentFinancialObservation
		}
	case StatusCaptured:
		if observation.CapturedMinor != observation.AmountMinor || observation.RefundedMinor != 0 {
			return ErrInconsistentFinancialObservation
		}
	case StatusRefunded:
		if observation.CapturedMinor != observation.AmountMinor || observation.RefundedMinor != observation.AmountMinor {
			return ErrInconsistentFinancialObservation
		}
	case StatusUnknown:
		// Unknown is never authority to advance state, but its bounded totals
		// remain useful evidence for manual reconciliation.
	default:
		return ErrInconsistentFinancialObservation
	}
	return nil
}

func validFinancialCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for _, character := range currency {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

type AuthorizeRequest struct {
	PaymentIntentID   string   `json:"payment_intent_id"`
	ProviderPaymentID string   `json:"-"`
	SyntheticToken    string   `json:"synthetic_token"`
	AmountMinor       int64    `json:"amount_minor"`
	Currency          string   `json:"currency"`
	IdempotencyKey    string   `json:"idempotency_key"`
	Metadata          Metadata `json:"metadata,omitempty"`
}

type CaptureRequest struct {
	PaymentIntentID   string   `json:"payment_intent_id"`
	ProviderPaymentID string   `json:"-"`
	AmountMinor       int64    `json:"amount_minor"`
	Currency          string   `json:"currency"`
	IdempotencyKey    string   `json:"idempotency_key"`
	Metadata          Metadata `json:"metadata,omitempty"`
}

type VoidRequest struct {
	PaymentIntentID   string   `json:"payment_intent_id"`
	ProviderPaymentID string   `json:"-"`
	IdempotencyKey    string   `json:"idempotency_key"`
	Metadata          Metadata `json:"metadata,omitempty"`
}

type RefundRequest struct {
	PaymentIntentID   string   `json:"payment_intent_id"`
	ProviderPaymentID string   `json:"-"`
	AmountMinor       int64    `json:"amount_minor"`
	Currency          string   `json:"currency"`
	IdempotencyKey    string   `json:"idempotency_key"`
	Metadata          Metadata `json:"metadata,omitempty"`
}

type OperationResult struct {
	ProviderPaymentID   string `json:"provider_payment_id"`
	ProviderOperationID string `json:"provider_operation_id"`
	Status              Status `json:"status"`
	AmountMinor         int64  `json:"amount_minor"`
	Currency            string `json:"currency"`
}

type WebhookHeaders struct {
	KeyID     string `json:"key_id"`
	Timestamp string `json:"timestamp"`
	Signature string `json:"signature"`
}

type EventType string

type WebhookEnvironment string

const (
	WebhookEnvironmentTest WebhookEnvironment = "test"
	WebhookEnvironmentLive WebhookEnvironment = "live"
)

const (
	EventCheckoutCreated EventType = "payment.checkout_created"
	EventAuthorized      EventType = "payment.authorized"
	EventCaptured        EventType = "payment.captured"
	EventVoided          EventType = "payment.voided"
	EventRefunded        EventType = "payment.refunded"
	EventUnknown         EventType = "unknown"
)

type WebhookEvent struct {
	ProviderEventID   string             `json:"provider_event_id"`
	VerifiedKeyID     string             `json:"-"`
	ProviderAccountID string             `json:"-"`
	Environment       WebhookEnvironment `json:"-"`
	Type              EventType          `json:"type"`
	OriginalType      string             `json:"-"`
	ProviderPaymentID string             `json:"provider_payment_id"`
	Status            Status             `json:"status"`
	AmountMinor       int64              `json:"amount_minor"`
	Currency          string             `json:"currency"`
	OccurredAt        time.Time          `json:"occurred_at"`
}

// Client is the complete provider boundary used by the payment saga.
type Client interface {
	CreateCheckout(context.Context, CreateCheckoutRequest) (Checkout, error)
	GetPaymentStatus(context.Context, string) (Payment, error)
	Authorize(context.Context, AuthorizeRequest) (OperationResult, error)
	Capture(context.Context, CaptureRequest) (OperationResult, error)
	Void(context.Context, VoidRequest) (OperationResult, error)
	Refund(context.Context, RefundRequest) (OperationResult, error)
	VerifyWebhook(context.Context, WebhookHeaders, []byte) (WebhookEvent, error)
}

// ValidateMetadata rejects oversized or security-sensitive metadata at the
// provider boundary.
func ValidateMetadata(metadata Metadata) error {
	if len(metadata) > 8 {
		return validationError("metadata exceeds provider boundary")
	}
	for key, value := range metadata {
		if key == "" || len(key) > 48 || len(value) > 128 {
			return validationError("metadata exceeds provider boundary")
		}
		normalized := strings.NewReplacer("-", "", "_", "", ".", "").Replace(strings.ToLower(key))
		for _, forbidden := range []string{"card", "pan", "cvv", "cvc", "pin", "track", "magnetic", "credential", "secret", "apikey", "dsn", "jwt", "shard"} {
			if strings.Contains(normalized, forbidden) {
				return validationError("sensitive metadata is not accepted")
			}
		}
	}
	return nil
}

func validationError(message string) *Error {
	return &Error{Category: ErrorPermanentValidation, Message: message}
}
