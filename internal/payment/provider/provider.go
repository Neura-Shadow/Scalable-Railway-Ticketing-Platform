// Package provider defines the provider-neutral payment boundary. It contains
// no provider endpoint, credential, database topology, or customer payment
// instrument fields.
package provider

import (
	"context"
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
	Message   string
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

const (
	EventCheckoutCreated EventType = "payment.checkout_created"
	EventAuthorized      EventType = "payment.authorized"
	EventCaptured        EventType = "payment.captured"
	EventVoided          EventType = "payment.voided"
	EventRefunded        EventType = "payment.refunded"
	EventUnknown         EventType = "unknown"
)

type WebhookEvent struct {
	ProviderEventID   string    `json:"provider_event_id"`
	Type              EventType `json:"type"`
	OriginalType      string    `json:"-"`
	ProviderPaymentID string    `json:"provider_payment_id"`
	Status            Status    `json:"status"`
	AmountMinor       int64     `json:"amount_minor"`
	Currency          string    `json:"currency"`
	OccurredAt        time.Time `json:"occurred_at"`
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
