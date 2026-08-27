package httpapi

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Application-facing sentinel errors may be wrapped by injected ports. The
// transport maps them to a fixed public contract and never serializes err.
var (
	ErrInvalidInput                  = errors.New("invalid input")
	ErrUnauthenticated               = errors.New("unauthenticated")
	ErrForbidden                     = errors.New("forbidden")
	ErrNotFound                      = errors.New("not found")
	ErrConflict                      = errors.New("conflict")
	ErrTooManyRequests               = errors.New("too many requests")
	ErrUnavailable                   = errors.New("unavailable")
	ErrAdmissionRequired             = errors.New("admission required")
	ErrAdmissionInvalid              = errors.New("admission invalid")
	ErrAdmissionExpired              = errors.New("admission expired")
	ErrAdmissionInProgress           = errors.New("admission in progress")
	ErrQueueFull                     = errors.New("waiting room full")
	ErrReservationQuotaExceeded      = errors.New("reservation quota exceeded")
	ErrReservationBackpressure       = errors.New("reservation backpressure")
	ErrServiceTemporarilyRebalancing = errors.New("service temporarily rebalancing")
	ErrPaymentNotEnabled             = errors.New("payment not enabled")
	ErrPaymentIntentConflict         = errors.New("payment intent conflict")
	ErrReservationNotPayable         = errors.New("reservation not payable")
	ErrPaymentAlreadyCompleted       = errors.New("payment already completed")
	ErrPaymentProviderUnavailable    = errors.New("payment provider unavailable")
	ErrPaymentProcessing             = errors.New("payment processing")
	ErrPaymentRequiresCustomerAction = errors.New("payment requires customer action")
	ErrPaymentFailed                 = errors.New("payment failed")
	ErrPaymentUnderReview            = errors.New("payment under review")
	ErrRefundProcessing              = errors.New("refund processing")
	ErrRefundFailed                  = errors.New("refund failed")
	ErrTicketIssuanceProcessing      = errors.New("ticket issuance processing")
	ErrWebhookInvalid                = errors.New("payment webhook invalid")
)

type retryAfterError struct {
	err     error
	seconds int
}

func (e retryAfterError) Error() string { return e.err.Error() }
func (e retryAfterError) Unwrap() error { return e.err }

// WithRetryAfter adds a bounded public retry hint without changing the
// underlying typed error identity.
func WithRetryAfter(err error, seconds int) error {
	if err == nil {
		return nil
	}
	if seconds < 1 {
		seconds = 1
	}
	if seconds > 60 {
		seconds = 60
	}
	return retryAfterError{err: err, seconds: seconds}
}

type errorEnvelope struct {
	Error publicError `json:"error"`
}

type publicError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(c *gin.Context, err error) {
	status, code, message := classifyError(err)
	var retry retryAfterError
	if errors.As(err, &retry) {
		c.Header("Retry-After", strconv.Itoa(retry.seconds))
	}
	writePublicError(c, status, code, message)
}

func writePublicError(c *gin.Context, status int, code, message string) {
	c.AbortWithStatusJSON(status, errorEnvelope{Error: publicError{Code: code, Message: message}})
}

func classifyError(err error) (int, string, string) {
	switch {
	case errors.Is(err, ErrInvalidInput):
		return http.StatusBadRequest, "invalid_request", "request is invalid"
	case errors.Is(err, ErrUnauthenticated):
		return http.StatusUnauthorized, "unauthenticated", "authentication is required"
	case errors.Is(err, ErrForbidden):
		return http.StatusForbidden, "forbidden", "permission denied"
	case errors.Is(err, ErrAdmissionInvalid):
		return http.StatusForbidden, "admission_invalid", "admission token is invalid"
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound, "not_found", "resource not found"
	case errors.Is(err, ErrAdmissionExpired):
		return http.StatusGone, "admission_expired", "admission token has expired"
	case errors.Is(err, ErrAdmissionRequired):
		return http.StatusPreconditionRequired, "admission_required", "waiting-room admission is required"
	case errors.Is(err, ErrAdmissionInProgress):
		return http.StatusConflict, "admission_in_progress", "admission attempt is already processing"
	case errors.Is(err, ErrQueueFull):
		return http.StatusTooManyRequests, "queue_full", "waiting room is full"
	case errors.Is(err, ErrReservationQuotaExceeded):
		return http.StatusTooManyRequests, "reservation_quota_exceeded", "active reservation quota exceeded"
	case errors.Is(err, ErrConflict):
		return http.StatusConflict, "conflict", "request conflicts with current state"
	case errors.Is(err, ErrTooManyRequests):
		return http.StatusTooManyRequests, "rate_limited", "too many requests"
	case errors.Is(err, ErrUnavailable):
		return http.StatusServiceUnavailable, "unavailable", "service is temporarily unavailable"
	case errors.Is(err, ErrServiceTemporarilyRebalancing):
		return http.StatusServiceUnavailable, "service_temporarily_rebalancing", "booking is temporarily rebalancing"
	case errors.Is(err, ErrReservationBackpressure):
		return http.StatusServiceUnavailable, "reservation_backpressure", "reservation capacity is temporarily full"
	case errors.Is(err, ErrPaymentNotEnabled):
		return http.StatusServiceUnavailable, "payment_not_enabled", "payment is not enabled"
	case errors.Is(err, ErrPaymentIntentConflict):
		return http.StatusConflict, "payment_intent_conflict", "payment intent conflicts with the request"
	case errors.Is(err, ErrReservationNotPayable):
		return http.StatusConflict, "reservation_not_payable", "reservation cannot enter payment"
	case errors.Is(err, ErrPaymentAlreadyCompleted):
		return http.StatusConflict, "payment_already_completed", "payment is already completed"
	case errors.Is(err, ErrPaymentProviderUnavailable):
		return http.StatusServiceUnavailable, "payment_provider_unavailable", "payment provider is unavailable"
	case errors.Is(err, ErrPaymentProcessing):
		return http.StatusAccepted, "payment_processing", "payment is processing"
	case errors.Is(err, ErrPaymentRequiresCustomerAction):
		return http.StatusConflict, "payment_requires_customer_action", "payment requires customer action"
	case errors.Is(err, ErrPaymentFailed):
		return http.StatusConflict, "payment_failed", "payment failed"
	case errors.Is(err, ErrPaymentUnderReview):
		return http.StatusConflict, "payment_under_review", "payment is under review"
	case errors.Is(err, ErrRefundProcessing):
		return http.StatusAccepted, "refund_processing", "refund is processing"
	case errors.Is(err, ErrRefundFailed):
		return http.StatusConflict, "refund_failed", "refund failed"
	case errors.Is(err, ErrTicketIssuanceProcessing):
		return http.StatusAccepted, "ticket_issuance_processing", "ticket issuance is processing"
	case errors.Is(err, ErrWebhookInvalid):
		return http.StatusUnauthorized, "payment_webhook_invalid", "payment webhook is invalid"
	default:
		return http.StatusInternalServerError, "internal_error", "internal server error"
	}
}
