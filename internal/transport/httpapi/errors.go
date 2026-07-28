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
	default:
		return http.StatusInternalServerError, "internal_error", "internal server error"
	}
}
