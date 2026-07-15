package httpapi

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Application-facing sentinel errors may be wrapped by injected ports. The
// transport maps them to a fixed public contract and never serializes err.
var (
	ErrInvalidInput    = errors.New("invalid input")
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrForbidden       = errors.New("forbidden")
	ErrNotFound        = errors.New("not found")
	ErrConflict        = errors.New("conflict")
	ErrTooManyRequests = errors.New("too many requests")
	ErrUnavailable     = errors.New("unavailable")
)

type errorEnvelope struct {
	Error publicError `json:"error"`
}

type publicError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(c *gin.Context, err error) {
	status, code, message := classifyError(err)
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
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound, "not_found", "resource not found"
	case errors.Is(err, ErrConflict):
		return http.StatusConflict, "conflict", "request conflicts with current state"
	case errors.Is(err, ErrTooManyRequests):
		return http.StatusTooManyRequests, "rate_limited", "too many requests"
	case errors.Is(err, ErrUnavailable):
		return http.StatusServiceUnavailable, "unavailable", "service is temporarily unavailable"
	default:
		return http.StatusInternalServerError, "internal_error", "internal server error"
	}
}
