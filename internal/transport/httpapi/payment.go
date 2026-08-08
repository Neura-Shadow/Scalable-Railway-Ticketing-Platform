package httpapi

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const defaultPaymentWebhookMaxBodyBytes int64 = 64 << 10

type emptyPaymentRequest struct{}

func registerPaymentRoutes(group *gin.RouterGroup, dependencies Dependencies) {
	reservations := group.Group("/reservations", authenticate(dependencies.TokenParser), authorize(RoleCustomer))
	reservations.POST("/:id/payment-intents", createPaymentIntentHandler(dependencies))

	intents := group.Group("/payment-intents", authenticate(dependencies.TokenParser), authorize(RoleCustomer))
	intents.GET("/:id", getPaymentIntentHandler(dependencies))
	intents.POST("/:id/cancel", cancelPaymentIntentHandler(dependencies))
}

func registerPaymentWebhookRoutes(router *gin.Engine, dependencies Dependencies) {
	handler := paymentWebhookHandler(dependencies)
	if dependencies.PaymentWebhookTimeout > 0 {
		router.POST("/webhooks/payments/:provider", requestTimeout(dependencies.PaymentWebhookTimeout), handler)
		return
	}
	router.POST("/webhooks/payments/:provider", handler)
}

func createPaymentIntentHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Payments == nil {
			writeError(c, ErrPaymentNotEnabled)
			return
		}
		var request emptyPaymentRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
		if !validIdempotencyKey(key) {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.Payments.CreatePaymentIntent(c.Request.Context(), CreatePaymentIntentCommand{
			OwnerID: identity.Subject, ReservationID: c.Param("id"), IdempotencyKey: key,
		})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusAccepted, result)
	}
}

func getPaymentIntentHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Payments == nil {
			writeError(c, ErrPaymentNotEnabled)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.Payments.GetPaymentIntent(c.Request.Context(), identity.Subject, c.Param("id"))
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func cancelPaymentIntentHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Payments == nil {
			writeError(c, ErrPaymentNotEnabled)
			return
		}
		var request emptyPaymentRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
		if !validIdempotencyKey(key) {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.Payments.CancelPaymentIntent(c.Request.Context(), CancelPaymentIntentCommand{
			OwnerID: identity.Subject, PaymentIntentID: c.Param("id"), IdempotencyKey: key,
		})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusAccepted, result)
	}
}

func paymentWebhookHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.PaymentWebhooks == nil {
			writeError(c, ErrPaymentNotEnabled)
			return
		}
		mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writePublicError(c, http.StatusUnsupportedMediaType, "unsupported_media_type", "content type must be application/json")
			return
		}
		limit := dependencies.PaymentWebhookMaxBodyBytes
		if limit <= 0 || limit > defaultMaxRequestBodyBytes {
			limit = defaultPaymentWebhookMaxBodyBytes
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
		body, err := io.ReadAll(c.Request.Body)
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writePublicError(c, http.StatusRequestEntityTooLarge, "request_too_large", "request body is too large")
			return
		}
		if err != nil || len(body) == 0 {
			writeError(c, ErrWebhookInvalid)
			return
		}
		disposition, err := dependencies.PaymentWebhooks.IngestPaymentWebhook(c.Request.Context(), PaymentWebhookRequest{
			Provider:  strings.ToLower(strings.TrimSpace(c.Param("provider"))),
			KeyID:     strings.TrimSpace(c.GetHeader("X-Payment-Key-ID")),
			Timestamp: strings.TrimSpace(c.GetHeader("X-Payment-Timestamp")),
			Signature: strings.TrimSpace(c.GetHeader("X-Payment-Signature")),
			Body:      body,
		})
		if err != nil {
			writeError(c, err)
			return
		}
		if disposition == PaymentWebhookAccepted {
			c.JSON(http.StatusAccepted, gin.H{"status": string(disposition)})
			return
		}
		if disposition != PaymentWebhookDuplicate && disposition != PaymentWebhookIgnored {
			writeError(c, ErrWebhookInvalid)
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": string(disposition)})
	}
}
