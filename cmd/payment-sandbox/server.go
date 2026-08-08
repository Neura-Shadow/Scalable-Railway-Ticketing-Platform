package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/sandbox"
)

type handlerConfig struct {
	maxBodyBytes        int64
	faultControlEnabled bool
	faultControlToken   string
	faults              *sandbox.Script
}

func newHandler(service *sandbox.Service, config handlerConfig) (http.Handler, error) {
	if service == nil || config.maxBodyBytes < 1 || config.maxBodyBytes > 1<<20 {
		return nil, errors.New("payment sandbox HTTP configuration invalid")
	}
	if config.faultControlEnabled && (config.faults == nil || len(config.faultControlToken) < 16) {
		return nil, errors.New("payment sandbox fault control configuration invalid")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /v1/checkouts", func(w http.ResponseWriter, r *http.Request) {
		var request provider.CreateCheckoutRequest
		if !decodeJSON(w, r, config.maxBodyBytes, &request) {
			return
		}
		result, err := service.CreateCheckout(r.Context(), request)
		if err != nil {
			writeProviderError(w, err, config.maxBodyBytes)
			return
		}
		writeJSON(w, http.StatusCreated, result)
	})
	mux.HandleFunc("GET /v1/payments/{payment_id}", func(w http.ResponseWriter, r *http.Request) {
		result, err := service.GetPaymentStatus(r.Context(), r.PathValue("payment_id"))
		if err != nil {
			writeProviderError(w, err, config.maxBodyBytes)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("POST /hosted/checkouts/{payment_id}/authorize", func(w http.ResponseWriter, r *http.Request) {
		_, err := service.CompleteHostedCheckout(r.Context(), "sandbox-checkout:"+r.PathValue("payment_id"))
		if err != nil {
			writeProviderError(w, err, config.maxBodyBytes)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "processing"})
	})
	mux.HandleFunc("POST /v1/payments/{payment_id}/authorize", func(w http.ResponseWriter, r *http.Request) {
		var request provider.AuthorizeRequest
		if !decodeJSON(w, r, config.maxBodyBytes, &request) {
			return
		}
		request.ProviderPaymentID = r.PathValue("payment_id")
		result, err := service.Authorize(r.Context(), request)
		writeOperation(w, result, err, config.maxBodyBytes)
	})
	mux.HandleFunc("POST /v1/payments/{payment_id}/capture", func(w http.ResponseWriter, r *http.Request) {
		var request provider.CaptureRequest
		if !decodeJSON(w, r, config.maxBodyBytes, &request) {
			return
		}
		request.ProviderPaymentID = r.PathValue("payment_id")
		result, err := service.Capture(r.Context(), request)
		writeOperation(w, result, err, config.maxBodyBytes)
	})
	mux.HandleFunc("POST /v1/payments/{payment_id}/void", func(w http.ResponseWriter, r *http.Request) {
		var request provider.VoidRequest
		if !decodeJSON(w, r, config.maxBodyBytes, &request) {
			return
		}
		request.ProviderPaymentID = r.PathValue("payment_id")
		result, err := service.Void(r.Context(), request)
		writeOperation(w, result, err, config.maxBodyBytes)
	})
	mux.HandleFunc("POST /v1/payments/{payment_id}/refund", func(w http.ResponseWriter, r *http.Request) {
		var request provider.RefundRequest
		if !decodeJSON(w, r, config.maxBodyBytes, &request) {
			return
		}
		request.ProviderPaymentID = r.PathValue("payment_id")
		result, err := service.Refund(r.Context(), request)
		writeOperation(w, result, err, config.maxBodyBytes)
	})
	if config.faultControlEnabled {
		mux.HandleFunc("POST /_sandbox/faults", func(w http.ResponseWriter, r *http.Request) {
			if !safeTokenEqual(r.Header.Get("X-Sandbox-Control-Token"), config.faultControlToken) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
			var request struct {
				Operation  sandbox.Operation `json:"operation"`
				Kind       sandbox.FaultKind `json:"kind"`
				DelaySteps uint64            `json:"delay_steps"`
			}
			if !decodeJSON(w, r, config.maxBodyBytes, &request) {
				return
			}
			if !validFault(request.Operation, request.Kind, request.DelaySteps) {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_fault"})
				return
			}
			if !config.faults.Push(request.Operation, sandbox.Fault{Kind: request.Kind, DelaySteps: request.DelaySteps}) {
				writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "fault_queue_full"})
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
		mux.HandleFunc("POST /_sandbox/advance", func(w http.ResponseWriter, r *http.Request) {
			if !safeTokenEqual(r.Header.Get("X-Sandbox-Control-Token"), config.faultControlToken) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
			var request struct {
				Steps uint64 `json:"steps"`
			}
			if !decodeJSON(w, r, config.maxBodyBytes, &request) {
				return
			}
			if request.Steps == 0 || request.Steps > 10000 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_advance"})
				return
			}
			service.Advance(request.Steps)
			w.WriteHeader(http.StatusNoContent)
		})
		mux.HandleFunc("GET /_sandbox/webhooks", func(w http.ResponseWriter, r *http.Request) {
			if !safeTokenEqual(r.Header.Get("X-Sandbox-Control-Token"), config.faultControlToken) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
			writeJSON(w, http.StatusOK, service.DrainWebhooksLimit(100))
		})
	}
	return mux, nil
}

func writeOperation(w http.ResponseWriter, result provider.OperationResult, err error, responseLimit int64) {
	if err != nil {
		writeProviderError(w, err, responseLimit)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, destination any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "content_type_required"})
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "body_too_large"})
			return false
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return false
	}
	return true
}

func writeProviderError(w http.ResponseWriter, err error, responseLimit int64) {
	if faultKind, ok := sandbox.FaultKindOf(err); ok {
		switch faultKind {
		case sandbox.FaultInvalidResponse:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"invalid"`))
			return
		case sandbox.FaultOversizedResponse:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"synthetic":"`))
			_, _ = w.Write([]byte(strings.Repeat("x", int(responseLimit)+1)))
			_, _ = w.Write([]byte(`"}`))
			return
		case sandbox.FaultProviderError:
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "provider_failure"})
			return
		case sandbox.FaultTimeoutBeforeCommit, sandbox.FaultTimeoutAfterCommit:
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "provider_timeout"})
			return
		case sandbox.FaultResponseLoss:
			if hijacker, ok := w.(http.Hijacker); ok {
				connection, _, hijackErr := hijacker.Hijack()
				if hijackErr == nil {
					_ = connection.Close()
					return
				}
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "response_lost"})
			return
		}
	}
	status := http.StatusBadGateway
	code := "provider_failure"
	var providerError *provider.Error
	if errors.As(err, &providerError) {
		code = string(providerError.Category)
		switch providerError.Category {
		case provider.ErrorPermanentValidation:
			status = http.StatusBadRequest
		case provider.ErrorAuthentication:
			status = http.StatusUnauthorized
		case provider.ErrorConflict:
			status = http.StatusConflict
		case provider.ErrorRateLimited:
			status = http.StatusTooManyRequests
		case provider.ErrorTransport, provider.ErrorTimeoutUnknown, provider.ErrorUnavailable:
			status = http.StatusServiceUnavailable
		case provider.ErrorInconsistentResponse:
			status = http.StatusBadGateway
		}
	}
	writeJSON(w, status, map[string]string{"error": code})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if status != http.StatusNoContent {
		_ = json.NewEncoder(w).Encode(value)
	}
}

func safeTokenEqual(provided, expected string) bool {
	providedHash := sha256.Sum256([]byte(provided))
	expectedHash := sha256.Sum256([]byte(expected))
	return hmac.Equal(providedHash[:], expectedHash[:])
}

func validFault(operation sandbox.Operation, kind sandbox.FaultKind, delay uint64) bool {
	switch operation {
	case sandbox.OperationCreateCheckout, sandbox.OperationGetStatus, sandbox.OperationAuthorize, sandbox.OperationCapture, sandbox.OperationVoid, sandbox.OperationRefund:
	default:
		return false
	}
	switch kind {
	case sandbox.FaultTimeoutBeforeCommit, sandbox.FaultRateLimited, sandbox.FaultProviderError, sandbox.FaultOutage, sandbox.FaultInvalidResponse, sandbox.FaultOversizedResponse:
		return delay == 0
	case sandbox.FaultTimeoutAfterCommit, sandbox.FaultResponseLoss, sandbox.FaultDuplicateWebhook, sandbox.FaultOutOfOrderWebhook:
		return operation != sandbox.OperationGetStatus && delay == 0
	case sandbox.FaultRefundTransient, sandbox.FaultRefundPermanent:
		return operation == sandbox.OperationRefund && delay == 0
	case sandbox.FaultDelayedWebhook:
		return operation != sandbox.OperationGetStatus && delay > 0 && delay <= 10000
	default:
		return false
	}
}
