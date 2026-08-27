// Package conformance contains reusable provider-adapter behavior suites.
// The suite exercises only the public provider boundary and a real HTTP test
// server; it does not mock adapter internals.
package conformance

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

const conformanceMaxResponseBytes = int64(1024)

// HTTPHarness supplies the provider-specific success representation while the
// reusable suite owns provider-neutral failure and safety assertions.
type HTTPHarness struct {
	NewClient                  func(t *testing.T, origin string, maxResponseBytes int64) provider.Client
	ValidCreateCheckout        provider.CreateCheckoutRequest
	WriteCreateCheckoutSuccess func(http.ResponseWriter)
	MutatingOperations         []HTTPMutation
	Expected5xx                ExpectedHTTPError
	ExpectedUnreadable         ExpectedHTTPError
}

type ExpectedHTTPError struct {
	Category  provider.ErrorCategory
	Retryable bool
}

// HTTPMutation invokes one valid mutating provider operation. RunHTTP supplies
// the failing contract server and verifies that a dispatched 5xx response is
// classified with an uncertain external outcome. Retry policy and the more
// specific bounded category remain adapter-specific observations.
type HTTPMutation struct {
	Name         string
	Invoke       func(context.Context, provider.Client) error
	WriteSuccess func(http.ResponseWriter)
}

// RunHTTP runs the shared contract for an HTTP-backed provider adapter.
func RunHTTP(t *testing.T, harness HTTPHarness) {
	t.Helper()
	if harness.NewClient == nil || harness.WriteCreateCheckoutSuccess == nil {
		t.Fatal("conformance harness is incomplete")
	}

	t.Run("maps a successful checkout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			harness.WriteCreateCheckoutSuccess(w)
		}))
		defer server.Close()
		checkout, err := harness.NewClient(t, server.URL, conformanceMaxResponseBytes).CreateCheckout(context.Background(), harness.ValidCreateCheckout)
		if err != nil {
			t.Fatalf("CreateCheckout: %v", err)
		}
		if checkout.ProviderPaymentID == "" || checkout.HostedReference == "" || checkout.AmountMinor != harness.ValidCreateCheckout.AmountMinor || checkout.Currency != harness.ValidCreateCheckout.Currency {
			t.Fatalf("checkout = %#v", checkout)
		}
	})

	t.Run("rejects invalid idempotency before dispatch", func(t *testing.T) {
		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
		defer server.Close()
		request := harness.ValidCreateCheckout
		request.IdempotencyKey = "contains whitespace"
		_, err := harness.NewClient(t, server.URL, conformanceMaxResponseBytes).CreateCheckout(context.Background(), request)
		providerErr := requireProviderError(t, err)
		if providerErr.Category != provider.ErrorPermanentValidation || providerErr.Retryable || providerErr.Uncertain || requests != 0 {
			t.Fatalf("error = %#v, requests = %d", providerErr, requests)
		}
	})

	operations := append([]HTTPMutation{{
		Name: "create checkout",
		Invoke: func(ctx context.Context, client provider.Client) error {
			_, err := client.CreateCheckout(ctx, harness.ValidCreateCheckout)
			return err
		},
		WriteSuccess: harness.WriteCreateCheckoutSuccess,
	}}, harness.MutatingOperations...)
	for _, operation := range operations {
		operation := operation
		if operation.Name == "" || operation.Invoke == nil || operation.WriteSuccess == nil {
			t.Fatal("HTTP mutation conformance harness is incomplete")
		}
		t.Run(operation.Name+" does not dispatch an already-cancelled request", func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			providerErr := requireProviderError(t, operation.Invoke(ctx, harness.NewClient(t, server.URL, conformanceMaxResponseBytes)))
			if providerErr.Uncertain || requests != 0 {
				t.Fatalf("error = %#v, requests = %d", providerErr, requests)
			}
		})
		for _, status := range []int{500, 502, 503, 504} {
			status := status
			t.Run(operation.Name+" marks "+http.StatusText(status)+" outcome unknown", func(t *testing.T) {
				requests := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					requests++
					w.WriteHeader(status)
					_, _ = w.Write([]byte("sensitive response body must remain private"))
				}))
				defer server.Close()
				err := operation.Invoke(context.Background(), harness.NewClient(t, server.URL, conformanceMaxResponseBytes))
				providerErr := requireProviderError(t, err)
				if providerErr.Category != harness.Expected5xx.Category || providerErr.Retryable != harness.Expected5xx.Retryable ||
					!providerErr.Uncertain || requests != 1 || strings.Contains(err.Error(), "sensitive") {
					t.Fatalf("error = %#v, requests = %d", providerErr, requests)
				}
			})
		}
		for _, unreadable := range []struct {
			name string
			body string
		}{
			{name: "malformed success", body: "{"},
			{name: "oversized success", body: strings.Repeat("x", int(conformanceMaxResponseBytes)+1)},
		} {
			unreadable := unreadable
			t.Run(operation.Name+" marks "+unreadable.name+" outcome unknown", func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(unreadable.body))
				}))
				defer server.Close()
				providerErr := requireProviderError(t, operation.Invoke(context.Background(), harness.NewClient(t, server.URL, conformanceMaxResponseBytes)))
				if providerErr.Category != harness.ExpectedUnreadable.Category || providerErr.Retryable != harness.ExpectedUnreadable.Retryable || !providerErr.Uncertain {
					t.Fatalf("error = %#v", providerErr)
				}
			})
		}
		t.Run(operation.Name+" response loss preserves stable idempotency replay", func(t *testing.T) {
			requests, logicalEffects, firstKey := 0, 0, ""
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				key := r.Header.Get("Idempotency-Key")
				if key == "" || (firstKey != "" && key != firstKey) {
					t.Errorf("idempotency key = %q, first = %q", key, firstKey)
				}
				if firstKey == "" {
					firstKey, logicalEffects = key, 1
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Errorf("Hijack: %v", err)
						return
					}
					_ = connection.Close()
					return
				}
				operation.WriteSuccess(w)
			}))
			defer server.Close()
			client := harness.NewClient(t, server.URL, conformanceMaxResponseBytes)
			firstErr := requireProviderError(t, operation.Invoke(context.Background(), client))
			if !firstErr.Uncertain {
				t.Fatalf("first error = %#v", firstErr)
			}
			if err := operation.Invoke(context.Background(), client); err != nil || requests != 2 || logicalEffects != 1 {
				t.Fatalf("replay error=%v requests=%d logical_effects=%d", err, requests, logicalEffects)
			}
		})
	}

	t.Run("classifies 429 without hidden retry", func(t *testing.T) {
		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests++
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer server.Close()
		_, err := harness.NewClient(t, server.URL, conformanceMaxResponseBytes).CreateCheckout(context.Background(), harness.ValidCreateCheckout)
		providerErr := requireProviderError(t, err)
		if providerErr.Category != provider.ErrorRateLimited || !providerErr.Retryable || providerErr.Uncertain || requests != 1 {
			t.Fatalf("error = %#v, requests = %d", providerErr, requests)
		}
	})

	t.Run("does not follow redirects", func(t *testing.T) {
		destinationRequests := 0
		destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destinationRequests++ }))
		defer destination.Close()
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
		}))
		defer origin.Close()
		_, err := harness.NewClient(t, origin.URL, conformanceMaxResponseBytes).CreateCheckout(context.Background(), harness.ValidCreateCheckout)
		if err == nil || destinationRequests != 0 {
			t.Fatalf("error = %v, redirected requests = %d", err, destinationRequests)
		}
	})
}

func requireProviderError(t *testing.T, err error) *provider.Error {
	t.Helper()
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) {
		t.Fatalf("error = %T %v, want *provider.Error", err, err)
	}
	return providerErr
}
