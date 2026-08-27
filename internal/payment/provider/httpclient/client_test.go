package httpclient_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/conformance"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/httpclient"
)

func TestHTTPProviderConformance(t *testing.T) {
	conformance.RunHTTP(t, conformance.HTTPHarness{
		NewClient: func(t *testing.T, origin string, maxResponseBytes int64) provider.Client {
			t.Helper()
			return mustClient(t, origin, maxResponseBytes)
		},
		ValidCreateCheckout: provider.CreateCheckoutRequest{
			PaymentIntentID: "intent-conformance", MerchantReference: "booking-conformance",
			AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "checkout-conformance",
		},
		WriteCreateCheckoutSuccess: func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(provider.Checkout{
				ProviderPaymentID: "pay_1", HostedReference: "sandbox-checkout:pay_1",
				SyntheticToken: "tok_1", Status: provider.StatusCreated, AmountMinor: 12500, Currency: "TWD",
			})
		},
		Expected5xx:        conformance.ExpectedHTTPError{Category: provider.ErrorTimeoutUnknown},
		ExpectedUnreadable: conformance.ExpectedHTTPError{Category: provider.ErrorInconsistentResponse},
		MutatingOperations: []conformance.HTTPMutation{
			{
				Name: "authorize",
				Invoke: func(ctx context.Context, client provider.Client) error {
					_, err := client.Authorize(ctx, provider.AuthorizeRequest{
						PaymentIntentID: "intent-conformance", ProviderPaymentID: "pay_1",
						SyntheticToken: "tok_1", AmountMinor: 12500, Currency: "TWD",
						IdempotencyKey: "authorize-conformance",
					})
					return err
				},
				WriteSuccess: func(w http.ResponseWriter) {
					writeConformanceOperation(w, "authorize_1", provider.StatusAuthorized, 12500)
				},
			},
			{
				Name: "capture",
				Invoke: func(ctx context.Context, client provider.Client) error {
					_, err := client.Capture(ctx, provider.CaptureRequest{
						PaymentIntentID: "intent-conformance", ProviderPaymentID: "pay_1",
						AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture-conformance",
					})
					return err
				},
				WriteSuccess: func(w http.ResponseWriter) { writeConformanceOperation(w, "capture_1", provider.StatusCaptured, 12500) },
			},
			{
				Name: "void",
				Invoke: func(ctx context.Context, client provider.Client) error {
					_, err := client.Void(ctx, provider.VoidRequest{
						PaymentIntentID: "intent-conformance", ProviderPaymentID: "pay_1",
						IdempotencyKey: "void-conformance",
					})
					return err
				},
				WriteSuccess: func(w http.ResponseWriter) { writeConformanceOperation(w, "void_1", provider.StatusVoided, 12500) },
			},
			{
				Name: "full refund",
				Invoke: func(ctx context.Context, client provider.Client) error {
					_, err := client.Refund(ctx, provider.RefundRequest{
						PaymentIntentID: "intent-conformance", ProviderPaymentID: "pay_1",
						AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "refund-conformance-full",
					})
					return err
				},
				WriteSuccess: func(w http.ResponseWriter) {
					writeConformanceOperation(w, "refund_full_1", provider.StatusRefunded, 12500)
				},
			},
			{
				Name: "partial refund",
				Invoke: func(ctx context.Context, client provider.Client) error {
					_, err := client.Refund(ctx, provider.RefundRequest{
						PaymentIntentID: "intent-conformance", ProviderPaymentID: "pay_1",
						AmountMinor: 2500, Currency: "TWD", IdempotencyKey: "refund-conformance-partial",
					})
					return err
				},
				WriteSuccess: func(w http.ResponseWriter) {
					writeConformanceOperation(w, "refund_partial_1", provider.StatusRefunded, 2500)
				},
			},
		},
	})
}

func writeConformanceOperation(w http.ResponseWriter, operationID string, status provider.Status, amount int64) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(provider.OperationResult{
		ProviderPaymentID: "pay_1", ProviderOperationID: operationID,
		Status: status, AmountMinor: amount, Currency: "TWD",
	})
}

func TestClientUsesFixedEndpointBoundedResponsesAndNoRedirects(t *testing.T) {
	t.Parallel()
	var authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		if r.URL.Path != "/v1/checkouts" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(provider.Checkout{
			ProviderPaymentID: "pay_1", HostedReference: "sandbox-checkout:pay_1",
			SyntheticToken: "tok_1", Status: provider.StatusCreated, AmountMinor: 12500, Currency: "TWD",
		})
	}))
	defer server.Close()

	client, err := httpclient.New(httpclient.Config{
		BaseURL: server.URL, APIKey: "synthetic-api-key", RequestTimeout: time.Second,
		ConnectTimeout: time.Second, MaxResponseBytes: 1024, WebhookKeys: testKeys(),
		WebhookClockSkew: time.Minute, Now: func() time.Time { return time.Unix(1_700_000_000, 0) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	checkout, err := client.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{
		PaymentIntentID: "intent_1", MerchantReference: "intent_1", AmountMinor: 12500,
		Currency: "TWD", IdempotencyKey: "checkout-intent-1",
	})
	if err != nil || checkout.ProviderPaymentID != "pay_1" {
		t.Fatalf("CreateCheckout() = %#v, %v", checkout, err)
	}
	if authorization != "Bearer synthetic-api-key" {
		t.Fatalf("Authorization = %q", authorization)
	}

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, server.URL, http.StatusFound)
	}))
	defer redirect.Close()
	client, err = httpclient.New(httpclient.Config{
		BaseURL: redirect.URL, RequestTimeout: time.Second, ConnectTimeout: time.Second,
		MaxResponseBytes: 1024, WebhookKeys: testKeys(), WebhookClockSkew: time.Minute,
	})
	if err != nil {
		t.Fatalf("New(redirect) error = %v", err)
	}
	_, err = client.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{
		PaymentIntentID: "intent_1", MerchantReference: "intent_1", AmountMinor: 1,
		Currency: "TWD", IdempotencyKey: "checkout-intent-1",
	})
	assertProviderError(t, err, provider.ErrorInconsistentResponse, false, true)
}

func TestClientRejectsHostedReferenceOutsidePersistenceContract(t *testing.T) {
	t.Parallel()
	for _, hostedReference := range []string{
		"https://provider.example/checkout/pay_1",
		"checkout reference",
		":checkout",
		strings.Repeat("a", 257),
	} {
		hostedReference := hostedReference
		t.Run(hostedReference[:min(len(hostedReference), 24)], func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(provider.Checkout{
					ProviderPaymentID: "pay_1", HostedReference: hostedReference,
					SyntheticToken: "tok_1", Status: provider.StatusCreated, AmountMinor: 12500, Currency: "TWD",
				})
			}))
			defer server.Close()
			client := mustClient(t, server.URL, 1024)
			_, err := client.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{
				PaymentIntentID: "intent_1", MerchantReference: "intent_1", AmountMinor: 12500,
				Currency: "TWD", IdempotencyKey: "checkout-intent-1",
			})
			assertProviderError(t, err, provider.ErrorInconsistentResponse, false, true)
		})
	}
}

func TestClientReadinessUsesFixedBoundedEndpoint(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	}))
	defer server.Close()
	client := mustClient(t, server.URL, 128)
	if err := client.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
	client.CloseIdleConnections()
}

func TestOutboundOnlyClientDoesNotRequireWebhookSecrets(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	}))
	defer server.Close()
	client, err := httpclient.New(httpclient.Config{
		BaseURL: server.URL, APIKey: "outbound-only", ConnectTimeout: time.Second,
		RequestTimeout: time.Second, MaxResponseBytes: 1024,
	})
	if err != nil {
		t.Fatalf("New(outbound-only) error = %v", err)
	}
	if err := client.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
	_, err = client.VerifyWebhook(context.Background(), provider.WebhookHeaders{
		KeyID: "unconfigured", Timestamp: "1700000000", Signature: strings.Repeat("0", 64),
	}, []byte(`{"event_id":"evt_1"}`))
	assertProviderError(t, err, provider.ErrorAuthentication, false, false)
}

func TestClientClassifiesMutationResponseLossAsUncertain(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hijacker := w.(http.Hijacker)
		connection, _, err := hijacker.Hijack()
		if err == nil {
			_ = connection.Close()
		}
	}))
	defer server.Close()
	client := mustClient(t, server.URL, 1024)
	_, err := client.Capture(context.Background(), provider.CaptureRequest{
		PaymentIntentID: "intent_1", ProviderPaymentID: "pay_1", AmountMinor: 100,
		Currency: "TWD", IdempotencyKey: "capture-intent-1",
	})
	assertProviderError(t, err, provider.ErrorTimeoutUnknown, false, true)
}

func TestClientClassifiesMutatingServerFailuresAsOutcomeUnknown(t *testing.T) {
	t.Parallel()
	for _, status := range []int{
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()

			client := mustClient(t, server.URL, 1024)
			_, err := client.Capture(context.Background(), provider.CaptureRequest{
				PaymentIntentID: "intent_1", ProviderPaymentID: "pay_1", AmountMinor: 100,
				Currency: "TWD", IdempotencyKey: "capture-intent-1",
			})
			assertProviderError(t, err, provider.ErrorTimeoutUnknown, false, true)
		})
	}
}

func TestClientVoidValidatesProviderReturnedOriginalMoney(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/payments/pay_1/void" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(provider.OperationResult{
			ProviderPaymentID: "pay_1", ProviderOperationID: "op_1", Status: provider.StatusVoided,
			AmountMinor: 100, Currency: "TWD",
		})
	}))
	defer server.Close()
	client := mustClient(t, server.URL, 1024)
	result, err := client.Void(context.Background(), provider.VoidRequest{
		PaymentIntentID: "intent_1", ProviderPaymentID: "pay_1", IdempotencyKey: "void-intent-1",
	})
	if err != nil || result.AmountMinor != 100 || result.Currency != "TWD" {
		t.Fatalf("Void() = %#v, %v", result, err)
	}
}

func TestClientLooksUpExactRefundWithoutReplayingMutation(t *testing.T) {
	t.Parallel()
	request := provider.RefundLookupRequest{
		PaymentIntentID: "intent_1", ProviderPaymentID: "pay_1", AmountMinor: 50,
		Currency: "TWD", IdempotencyKey: "fixture", Limit: 100,
		Metadata: provider.Metadata{
			"refund_request_id": "request-1", "refund_operation_id": "operation-1",
			"refund_idempotency_key": "fixture",
		},
	}
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/refund-lookups" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		var got provider.RefundLookupRequest
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil || got.IdempotencyKey != request.IdempotencyKey || got.Limit != 100 {
			t.Fatalf("lookup request = %#v, %v", got, err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(provider.RefundLookupResult{
			Found: true, Definitive: true,
			Refund: provider.OperationResult{ProviderPaymentID: "pay_1", ProviderOperationID: "refund_1", Status: provider.StatusRefunded, AmountMinor: 50, Currency: "TWD"},
		})
	}))
	defer server.Close()

	client := mustClient(t, server.URL, 2048)
	reader, ok := any(client).(provider.RefundLookupReader)
	if !ok {
		t.Fatal("HTTP sandbox client does not expose exact refund lookup")
	}
	result, err := reader.LookupRefund(context.Background(), request)
	if err != nil || !result.Found || !result.Definitive || result.Refund.ProviderOperationID != "refund_1" || calls != 1 {
		t.Fatalf("LookupRefund() = %#v, %v; calls=%d", result, err, calls)
	}
}

func TestClientRejectsContradictoryAuthorizedFinancialObservation(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(provider.Payment{
			ProviderPaymentID: "pay_1", Status: provider.StatusAuthorized,
			AmountMinor: 100, Currency: "TWD", CapturedMinor: 100,
			ProviderUpdatedAt: time.Now().UTC(),
		})
	}))
	defer server.Close()

	client := mustClient(t, server.URL, 1024)
	_, err := client.GetPaymentStatus(context.Background(), "pay_1")
	assertProviderError(t, err, provider.ErrorInconsistentResponse, false, false)
}

func TestClientRejectsMalformedAndOversizedProviderResponses(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{"broken"`},
		{name: "oversized", body: `{"value":"` + strings.Repeat("x", 256) + `"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			client := mustClient(t, server.URL, 128)
			_, err := client.Capture(context.Background(), provider.CaptureRequest{
				PaymentIntentID: "intent_1", ProviderPaymentID: "pay_1", AmountMinor: 100,
				Currency: "TWD", IdempotencyKey: "capture-intent-1",
			})
			assertProviderError(t, err, provider.ErrorInconsistentResponse, false, true)
		})
	}
}

func TestClientVerifiesStandardHMACAndNormalizesUnknownEvent(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0).UTC()
	client, err := httpclient.New(httpclient.Config{
		BaseURL: "https://payments.example", RequestTimeout: time.Second, ConnectTimeout: time.Second,
		MaxResponseBytes: 1024, MaxWebhookBodyBytes: 1024, WebhookKeys: testKeys(),
		WebhookClockSkew: time.Minute, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	event := provider.WebhookEvent{
		ProviderEventID: "evt_1", Type: "payment.future", ProviderPaymentID: "pay_1",
		Status: provider.StatusCaptured, AmountMinor: 100, Currency: "TWD", OccurredAt: now,
	}
	body, _ := json.Marshal(event)
	timestamp := "1700000000"
	headers := provider.WebhookHeaders{KeyID: "current", Timestamp: timestamp, Signature: sign(testKeys()["current"], timestamp, body)}
	verified, err := client.VerifyWebhook(context.Background(), headers, body)
	if err != nil {
		t.Fatalf("VerifyWebhook() error = %v", err)
	}
	if verified.Type != provider.EventUnknown || verified.OriginalType != "payment.future" {
		t.Fatalf("event = %#v", verified)
	}

	headers.Signature = sign(testKeys()["current"], timestamp, append(body, 'x'))
	_, err = client.VerifyWebhook(context.Background(), headers, body)
	assertProviderError(t, err, provider.ErrorAuthentication, false, false)
	headers.Signature = sign(testKeys()["current"], "1699999000", body)
	headers.Timestamp = "1699999000"
	_, err = client.VerifyWebhook(context.Background(), headers, body)
	assertProviderError(t, err, provider.ErrorAuthentication, false, false)
}

func mustClient(t *testing.T, baseURL string, limit int64) *httpclient.Client {
	t.Helper()
	client, err := httpclient.New(httpclient.Config{
		BaseURL: baseURL, RequestTimeout: time.Second, ConnectTimeout: time.Second,
		MaxResponseBytes: limit, MaxWebhookBodyBytes: 1024, WebhookKeys: testKeys(),
		WebhookClockSkew: time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return client
}

func testKeys() map[string][]byte {
	return map[string][]byte{"current": []byte("0123456789abcdef0123456789abcdef")}
}

func sign(key []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func assertProviderError(t *testing.T, err error, category provider.ErrorCategory, retryable, uncertain bool) {
	t.Helper()
	var providerError *provider.Error
	if !errors.As(err, &providerError) {
		t.Fatalf("error = %T %v", err, err)
	}
	if providerError.Category != category || providerError.Retryable != retryable || providerError.Uncertain != uncertain {
		t.Fatalf("provider error = %#v", providerError)
	}
}
