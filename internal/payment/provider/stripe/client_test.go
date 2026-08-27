package stripe_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/conformance"
	stripeprovider "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
)

func TestCreateCheckoutUsesManualCaptureAndStableStripeHeaders(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/checkout/sessions" {
			t.Errorf("request = %s %s, want POST /v1/checkout/sessions", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk_test_contract" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Stripe-Account"); got != "acct_contract" {
			t.Errorf("Stripe-Account = %q", got)
		}
		if got := r.Header.Get("Stripe-Version"); got != "2026-07-29.dahlia" {
			t.Errorf("Stripe-Version = %q", got)
		}
		if got := r.Header.Get("Idempotency-Key"); got != "checkout-001" {
			t.Errorf("Idempotency-Key = %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
		}
		wantForm := map[string]string{
			"mode":                                             "payment",
			"payment_method_types[0]":                          "card",
			"payment_intent_data[capture_method]":              "manual",
			"line_items[0][quantity]":                          "1",
			"line_items[0][price_data][currency]":              "twd",
			"line_items[0][price_data][unit_amount]":           "129900",
			"line_items[0][price_data][product_data][name]":    "Railway ticket",
			"client_reference_id":                              "booking-001",
			"payment_intent_data[metadata][payment_intent_id]": "pay-001",
		}
		for key, want := range wantForm {
			if got := r.PostForm.Get(key); got != want {
				t.Errorf("form[%q] = %q, want %q", key, got, want)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":             "cs_test_contract",
			"url":            "https://checkout.stripe.test/c/pay_contract",
			"payment_intent": "pi_contract",
			"payment_status": "unpaid",
			"amount_total":   129900,
			"currency":       "twd",
		})
	}))
	defer server.Close()

	client, err := stripeprovider.New(stripeprovider.Config{
		SecretKey:            "sk_test_contract",
		AccountID:            "acct_contract",
		APIOrigin:            server.URL,
		SuccessURL:           "https://merchant.test/payments/success",
		CancelURL:            "https://merchant.test/payments/cancel",
		RequestTimeout:       time.Second,
		ConnectTimeout:       time.Second,
		MaxResponseBodyBytes: 16 << 10,
		AllowInsecureForTest: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	checkout, err := client.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{
		PaymentIntentID:   "pay-001",
		MerchantReference: "booking-001",
		AmountMinor:       129900,
		Currency:          "TWD",
		IdempotencyKey:    "checkout-001",
	})
	if err != nil {
		t.Fatalf("CreateCheckout: %v", err)
	}
	if checkout.ProviderPaymentID != "pi_contract" || checkout.HostedReference != "cs_test_contract" || checkout.SyntheticToken != "cs_test_contract" {
		t.Fatalf("checkout identifiers = %#v", checkout)
	}
	if checkout.Status != provider.StatusCreated || checkout.AmountMinor != 129900 || checkout.Currency != "TWD" {
		t.Fatalf("checkout financial result = %#v", checkout)
	}
}

func TestReadyVerifiesConfiguredStripeAccount(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/account" {
			t.Errorf("request = %s %s, want GET /v1/account", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk_test_contract" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Stripe-Account"); got != "acct_contract" {
			t.Errorf("Stripe-Account = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "acct_contract"})
	}))
	defer server.Close()

	client := mustTestClient(t, server.URL)
	if err := client.Ready(context.Background()); err != nil {
		t.Fatalf("Ready: %v", err)
	}
	client.CloseIdleConnections()
}

func TestReadyRejectsDifferentStripeAccount(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "acct_other"})
	}))
	defer server.Close()

	err := mustTestClient(t, server.URL).Ready(context.Background())
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorInconsistentResponse {
		t.Fatalf("Ready error = %#v", err)
	}
}

func TestStatusClientDoesNotRequireCheckoutRedirectsAndCannotCreateCheckout(t *testing.T) {
	t.Parallel()

	client, err := stripeprovider.NewStatusClient(stripeprovider.Config{
		SecretKey: "rk_test_contract", AccountID: "acct_contract",
		APIOrigin: "https://api.stripe.com",
	})
	if err != nil {
		t.Fatalf("NewStatusClient: %v", err)
	}
	defer client.CloseIdleConnections()
	_, err = client.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{
		PaymentIntentID: "pay-001", MerchantReference: "booking-001", AmountMinor: 129900,
		Currency: "TWD", IdempotencyKey: "checkout-001",
	})
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorPermanentValidation {
		t.Fatalf("CreateCheckout error = %#v", err)
	}
}

func TestGetPaymentStatusNormalizesFinancialState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		stripeStatus   string
		amountReceived int64
		refunded       int64
		wantStatus     provider.Status
		wantCaptured   int64
	}{
		{name: "authorized", stripeStatus: "requires_capture", wantStatus: provider.StatusAuthorized},
		{name: "captured", stripeStatus: "succeeded", amountReceived: 129900, wantStatus: provider.StatusCaptured, wantCaptured: 129900},
		{name: "partial refund remains non-authoritative evidence", stripeStatus: "succeeded", amountReceived: 129900, refunded: 30000, wantStatus: provider.StatusUnknown, wantCaptured: 129900},
		{name: "fully refunded", stripeStatus: "succeeded", amountReceived: 129900, refunded: 129900, wantStatus: provider.StatusRefunded, wantCaptured: 129900},
		{name: "voided", stripeStatus: "canceled", wantStatus: provider.StatusVoided},
		{name: "customer action", stripeStatus: "requires_action", wantStatus: provider.StatusRequiresCustomerAction},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1/payment_intents/pi_contract" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				if got := r.URL.Query()["expand[]"]; len(got) != 1 || got[0] != "latest_charge" {
					t.Errorf("expand[] = %#v", got)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":                "pi_contract",
					"amount":            129900,
					"amount_received":   test.amountReceived,
					"amount_capturable": 0,
					"currency":          "twd",
					"status":            test.stripeStatus,
					"created":           1786406400,
					"canceled_at":       1786406500,
					"latest_charge": map[string]any{
						"id":              "ch_contract",
						"amount":          129900,
						"amount_refunded": test.refunded,
						"captured":        test.amountReceived > 0,
						"refunded":        test.refunded == 129900,
						"created":         1786406450,
					},
				})
			}))
			defer server.Close()
			client := mustTestClient(t, server.URL)

			payment, err := client.GetPaymentStatus(context.Background(), "pi_contract")
			if err != nil {
				t.Fatalf("GetPaymentStatus: %v", err)
			}
			if payment.Status != test.wantStatus || payment.CapturedMinor != test.wantCaptured || payment.RefundedMinor != test.refunded {
				t.Fatalf("payment = %#v", payment)
			}
			if payment.ProviderPaymentID != "pi_contract" || payment.AmountMinor != 129900 || payment.Currency != "TWD" || payment.ProviderUpdatedAt.IsZero() {
				t.Fatalf("payment identity = %#v", payment)
			}
			if err := provider.EvaluateFinancialObservation(
				provider.FinancialExpectation{AmountMinor: 129900, Currency: "TWD"},
				provider.FinancialObservation{Status: payment.Status, AmountMinor: payment.AmountMinor, Currency: payment.Currency, CapturedMinor: payment.CapturedMinor, RefundedMinor: payment.RefundedMinor},
			); err != nil {
				t.Fatalf("normalized observation: %v", err)
			}
		})
	}
}

func TestCaptureUsesStripeIdempotencyAndReturnsRequestIdentity(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/payment_intents/pi_contract/capture" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Idempotency-Key"); got != "capture-001" {
			t.Errorf("Idempotency-Key = %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.PostForm.Get("amount_to_capture"); got != "129900" {
			t.Errorf("amount_to_capture = %q", got)
		}
		w.Header().Set("Request-Id", "req_capture_contract")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":              "pi_contract",
			"amount":          129900,
			"amount_received": 129900,
			"currency":        "twd",
			"status":          "succeeded",
			"created":         1786406400,
			"latest_charge": map[string]any{
				"id":              "ch_contract",
				"amount":          129900,
				"amount_refunded": 0,
				"captured":        true,
				"created":         1786406450,
			},
		})
	}))
	defer server.Close()

	result, err := mustTestClient(t, server.URL).Capture(context.Background(), provider.CaptureRequest{
		PaymentIntentID:   "pay-001",
		ProviderPaymentID: "pi_contract",
		AmountMinor:       129900,
		Currency:          "TWD",
		IdempotencyKey:    "capture-001",
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if result != (provider.OperationResult{ProviderPaymentID: "pi_contract", ProviderOperationID: "req_capture_contract", Status: provider.StatusCaptured, AmountMinor: 129900, Currency: "TWD"}) {
		t.Fatalf("result = %#v", result)
	}
}

func TestVoidCancelsAnUncapturedPaymentIntent(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/payment_intents/pi_contract/cancel" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Idempotency-Key"); got != "void-001" {
			t.Errorf("Idempotency-Key = %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.PostForm.Get("cancellation_reason"); got != "requested_by_customer" {
			t.Errorf("cancellation_reason = %q", got)
		}
		w.Header().Set("Request-Id", "req_void_contract")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":              "pi_contract",
			"amount":          129900,
			"amount_received": 0,
			"currency":        "twd",
			"status":          "canceled",
			"created":         1786406400,
			"canceled_at":     1786406500,
		})
	}))
	defer server.Close()

	result, err := mustTestClient(t, server.URL).Void(context.Background(), provider.VoidRequest{
		PaymentIntentID:   "pay-001",
		ProviderPaymentID: "pi_contract",
		IdempotencyKey:    "void-001",
	})
	if err != nil {
		t.Fatalf("Void: %v", err)
	}
	if result != (provider.OperationResult{ProviderPaymentID: "pi_contract", ProviderOperationID: "req_void_contract", Status: provider.StatusVoided, AmountMinor: 129900, Currency: "TWD"}) {
		t.Fatalf("result = %#v", result)
	}
}

func TestRefundSupportsFullAndPartialAmounts(t *testing.T) {
	t.Parallel()

	for _, amount := range []int64{129900, 30000} {
		amount := amount
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/refunds" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				if got := r.Header.Get("Idempotency-Key"); got != fmt.Sprintf("refund-%d", amount) {
					t.Errorf("Idempotency-Key = %q", got)
				}
				if err := r.ParseForm(); err != nil {
					t.Fatalf("ParseForm: %v", err)
				}
				if got := r.PostForm.Get("payment_intent"); got != "pi_contract" {
					t.Errorf("payment_intent = %q", got)
				}
				if got := r.PostForm.Get("amount"); got != fmt.Sprint(amount) {
					t.Errorf("amount = %q", got)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":             fmt.Sprintf("re_%d", amount),
					"amount":         amount,
					"currency":       "twd",
					"payment_intent": "pi_contract",
					"status":         "succeeded",
					"created":        1786406600,
				})
			}))
			defer server.Close()

			result, err := mustTestClient(t, server.URL).Refund(context.Background(), provider.RefundRequest{
				PaymentIntentID:   "pay-001",
				ProviderPaymentID: "pi_contract",
				AmountMinor:       amount,
				Currency:          "TWD",
				IdempotencyKey:    fmt.Sprintf("refund-%d", amount),
			})
			if err != nil {
				t.Fatalf("Refund: %v", err)
			}
			if result.ProviderPaymentID != "pi_contract" || result.ProviderOperationID != fmt.Sprintf("re_%d", amount) || result.Status != provider.StatusRefunded || result.AmountMinor != amount || result.Currency != "TWD" {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestAuthorizeObservesHostedManualCaptureWithoutAnotherMutation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/payment_intents/pi_contract" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Idempotency-Key"); got != "" {
			t.Errorf("read-only authorization sent idempotency key %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":              "pi_contract",
			"amount":          129900,
			"amount_received": 0,
			"currency":        "twd",
			"status":          "requires_capture",
			"created":         1786406400,
		})
	}))
	defer server.Close()

	result, err := mustTestClient(t, server.URL).Authorize(context.Background(), provider.AuthorizeRequest{
		PaymentIntentID:   "pay-001",
		ProviderPaymentID: "pi_contract",
		SyntheticToken:    "cs_test_contract",
		AmountMinor:       129900,
		Currency:          "TWD",
		IdempotencyKey:    "authorize-001",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if result != (provider.OperationResult{ProviderPaymentID: "pi_contract", ProviderOperationID: "pi_contract.authorization", Status: provider.StatusAuthorized, AmountMinor: 129900, Currency: "TWD"}) {
		t.Fatalf("result = %#v", result)
	}
}

func TestMutatingStripe5xxClassifiesOnlyOutcomeUnknownStatuses(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		status        int
		wantUncertain bool
	}{
		{status: 500, wantUncertain: true},
		{status: 501, wantUncertain: false},
		{status: 502, wantUncertain: true},
		{status: 503, wantUncertain: true},
		{status: 504, wantUncertain: true},
	} {
		test := test
		t.Run(fmt.Sprint(test.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(`{"error":{"message":"card 4242424242424242 secret"}}`))
			}))
			defer server.Close()

			_, err := mustTestClient(t, server.URL).Capture(context.Background(), provider.CaptureRequest{
				PaymentIntentID:   "pay-001",
				ProviderPaymentID: "pi_contract",
				AmountMinor:       129900,
				Currency:          "TWD",
				IdempotencyKey:    "capture-001",
			})
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) {
				t.Fatalf("error = %T %v", err, err)
			}
			if providerErr.Category != provider.ErrorUnavailable || !providerErr.Retryable || providerErr.Uncertain != test.wantUncertain {
				t.Fatalf("provider error = %#v", providerErr)
			}
			if strings.Contains(err.Error(), "4242424242424242") || strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaked response body: %v", err)
			}
		})
	}
}

func TestStripe429IsRetryableButNotOutcomeUnknown(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Retry-After", "3")
		http.Error(w, "bounded rate limit", http.StatusTooManyRequests)
	}))
	defer server.Close()

	_, err := mustTestClient(t, server.URL).Refund(context.Background(), provider.RefundRequest{
		PaymentIntentID:   "pay-001",
		ProviderPaymentID: "pi_contract",
		AmountMinor:       30000,
		Currency:          "TWD",
		IdempotencyKey:    "refund-30000",
	})
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) {
		t.Fatalf("error = %T %v", err, err)
	}
	if providerErr.Operation != "refund" || providerErr.Category != provider.ErrorRateLimited || !providerErr.Retryable || providerErr.Uncertain || providerErr.RetryAfter != 3*time.Second {
		t.Fatalf("provider error = %#v", providerErr)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, adapter must not hide retries from the durable saga", requests)
	}
}

func TestVerifyWebhookAcceptsBoundedSecretRotation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"id":"evt_contract","type":"payment_intent.amount_capturable_updated","api_version":"2026-07-29.dahlia","livemode":false,"created":1786449600,"data":{"object":{"id":"pi_contract","amount":129900,"amount_received":0,"currency":"twd","status":"requires_capture","created":1786449500}}}`)
	client, err := stripeprovider.New(stripeprovider.Config{
		SecretKey:            "sk_test_contract",
		AccountID:            "acct_contract",
		APIOrigin:            "https://api.stripe.test",
		SuccessURL:           "https://merchant.test/payments/success",
		CancelURL:            "https://merchant.test/payments/cancel",
		RequestTimeout:       time.Second,
		ConnectTimeout:       time.Second,
		MaxResponseBodyBytes: 16 << 10,
		MaxWebhookBodyBytes:  16 << 10,
		WebhookTolerance:     5 * time.Minute,
		WebhookSecrets:       []string{"whsec_current_contract", "whsec_previous_contract"},
		WebhookSecretIDs:     []string{"stripe-current", "stripe-previous"},
		Now:                  func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	signature := stripeSignature(now.Unix(), payload, "whsec_previous_contract")
	event, err := client.VerifyWebhook(context.Background(), provider.WebhookHeaders{Signature: signature}, payload)
	if err != nil {
		t.Fatalf("VerifyWebhook: %v", err)
	}
	if event.ProviderEventID != "evt_contract" || event.Type != provider.EventAuthorized || event.OriginalType != "payment_intent.amount_capturable_updated" || event.ProviderPaymentID != "pi_contract" || event.Status != provider.StatusAuthorized || event.AmountMinor != 129900 || event.Currency != "TWD" || event.VerifiedKeyID != "stripe-previous" || event.ProviderAccountID != "acct_contract" || event.Environment != provider.WebhookEnvironmentTest || !event.OccurredAt.Equal(now) {
		t.Fatalf("event = %#v", event)
	}

	_, err = client.VerifyWebhook(context.Background(), provider.WebhookHeaders{Signature: stripeSignature(now.Unix(), payload, "whsec_not_configured")}, payload)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorAuthentication {
		t.Fatalf("invalid signature error = %#v", err)
	}

	_, err = stripeprovider.New(stripeprovider.Config{
		SecretKey: "sk_test_contract", AccountID: "acct_contract", APIOrigin: "https://api.stripe.test",
		SuccessURL: "https://merchant.test/success", CancelURL: "https://merchant.test/cancel",
		WebhookSecrets: []string{"whsec_one_contract", "whsec_two_contract", "whsec_three_contract"},
	})
	if err == nil {
		t.Fatal("New accepted more than current and previous webhook secrets")
	}
}

func TestWebhookVerifierDoesNotRequireOutboundAPICredential(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"id":"evt_ingress","type":"payment_intent.amount_capturable_updated","api_version":"2026-07-29.dahlia","account":"acct_contract","livemode":false,"created":1786449600,"data":{"object":{"id":"pi_contract","amount":129900,"amount_received":0,"currency":"twd","status":"requires_capture","created":1786449500}}}`)
	verifier, err := stripeprovider.NewWebhookVerifier(stripeprovider.WebhookConfig{
		MaxBodyBytes: 16 << 10, Tolerance: 5 * time.Minute,
		Secrets: []string{"whsec_ingress_contract"}, SecretIDs: []string{"stripe-primary"},
		AccountID: "acct_contract", LiveMode: false,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewWebhookVerifier: %v", err)
	}
	event, err := verifier.VerifyWebhook(context.Background(), provider.WebhookHeaders{
		Signature: stripeSignature(now.Unix(), payload, "whsec_ingress_contract"),
	}, payload)
	if err != nil || event.Type != provider.EventAuthorized || event.VerifiedKeyID != "stripe-primary" ||
		event.ProviderAccountID != "acct_contract" || event.Environment != provider.WebhookEnvironmentTest {
		t.Fatalf("event=%#v error=%v", event, err)
	}
}

func TestWebhookVerifierRejectsSignedWrongAccountOrEnvironment(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	verifier, err := stripeprovider.NewWebhookVerifier(stripeprovider.WebhookConfig{
		MaxBodyBytes: 16 << 10, Tolerance: 5 * time.Minute,
		Secrets: []string{"whsec_ingress_contract"}, SecretIDs: []string{"stripe-primary"},
		AccountID: "acct_contract", LiveMode: false, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{
		"wrong account":       []byte(`{"id":"evt_wrong_account","type":"payment_intent.amount_capturable_updated","api_version":"2026-07-29.dahlia","account":"acct_other","livemode":false,"created":1786449600,"data":{"object":{"id":"pi_contract","amount":129900,"amount_received":0,"currency":"twd","status":"requires_capture","created":1786449500}}}`),
		"wrong environment":   []byte(`{"id":"evt_wrong_mode","type":"payment_intent.amount_capturable_updated","api_version":"2026-07-29.dahlia","account":"acct_contract","livemode":true,"created":1786449600,"data":{"object":{"id":"pi_contract","amount":129900,"amount_received":0,"currency":"twd","status":"requires_capture","created":1786449500}}}`),
		"missing environment": []byte(`{"id":"evt_missing_mode","type":"payment_intent.amount_capturable_updated","api_version":"2026-07-29.dahlia","account":"acct_contract","created":1786449600,"data":{"object":{"id":"pi_contract","amount":129900,"amount_received":0,"currency":"twd","status":"requires_capture","created":1786449500}}}`),
	} {
		t.Run(name, func(t *testing.T) {
			_, verifyErr := verifier.VerifyWebhook(context.Background(), provider.WebhookHeaders{
				Signature: stripeSignature(now.Unix(), payload, "whsec_ingress_contract"),
			}, payload)
			var providerErr *provider.Error
			if !errors.As(verifyErr, &providerErr) || providerErr.Category != provider.ErrorInconsistentResponse {
				t.Fatalf("error = %#v", verifyErr)
			}
		})
	}
}

func TestWebhookVerifierRequiresConfiguredAccountBinding(t *testing.T) {
	t.Parallel()
	_, err := stripeprovider.NewWebhookVerifier(stripeprovider.WebhookConfig{
		MaxBodyBytes: 16 << 10, Tolerance: 5 * time.Minute,
		Secrets: []string{"whsec_ingress_contract"}, SecretIDs: []string{"stripe-primary"},
		Now: time.Now,
	})
	if err == nil {
		t.Fatal("NewWebhookVerifier accepted an unbound endpoint secret")
	}
}

func TestWebhookVerifierRejectsPreviousSecretAtRetirementDeadline(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)
	clock := now
	verifier, err := stripeprovider.NewWebhookVerifier(stripeprovider.WebhookConfig{
		MaxBodyBytes: 1 << 20, Tolerance: 5 * time.Minute,
		Secrets:   []string{"whsec_current_contract", "whsec_previous_contract"},
		SecretIDs: []string{"current", "previous"}, SecretNotAfter: []time.Time{{}, deadline},
		AccountID: "acct_contract", LiveMode: false,
		Now: func() time.Time { return clock },
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"id":"evt_retirement","type":"payment_intent.amount_capturable_updated","api_version":"2026-07-29.dahlia","livemode":false,"created":1786449600,"data":{"object":{"id":"pi_contract","amount":129900,"amount_received":0,"currency":"twd","status":"requires_capture","created":1786449500}}}`)
	headers := provider.WebhookHeaders{Signature: stripeSignature(now.Unix(), payload, "whsec_previous_contract")}
	if _, err := verifier.VerifyWebhook(context.Background(), headers, payload); err != nil {
		t.Fatalf("previous secret before deadline: %v", err)
	}
	clock = deadline
	if _, err := verifier.VerifyWebhook(context.Background(), headers, payload); err == nil {
		t.Fatal("previous secret remained accepted at retirement deadline")
	}
}

func TestWebhookVerifierRejectsAmbiguousMultiKeySignature(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"id":"evt_ambiguous","type":"payment_intent.amount_capturable_updated","api_version":"2026-07-29.dahlia","livemode":false,"created":1786449600,"data":{"object":{"id":"pi_contract","amount":129900,"amount_received":0,"currency":"twd","status":"requires_capture","created":1786449500}}}`)
	verifier, err := stripeprovider.NewWebhookVerifier(stripeprovider.WebhookConfig{
		MaxBodyBytes: 1 << 20, Tolerance: 5 * time.Minute,
		Secrets: []string{"whsec_current_contract", "whsec_previous_contract"}, SecretIDs: []string{"current", "previous"},
		AccountID: "acct_contract", LiveMode: false, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	header := stripeSignature(now.Unix(), payload, "whsec_current_contract") + "," +
		strings.TrimPrefix(stripeSignature(now.Unix(), payload, "whsec_previous_contract"), fmt.Sprintf("t=%d,", now.Unix()))
	if _, err := verifier.VerifyWebhook(context.Background(), provider.WebhookHeaders{Signature: header}, payload); err == nil {
		t.Fatal("ambiguous signature matched two configured key generations")
	}
}

func TestBalanceTransactionReaderUsesExplicitCursorPagination(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/balance_transactions" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("limit"); got != "1" {
			t.Errorf("limit = %q", got)
		}
		if got := r.URL.Query().Get("expand[]"); got != "data.source" {
			t.Errorf("expand = %q", got)
		}
		cursor := r.URL.Query().Get("starting_after")
		switch cursor {
		case "":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"has_more": true,
				"data": []map[string]any{{
					"id": "txn_first", "amount": 1000, "fee": 30, "net": 970, "currency": "twd",
					"created": 1786406400, "available_on": 1786492800, "status": "available", "type": "charge", "reporting_category": "charge",
					"source": map[string]any{"id": "ch_first", "object": "charge", "payment_intent": "pi_first"},
				}},
			})
		case "txn_first":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"has_more": false,
				"data": []map[string]any{{
					"id": "txn_second", "amount": -300, "fee": 0, "net": -300, "currency": "twd",
					"created": 1786406500, "available_on": 1786492900, "status": "pending", "type": "refund", "source": "re_second",
				}},
			})
		default:
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client := mustTestClient(t, server.URL)

	first, err := client.ListBalanceTransactions(context.Background(), stripeprovider.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if !first.HasMore || first.NextStartingAfter != "txn_first" || len(first.Items) != 1 {
		t.Fatalf("first page = %#v", first)
	}
	if item := first.Items[0]; item.GrossMinor != 1000 || item.FeeMinor != 30 || item.NetMinor != 970 || item.Currency != "TWD" || item.SourceID != "ch_first" || item.PaymentCorrelation != "pi_first" || item.ReportingCategory != "charge" {
		t.Fatalf("first item = %#v", item)
	}
	second, err := client.ListBalanceTransactions(context.Background(), stripeprovider.ListOptions{Limit: 1, StartingAfter: first.NextStartingAfter})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if second.HasMore || second.NextStartingAfter != "" || len(second.Items) != 1 || second.Items[0].GrossMinor != -300 || second.Items[0].NetMinor != -300 {
		t.Fatalf("second page = %#v", second)
	}
}

func TestHTTPProviderConformance(t *testing.T) {
	t.Parallel()

	conformance.RunHTTP(t, conformance.HTTPHarness{
		NewClient: func(t *testing.T, origin string, maxResponseBytes int64) provider.Client {
			t.Helper()
			client, err := stripeprovider.New(stripeprovider.Config{
				SecretKey: "sk_test_contract", AccountID: "acct_contract", APIOrigin: origin,
				SuccessURL: "https://merchant.test/success", CancelURL: "https://merchant.test/cancel",
				RequestTimeout: time.Second, ConnectTimeout: time.Second, MaxResponseBodyBytes: maxResponseBytes,
				AllowInsecureForTest: true,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			return client
		},
		ValidCreateCheckout: provider.CreateCheckoutRequest{
			PaymentIntentID: "pay-conformance", MerchantReference: "booking-conformance",
			AmountMinor: 129900, Currency: "TWD", IdempotencyKey: "checkout-conformance",
		},
		WriteCreateCheckoutSuccess: func(w http.ResponseWriter) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "cs_conformance", "url": "https://checkout.stripe.test/c/pay",
				"payment_intent": "pi_conformance", "payment_status": "unpaid", "amount_total": 129900, "currency": "twd",
			})
		},
		Expected5xx:        conformance.ExpectedHTTPError{Category: provider.ErrorUnavailable, Retryable: true},
		ExpectedUnreadable: conformance.ExpectedHTTPError{Category: provider.ErrorTimeoutUnknown, Retryable: true},
		MutatingOperations: []conformance.HTTPMutation{
			{
				Name: "capture",
				Invoke: func(ctx context.Context, client provider.Client) error {
					_, err := client.Capture(ctx, provider.CaptureRequest{
						PaymentIntentID: "intent-conformance", ProviderPaymentID: "pi_conformance",
						AmountMinor: 129900, Currency: "TWD", IdempotencyKey: "capture-conformance",
					})
					return err
				},
				WriteSuccess: func(w http.ResponseWriter) { writeStripeCaptureSuccess(w) },
			},
			{
				Name: "void",
				Invoke: func(ctx context.Context, client provider.Client) error {
					_, err := client.Void(ctx, provider.VoidRequest{
						PaymentIntentID: "intent-conformance", ProviderPaymentID: "pi_conformance",
						IdempotencyKey: "void-conformance",
					})
					return err
				},
				WriteSuccess: func(w http.ResponseWriter) { writeStripeVoidSuccess(w) },
			},
			{
				Name: "full refund",
				Invoke: func(ctx context.Context, client provider.Client) error {
					_, err := client.Refund(ctx, provider.RefundRequest{
						PaymentIntentID: "intent-conformance", ProviderPaymentID: "pi_conformance",
						AmountMinor: 129900, Currency: "TWD", IdempotencyKey: "refund-conformance-full",
					})
					return err
				},
				WriteSuccess: func(w http.ResponseWriter) { writeStripeRefundSuccess(w, "re_conformance_full", 129900) },
			},
			{
				Name: "partial refund",
				Invoke: func(ctx context.Context, client provider.Client) error {
					_, err := client.Refund(ctx, provider.RefundRequest{
						PaymentIntentID: "intent-conformance", ProviderPaymentID: "pi_conformance",
						AmountMinor: 29900, Currency: "TWD", IdempotencyKey: "refund-conformance-partial",
					})
					return err
				},
				WriteSuccess: func(w http.ResponseWriter) { writeStripeRefundSuccess(w, "re_conformance_partial", 29900) },
			},
		},
	})
}

func writeStripeCaptureSuccess(w http.ResponseWriter) {
	w.Header().Set("Request-Id", "req_conformance_capture")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": "pi_conformance", "amount": 129900, "amount_received": 129900,
		"currency": "twd", "status": "succeeded", "created": 1786406400,
		"latest_charge": map[string]any{"id": "ch_conformance", "amount": 129900, "amount_refunded": 0, "captured": true, "refunded": false, "created": 1786406401},
	})
}

func writeStripeVoidSuccess(w http.ResponseWriter) {
	w.Header().Set("Request-Id", "req_conformance_void")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": "pi_conformance", "amount": 129900, "amount_received": 0,
		"currency": "twd", "status": "canceled", "created": 1786406400, "canceled_at": 1786406401,
	})
}

func writeStripeRefundSuccess(w http.ResponseWriter, refundID string, amount int64) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": refundID, "payment_intent": "pi_conformance", "amount": amount,
		"currency": "twd", "status": "succeeded",
	})
}

func TestProviderOperationConformance(t *testing.T) {
	conformance.RunOperations(t, conformance.OperationHarness{
		NewClient: newStripeOperationClient,
		ValidCreateCheckout: provider.CreateCheckoutRequest{
			PaymentIntentID: "intent-conformance", MerchantReference: "booking-conformance",
			AmountMinor: 129900, Currency: "TWD", IdempotencyKey: "checkout:intent-conformance:v1",
		},
		AuthorizeRequest: func(checkout provider.Checkout) provider.AuthorizeRequest {
			return provider.AuthorizeRequest{
				PaymentIntentID: "intent-conformance", ProviderPaymentID: checkout.ProviderPaymentID,
				SyntheticToken: checkout.SyntheticToken, AmountMinor: 129900, Currency: "TWD",
				IdempotencyKey: "authorize:intent-conformance:v1",
			}
		},
		CaptureRequest: func(checkout provider.Checkout) provider.CaptureRequest {
			return provider.CaptureRequest{
				PaymentIntentID: "intent-conformance", ProviderPaymentID: checkout.ProviderPaymentID,
				AmountMinor: 129900, Currency: "TWD", IdempotencyKey: "capture:intent-conformance:v1",
			}
		},
		VoidRequest: func(checkout provider.Checkout) provider.VoidRequest {
			return provider.VoidRequest{
				PaymentIntentID: "intent-conformance", ProviderPaymentID: checkout.ProviderPaymentID,
				IdempotencyKey: "void:intent-conformance:v1",
			}
		},
		FullRefundRequest: func(checkout provider.Checkout) provider.RefundRequest {
			return provider.RefundRequest{
				PaymentIntentID: "intent-conformance", ProviderPaymentID: checkout.ProviderPaymentID,
				AmountMinor: 129900, Currency: "TWD", IdempotencyKey: "refund:intent-conformance:full:v1",
			}
		},
		PartialRefundRequest: func(checkout provider.Checkout) provider.RefundRequest {
			return provider.RefundRequest{
				PaymentIntentID: "intent-conformance", ProviderPaymentID: checkout.ProviderPaymentID,
				AmountMinor: 29900, Currency: "TWD", IdempotencyKey: "refund:intent-conformance:partial:v1",
				Metadata: provider.Metadata{"refund_request_id": "request-conformance-partial", "refund_operation_id": "operation-conformance-partial", "refund_idempotency_key": "refund:intent-conformance:partial:v1"},
			}
		},
		StatusAfterCheckout: []provider.Status{provider.StatusAuthorized},
	})
}

func TestProviderEvidenceConformance(t *testing.T) {
	conformance.RunEvidence(t, conformance.EvidenceHarness{
		NewClient: func(t *testing.T) provider.Described {
			t.Helper()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/balance_transactions":
					if r.Method != http.MethodGet || r.URL.Query().Get("expand[]") != "data.source" {
						http.Error(w, "unexpected request", http.StatusBadRequest)
						return
					}
					startingAfter := r.URL.Query().Get("starting_after")
					id, amount, fee, net, hasMore := "txn_contract", 1000, 30, 970, true
					if startingAfter == "txn_contract" {
						id, amount, fee, net, hasMore = "txn_contract_next", -200, 0, -200, false
					} else if startingAfter != "" {
						http.Error(w, "unexpected cursor", http.StatusBadRequest)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"has_more": hasMore,
						"data": []map[string]any{{
							"id": id, "amount": amount, "fee": fee, "net": net,
							"currency": "twd", "type": "charge", "reporting_category": "charge",
							"status": "available", "created": 1786406400, "available_on": 1786492800,
							"source": map[string]any{"id": "ch_contract", "object": "charge", "payment_intent": "pi_contract"},
						}},
					})
				case "/v1/payouts":
					if r.Method != http.MethodGet || r.URL.Query().Get("expand[]") != "data.balance_transaction" {
						http.Error(w, "unexpected request", http.StatusBadRequest)
						return
					}
					startingAfter := r.URL.Query().Get("starting_after")
					id, amount, hasMore := "po_contract", 970, true
					if startingAfter == "po_contract" {
						id, amount, hasMore = "po_contract_next", 700, false
					} else if startingAfter != "" {
						http.Error(w, "unexpected cursor", http.StatusBadRequest)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"has_more": hasMore,
						"data": []map[string]any{{
							"id": id, "amount": amount, "currency": "twd", "status": "paid",
							"automatic": true, "created": 1786406400, "arrival_date": 1786492800,
							"balance_transaction": "txn_payout",
						}},
					})
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			return mustTestClient(t, server.URL)
		},
	})
}

func TestProviderWebhookConformance(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"id":"evt_conformance","type":"payment_intent.amount_capturable_updated","api_version":"2026-07-29.dahlia","livemode":false,"created":1786449600,"data":{"object":{"id":"pi_contract","amount":129900,"amount_received":0,"currency":"twd","status":"requires_capture","created":1786449500}}}`)
	expected := provider.WebhookEvent{ProviderEventID: "evt_conformance", Type: provider.EventAuthorized, ProviderPaymentID: "pi_contract", Status: provider.StatusAuthorized, AmountMinor: 129900, Currency: "TWD"}
	makeVerifier := func(t *testing.T, secrets, ids []string) *stripeprovider.WebhookVerifier {
		t.Helper()
		verifier, err := stripeprovider.NewWebhookVerifier(stripeprovider.WebhookConfig{
			MaxBodyBytes: 16 << 10, Tolerance: 5 * time.Minute, Secrets: secrets, SecretIDs: ids,
			AccountID: "acct_contract", LiveMode: false,
			Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("NewWebhookVerifier: %v", err)
		}
		return verifier
	}
	oldHeaders := provider.WebhookHeaders{Signature: stripeSignature(now.Unix(), payload, "whsec_old_contract")}
	conformance.RunWebhook(t, conformance.WebhookHarness{
		Descriptor: mustTestClient(t, "https://api.stripe.test").Descriptor(),
		Current: func(t *testing.T) conformance.WebhookCase {
			return conformance.WebhookCase{Verifier: makeVerifier(t, []string{"whsec_old_contract"}, []string{"old"}), Headers: oldHeaders, Body: payload, Expected: expected}
		},
		Rotated: func(t *testing.T) conformance.WebhookCase {
			return conformance.WebhookCase{Verifier: makeVerifier(t, []string{"whsec_new_contract", "whsec_old_contract"}, []string{"new", "old"}), Headers: oldHeaders, Body: payload, Expected: expected}
		},
		Retired: func(t *testing.T) conformance.WebhookCase {
			return conformance.WebhookCase{Verifier: makeVerifier(t, []string{"whsec_new_contract"}, []string{"new"}), Headers: oldHeaders, Body: payload, Expected: expected}
		},
	})
}

func TestPayoutReaderNormalizesASettlementPage(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/payouts" || r.URL.Query().Get("expand[]") != "data.balance_transaction" {
			t.Errorf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"has_more": false,
			"data": []map[string]any{{
				"id": "po_contract", "amount": 970, "currency": "twd", "status": "paid", "automatic": true,
				"created": 1786406400, "arrival_date": 1786492800, "balance_transaction": "txn_payout",
			}},
		})
	}))
	defer server.Close()

	page, err := mustTestClient(t, server.URL).ListPayouts(context.Background(), stripeprovider.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("ListPayouts: %v", err)
	}
	if page.HasMore || page.NextStartingAfter != "" || len(page.Items) != 1 {
		t.Fatalf("page = %#v", page)
	}
	if item := page.Items[0]; item.ID != "po_contract" || item.AmountMinor != 970 || item.Currency != "TWD" || item.Status != "paid" || item.BalanceTransactionID != "txn_payout" || !item.Automatic || item.CreatedAt.IsZero() || item.ArrivalAt.IsZero() {
		t.Fatalf("item = %#v", item)
	}
}

func TestGetPaymentStatusRejectsContradictoryExpandedChargeTotals(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		chargeAmount  int64
		refundedMinor int64
		refunded      bool
	}{
		{name: "charge amount differs", chargeAmount: 999},
		{name: "full refund total lacks refunded flag", chargeAmount: 129900, refundedMinor: 129900},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id": "pi_contract", "amount": 129900, "amount_received": 129900,
					"currency": "twd", "status": "succeeded", "created": 1786406400,
					"latest_charge": map[string]any{
						"id": "ch_contract", "amount": test.chargeAmount, "amount_refunded": test.refundedMinor,
						"captured": true, "refunded": test.refunded, "created": 1786406450,
					},
				})
			}))
			defer server.Close()

			_, err := mustTestClient(t, server.URL).GetPaymentStatus(context.Background(), "pi_contract")
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorInconsistentResponse {
				t.Fatalf("error = %#v", err)
			}
		})
	}
}

func TestVerifyWebhookRejectsOversizeStaleAndMalformedPayloads(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	client, err := stripeprovider.New(stripeprovider.Config{
		SecretKey: "sk_test_contract", AccountID: "acct_contract", APIOrigin: "https://api.stripe.test",
		SuccessURL: "https://merchant.test/success", CancelURL: "https://merchant.test/cancel",
		MaxWebhookBodyBytes: 1024, WebhookTolerance: 5 * time.Minute,
		WebhookSecrets: []string{"whsec_contract_secret"}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	validPayload := []byte(`{"id":"evt_contract","type":"payment_intent.amount_capturable_updated","api_version":"2026-07-29.dahlia","livemode":false,"created":1786449600,"data":{"object":{"id":"pi_contract","amount":129900,"amount_received":0,"currency":"twd","status":"requires_capture","created":1786449500}}}`)
	tests := []struct {
		name      string
		payload   []byte
		timestamp int64
		category  provider.ErrorCategory
	}{
		{name: "oversize", payload: []byte(strings.Repeat("x", 1025)), timestamp: now.Unix(), category: provider.ErrorPermanentValidation},
		{name: "stale", payload: validPayload, timestamp: now.Add(-6 * time.Minute).Unix(), category: provider.ErrorAuthentication},
		{name: "malformed signed JSON", payload: []byte("{"), timestamp: now.Unix(), category: provider.ErrorInconsistentResponse},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := client.VerifyWebhook(context.Background(), provider.WebhookHeaders{Signature: stripeSignature(test.timestamp, test.payload, "whsec_contract_secret")}, test.payload)
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Category != test.category {
				t.Fatalf("error = %#v", err)
			}
		})
	}
}

func TestClientDoesNotUseAmbientProxy(t *testing.T) {
	var proxyRequests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyRequests.Add(1)
		http.Error(w, "ambient proxy must not be used", http.StatusBadGateway)
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")

	client, err := stripeprovider.New(stripeprovider.Config{
		SecretKey: "sk_test_contract", AccountID: "acct_contract", APIOrigin: "https://stripe-api.invalid",
		SuccessURL: "https://merchant.test/success", CancelURL: "https://merchant.test/cancel",
		ConnectTimeout: 100 * time.Millisecond, RequestTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = client.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{
		PaymentIntentID: "pay-001", MerchantReference: "booking-001", AmountMinor: 129900,
		Currency: "TWD", IdempotencyKey: "checkout-001",
	})
	if err == nil {
		t.Fatal("CreateCheckout unexpectedly reached an endpoint")
	}
	if proxyRequests.Load() != 0 {
		t.Fatalf("ambient proxy received %d requests", proxyRequests.Load())
	}
}

func TestMutatingRequestTimeoutIsOutcomeUnknown(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	defer server.Close()
	client, err := stripeprovider.New(stripeprovider.Config{
		SecretKey: "sk_test_contract", AccountID: "acct_contract", APIOrigin: server.URL,
		SuccessURL: "https://merchant.test/success", CancelURL: "https://merchant.test/cancel",
		ConnectTimeout: time.Second, RequestTimeout: 25 * time.Millisecond, AllowInsecureForTest: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = client.Capture(context.Background(), provider.CaptureRequest{
		PaymentIntentID: "pay-001", ProviderPaymentID: "pi_contract", AmountMinor: 129900,
		Currency: "TWD", IdempotencyKey: "capture-001",
	})
	<-started
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorTimeoutUnknown || !providerErr.Retryable || !providerErr.Uncertain {
		t.Fatalf("error = %#v", err)
	}
}

func TestPreCanceledMutationIsNotOutcomeUnknownAndDoesNotDispatch(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := mustTestClient(t, server.URL).Capture(ctx, provider.CaptureRequest{
		PaymentIntentID: "pay-001", ProviderPaymentID: "pi_contract", AmountMinor: 129900,
		Currency: "TWD", IdempotencyKey: "capture-001",
	})
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Uncertain {
		t.Fatalf("error = %#v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("requests = %d", requests.Load())
	}
}

func TestCreateCheckoutRejectsReservedStripeMetadataBeforeDispatch(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()

	_, err := mustTestClient(t, server.URL).CreateCheckout(context.Background(), provider.CreateCheckoutRequest{
		PaymentIntentID: "pay-001", MerchantReference: "booking-001", AmountMinor: 129900,
		Currency: "TWD", IdempotencyKey: "checkout-001",
		Metadata: provider.Metadata{"payment_intent_id": "attacker-controlled"},
	})
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorPermanentValidation {
		t.Fatalf("error = %#v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("requests = %d", requests.Load())
	}
}

func TestPartialRefundWebhookRemainsNonAuthoritativeEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	client, err := stripeprovider.New(stripeprovider.Config{
		SecretKey: "sk_test_contract", AccountID: "acct_contract", APIOrigin: "https://api.stripe.test",
		SuccessURL: "https://merchant.test/success", CancelURL: "https://merchant.test/cancel",
		WebhookSecrets: []string{"whsec_contract_secret"}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	payload := []byte(`{"id":"evt_partial_refund","type":"charge.refunded","api_version":"2026-07-29.dahlia","livemode":false,"created":1786449600,"data":{"object":{"id":"ch_contract","amount":129900,"amount_refunded":30000,"captured":true,"refunded":false,"currency":"twd","payment_intent":"pi_contract","created":1786449500}}}`)
	event, err := client.VerifyWebhook(context.Background(), provider.WebhookHeaders{Signature: stripeSignature(now.Unix(), payload, "whsec_contract_secret")}, payload)
	if err != nil {
		t.Fatalf("VerifyWebhook: %v", err)
	}
	if event.Type != provider.EventUnknown || event.OriginalType != "charge.refunded" || event.Status != provider.StatusUnknown || event.ProviderPaymentID != "pi_contract" || event.AmountMinor != 129900 || event.Currency != "TWD" {
		t.Fatalf("event = %#v", event)
	}
}

func stripeSignature(timestamp int64, payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(payload)
	return "t=" + strconv.FormatInt(timestamp, 10) + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func mustTestClient(t *testing.T, origin string) *stripeprovider.Client {
	t.Helper()
	client, err := stripeprovider.New(stripeprovider.Config{
		SecretKey:            "sk_test_contract",
		AccountID:            "acct_contract",
		APIOrigin:            origin,
		SuccessURL:           "https://merchant.test/payments/success",
		CancelURL:            "https://merchant.test/payments/cancel",
		RequestTimeout:       time.Second,
		ConnectTimeout:       time.Second,
		MaxResponseBodyBytes: 16 << 10,
		AllowInsecureForTest: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

func newStripeOperationClient(t *testing.T) provider.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/checkout/sessions":
			if !requireStripeOperationIdempotency(w, r, "checkout:intent-conformance:v1") {
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "cs_contract", "object": "checkout.session", "payment_intent": "pi_contract",
				"url": "https://checkout.stripe.test/c/pay/cs_contract", "amount_total": 129900, "currency": "twd",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/payment_intents/pi_contract":
			writeStripeOperationIntent(w, "requires_capture", 0)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/payment_intents/pi_contract/capture":
			if !requireStripeOperationIdempotency(w, r, "capture:intent-conformance:v1") {
				return
			}
			w.Header().Set("Request-Id", "req_capture_contract")
			writeStripeOperationIntent(w, "succeeded", 129900)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/payment_intents/pi_contract/cancel":
			if !requireStripeOperationIdempotency(w, r, "void:intent-conformance:v1") {
				return
			}
			w.Header().Set("Request-Id", "req_void_contract")
			writeStripeOperationIntent(w, "canceled", 0)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/refunds":
			if err := r.ParseForm(); err != nil {
				http.Error(w, "invalid form", http.StatusBadRequest)
				return
			}
			amount, err := strconv.ParseInt(r.Form.Get("amount"), 10, 64)
			if err != nil || amount <= 0 {
				http.Error(w, "invalid amount", http.StatusBadRequest)
				return
			}
			expectedKey := "refund:intent-conformance:partial:v1"
			if amount == 129900 {
				expectedKey = "refund:intent-conformance:full:v1"
			}
			if !requireStripeOperationIdempotency(w, r, expectedKey) {
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": fmt.Sprintf("re_contract_%d", amount), "object": "refund", "payment_intent": "pi_contract",
				"amount": amount, "currency": "twd", "status": "succeeded",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/refunds":
			if r.URL.Query().Get("payment_intent") != "pi_contract" || r.URL.Query().Get("limit") != "100" {
				http.Error(w, "invalid bounded lookup", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"has_more": false,
				"data": []map[string]any{{
					"id": "re_contract_29900", "object": "refund", "payment_intent": "pi_contract",
					"amount": 29900, "currency": "twd", "status": "succeeded",
					"metadata": map[string]string{
						"refund_request_id":      "request-conformance-partial",
						"refund_operation_id":    "operation-conformance-partial",
						"refund_idempotency_key": "refund:intent-conformance:partial:v1",
					},
				}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client := mustTestClient(t, server.URL)
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func TestRefundLookupSelectsExactIdentityAmongEqualRefundsWithoutMutation(t *testing.T) {
	t.Parallel()
	var getCalls, postCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			postCalls.Add(1)
			http.Error(w, "mutation forbidden", http.StatusMethodNotAllowed)
			return
		}
		getCalls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/v1/refunds" ||
			r.URL.Query().Get("payment_intent") != "pi_shared" || r.URL.Query().Get("limit") != "100" {
			http.Error(w, "unexpected lookup", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"has_more": false,
			"data": []map[string]any{
				{"id": "re_equal_a", "object": "refund", "payment_intent": "pi_shared", "amount": 200, "currency": "twd", "status": "succeeded", "metadata": map[string]string{"refund_request_id": "request-a", "refund_operation_id": "operation-a", "refund_idempotency_key": "refund:operation-a"}},
				{"id": "re_equal_b", "object": "refund", "payment_intent": "pi_shared", "amount": 200, "currency": "twd", "status": "succeeded", "metadata": map[string]string{"refund_request_id": "request-b", "refund_operation_id": "operation-b", "refund_idempotency_key": "refund:operation-b"}},
			},
		})
	}))
	defer server.Close()

	result, err := mustTestClient(t, server.URL).LookupRefund(context.Background(), provider.RefundLookupRequest{
		PaymentIntentID: "intent-shared", ProviderPaymentID: "pi_shared", AmountMinor: 200, Currency: "TWD",
		IdempotencyKey: "refund:operation-b", Limit: 100,
		Metadata: provider.Metadata{"refund_request_id": "request-b", "refund_operation_id": "operation-b", "refund_idempotency_key": "refund:operation-b"},
	})
	if err != nil || !result.Found || !result.Definitive || result.Refund.ProviderOperationID != "re_equal_b" {
		t.Fatalf("lookup = %#v, %v", result, err)
	}
	if getCalls.Load() != 1 || postCalls.Load() != 0 {
		t.Fatalf("GET calls=%d POST calls=%d", getCalls.Load(), postCalls.Load())
	}
}

func TestRefundLookupDoesNotClaimAbsencePastBoundedPage(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"has_more": true,
			"data": []map[string]any{{
				"id": "re_other", "object": "refund", "payment_intent": "pi_shared", "amount": 200,
				"currency": "twd", "status": "succeeded", "metadata": map[string]string{"refund_operation_id": "operation-other"},
			}},
		})
	}))
	defer server.Close()
	result, err := mustTestClient(t, server.URL).LookupRefund(context.Background(), provider.RefundLookupRequest{
		PaymentIntentID: "intent-shared", ProviderPaymentID: "pi_shared", AmountMinor: 200, Currency: "TWD",
		IdempotencyKey: "refund:operation-missing", Limit: 1,
		Metadata: provider.Metadata{"refund_request_id": "request-missing", "refund_operation_id": "operation-missing", "refund_idempotency_key": "refund:operation-missing"},
	})
	if err != nil || result.Found || result.Definitive {
		t.Fatalf("bounded miss = %#v, %v", result, err)
	}
}

func TestRefundLookupDoesNotClaimMatchedRefundDefinitiveWhenMorePagesRemain(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"has_more": true,
			"data": []map[string]any{{
				"id": "re_match", "object": "refund", "payment_intent": "pi_shared", "amount": 200,
				"currency": "twd", "status": "succeeded", "metadata": map[string]string{
					"refund_request_id": "request-match", "refund_operation_id": "operation-match",
					"refund_idempotency_key": "refund:operation-match",
				},
			}},
		})
	}))
	defer server.Close()
	result, err := mustTestClient(t, server.URL).LookupRefund(context.Background(), provider.RefundLookupRequest{
		PaymentIntentID: "intent-shared", ProviderPaymentID: "pi_shared", AmountMinor: 200, Currency: "TWD",
		IdempotencyKey: "refund:operation-match", Limit: 1,
		Metadata: provider.Metadata{"refund_request_id": "request-match", "refund_operation_id": "operation-match", "refund_idempotency_key": "refund:operation-match"},
	})
	if err != nil || !result.Found || result.Definitive || result.Refund.ProviderOperationID != "re_match" {
		t.Fatalf("bounded match = %#v, %v", result, err)
	}
}

func requireStripeOperationIdempotency(w http.ResponseWriter, r *http.Request, expected string) bool {
	if r.Header.Get("Idempotency-Key") == expected {
		return true
	}
	http.Error(w, "invalid idempotency identity", http.StatusBadRequest)
	return false
}

func writeStripeOperationIntent(w http.ResponseWriter, status string, amountReceived int64) {
	intent := map[string]any{
		"id": "pi_contract", "object": "payment_intent", "amount": 129900, "currency": "twd",
		"status": status, "amount_received": amountReceived, "created": 1786406400,
	}
	if amountReceived > 0 {
		intent["latest_charge"] = map[string]any{
			"id": "ch_contract", "object": "charge", "amount": 129900, "amount_refunded": 0,
			"captured": true, "refunded": false, "currency": "twd", "created": 1786406401,
		}
	}
	_ = json.NewEncoder(w).Encode(intent)
}
