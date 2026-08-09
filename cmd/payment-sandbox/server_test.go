package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/sandbox"
)

func TestHTTPCheckoutUsesBoundedStrictSyntheticContract(t *testing.T) {
	t.Parallel()

	service := newHTTPService(t, nil)
	handler, err := newHandler(service, handlerConfig{maxBodyBytes: 4096})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	valid := `{"payment_intent_id":"intent-123","merchant_reference":"intent-123","amount_minor":12500,"currency":"TWD","idempotency_key":"checkout:intent-123:v1"}`
	response := serveJSON(handler, http.MethodPost, "/v1/checkouts", valid, "")
	if response.Code != http.StatusCreated {
		t.Fatalf("valid checkout status = %d, body = %s", response.Code, response.Body.String())
	}
	var checkout provider.Checkout
	if err := json.Unmarshal(response.Body.Bytes(), &checkout); err != nil || !strings.HasPrefix(checkout.SyntheticToken, "tok_sandbox_") {
		t.Fatalf("checkout response = %#v, %v", checkout, err)
	}
	hosted := serveJSON(handler, http.MethodPost, "/hosted/checkouts/"+checkout.ProviderPaymentID+"/authorize", ``, "")
	if hosted.Code != http.StatusAccepted || strings.Contains(hosted.Body.String(), "tok_sandbox_") {
		t.Fatalf("hosted checkout status = %d, body = %s", hosted.Code, hosted.Body.String())
	}
	status := serveJSON(handler, http.MethodGet, "/v1/payments/"+checkout.ProviderPaymentID, ``, "")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"status":"authorized"`) {
		t.Fatalf("hosted checkout provider status = %d, body = %s", status.Code, status.Body.String())
	}

	forbiddenField := "ca" + "rd_number"
	for name, body := range map[string]string{
		"forbidden payment field": `{"payment_intent_id":"intent-123","amount_minor":12500,"currency":"TWD","idempotency_key":"key","` + forbiddenField + `":"synthetic-rejected"}`,
		"oversized body":          `{"payment_intent_id":"` + strings.Repeat("x", 5000) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			response := serveJSON(handler, http.MethodPost, "/v1/checkouts", body, "")
			if response.Code != http.StatusBadRequest && response.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHTTPFaultControlIsExplicitAuthenticatedAndDeterministic(t *testing.T) {
	t.Parallel()

	faults := sandbox.NewScript()
	service := newHTTPService(t, faults)
	disabled, err := newHandler(service, handlerConfig{maxBodyBytes: 4096})
	if err != nil {
		t.Fatalf("new disabled handler: %v", err)
	}
	if response := serveJSON(disabled, http.MethodPost, "/_sandbox/faults", `{"operation":"capture","kind":"rate_limited"}`, "control-token"); response.Code != http.StatusNotFound {
		t.Fatalf("disabled fault endpoint status = %d", response.Code)
	}

	enabled, err := newHandler(service, handlerConfig{maxBodyBytes: 4096, faultControlEnabled: true, faultControlToken: "synthetic-control-token", faults: faults})
	if err != nil {
		t.Fatalf("new enabled handler: %v", err)
	}
	if response := serveJSON(enabled, http.MethodPost, "/_sandbox/faults", `{"operation":"capture","kind":"rate_limited"}`, "wrong"); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized fault endpoint status = %d", response.Code)
	}
	if response := serveJSON(enabled, http.MethodPost, "/_sandbox/faults", `{"operation":"capture","kind":"rate_limited"}`, "synthetic-control-token"); response.Code != http.StatusNoContent {
		t.Fatalf("fault injection status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestHTTPResponseFaultsExerciseBoundedAdapterFailures(t *testing.T) {
	t.Parallel()

	const bodyLimit = int64(4096)
	tests := []struct {
		name       string
		fault      sandbox.FaultKind
		status     int
		assertBody func(*testing.T, string)
	}{
		{name: "provider 500", fault: sandbox.FaultProviderError, status: http.StatusInternalServerError},
		{name: "invalid response", fault: sandbox.FaultInvalidResponse, status: http.StatusOK, assertBody: func(t *testing.T, body string) {
			t.Helper()
			if json.Valid([]byte(body)) {
				t.Fatalf("invalid-response fault emitted valid JSON: %q", body)
			}
		}},
		{name: "oversized response", fault: sandbox.FaultOversizedResponse, status: http.StatusOK, assertBody: func(t *testing.T, body string) {
			t.Helper()
			if int64(len(body)) <= bodyLimit || int64(len(body)) > bodyLimit+64 {
				t.Fatalf("oversized-response bytes = %d, want bounded value just over %d", len(body), bodyLimit)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			faults := sandbox.NewScript()
			faults.Push(sandbox.OperationCreateCheckout, sandbox.Fault{Kind: test.fault})
			service := newHTTPService(t, faults)
			handler, err := newHandler(service, handlerConfig{maxBodyBytes: bodyLimit})
			if err != nil {
				t.Fatalf("new handler: %v", err)
			}
			body := `{"payment_intent_id":"intent-fault","merchant_reference":"intent-fault","amount_minor":100,"currency":"TWD","idempotency_key":"checkout:intent-fault:v1"}`
			response := serveJSON(handler, http.MethodPost, "/v1/checkouts", body, "")
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.status, response.Body.String())
			}
			if test.assertBody != nil {
				test.assertBody(t, response.Body.String())
			}
		})
	}
}

func TestHTTPResponseLossCommitsOnceAndStatusQueryRecovers(t *testing.T) {
	t.Parallel()

	faults := sandbox.NewScript()
	service := newHTTPService(t, faults)
	handler, err := newHandler(service, handlerConfig{maxBodyBytes: 4096})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &http.Client{Timeout: 2 * time.Second}

	checkoutBody := `{"payment_intent_id":"intent-loss","merchant_reference":"intent-loss","amount_minor":100,"currency":"TWD","idempotency_key":"checkout:intent-loss:v1"}`
	var checkout provider.Checkout
	if status := doHTTPJSON(t, client, http.MethodPost, server.URL+"/v1/checkouts", checkoutBody, &checkout); status != http.StatusCreated {
		t.Fatalf("checkout status = %d", status)
	}
	authorizeBody := `{"payment_intent_id":"intent-loss","synthetic_token":"` + checkout.SyntheticToken + `","amount_minor":100,"currency":"TWD","idempotency_key":"authorize:intent-loss:v1"}`
	if status := doHTTPJSON(t, client, http.MethodPost, server.URL+"/v1/payments/"+checkout.ProviderPaymentID+"/authorize", authorizeBody, nil); status != http.StatusOK {
		t.Fatalf("authorize status = %d", status)
	}

	faults.Push(sandbox.OperationCapture, sandbox.Fault{Kind: sandbox.FaultResponseLoss})
	captureBody := `{"payment_intent_id":"intent-loss","amount_minor":100,"currency":"TWD","idempotency_key":"capture:intent-loss:v1"}`
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/payments/"+checkout.ProviderPaymentID+"/capture", strings.NewReader(captureBody))
	if err != nil {
		t.Fatalf("new capture request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("response-loss fault unexpectedly returned an HTTP response")
	}

	var payment provider.Payment
	if status := doHTTPJSON(t, client, http.MethodGet, server.URL+"/v1/payments/"+checkout.ProviderPaymentID, "", &payment); status != http.StatusOK || payment.Status != provider.StatusCaptured || payment.CapturedMinor != 100 {
		t.Fatalf("status after response loss = %d, %#v", status, payment)
	}
	var replay provider.OperationResult
	if status := doHTTPJSON(t, client, http.MethodPost, server.URL+"/v1/payments/"+checkout.ProviderPaymentID+"/capture", captureBody, &replay); status != http.StatusOK || replay.Status != provider.StatusCaptured {
		t.Fatalf("capture replay = %d, %#v", status, replay)
	}
}

func TestConfigurationRequiresWebhookSecretAndRejectsProductionSandbox(t *testing.T) {
	t.Parallel()

	_, err := loadConfig(func(string) string { return "" })
	if err == nil {
		t.Fatal("missing webhook keyring unexpectedly accepted")
	}
	values := map[string]string{
		"PAYMENT_SANDBOX_ENVIRONMENT":          "production",
		"PAYMENT_SANDBOX_WEBHOOK_KEYRING":      "key=" + base64.StdEncoding.EncodeToString(commandTestKey("configuration")),
		"PAYMENT_SANDBOX_WEBHOOK_ISSUE_KEY_ID": "key",
		"PAYMENT_SANDBOX_STATE_PATH":           filepath.Join(t.TempDir(), "provider-state.jsonl"),
	}
	config, err := loadConfig(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("load production config: %v", err)
	}
	_, err = sandbox.New(config.sandbox)
	if err == nil {
		t.Fatal("production sandbox unexpectedly accepted")
	}
}

func newHTTPService(t *testing.T, faults sandbox.FaultPlan) *sandbox.Service {
	t.Helper()
	service, err := sandbox.New(sandbox.Config{Environment: "test", Now: func() time.Time { return time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC) }, WebhookKeys: map[string][]byte{"key": commandTestKey("service")}, IssueKeyID: "key", Faults: faults})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return service
}

func commandTestKey(label string) []byte {
	sum := sha256.Sum256([]byte("payment-sandbox-command-test:" + label))
	return append([]byte(nil), sum[:]...)
}

func serveJSON(handler http.Handler, method, path, body, controlToken string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if controlToken != "" {
		request.Header.Set("X-Sandbox-Control-Token", controlToken)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func doHTTPJSON(t *testing.T, client *http.Client, method, url, body string, destination any) int {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if destination != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
		if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
	return response.StatusCode
}
