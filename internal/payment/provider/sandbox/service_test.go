package sandbox_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/sandbox"
)

func TestCreateCheckoutReplaysSameResultAndRejectsFingerprintConflict(t *testing.T) {
	t.Parallel()

	service := newService(t, nil)
	request := provider.CreateCheckoutRequest{
		PaymentIntentID:   "intent-123",
		MerchantReference: "intent-123",
		AmountMinor:       12500,
		Currency:          "TWD",
		IdempotencyKey:    "checkout:intent-123:v1",
	}

	first, err := service.CreateCheckout(context.Background(), request)
	if err != nil {
		t.Fatalf("create checkout: %v", err)
	}
	replayed, err := service.CreateCheckout(context.Background(), request)
	if err != nil {
		t.Fatalf("replay checkout: %v", err)
	}
	if first != replayed {
		t.Fatalf("replayed checkout = %#v, want %#v", replayed, first)
	}

	request.AmountMinor++
	_, err = service.CreateCheckout(context.Background(), request)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorConflict {
		t.Fatalf("conflicting replay error = %v, want provider conflict", err)
	}
}

func TestHostedCheckoutAuthorizesWithoutExposingSyntheticToken(t *testing.T) {
	t.Parallel()

	service := newService(t, nil)
	checkout := createCheckout(t, service)
	first, err := service.CompleteHostedCheckout(context.Background(), checkout.HostedReference)
	if err != nil || first.Status != provider.StatusAuthorized {
		t.Fatalf("complete hosted checkout = %#v, %v", first, err)
	}
	replayed, err := service.CompleteHostedCheckout(context.Background(), checkout.HostedReference)
	if err != nil || replayed != first {
		t.Fatalf("replay hosted checkout = %#v, %v; want %#v", replayed, err, first)
	}
	if _, err := service.CompleteHostedCheckout(context.Background(), "sandbox-checkout:missing"); err == nil {
		t.Fatal("missing hosted checkout unexpectedly succeeded")
	}
}

func TestAuthorizeCaptureAndFullRefundAreIdempotent(t *testing.T) {
	t.Parallel()

	service := newService(t, nil)
	checkout := createCheckout(t, service)
	authorization := authorize(t, service, checkout)
	if authorization.Status != provider.StatusAuthorized {
		t.Fatalf("authorization status = %q, want authorized", authorization.Status)
	}
	captureRequest := provider.CaptureRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1"}
	firstCapture, err := service.Capture(context.Background(), captureRequest)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	secondCapture, err := service.Capture(context.Background(), captureRequest)
	if err != nil || secondCapture != firstCapture {
		t.Fatalf("duplicate capture = %#v, %v; want %#v", secondCapture, err, firstCapture)
	}

	partialRefund := provider.RefundRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, AmountMinor: 100, Currency: "TWD", IdempotencyKey: "refund:intent-123:partial"}
	_, err = service.Refund(context.Background(), partialRefund)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorConflict {
		t.Fatalf("partial refund error = %v, want conflict", err)
	}

	refundRequest := provider.RefundRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "refund:intent-123:v1"}
	firstRefund, err := service.Refund(context.Background(), refundRequest)
	if err != nil {
		t.Fatalf("refund: %v", err)
	}
	secondRefund, err := service.Refund(context.Background(), refundRequest)
	if err != nil || secondRefund != firstRefund {
		t.Fatalf("duplicate refund = %#v, %v; want %#v", secondRefund, err, firstRefund)
	}
	status, err := service.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if err != nil {
		t.Fatalf("get refunded payment: %v", err)
	}
	if status.Status != provider.StatusRefunded || status.CapturedMinor != 12500 || status.RefundedMinor != 12500 {
		t.Fatalf("refunded payment = %#v", status)
	}
}

func TestUnknownCaptureOutcomeIsObservableAndReplayDoesNotCaptureTwice(t *testing.T) {
	t.Parallel()

	faults := sandbox.NewScript()
	service := newService(t, faults)
	checkout := createCheckout(t, service)
	authorize(t, service, checkout)
	faults.Push(sandbox.OperationCapture, sandbox.Fault{Kind: sandbox.FaultResponseLoss})
	request := provider.CaptureRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1"}
	_, err := service.Capture(context.Background(), request)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || !providerErr.Uncertain || providerErr.Retryable {
		t.Fatalf("capture response-loss error = %#v, want uncertain non-retryable", providerErr)
	}
	status, err := service.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if err != nil || status.Status != provider.StatusCaptured || status.CapturedMinor != 12500 {
		t.Fatalf("status after response loss = %#v, %v", status, err)
	}
	replayed, err := service.Capture(context.Background(), request)
	if err != nil || replayed.Status != provider.StatusCaptured {
		t.Fatalf("capture replay = %#v, %v", replayed, err)
	}
}

func TestTimeoutBeforeCommitCanRetryWithSameIdentity(t *testing.T) {
	t.Parallel()

	faults := sandbox.NewScript()
	service := newService(t, faults)
	checkout := createCheckout(t, service)
	authorize(t, service, checkout)
	faults.Push(sandbox.OperationCapture, sandbox.Fault{Kind: sandbox.FaultTimeoutBeforeCommit})
	request := provider.CaptureRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1"}
	_, err := service.Capture(context.Background(), request)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || !providerErr.Retryable || providerErr.Uncertain {
		t.Fatalf("timeout-before error = %#v, want retryable known outcome", providerErr)
	}
	status, _ := service.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if status.Status != provider.StatusAuthorized || status.CapturedMinor != 0 {
		t.Fatalf("status after pre-commit timeout = %#v", status)
	}
	if _, err := service.Capture(context.Background(), request); err != nil {
		t.Fatalf("retry capture: %v", err)
	}
}

func TestTimeoutAfterCommitRequiresStatusQueryAndReplaysCommittedResult(t *testing.T) {
	t.Parallel()

	faults := sandbox.NewScript()
	service := newService(t, faults)
	checkout := createCheckout(t, service)
	authorize(t, service, checkout)
	faults.Push(sandbox.OperationCapture, sandbox.Fault{Kind: sandbox.FaultTimeoutAfterCommit})
	request := provider.CaptureRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1"}
	_, err := service.Capture(context.Background(), request)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorTimeoutUnknown || !providerErr.Uncertain || providerErr.Retryable {
		t.Fatalf("timeout-after error = %#v", providerErr)
	}
	status, err := service.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if err != nil || status.Status != provider.StatusCaptured {
		t.Fatalf("status after timeout = %#v, %v", status, err)
	}
	result, err := service.Capture(context.Background(), request)
	if err != nil || result.Status != provider.StatusCaptured {
		t.Fatalf("replay after query = %#v, %v", result, err)
	}
}

func TestBoundedFaultCategoriesDoNotCommit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fault     sandbox.FaultKind
		category  provider.ErrorCategory
		retryable bool
	}{
		{name: "rate limited", fault: sandbox.FaultRateLimited, category: provider.ErrorRateLimited, retryable: true},
		{name: "provider error", fault: sandbox.FaultProviderError, category: provider.ErrorTransport, retryable: true},
		{name: "outage", fault: sandbox.FaultOutage, category: provider.ErrorUnavailable, retryable: true},
		{name: "invalid response", fault: sandbox.FaultInvalidResponse, category: provider.ErrorInconsistentResponse},
		{name: "oversized response", fault: sandbox.FaultOversizedResponse, category: provider.ErrorInconsistentResponse},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			faults := sandbox.NewScript()
			faults.Push(sandbox.OperationCreateCheckout, sandbox.Fault{Kind: test.fault})
			service := newService(t, faults)
			_, err := service.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{PaymentIntentID: "intent-fault", MerchantReference: "intent-fault", AmountMinor: 100, Currency: "TWD", IdempotencyKey: "checkout:intent-fault:v1"})
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Category != test.category || providerErr.Retryable != test.retryable || providerErr.Uncertain {
				t.Fatalf("fault error = %#v", providerErr)
			}
			checkout, err := service.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{PaymentIntentID: "intent-fault", MerchantReference: "intent-fault", AmountMinor: 100, Currency: "TWD", IdempotencyKey: "checkout:intent-fault:v1"})
			if err != nil || checkout.ProviderPaymentID != "pay_sandbox_000000000001" {
				t.Fatalf("retry after non-commit = %#v, %v", checkout, err)
			}
		})
	}
}

func TestRefundFailuresDoNotChangeCapturedState(t *testing.T) {
	t.Parallel()

	for _, fault := range []sandbox.FaultKind{sandbox.FaultRefundTransient, sandbox.FaultRefundPermanent} {
		t.Run(string(fault), func(t *testing.T) {
			t.Parallel()
			faults := sandbox.NewScript()
			service := newService(t, faults)
			checkout := createCheckout(t, service)
			authorize(t, service, checkout)
			_, err := service.Capture(context.Background(), provider.CaptureRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1"})
			if err != nil {
				t.Fatalf("capture: %v", err)
			}
			faults.Push(sandbox.OperationRefund, sandbox.Fault{Kind: fault})
			_, err = service.Refund(context.Background(), provider.RefundRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "refund:intent-123:v1"})
			if err == nil {
				t.Fatal("refund fault unexpectedly succeeded")
			}
			status, statusErr := service.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
			if statusErr != nil || status.Status != provider.StatusCaptured || status.RefundedMinor != 0 {
				t.Fatalf("status after refund failure = %#v, %v", status, statusErr)
			}
		})
	}
}

func TestWebhookVerificationEnforcesReplayWindowAndKeyRotation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	oldService := newServiceAt(t, now, "old", map[string][]byte{
		"old": testWebhookKey("old"),
		"new": testWebhookKey("new"),
	})
	event := provider.WebhookEvent{ProviderEventID: "evt-1", Type: provider.EventCaptured, ProviderPaymentID: "pay-1", Status: provider.StatusCaptured, AmountMinor: 12500, Currency: "TWD", OccurredAt: now}
	headers, body, err := oldService.SignWebhook(event)
	if err != nil {
		t.Fatalf("sign old-key webhook: %v", err)
	}
	rotated := newServiceAt(t, now, "new", map[string][]byte{
		"old": testWebhookKey("old"),
		"new": testWebhookKey("new"),
	})
	verified, err := rotated.VerifyWebhook(context.Background(), headers, body)
	if err != nil || verified.ProviderEventID != event.ProviderEventID {
		t.Fatalf("verify retained old key = %#v, %v", verified, err)
	}

	tampered := append([]byte(nil), body...)
	tampered[len(tampered)-2] ^= 1
	_, err = rotated.VerifyWebhook(context.Background(), headers, tampered)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorAuthentication {
		t.Fatalf("tampered webhook error = %v, want authentication", err)
	}

	replayVerifier := newServiceAt(t, now.Add(6*time.Minute), "new", map[string][]byte{
		"old": testWebhookKey("old"),
		"new": testWebhookKey("new"),
	})
	_, err = replayVerifier.VerifyWebhook(context.Background(), headers, body)
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorAuthentication {
		t.Fatalf("replayed webhook error = %v, want authentication", err)
	}
}

func TestWebhookFaultsAreDeterministicWithoutSleeping(t *testing.T) {
	t.Parallel()

	faults := sandbox.NewScript()
	service := newService(t, faults)
	faults.Push(sandbox.OperationCreateCheckout, sandbox.Fault{Kind: sandbox.FaultDuplicateWebhook})
	createCheckout(t, service)
	if webhooks := service.DrainWebhooks(); len(webhooks) != 2 || string(webhooks[0].Body) != string(webhooks[1].Body) {
		t.Fatalf("duplicate webhooks = %#v", webhooks)
	}

	service = newService(t, faults)
	faults.Push(sandbox.OperationCreateCheckout, sandbox.Fault{Kind: sandbox.FaultDelayedWebhook, DelaySteps: 2})
	createCheckout(t, service)
	if webhooks := service.DrainWebhooks(); len(webhooks) != 0 {
		t.Fatalf("delayed webhook delivered early: %#v", webhooks)
	}
	service.Advance(2)
	if webhooks := service.DrainWebhooks(); len(webhooks) != 1 {
		t.Fatalf("delayed webhooks after advance = %#v", webhooks)
	}

	service = newService(t, faults)
	checkout := createCheckout(t, service)
	faults.Push(sandbox.OperationAuthorize, sandbox.Fault{Kind: sandbox.FaultOutOfOrderWebhook})
	authorize(t, service, checkout)
	webhooks := service.DrainWebhooks()
	if len(webhooks) != 2 {
		t.Fatalf("out-of-order webhook count = %d", len(webhooks))
	}
	first, err := service.VerifyWebhook(context.Background(), webhooks[0].Headers, webhooks[0].Body)
	if err != nil || first.Type != provider.EventAuthorized {
		t.Fatalf("first out-of-order event = %#v, %v", first, err)
	}
}

func TestSandboxRejectsProductionAndSensitiveMetadata(t *testing.T) {
	t.Parallel()

	_, err := sandbox.New(sandbox.Config{Environment: "production", WebhookKeys: map[string][]byte{"key": testWebhookKey("production-rejection")}, IssueKeyID: "key"})
	if err == nil {
		t.Fatal("production sandbox unexpectedly enabled")
	}
	service := newService(t, nil)
	_, err = service.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{PaymentIntentID: "intent-123", MerchantReference: "intent-123", AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "checkout:intent-123:v1", Metadata: provider.Metadata{"ca" + "rd_number": "not-accepted"}})
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorPermanentValidation {
		t.Fatalf("sensitive metadata error = %v, want permanent validation", err)
	}
}

func createCheckout(t *testing.T, service *sandbox.Service) provider.Checkout {
	t.Helper()
	checkout, err := service.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{PaymentIntentID: "intent-123", MerchantReference: "intent-123", AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "checkout:intent-123:v1"})
	if err != nil {
		t.Fatalf("create checkout: %v", err)
	}
	return checkout
}

func authorize(t *testing.T, service *sandbox.Service, checkout provider.Checkout) provider.OperationResult {
	t.Helper()
	result, err := service.Authorize(context.Background(), provider.AuthorizeRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, SyntheticToken: checkout.SyntheticToken, AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "authorize:intent-123:v1"})
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	return result
}

func newService(t *testing.T, faults sandbox.FaultPlan) *sandbox.Service {
	t.Helper()
	service, err := sandbox.New(sandbox.Config{
		Environment: "test",
		Now: func() time.Time {
			return time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
		},
		WebhookKeys: map[string][]byte{"test-key": testWebhookKey("default")},
		IssueKeyID:  "test-key",
		Faults:      faults,
	})
	if err != nil {
		t.Fatalf("new sandbox: %v", err)
	}
	return service
}

func newServiceAt(t *testing.T, now time.Time, issueKeyID string, keys map[string][]byte) *sandbox.Service {
	t.Helper()
	service, err := sandbox.New(sandbox.Config{Environment: "test", Now: func() time.Time { return now }, WebhookKeys: keys, IssueKeyID: issueKeyID})
	if err != nil {
		t.Fatalf("new sandbox: %v", err)
	}
	return service
}

func testWebhookKey(label string) []byte {
	sum := sha256.Sum256([]byte("payment-sandbox-test:" + label))
	return append([]byte(nil), sum[:]...)
}
