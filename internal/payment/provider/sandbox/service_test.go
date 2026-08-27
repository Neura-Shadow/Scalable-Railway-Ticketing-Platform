package sandbox_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/conformance"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/sandbox"
)

func TestProviderOperationConformance(t *testing.T) {
	conformance.RunOperations(t, conformance.OperationHarness{
		NewClient: func(t *testing.T) provider.Client {
			t.Helper()
			return newService(t, nil)
		},
		ValidCreateCheckout: provider.CreateCheckoutRequest{
			PaymentIntentID: "intent-conformance", MerchantReference: "booking-conformance",
			AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "checkout:intent-conformance:v1",
		},
		AuthorizeRequest: func(checkout provider.Checkout) provider.AuthorizeRequest {
			return provider.AuthorizeRequest{
				PaymentIntentID: "intent-conformance", ProviderPaymentID: checkout.ProviderPaymentID,
				SyntheticToken: checkout.SyntheticToken, AmountMinor: 12500, Currency: "TWD",
				IdempotencyKey: "authorize:intent-conformance:v1",
			}
		},
		CaptureRequest: func(checkout provider.Checkout) provider.CaptureRequest {
			return provider.CaptureRequest{
				PaymentIntentID: "intent-conformance", ProviderPaymentID: checkout.ProviderPaymentID,
				AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-conformance:v1",
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
				AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "refund:intent-conformance:full:v1",
			}
		},
		PartialRefundRequest: func(checkout provider.Checkout) provider.RefundRequest {
			return provider.RefundRequest{
				PaymentIntentID: "intent-conformance", ProviderPaymentID: checkout.ProviderPaymentID,
				AmountMinor: 2500, Currency: "TWD", IdempotencyKey: "refund:intent-conformance:partial:v1",
				Metadata: provider.Metadata{"refund_request_id": "request-conformance-partial", "refund_operation_id": "operation-conformance-partial", "refund_idempotency_key": "refund:intent-conformance:partial:v1"},
			}
		},
		StatusAfterCheckout: []provider.Status{provider.StatusCreated},
	})
}

func TestProviderEvidenceConformance(t *testing.T) {
	conformance.RunEvidence(t, conformance.EvidenceHarness{
		NewClient: func(t *testing.T) provider.Described {
			t.Helper()
			return newService(t, nil)
		},
	})
}

func TestProviderWebhookConformance(t *testing.T) {
	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	event := provider.WebhookEvent{
		ProviderEventID: "evt-conformance", Type: provider.EventCaptured,
		ProviderPaymentID: "pay-conformance", Status: provider.StatusCaptured,
		AmountMinor: 12500, Currency: "TWD", OccurredAt: now,
	}
	oldKey := testWebhookKey("conformance-old")
	newKey := testWebhookKey("conformance-new")
	makeCase := func(t *testing.T, verifier *sandbox.Service) conformance.WebhookCase {
		t.Helper()
		signer := newServiceAt(t, now, "old", map[string][]byte{"old": oldKey})
		headers, body, err := signer.SignWebhook(event)
		if err != nil {
			t.Fatalf("SignWebhook: %v", err)
		}
		return conformance.WebhookCase{Verifier: verifier, Headers: headers, Body: body, Expected: event}
	}
	conformance.RunWebhook(t, conformance.WebhookHarness{
		Descriptor: newService(t, nil).Descriptor(),
		Current: func(t *testing.T) conformance.WebhookCase {
			return makeCase(t, newServiceAt(t, now, "old", map[string][]byte{"old": oldKey}))
		},
		Rotated: func(t *testing.T) conformance.WebhookCase {
			return makeCase(t, newServiceAt(t, now, "new", map[string][]byte{"old": oldKey, "new": newKey}))
		},
		Retired: func(t *testing.T) conformance.WebhookCase {
			return makeCase(t, newServiceAt(t, now, "new", map[string][]byte{"new": newKey}))
		},
	})
}

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
	partialResult, err := service.Refund(context.Background(), partialRefund)
	if err != nil || partialResult.Status != provider.StatusRefunded || partialResult.AmountMinor != 100 {
		t.Fatalf("partial refund = %#v, %v", partialResult, err)
	}
	partialStatus, err := service.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if err != nil || partialStatus.Status != provider.StatusUnknown || partialStatus.CapturedMinor != 12500 || partialStatus.RefundedMinor != 100 {
		t.Fatalf("partial refund status = %#v, %v", partialStatus, err)
	}
	if err := provider.EvaluateFinancialObservation(
		provider.FinancialExpectation{AmountMinor: 12500, Currency: "TWD"},
		provider.FinancialObservation{Status: partialStatus.Status, AmountMinor: partialStatus.AmountMinor, Currency: partialStatus.Currency, CapturedMinor: partialStatus.CapturedMinor, RefundedMinor: partialStatus.RefundedMinor},
	); err != nil {
		t.Fatalf("partial refund observation: %v", err)
	}

	refundRequest := provider.RefundRequest{PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID, AmountMinor: 12400, Currency: "TWD", IdempotencyKey: "refund:intent-123:v1"}
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

func TestProviderStateSurvivesServiceRestartAfterCaptureResponseLoss(t *testing.T) {
	t.Parallel()

	statePath := filepath.Join(t.TempDir(), "provider-state.jsonl")
	store, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("new file state store: %v", err)
	}
	faults := sandbox.NewScript()
	service := newServiceWithStore(t, faults, store)
	checkout := createCheckout(t, service)
	authorize(t, service, checkout)
	faults.Push(sandbox.OperationCapture, sandbox.Fault{Kind: sandbox.FaultResponseLoss})
	request := provider.CaptureRequest{
		PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID,
		AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1",
	}
	_, err = service.Capture(context.Background(), request)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || !providerErr.Uncertain {
		t.Fatalf("capture response-loss error = %#v, want uncertain", providerErr)
	}

	reopened, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("reopen file state store: %v", err)
	}
	restarted := newServiceWithStore(t, nil, reopened)
	status, err := restarted.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if err != nil || status.Status != provider.StatusCaptured || status.CapturedMinor != 12500 {
		t.Fatalf("status after provider restart = %#v, %v", status, err)
	}
	replayed, err := restarted.Capture(context.Background(), request)
	if err != nil || replayed.Status != provider.StatusCaptured || replayed.ProviderOperationID == "" {
		t.Fatalf("capture replay after provider restart = %#v, %v", replayed, err)
	}

	reopenedAgain, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("reopen file state store again: %v", err)
	}
	restartedAgain := newServiceWithStore(t, nil, reopenedAgain)
	replayedAgain, err := restartedAgain.Capture(context.Background(), request)
	if err != nil || replayedAgain != replayed {
		t.Fatalf("second restart replay = %#v, %v; want %#v", replayedAgain, err, replayed)
	}
}

func TestProviderStateSurvivesServiceRestartAfterRefundResponseLoss(t *testing.T) {
	t.Parallel()

	statePath := filepath.Join(t.TempDir(), "provider-state.jsonl")
	store, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("new file state store: %v", err)
	}
	faults := sandbox.NewScript()
	service := newServiceWithStore(t, faults, store)
	checkout := createCheckout(t, service)
	authorize(t, service, checkout)
	if _, err = service.Capture(context.Background(), provider.CaptureRequest{
		PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID,
		AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1",
	}); err != nil {
		t.Fatalf("capture: %v", err)
	}
	faults.Push(sandbox.OperationRefund, sandbox.Fault{Kind: sandbox.FaultResponseLoss})
	request := provider.RefundRequest{
		PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID,
		AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "refund:intent-123:v1",
	}
	_, err = service.Refund(context.Background(), request)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || !providerErr.Uncertain {
		t.Fatalf("refund response-loss error = %#v, want uncertain", providerErr)
	}

	reopened, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("reopen file state store: %v", err)
	}
	restarted := newServiceWithStore(t, nil, reopened)
	status, err := restarted.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if err != nil || status.Status != provider.StatusRefunded || status.RefundedMinor != 12500 {
		t.Fatalf("status after refund restart = %#v, %v", status, err)
	}
	replayed, err := restarted.Refund(context.Background(), request)
	if err != nil || replayed.Status != provider.StatusRefunded || replayed.ProviderOperationID == "" {
		t.Fatalf("refund replay after restart = %#v, %v", replayed, err)
	}
}

func TestPartialRefundLookupIsExactReadOnlyAndSurvivesRestart(t *testing.T) {
	t.Parallel()

	statePath := filepath.Join(t.TempDir(), "provider-state.jsonl")
	store, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	faults := sandbox.NewScript()
	service := newServiceWithStore(t, faults, store)
	checkout := createCheckout(t, service)
	authorize(t, service, checkout)
	if _, err := service.Capture(context.Background(), provider.CaptureRequest{
		PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID,
		AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1",
	}); err != nil {
		t.Fatal(err)
	}
	metadata := provider.Metadata{
		"refund_request_id": "request-a", "refund_operation_id": "operation-a",
		"refund_idempotency_key": "refund:operation-a",
	}
	faults.Push(sandbox.OperationRefund, sandbox.Fault{Kind: sandbox.FaultResponseLoss})
	request := provider.RefundRequest{
		PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID,
		AmountMinor: 200, Currency: "TWD", IdempotencyKey: "refund:operation-a", Metadata: metadata,
	}
	if _, err := service.Refund(context.Background(), request); err == nil {
		t.Fatal("response-loss refund unexpectedly returned success")
	}

	reopened, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	restarted := newServiceWithStore(t, nil, reopened)
	lookupRequest := provider.RefundLookupRequest{
		PaymentIntentID: request.PaymentIntentID, ProviderPaymentID: request.ProviderPaymentID,
		AmountMinor: request.AmountMinor, Currency: request.Currency, IdempotencyKey: request.IdempotencyKey,
		Metadata: request.Metadata, Limit: 100,
	}
	found, err := restarted.LookupRefund(context.Background(), lookupRequest)
	if err != nil || !found.Found || !found.Definitive || found.Refund.ProviderOperationID == "" || found.Refund.AmountMinor != 200 {
		t.Fatalf("lookup after restart = %#v, %v", found, err)
	}
	status, err := restarted.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if err != nil || status.Status != provider.StatusUnknown || status.RefundedMinor != 200 {
		t.Fatalf("partial aggregate = %#v, %v", status, err)
	}
	conflicting := lookupRequest
	conflicting.AmountMinor = 201
	if _, err := restarted.LookupRefund(context.Background(), conflicting); err == nil {
		t.Fatal("same idempotency identity with different refund fingerprint was accepted")
	}

	lookupRequest.IdempotencyKey = "refund:operation-b"
	lookupRequest.Metadata = provider.Metadata{
		"refund_request_id": "request-b", "refund_operation_id": "operation-b",
		"refund_idempotency_key": "refund:operation-b",
	}
	absent, err := restarted.LookupRefund(context.Background(), lookupRequest)
	if err != nil || absent.Found || !absent.Definitive {
		t.Fatalf("equal absent refund lookup = %#v, %v", absent, err)
	}
}

func TestProviderStateSurvivesRestartBeforeAuthorizedWebhookDelivery(t *testing.T) {
	t.Parallel()

	statePath := filepath.Join(t.TempDir(), "provider-state.jsonl")
	store, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("new file state store: %v", err)
	}
	service := newServiceWithStore(t, nil, store)
	checkout := createCheckout(t, service)
	authorize(t, service, checkout)

	reopened, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("reopen file state store: %v", err)
	}
	restarted := newServiceWithStore(t, nil, reopened)
	queued := restarted.DrainWebhooks()
	if len(queued) != 2 {
		t.Fatalf("queued webhooks after restart = %d, want checkout and authorization", len(queued))
	}
	foundAuthorized := false
	for _, item := range queued {
		event, verifyErr := restarted.VerifyWebhook(context.Background(), item.Headers, item.Body)
		if verifyErr != nil {
			t.Fatalf("verify restored webhook: %v", verifyErr)
		}
		if event.Type == provider.EventAuthorized && event.ProviderPaymentID == checkout.ProviderPaymentID {
			foundAuthorized = true
		}
	}
	if !foundAuthorized {
		t.Fatal("authorized webhook was not restored after provider restart")
	}
}

func TestRestoredWebhookUsesCurrentActiveSigningKey(t *testing.T) {
	t.Parallel()

	statePath := filepath.Join(t.TempDir(), "provider-state.jsonl")
	store, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("new file state store: %v", err)
	}
	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	oldKey := testWebhookKey("old")
	newKey := testWebhookKey("new")
	service, err := sandbox.New(sandbox.Config{
		Environment: "test", Now: func() time.Time { return now },
		WebhookKeys: map[string][]byte{"old": oldKey}, IssueKeyID: "old", StateStore: store,
	})
	if err != nil {
		t.Fatalf("new old-key sandbox: %v", err)
	}
	createCheckout(t, service)

	reopened, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("reopen file state store: %v", err)
	}
	restarted, err := sandbox.New(sandbox.Config{
		Environment: "test", Now: func() time.Time { return now },
		WebhookKeys: map[string][]byte{"old": oldKey, "new": newKey}, IssueKeyID: "new", StateStore: reopened,
	})
	if err != nil {
		t.Fatalf("new rotated-key sandbox: %v", err)
	}
	queued := restarted.DrainWebhooks()
	if len(queued) != 1 || queued[0].Headers.KeyID != "new" {
		t.Fatalf("restored webhook headers = %#v, want current key", queued)
	}
	if _, err = restarted.VerifyWebhook(context.Background(), queued[0].Headers, queued[0].Body); err != nil {
		t.Fatalf("verify re-signed webhook: %v", err)
	}
}

func TestAdvancedDelayedWebhookRemainsDeliverableAfterRestart(t *testing.T) {
	t.Parallel()

	statePath := filepath.Join(t.TempDir(), "provider-state.jsonl")
	store, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("new file state store: %v", err)
	}
	faults := sandbox.NewScript()
	service := newServiceWithStore(t, faults, store)
	faults.Push(sandbox.OperationCreateCheckout, sandbox.Fault{Kind: sandbox.FaultDelayedWebhook, DelaySteps: 2})
	createCheckout(t, service)
	if webhooks := service.DrainWebhooks(); len(webhooks) != 0 {
		t.Fatalf("delayed webhook delivered before advance: %#v", webhooks)
	}
	if err = service.Advance(2); err != nil {
		t.Fatalf("advance webhook clock: %v", err)
	}

	reopened, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("reopen file state store: %v", err)
	}
	restarted := newServiceWithStore(t, nil, reopened)
	if webhooks := restarted.DrainWebhooks(); len(webhooks) != 1 {
		t.Fatalf("delayed webhooks after restart = %#v, want one", webhooks)
	}
}

func TestProviderMutationRollsBackWhenDurableStateCannotCommit(t *testing.T) {
	t.Parallel()

	store := &failingStateStore{}
	service := newServiceWithStore(t, nil, store)
	checkout := createCheckout(t, service)
	authorize(t, service, checkout)
	store.fail = true
	request := provider.CaptureRequest{
		PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID,
		AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1",
	}
	_, err := service.Capture(context.Background(), request)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorUnavailable || !providerErr.Retryable {
		t.Fatalf("capture with failed durable state = %#v, want retryable unavailable", providerErr)
	}
	if service.Ready() {
		t.Fatal("sandbox remained ready after durable state failure")
	}
	status, err := service.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if err != nil || status.Status != provider.StatusAuthorized || status.CapturedMinor != 0 {
		t.Fatalf("status after failed durable commit = %#v, %v; want authorized and uncaptured", status, err)
	}
}

func TestProviderStateFileRejectsParseableTamperingAndStoresHashedKeys(t *testing.T) {
	t.Parallel()

	statePath := filepath.Join(t.TempDir(), "provider-state.jsonl")
	store, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("new file state store: %v", err)
	}
	service := newServiceWithStore(t, nil, store)
	checkout := createCheckout(t, service)
	authorize(t, service, checkout)
	request := provider.CaptureRequest{
		PaymentIntentID: "intent-123", ProviderPaymentID: checkout.ProviderPaymentID,
		AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "capture:intent-123:v1",
	}
	if _, err := service.Capture(context.Background(), request); err != nil {
		t.Fatalf("capture: %v", err)
	}
	contents, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read provider state: %v", err)
	}
	for _, rawKey := range [][]byte{[]byte("checkout:intent-123:v1"), []byte("authorize:intent-123:v1"), []byte("capture:intent-123:v1")} {
		if bytes.Contains(contents, rawKey) {
			t.Fatalf("provider state contains raw idempotency key %q", rawKey)
		}
	}
	tampered := bytes.ReplaceAll(contents, []byte(`"amount_minor":12500`), []byte(`"amount_minor":12501`))
	if bytes.Equal(tampered, contents) {
		t.Fatal("test failed to produce parseable state tampering")
	}
	if err := os.WriteFile(statePath, tampered, 0o600); err != nil {
		t.Fatalf("write tampered provider state: %v", err)
	}
	reopened, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("reopen file state store: %v", err)
	}
	if _, err := sandbox.New(sandbox.Config{
		Environment: "test", Now: func() time.Time { return time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC) },
		WebhookKeys: map[string][]byte{"test-key": testWebhookKey("default")}, IssueKeyID: "test-key", StateStore: reopened,
	}); err == nil {
		t.Fatal("parseable tampered provider state unexpectedly loaded")
	}
}

func TestProviderStateFileKeepsOneBoundedSnapshot(t *testing.T) {
	t.Parallel()

	const stateLimit = 128 << 10
	statePath := filepath.Join(t.TempDir(), "provider-state.jsonl")
	store, err := sandbox.NewFileStateStore(statePath, stateLimit)
	if err != nil {
		t.Fatalf("new file state store: %v", err)
	}
	service := newServiceWithStore(t, nil, store)
	var last provider.Checkout
	for index := 0; index < 128; index++ {
		intentID := fmt.Sprintf("intent-bounded-%03d", index)
		last, err = service.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{
			PaymentIntentID: intentID, MerchantReference: intentID,
			AmountMinor: 12500, Currency: "TWD", IdempotencyKey: "checkout:" + intentID + ":v1",
		})
		if err != nil {
			t.Fatalf("create checkout %d: %v", index, err)
		}
	}
	info, err := os.Stat(statePath)
	if err != nil {
		t.Fatalf("stat provider state: %v", err)
	}
	if info.Size() <= 0 || info.Size() > stateLimit {
		t.Fatalf("provider state size = %d, want 1..%d", info.Size(), stateLimit)
	}
	reopened, err := sandbox.NewFileStateStore(statePath, stateLimit)
	if err != nil {
		t.Fatalf("reopen file state store: %v", err)
	}
	restarted := newServiceWithStore(t, nil, reopened)
	status, err := restarted.GetPaymentStatus(context.Background(), last.ProviderPaymentID)
	if err != nil || status.Status != provider.StatusCreated {
		t.Fatalf("last payment after bounded restart = %#v, %v", status, err)
	}
}

func TestProviderStateFileIgnoresOnlyAnIncompleteTrailingWrite(t *testing.T) {
	t.Parallel()

	statePath := filepath.Join(t.TempDir(), "provider-state.jsonl")
	store, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("new file state store: %v", err)
	}
	service := newServiceWithStore(t, nil, store)
	checkout := createCheckout(t, service)
	file, err := os.OpenFile(statePath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open provider state: %v", err)
	}
	if _, err = file.WriteString(`{"version":1,"state":`); err != nil {
		_ = file.Close()
		t.Fatalf("append incomplete provider state: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close incomplete provider state: %v", err)
	}
	reopened, err := sandbox.NewFileStateStore(statePath, 1<<20)
	if err != nil {
		t.Fatalf("reopen file state store: %v", err)
	}
	restarted := newServiceWithStore(t, nil, reopened)
	status, err := restarted.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
	if err != nil || status.Status != provider.StatusCreated {
		t.Fatalf("payment after incomplete trailing write = %#v, %v", status, err)
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
	if err := service.Advance(2); err != nil {
		t.Fatalf("advance webhook clock: %v", err)
	}
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
	return newServiceWithStore(t, faults, nil)
}

func newServiceWithStore(t *testing.T, faults sandbox.FaultPlan, store sandbox.StateStore) *sandbox.Service {
	t.Helper()
	service, err := sandbox.New(sandbox.Config{
		Environment: "test",
		Now: func() time.Time {
			return time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
		},
		WebhookKeys: map[string][]byte{"test-key": testWebhookKey("default")},
		IssueKeyID:  "test-key",
		Faults:      faults,
		StateStore:  store,
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

type failingStateStore struct {
	state []byte
	fail  bool
}

func (s *failingStateStore) Load() ([]byte, error) {
	return append([]byte(nil), s.state...), nil
}

func (s *failingStateStore) Save(state []byte) error {
	if s.fail {
		return errors.New("injected durable state failure")
	}
	s.state = append(s.state[:0], state...)
	return nil
}
