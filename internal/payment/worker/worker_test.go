package worker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/httpclient"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/sandbox"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
)

var fixedNow = time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC)

func TestRunOnceReportsBoundedFailureSummaryWithoutRawStoreError(t *testing.T) {
	t.Parallel()
	const sensitive = "postgres://user:secret@example synthetic-password-never-log"
	store := &storeFake{claimActionErr: errors.Join(ErrStoreUnavailable, errors.New(sensitive))}
	value := newTestWorker(t, store, &providerFake{}, &shardFake{}, 4)

	result, err := value.RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce() error = nil, want isolated store failure")
	}
	if result.Failures != 1 || len(result.FailureSummaries) != 1 {
		t.Fatalf("result = %+v, want one bounded failure summary", result)
	}
	want := FailureSummary{Lane: FailureLaneClaimActions, Reason: FailureReasonStoreUnavailable}
	if result.FailureSummaries[0] != want {
		t.Fatalf("failure summary = %+v, want %+v", result.FailureSummaries[0], want)
	}
	encoded, marshalErr := json.Marshal(result.FailureSummaries)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(encoded), "postgres://") || strings.Contains(string(encoded), "synthetic-password") {
		t.Fatalf("bounded summaries leaked raw store error: %s", encoded)
	}
}

func TestRunOnceQueriesBeforeRetryingUncertainCapture(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationCapture)
	claim.PreviousState = domain.OperationUncertain
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{payment: provider.Payment{
		ProviderPaymentID: claim.ProviderPaymentID, Status: provider.StatusAuthorized,
		AmountMinor: claim.AmountMinor, Currency: claim.Currency, ProviderUpdatedAt: fixedNow,
	}}
	worker := newTestWorker(t, store, client, &shardFake{}, 4)

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if client.statusCalls != 1 || client.captureCalls != 0 {
		t.Fatalf("provider calls = status %d capture %d, want query only", client.statusCalls, client.captureCalls)
	}
	if store.beginCalls != 0 {
		t.Fatalf("BeginOperation calls = %d, uncertain query must not reissue side effect", store.beginCalls)
	}
	if len(store.operationFailures) != 1 || store.operationFailures[0].ManualReview || result.Retried != 1 {
		t.Fatalf("failure/result = %+v %+v, want retry after confirmed not-applied", store.operationFailures, result)
	}
}

func TestUncertainVoidObservedCapturedConvergesToRefund(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationVoid)
	claim.PreviousState = domain.OperationUncertain
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{payment: provider.Payment{
		ProviderPaymentID: claim.ProviderPaymentID, Status: provider.StatusCaptured,
		AmountMinor: claim.AmountMinor, Currency: claim.Currency, CapturedMinor: claim.AmountMinor,
		ProviderUpdatedAt: fixedNow,
	}}
	shards := &shardFake{}
	value := newTestWorker(t, store, client, shards, 4)
	result, err := value.RunOnce(context.Background())
	if err != nil || result.OperationsDone != 1 || store.supersedeVoidCalls != 1 ||
		store.completeOperationCalls != 0 || len(store.operationFailures) != 0 {
		t.Fatalf("result=%+v err=%v supersede=%d complete=%d failures=%+v",
			result, err, store.supersedeVoidCalls, store.completeOperationCalls, store.operationFailures)
	}
	if client.statusCalls != 1 {
		t.Fatalf("status calls = %d, want query-only reconciliation", client.statusCalls)
	}
}

func TestUncertainVoidCapturedWithContradictoryMoneyStaysManualReview(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationVoid)
	claim.PreviousState = domain.OperationUncertain
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{payment: provider.Payment{
		ProviderPaymentID: claim.ProviderPaymentID, Status: provider.StatusCaptured,
		AmountMinor: claim.AmountMinor, Currency: claim.Currency, CapturedMinor: claim.AmountMinor - 1,
		ProviderUpdatedAt: fixedNow,
	}}
	shards := &shardFake{}
	value := newTestWorker(t, store, client, shards, 4)
	result, err := value.RunOnce(context.Background())
	if err != nil || result.ManualReview != 1 || store.supersedeVoidCalls != 0 ||
		len(store.operationFailures) != 1 || !store.operationFailures[0].ManualReview || shards.lastCancel.CommandID != uuid.Nil {
		t.Fatalf("result=%+v err=%v supersede=%d failures=%+v",
			result, err, store.supersedeVoidCalls, store.operationFailures)
	}
}

func TestUncertainVoidUnknownStateStaysManualReviewAndRetainsInventory(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationVoid)
	claim.PreviousState = domain.OperationUncertain
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{payment: provider.Payment{
		ProviderPaymentID: claim.ProviderPaymentID, Status: provider.StatusUnknown,
		AmountMinor: claim.AmountMinor, Currency: claim.Currency, ProviderUpdatedAt: fixedNow,
	}}
	shards := &shardFake{}
	value := newTestWorker(t, store, client, shards, 4)
	result, err := value.RunOnce(context.Background())
	if err != nil || result.ManualReview != 1 || store.supersedeVoidCalls != 0 ||
		len(store.operationFailures) != 1 || !store.operationFailures[0].ManualReview ||
		shards.lastCancel.CommandID != uuid.Nil {
		t.Fatalf("result=%+v err=%v supersede=%d failures=%+v shard=%+v",
			result, err, store.supersedeVoidCalls, store.operationFailures, shards.lastCancel)
	}
}

func TestRunOnceFinalizesCaptureOnlyAfterProviderCallReturns(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationCapture)
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{operation: provider.OperationResult{
		ProviderPaymentID: claim.ProviderPaymentID, ProviderOperationID: "op-1",
		Status: provider.StatusCaptured, AmountMinor: claim.AmountMinor, Currency: claim.Currency,
	}}
	client.onCapture = func() {
		if store.claimActive || store.beginActive {
			t.Error("provider I/O overlapped a store claim/begin transaction")
		}
		store.providerReturned = true
	}
	store.onCompleteOperation = func() {
		if !store.providerReturned {
			t.Error("operation finalized before provider response")
		}
	}
	worker := newTestWorker(t, store, client, &shardFake{}, 4)

	result, err := worker.RunOnce(context.Background())
	if err != nil || result.OperationsDone != 1 {
		t.Fatalf("RunOnce() = %+v, %v", result, err)
	}
	if store.beginCalls != 1 || store.completeOperationCalls != 1 {
		t.Fatalf("store calls = begin %d complete %d", store.beginCalls, store.completeOperationCalls)
	}
}

func TestRunOnceUnknownProviderOutcomeNeverBlindlyRetries(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationRefund)
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{err: &provider.Error{
		Category: provider.ErrorTimeoutUnknown, Operation: "refund",
		Retryable: true, Uncertain: true, Message: "bounded provider timeout",
	}}
	worker := newTestWorker(t, store, client, &shardFake{}, 4)

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if client.refundCalls != 1 || len(store.operationFailures) != 1 {
		t.Fatalf("provider/store calls = refund %d failures %d", client.refundCalls, len(store.operationFailures))
	}
	// The durable store records uncertainty. Its next claim must surface the
	// same operation as OperationUncertain, which forces the query-only path.
	if store.operationFailures[0].Category != "provider_outcome_unknown" {
		t.Fatalf("failure = %+v", store.operationFailures[0])
	}
	if !store.operationFailures[0].Uncertain {
		t.Fatalf("failure = %+v, want durable uncertain state", store.operationFailures[0])
	}
	if result.Retried != 1 {
		t.Fatalf("result = %+v, want one scheduled reconciliation", result)
	}
}

func TestUncertainFullRefundRequiresExactCumulativeRefundTotal(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationRefund)
	claim.PreviousState = domain.OperationUncertain
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{payment: provider.Payment{
		ProviderPaymentID: claim.ProviderPaymentID, Status: provider.StatusRefunded,
		AmountMinor: claim.AmountMinor, Currency: claim.Currency,
		CapturedMinor: claim.AmountMinor, RefundedMinor: claim.AmountMinor - 1,
		ProviderUpdatedAt: fixedNow,
	}}
	value := newTestWorker(t, store, client, &shardFake{}, 4)

	result, err := value.RunOnce(context.Background())
	if err != nil || result.ManualReview != 1 || result.OperationsDone != 0 ||
		store.completeOperationCalls != 0 || len(store.operationFailures) != 1 ||
		!store.operationFailures[0].ManualReview {
		t.Fatalf("result=%+v err=%v complete=%d failures=%+v", result, err,
			store.completeOperationCalls, store.operationFailures)
	}
}

func TestSynchronousCaptureContradictionHasNoDurableOrShardEffects(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationCapture)
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{operation: provider.OperationResult{
		ProviderPaymentID: claim.ProviderPaymentID, ProviderOperationID: "capture-1",
		Status: provider.StatusAuthorized, AmountMinor: claim.AmountMinor, Currency: claim.Currency,
	}}
	shards := &shardFake{}
	value := newTestWorker(t, store, client, shards, 4)

	result, err := value.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Retried != 1 || result.ManualReview != 0 || len(store.operationFailures) != 1 ||
		!store.operationFailures[0].Uncertain || store.completeOperationCalls != 0 ||
		shards.lastIssue.CommandID != uuid.Nil {
		t.Fatalf("result=%+v failures=%+v complete=%d shard=%+v", result,
			store.operationFailures, store.completeOperationCalls, shards.lastIssue)
	}
}

func TestSandboxCommittedMutationLossQueriesBeforeDecision(t *testing.T) {
	t.Parallel()
	for _, kind := range []domain.OperationType{domain.OperationCapture, domain.OperationVoid, domain.OperationRefund} {
		for _, fault := range []sandbox.FaultKind{sandbox.FaultResponseLoss, sandbox.FaultTimeoutAfterCommit} {
			kind, fault := kind, fault
			t.Run(string(kind)+"_"+string(fault), func(t *testing.T) {
				t.Parallel()
				script := sandbox.NewScript()
				service := newWorkerSandbox(t, script)
				claim := validOperation(kind)
				checkout, err := service.CreateCheckout(context.Background(), provider.CreateCheckoutRequest{
					PaymentIntentID: claim.PaymentIntentID.String(), MerchantReference: claim.PaymentIntentID.String(),
					AmountMinor: claim.AmountMinor, Currency: claim.Currency, IdempotencyKey: "setup-checkout-" + string(kind),
				})
				if err != nil {
					t.Fatal(err)
				}
				_, err = service.Authorize(context.Background(), provider.AuthorizeRequest{
					PaymentIntentID: claim.PaymentIntentID.String(), ProviderPaymentID: checkout.ProviderPaymentID,
					SyntheticToken: checkout.SyntheticToken, AmountMinor: claim.AmountMinor, Currency: claim.Currency,
					IdempotencyKey: "setup-authorize-" + string(kind),
				})
				if err != nil {
					t.Fatal(err)
				}
				if kind == domain.OperationRefund {
					if _, err := service.Capture(context.Background(), provider.CaptureRequest{
						PaymentIntentID: claim.PaymentIntentID.String(), ProviderPaymentID: checkout.ProviderPaymentID,
						AmountMinor: claim.AmountMinor, Currency: claim.Currency, IdempotencyKey: "setup-capture-refund",
					}); err != nil {
						t.Fatal(err)
					}
				}
				claim.ProviderPaymentID = checkout.ProviderPaymentID
				operation := sandbox.Operation(kind)
				if !script.Push(operation, sandbox.Fault{Kind: fault}) {
					t.Fatal("push sandbox fault")
				}
				client := &countingProvider{Client: service}
				store := &storeFake{operations: []OperationClaim{claim}}
				value := newTestWorker(t, store, client, &shardFake{}, 1)
				first, err := value.RunOnce(context.Background())
				if err != nil || first.Retried != 1 || first.ManualReview != 0 ||
					len(store.operationFailures) != 1 || !store.operationFailures[0].Uncertain ||
					store.operationFailures[0].ManualReview {
					t.Fatalf("first=%+v err=%v failures=%+v", first, err, store.operationFailures)
				}
				claim.PreviousState = domain.OperationUncertain
				claim.Attempts++
				store.operations = []OperationClaim{claim}
				store.operationFailures = nil
				second, err := value.RunOnce(context.Background())
				if err != nil || second.OperationsDone != 1 || store.completeOperationCalls != 1 || client.statusCalls.Load() != 1 {
					t.Fatalf("second=%+v err=%v complete=%d status=%d", second, err, store.completeOperationCalls, client.statusCalls.Load())
				}
				if got := client.mutationCalls(kind); got != 1 {
					t.Fatalf("%s mutation calls = %d, want no replay before status", kind, got)
				}
			})
		}
	}
}

func TestSandboxCheckoutAmbiguityReplaysExactStableKey(t *testing.T) {
	t.Parallel()
	script := sandbox.NewScript()
	if !script.Push(sandbox.OperationCreateCheckout, sandbox.Fault{Kind: sandbox.FaultResponseLoss}) {
		t.Fatal("push checkout fault")
	}
	service := newWorkerSandbox(t, script)
	client := &countingProvider{Client: service}
	claim := validOperation(domain.OperationCreateCheckout)
	claim.ProviderPaymentID = ""
	store := &storeFake{operations: []OperationClaim{claim}}
	value := newTestWorker(t, store, client, &shardFake{}, 1)
	first, err := value.RunOnce(context.Background())
	if err != nil || first.Retried != 1 || first.ManualReview != 0 ||
		len(store.operationFailures) != 1 || !store.operationFailures[0].Uncertain {
		t.Fatalf("first=%+v err=%v failures=%+v", first, err, store.operationFailures)
	}
	claim.PreviousState = domain.OperationUncertain
	claim.Attempts++
	store.operations = []OperationClaim{claim}
	store.operationFailures = nil
	second, err := value.RunOnce(context.Background())
	if err != nil || second.OperationsDone != 1 || store.operationEvidence.ProviderPaymentID != "pay_sandbox_000000000001" {
		t.Fatalf("second=%+v err=%v evidence=%+v", second, err, store.operationEvidence)
	}
	if client.checkoutCalls.Load() != 2 || client.statusCalls.Load() != 0 || len(client.checkoutKeys) != 2 ||
		client.checkoutKeys[0] != claim.ProviderIdempotencyKey || client.checkoutKeys[1] != claim.ProviderIdempotencyKey {
		t.Fatalf("checkout calls=%d status=%d keys=%v", client.checkoutCalls.Load(), client.statusCalls.Load(), client.checkoutKeys)
	}
}

func TestHTTPAdapterNonRetryableUncertainCaptureQueriesBeforeFinalize(t *testing.T) {
	t.Parallel()
	var posts, gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/payments/pay-1/capture":
			posts.Add(1)
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/payments/pay-1":
			gets.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(provider.Payment{
				ProviderPaymentID: "pay-1", Status: provider.StatusCaptured,
				AmountMinor: 2500, Currency: "TWD", CapturedMinor: 2500, ProviderUpdatedAt: fixedNow,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := httpclient.New(httpclient.Config{
		BaseURL: server.URL, RequestTimeout: time.Second, ConnectTimeout: time.Second,
		MaxResponseBytes: 4096, MaxWebhookBodyBytes: 4096,
		WebhookKeys:      map[string][]byte{"test": []byte("0123456789abcdef0123456789abcdef")},
		WebhookClockSkew: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	claim := validOperation(domain.OperationCapture)
	claim.ProviderPaymentID = "pay-1"
	store := &storeFake{operations: []OperationClaim{claim}}
	value := newTestWorker(t, store, client, &shardFake{}, 1)
	first, err := value.RunOnce(context.Background())
	if err != nil || first.Retried != 1 || first.ManualReview != 0 ||
		len(store.operationFailures) != 1 || !store.operationFailures[0].Uncertain || store.operationFailures[0].ManualReview {
		t.Fatalf("first=%+v err=%v failures=%+v", first, err, store.operationFailures)
	}
	claim.PreviousState = domain.OperationUncertain
	claim.Attempts++
	store.operations = []OperationClaim{claim}
	store.operationFailures = nil
	second, err := value.RunOnce(context.Background())
	if err != nil || second.OperationsDone != 1 || posts.Load() != 1 || gets.Load() != 1 {
		t.Fatalf("second=%+v err=%v posts=%d gets=%d", second, err, posts.Load(), gets.Load())
	}
}

func TestWebhookOrderPermutationsAlwaysUseCurrentProviderState(t *testing.T) {
	t.Parallel()
	events := []provider.EventType{
		provider.EventCheckoutCreated, provider.EventAuthorized, provider.EventCaptured,
		provider.EventVoided, provider.EventRefunded,
	}
	states := []provider.Status{
		provider.StatusAuthorized, provider.StatusCaptured, provider.StatusVoided, provider.StatusRefunded,
	}
	for _, eventType := range events {
		for _, currentState := range states {
			eventType, currentState := eventType, currentState
			t.Run(string(eventType)+"_current_"+string(currentState), func(t *testing.T) {
				t.Parallel()
				claim := validWebhook(eventType)
				store := &storeFake{webhooks: []WebhookClaim{claim}}
				client := &providerFake{payment: provider.Payment{
					ProviderPaymentID: claim.ProviderPaymentID, Status: currentState,
					AmountMinor: 2500, Currency: "TWD",
					ProviderUpdatedAt: fixedNow,
				}}
				if currentState == provider.StatusCaptured || currentState == provider.StatusRefunded {
					client.payment.CapturedMinor = 2500
				}
				if currentState == provider.StatusRefunded {
					client.payment.RefundedMinor = 2500
				}
				worker := newTestWorker(t, store, client, &shardFake{}, 4)
				_, err := worker.RunOnce(context.Background())
				if err != nil {
					t.Fatalf("RunOnce() error = %v", err)
				}
				if client.statusCalls != 1 || store.webhookEvidence.Status != currentState {
					t.Fatalf("calls/evidence = %d %+v", client.statusCalls, store.webhookEvidence)
				}
			})
		}
	}
}

func TestMissingProviderIsFinalizedOnlyAfterOperationBegins(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationCapture)
	claim.Provider = "missing"
	store := &storeFake{operations: []OperationClaim{claim}}
	value, err := New(store, Providers{"sandbox": &providerFake{}}, &shardFake{}, nil, Config{
		WorkerID: "payment-test", BatchSize: 10, MaxAttempts: 4,
		LeaseTTL: time.Minute, RetryBase: time.Second, RetryMax: time.Minute,
		Interval: time.Second, Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := value.RunOnce(context.Background())
	if err != nil || store.beginCalls != 1 || len(store.operationFailures) != 1 || result.ManualReview != 1 {
		t.Fatalf("RunOnce() = %+v, %v; begin=%d failures=%+v", result, err, store.beginCalls, store.operationFailures)
	}
}

func TestRunOnceWebhookUsesCurrentProviderStateAndUnknownTypeNeverAdvances(t *testing.T) {
	t.Parallel()
	known := validWebhook(provider.EventCaptured)
	unknown := validWebhook(provider.EventUnknown)
	unknown.InboxID = uuid.New()
	store := &storeFake{webhooks: []WebhookClaim{known, unknown}}
	client := &providerFake{payment: provider.Payment{
		ProviderPaymentID: known.ProviderPaymentID, Status: provider.StatusRefunded,
		AmountMinor: 2500, Currency: "TWD", CapturedMinor: 2500, RefundedMinor: 2500,
		ProviderUpdatedAt: fixedNow,
	}}
	worker := newTestWorker(t, store, client, &shardFake{}, 4)

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if client.statusCalls != 1 || store.completeWebhookCalls != 1 || store.ignoreWebhookCalls != 1 {
		t.Fatalf("calls = status %d complete %d ignore %d", client.statusCalls, store.completeWebhookCalls, store.ignoreWebhookCalls)
	}
	if store.webhookEvidence.Status != provider.StatusRefunded || result.WebhooksDone != 2 {
		t.Fatalf("evidence/result = %+v %+v", store.webhookEvidence, result)
	}
}

func TestWebhookContradictoryFinancialObservationHasZeroEffects(t *testing.T) {
	t.Parallel()
	claim := validWebhook(provider.EventCaptured)
	store := &storeFake{webhooks: []WebhookClaim{claim}}
	client := &providerFake{payment: provider.Payment{
		ProviderPaymentID: claim.ProviderPaymentID, Status: provider.StatusAuthorized,
		AmountMinor: 2500, Currency: "TWD", CapturedMinor: 2500,
		ProviderUpdatedAt: fixedNow,
	}}
	shards := &shardFake{}
	value := newTestWorker(t, store, client, shards, 4)

	result, err := value.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.ManualReview != 1 || store.completeWebhookCalls != 0 || len(store.webhookFailures) != 1 ||
		!store.webhookFailures[0].ManualReview || shards.lastIssue.CommandID != uuid.Nil {
		t.Fatalf("result=%+v webhook failures=%+v complete=%d shard=%+v", result,
			store.webhookFailures, store.completeWebhookCalls, shards.lastIssue)
	}
}

func TestRunOnceRetriesTicketIssuanceWithSameCommandIdentity(t *testing.T) {
	t.Parallel()
	commandID := uuid.New()
	claim := ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: ActionIssueTickets, Provider: "sandbox", Attempts: 1,
		LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute),
		Issue: shard.IssueTicketsCommand{CommandID: commandID, IssuanceID: uuid.New(), PaymentIntentID: uuid.New()},
	}
	store := &storeFake{actions: []ActionClaim{claim}}
	shards := &shardFake{issueErr: errors.New("shard unavailable")}
	worker := newTestWorker(t, store, &providerFake{}, shards, 4)

	first, err := worker.RunOnce(context.Background())
	if err != nil || first.Retried != 1 || shards.lastIssue.CommandID != commandID {
		t.Fatalf("first RunOnce() = %+v, %v; command = %s", first, err, shards.lastIssue.CommandID)
	}
	store.actions = []ActionClaim{claim}
	shards.issueErr = nil
	shards.issueReceipt = shard.IssueTicketsReceipt{
		CommandID: commandID, IssuanceID: claim.Issue.IssuanceID,
		PaymentIntentID: claim.Issue.PaymentIntentID, ReservationID: claim.Issue.ReservationID,
		AmountMinor: claim.Issue.AmountMinor, Currency: claim.Issue.Currency,
		TicketOrderID: uuid.New(), TicketIDs: []uuid.UUID{uuid.New()}, TicketCodes: []string{"ticket_code_000001"},
		OrderCreatedAt: fixedNow.Add(-time.Minute), IssuedAt: fixedNow,
	}
	second, err := worker.RunOnce(context.Background())
	if err != nil || second.ActionsDone != 1 || shards.lastIssue.CommandID != commandID {
		t.Fatalf("second RunOnce() = %+v, %v; command = %s", second, err, shards.lastIssue.CommandID)
	}
}

func TestFinalAttemptActionCrashReclaimsDurableShardReceipt(t *testing.T) {
	t.Parallel()
	claim := ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: ActionIssueTickets, Provider: "sandbox", Attempts: 1,
		LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute),
		Issue: shard.IssueTicketsCommand{
			CommandID: uuid.New(), IssuanceID: uuid.New(), PaymentIntentID: uuid.New(),
			ReservationID: uuid.New(), AmountMinor: 2_500, Currency: "TWD",
		},
	}
	receipt := shard.IssueTicketsReceipt{
		CommandID: claim.Issue.CommandID, IssuanceID: claim.Issue.IssuanceID,
		PaymentIntentID: claim.Issue.PaymentIntentID, ReservationID: claim.Issue.ReservationID,
		AmountMinor: claim.Issue.AmountMinor, Currency: claim.Issue.Currency,
		TicketOrderID: uuid.New(), TicketIDs: []uuid.UUID{uuid.New()}, TicketCodes: []string{"ticket_code_000001"},
		OrderCreatedAt: fixedNow.Add(-time.Minute), IssuedAt: fixedNow,
	}
	store := &storeFake{actions: []ActionClaim{claim}, completeActionErr: errors.New("control commit lost after shard effect")}
	shards := &shardFake{issueReceipt: receipt}
	value := newTestWorker(t, store, &providerFake{}, shards, 1)

	first, err := value.RunOnce(context.Background())
	if err == nil || first.ActionsDone != 0 || shards.issueCalls != 1 || store.completeActionCalls != 1 {
		t.Fatalf("crash pass result=%+v err=%v issue_calls=%d complete_calls=%d", first, err, shards.issueCalls, store.completeActionCalls)
	}

	recovery := claim
	recovery.Attempts = 2 // the store's one bounded receipt-recovery claim
	store.actions = []ActionClaim{recovery}
	store.completeActionErr = nil
	second, err := value.RunOnce(context.Background())
	if err != nil || second.ActionsDone != 1 || shards.issueCalls != 2 || store.completeActionCalls != 2 ||
		shards.lastIssue.CommandID != claim.Issue.CommandID {
		t.Fatalf("recovery pass result=%+v err=%v issue_calls=%d complete_calls=%d command=%s",
			second, err, shards.issueCalls, store.completeActionCalls, shards.lastIssue.CommandID)
	}
}

func TestShardActionsHaveIndependentBackoffIdentity(t *testing.T) {
	t.Parallel()
	sagaID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	claims := []ActionClaim{
		{
			ActionID: uuid.MustParse("11111111-1111-4111-8111-111111111111"), SagaID: sagaID,
			Type: ActionIssueTickets, Provider: "sandbox", Attempts: 1,
			LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute),
			Issue: shard.IssueTicketsCommand{CommandID: uuid.New(), IssuanceID: uuid.New(), PaymentIntentID: uuid.New()},
		},
		{
			ActionID: uuid.MustParse("22222222-2222-4222-8222-222222222222"), SagaID: sagaID,
			Type: ActionIssueTickets, Provider: "sandbox", Attempts: 1,
			LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute),
			Issue: shard.IssueTicketsCommand{CommandID: uuid.New(), IssuanceID: uuid.New(), PaymentIntentID: uuid.New()},
		},
	}
	delays := make([]time.Time, 0, len(claims))
	for _, claim := range claims {
		store := &storeFake{actions: []ActionClaim{claim}}
		value := newTestWorker(t, store, &providerFake{}, &shardFake{issueErr: errors.New("transient shard outage")}, 4)
		if _, err := value.RunOnce(context.Background()); err != nil || len(store.actionFailures) != 1 {
			t.Fatalf("RunOnce() error=%v failures=%+v", err, store.actionFailures)
		}
		delays = append(delays, store.actionFailures[0].NextAttemptAt)
	}
	if delays[0] == delays[1] {
		t.Fatalf("independent actions inherited saga backoff identity: %v", delays)
	}
}

func TestShardActionWithoutDurableIdentityNeverReachesShard(t *testing.T) {
	t.Parallel()
	claim := ActionClaim{
		SagaID: uuid.New(), Type: ActionIssueTickets, Provider: "sandbox", Attempts: 1,
		LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute),
		Issue: shard.IssueTicketsCommand{CommandID: uuid.New(), IssuanceID: uuid.New(), PaymentIntentID: uuid.New()},
	}
	store := &storeFake{actions: []ActionClaim{claim}}
	shards := &shardFake{}
	value := newTestWorker(t, store, &providerFake{}, shards, 4)

	result, err := value.RunOnce(context.Background())
	if err != nil || result.ManualReview != 1 || shards.lastIssue.CommandID != uuid.Nil || len(store.actionFailures) != 1 {
		t.Fatalf("result=%+v err=%v shard=%+v failures=%+v", result, err, shards.lastIssue, store.actionFailures)
	}
}

func TestVoidShardReceiptMustSucceedBeforeControlFinalization(t *testing.T) {
	t.Parallel()
	command := shard.CancelVoidedReservationCommand{
		CommandID: uuid.New(), VoidOperationID: uuid.New(), PaymentIntentID: uuid.New(),
		ReservationID: uuid.New(), TrainRunID: uuid.New(), OwnerID: uuid.New(),
		AmountMinor: 2500, Currency: "TWD", VoidProofHash: [32]byte{1}, VoidedAt: fixedNow,
	}
	command.RequestFingerprint = shard.VoidCancellationFingerprint(command)
	claim := ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: ActionCancelVoided, Provider: "sandbox", Attempts: 1,
		LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute), CancelVoided: command,
	}
	store := &storeFake{actions: []ActionClaim{claim}}
	shards := &shardFake{cancelErr: errors.New("shard unavailable")}
	value := newTestWorker(t, store, &providerFake{}, shards, 4)
	result, err := value.RunOnce(context.Background())
	if err != nil || result.Retried != 1 || store.completeActionCalls != 0 {
		t.Fatalf("failed shard result=%+v err=%v complete=%d", result, err, store.completeActionCalls)
	}
	store.actions = []ActionClaim{claim}
	shards.cancelErr = nil
	shards.cancelReceipt = shard.CancelVoidedReservationReceipt{
		CommandID: command.CommandID, VoidOperationID: command.VoidOperationID,
		PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID,
		TicketOrderID: uuid.New(), ReleasedSeatCount: 2, CancelledAt: fixedNow,
	}
	result, err = value.RunOnce(context.Background())
	if err != nil || result.ActionsDone != 1 || store.completeActionCalls != 1 ||
		store.actionEvidence.CancelVoided.CommandID != command.CommandID {
		t.Fatalf("successful shard result=%+v err=%v complete=%d evidence=%+v",
			result, err, store.completeActionCalls, store.actionEvidence)
	}
}

func TestShardReceiptMismatchEscalatesWithoutControlFinalization(t *testing.T) {
	t.Parallel()
	claim := ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: ActionIssueTickets, Provider: "sandbox", Attempts: 1,
		LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute),
		Issue: shard.IssueTicketsCommand{
			CommandID: uuid.New(), IssuanceID: uuid.New(), PaymentIntentID: uuid.New(),
			ReservationID: uuid.New(), AmountMinor: 100, Currency: "TWD",
		},
	}
	store := &storeFake{actions: []ActionClaim{claim}}
	shards := &shardFake{issueReceipt: shard.IssueTicketsReceipt{CommandID: uuid.New()}}
	worker := newTestWorker(t, store, &providerFake{}, shards, 4)
	result, err := worker.RunOnce(context.Background())
	if err != nil || result.Compensating != 1 || len(store.actionFailures) != 1 || !store.actionFailures[0].Compensate {
		t.Fatalf("RunOnce() = %+v, %v; failures=%+v", result, err, store.actionFailures)
	}
}

func TestCommittedIssuanceControlFinalizeFailureNeverStartsRefund(t *testing.T) {
	t.Parallel()
	claim := ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: ActionIssueTickets, Provider: "sandbox", Attempts: 1,
		LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute),
		Issue: shard.IssueTicketsCommand{
			CommandID: uuid.New(), IssuanceID: uuid.New(), PaymentIntentID: uuid.New(),
			ReservationID: uuid.New(), AmountMinor: 100, Currency: "TWD",
		},
	}
	ticketID := uuid.New()
	shards := &shardFake{issueReceipt: shard.IssueTicketsReceipt{
		CommandID: claim.Issue.CommandID, IssuanceID: claim.Issue.IssuanceID,
		PaymentIntentID: claim.Issue.PaymentIntentID, ReservationID: claim.Issue.ReservationID,
		TicketOrderID: uuid.New(), TicketIDs: []uuid.UUID{ticketID}, AmountMinor: 100,
		TicketCodes: []string{"ticket_code_000001"}, Currency: "TWD",
		OrderCreatedAt: fixedNow.Add(-time.Minute), IssuedAt: fixedNow,
	}}
	store := &storeFake{actions: []ActionClaim{claim}, completeActionErr: errors.New("control commit failed")}
	value := newTestWorker(t, store, &providerFake{}, shards, 1)
	result, err := value.RunOnce(context.Background())
	if err == nil || result.ManualReview != 1 || len(store.actionFailures) != 1 {
		t.Fatalf("result=%+v err=%v failures=%+v", result, err, store.actionFailures)
	}
	failure := store.actionFailures[0]
	if failure.Compensate || !failure.ManualReview || failure.Category != "database_finalize_failed" {
		t.Fatalf("failure=%+v", failure)
	}
}

func TestRefundActionReceiptsRequireOrderAndReleasedSeats(t *testing.T) {
	t.Parallel()
	mark := ActionClaim{Type: ActionMarkRefundPending, MarkRefund: shard.MarkRefundPendingCommand{
		CommandID: uuid.New(), PaymentIntentID: uuid.New(), ReservationID: uuid.New(),
	}}
	markEvidence := ActionEvidence{MarkRefund: shard.MarkRefundPendingReceipt{
		CommandID: mark.MarkRefund.CommandID, PaymentIntentID: mark.MarkRefund.PaymentIntentID,
		ReservationID: mark.MarkRefund.ReservationID,
	}}
	if validActionEvidence(mark, markEvidence) {
		t.Fatal("refund-pending receipt without ticket order accepted")
	}
	markEvidence.MarkRefund.TicketOrderID = uuid.New()
	if !validActionEvidence(mark, markEvidence) {
		t.Fatal("valid refund-pending receipt rejected")
	}

	compensate := ActionClaim{Type: ActionCompensate, Compensation: shard.ApplyRefundCompensationCommand{
		CommandID: uuid.New(), CompensationID: uuid.New(), PaymentIntentID: uuid.New(), ReservationID: uuid.New(),
	}}
	compensationEvidence := ActionEvidence{Compensation: shard.ApplyRefundCompensationReceipt{
		CommandID: compensate.Compensation.CommandID, CompensationID: compensate.Compensation.CompensationID,
		PaymentIntentID: compensate.Compensation.PaymentIntentID, ReservationID: compensate.Compensation.ReservationID,
		TicketOrderID: uuid.New(),
	}}
	if validActionEvidence(compensate, compensationEvidence) {
		t.Fatal("compensation receipt without released seats accepted")
	}
	compensationEvidence.Compensation.ReleasedSeatCount = 1
	if !validActionEvidence(compensate, compensationEvidence) {
		t.Fatal("valid compensation receipt rejected")
	}
}

func TestMaxAttemptsTransitionsToManualReview(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationCapture)
	claim.Attempts = 3
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{err: &provider.Error{Category: provider.ErrorUnavailable, Retryable: true}}
	worker := newTestWorker(t, store, client, &shardFake{}, 3)

	result, err := worker.RunOnce(context.Background())
	if err != nil || result.ManualReview != 1 || len(store.operationFailures) != 1 || !store.operationFailures[0].ManualReview {
		t.Fatalf("RunOnce() = %+v, %v; failures = %+v", result, err, store.operationFailures)
	}
}

func TestRunLoopStopsCleanlyWithoutAdditionalPass(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- RunLoop(ctx, time.Hour, func(context.Context) (Result, error) {
			calls.Add(1)
			cancel()
			return Result{}, nil
		}, nil)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunLoop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunLoop did not stop after cancellation")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestRetryDelayIsBoundedAndDeterministic(t *testing.T) {
	t.Parallel()
	id := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	first := retryDelay(id, 2, time.Second, 5*time.Second)
	second := retryDelay(id, 2, time.Second, 5*time.Second)
	if first != second || first < 2*time.Second || first > 2500*time.Millisecond {
		t.Fatalf("retryDelay = %s and %s", first, second)
	}
	if got := retryDelay(id, 100, time.Second, 5*time.Second); got != 5*time.Second {
		t.Fatalf("bounded retryDelay = %s", got)
	}
}

func TestRunOnceRecordsMeasuredWebhookLagAndDurableUncertainty(t *testing.T) {
	t.Parallel()
	webhookClaim := validWebhook(provider.EventCaptured)
	operationClaim := validOperation(domain.OperationCapture)
	metrics := &metricsFake{}
	store := &storeFake{operations: []OperationClaim{operationClaim}, webhooks: []WebhookClaim{webhookClaim}}
	client := &providerFake{
		err:       &provider.Error{Category: provider.ErrorTimeoutUnknown, Retryable: true, Uncertain: true},
		onCapture: func() { time.Sleep(2 * time.Millisecond) },
		onStatus:  func() { time.Sleep(2 * time.Millisecond) },
	}
	value, err := New(store, Providers{"sandbox": client}, &shardFake{}, metrics, Config{
		WorkerID: "payment-test", BatchSize: 10, MaxAttempts: 4,
		LeaseTTL: time.Minute, RetryBase: time.Second, RetryMax: time.Minute,
		Interval: time.Second, Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = value.RunOnce(context.Background())

	var operation, webhook MetricObservation
	for _, observation := range metrics.observations {
		switch observation.Lane {
		case "operation":
			if observation.Operation == string(domain.OperationCapture) {
				operation = observation
			}
		case "webhook":
			if observation.Operation == string(provider.EventCaptured) {
				webhook = observation
			}
		}
	}
	if operation.Result != "retry" || !operation.Uncertain || operation.Duration <= 0 {
		t.Fatalf("operation observation = %#v", operation)
	}
	if webhook.Lag != 2*time.Minute || webhook.Duration <= 0 {
		t.Fatalf("webhook observation = %#v", webhook)
	}
}

func TestUncertaintyWindowExpiresIntoManualReviewWithoutProviderCall(t *testing.T) {
	t.Parallel()
	claim := validOperation(domain.OperationCapture)
	claim.PreviousState = domain.OperationUncertain
	claim.CreatedAt = fixedNow.Add(-2 * time.Hour)
	store := &storeFake{operations: []OperationClaim{claim}}
	client := &providerFake{}
	value, err := New(store, Providers{"sandbox": client}, &shardFake{}, nil, Config{
		WorkerID: "payment-test", BatchSize: 10, MaxAttempts: 4,
		LeaseTTL: time.Minute, RetryBase: time.Second, RetryMax: time.Minute,
		MaxUncertain: time.Hour, Interval: time.Second, Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := value.RunOnce(context.Background())
	if err != nil || result.ManualReview != 1 || client.statusCalls != 0 || len(store.operationFailures) != 1 {
		t.Fatalf("result=%+v err=%v statusCalls=%d failures=%+v", result, err, client.statusCalls, store.operationFailures)
	}
	failure := store.operationFailures[0]
	if !failure.ManualReview || !failure.Uncertain || failure.Category != "uncertainty_window_exceeded" {
		t.Fatalf("failure=%+v", failure)
	}
}

func newTestWorker(t *testing.T, store Store, client provider.Client, shards ShardGateway, maxAttempts int) *Worker {
	t.Helper()
	value, err := New(store, Providers{"sandbox": client}, shards, nil, Config{
		WorkerID: "payment-test", BatchSize: 10, MaxAttempts: maxAttempts,
		LeaseTTL: time.Minute, RetryBase: time.Second, RetryMax: time.Minute,
		Interval: time.Second, Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return value
}

func validOperation(kind domain.OperationType) OperationClaim {
	return OperationClaim{
		OperationID: uuid.New(), PaymentIntentID: uuid.New(), ReservationID: uuid.New(),
		TrainRunID: uuid.New(), OwnerID: uuid.New(), Provider: "sandbox", Type: kind,
		PreviousState: domain.OperationPending, ProviderPaymentID: "pay-1",
		ProviderIdempotencyKey: "stable-key-1", AmountMinor: 2500, Currency: "TWD",
		Attempts: 1, CreatedAt: fixedNow.Add(-time.Minute), LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute),
	}
}

func validWebhook(kind provider.EventType) WebhookClaim {
	return WebhookClaim{
		InboxID: uuid.New(), Provider: "sandbox", ProviderEventID: "evt-1", EventType: kind,
		ProviderPaymentID: "pay-1", EventCreatedAt: fixedNow.Add(-2 * time.Minute), Attempts: 1, LeaseOwner: "payment-test",
		LeaseUntil: fixedNow.Add(time.Minute),
	}
}

type metricsFake struct{ observations []MetricObservation }

func (fake *metricsFake) RecordPaymentWorker(observation MetricObservation) {
	fake.observations = append(fake.observations, observation)
}

type storeFake struct {
	operations             []OperationClaim
	webhooks               []WebhookClaim
	actions                []ActionClaim
	claimActive            bool
	beginActive            bool
	providerReturned       bool
	beginCalls             int
	completeOperationCalls int
	supersedeVoidCalls     int
	operationEvidence      OperationEvidence
	completedOperation     OperationClaim
	completeWebhookCalls   int
	ignoreWebhookCalls     int
	completeActionCalls    int
	webhookEvidence        WebhookEvidence
	operationFailures      []Failure
	webhookFailures        []Failure
	actionFailures         []Failure
	actionEvidence         ActionEvidence
	completeActionErr      error
	claimActionErr         error
	onCompleteOperation    func()
}

func (store *storeFake) ClaimOperations(context.Context, ClaimOptions) ([]OperationClaim, error) {
	store.claimActive = true
	defer func() { store.claimActive = false }()
	return append([]OperationClaim(nil), store.operations...), nil
}
func (store *storeFake) BeginOperation(context.Context, OperationClaim) error {
	store.beginActive = true
	store.beginCalls++
	store.beginActive = false
	return nil
}
func (store *storeFake) CompleteOperation(_ context.Context, claim OperationClaim, evidence OperationEvidence) error {
	store.completeOperationCalls++
	store.completedOperation = claim
	store.operationEvidence = evidence
	if store.onCompleteOperation != nil {
		store.onCompleteOperation()
	}
	return nil
}
func (store *storeFake) SupersedeVoidWithRefund(context.Context, OperationClaim, OperationEvidence) error {
	store.supersedeVoidCalls++
	return nil
}
func (store *storeFake) FailOperation(_ context.Context, _ OperationClaim, failure Failure) error {
	store.operationFailures = append(store.operationFailures, failure)
	return nil
}
func (store *storeFake) ClaimWebhooks(context.Context, ClaimOptions) ([]WebhookClaim, error) {
	return append([]WebhookClaim(nil), store.webhooks...), nil
}
func (store *storeFake) CompleteWebhook(_ context.Context, _ WebhookClaim, evidence WebhookEvidence) error {
	store.completeWebhookCalls++
	store.webhookEvidence = evidence
	return nil
}
func (store *storeFake) IgnoreWebhook(context.Context, WebhookClaim) error {
	store.ignoreWebhookCalls++
	return nil
}
func (store *storeFake) FailWebhook(_ context.Context, _ WebhookClaim, failure Failure) error {
	store.webhookFailures = append(store.webhookFailures, failure)
	return nil
}
func (store *storeFake) ClaimActions(context.Context, ClaimOptions) ([]ActionClaim, error) {
	return append([]ActionClaim(nil), store.actions...), store.claimActionErr
}
func (store *storeFake) CompleteAction(_ context.Context, _ ActionClaim, evidence ActionEvidence) error {
	store.completeActionCalls++
	store.actionEvidence = evidence
	return store.completeActionErr
}
func (store *storeFake) FailAction(_ context.Context, _ ActionClaim, failure Failure) error {
	store.actionFailures = append(store.actionFailures, failure)
	return nil
}

type providerFake struct {
	payment      provider.Payment
	operation    provider.OperationResult
	checkout     provider.Checkout
	err          error
	statusCalls  int
	captureCalls int
	refundCalls  int
	onCapture    func()
	onStatus     func()
}

func (fake *providerFake) CreateCheckout(context.Context, provider.CreateCheckoutRequest) (provider.Checkout, error) {
	return fake.checkout, fake.err
}
func (fake *providerFake) GetPaymentStatus(context.Context, string) (provider.Payment, error) {
	fake.statusCalls++
	if fake.onStatus != nil {
		fake.onStatus()
	}
	return fake.payment, fake.err
}
func (fake *providerFake) Authorize(context.Context, provider.AuthorizeRequest) (provider.OperationResult, error) {
	return fake.operation, fake.err
}
func (fake *providerFake) Capture(context.Context, provider.CaptureRequest) (provider.OperationResult, error) {
	fake.captureCalls++
	if fake.onCapture != nil {
		fake.onCapture()
	}
	return fake.operation, fake.err
}
func (fake *providerFake) Void(context.Context, provider.VoidRequest) (provider.OperationResult, error) {
	return fake.operation, fake.err
}
func (fake *providerFake) Refund(context.Context, provider.RefundRequest) (provider.OperationResult, error) {
	fake.refundCalls++
	return fake.operation, fake.err
}
func (fake *providerFake) VerifyWebhook(context.Context, provider.WebhookHeaders, []byte) (provider.WebhookEvent, error) {
	return provider.WebhookEvent{}, errors.New("not used")
}

type countingProvider struct {
	provider.Client
	checkoutCalls atomic.Int32
	statusCalls   atomic.Int32
	captureCalls  atomic.Int32
	voidCalls     atomic.Int32
	refundCalls   atomic.Int32
	checkoutKeys  []string
}

func (client *countingProvider) CreateCheckout(ctx context.Context, request provider.CreateCheckoutRequest) (provider.Checkout, error) {
	client.checkoutCalls.Add(1)
	client.checkoutKeys = append(client.checkoutKeys, request.IdempotencyKey)
	return client.Client.CreateCheckout(ctx, request)
}

func (client *countingProvider) GetPaymentStatus(ctx context.Context, paymentID string) (provider.Payment, error) {
	client.statusCalls.Add(1)
	return client.Client.GetPaymentStatus(ctx, paymentID)
}

func (client *countingProvider) Capture(ctx context.Context, request provider.CaptureRequest) (provider.OperationResult, error) {
	client.captureCalls.Add(1)
	return client.Client.Capture(ctx, request)
}

func (client *countingProvider) Void(ctx context.Context, request provider.VoidRequest) (provider.OperationResult, error) {
	client.voidCalls.Add(1)
	return client.Client.Void(ctx, request)
}

func (client *countingProvider) Refund(ctx context.Context, request provider.RefundRequest) (provider.OperationResult, error) {
	client.refundCalls.Add(1)
	return client.Client.Refund(ctx, request)
}

func (client *countingProvider) mutationCalls(kind domain.OperationType) int32 {
	switch kind {
	case domain.OperationCapture:
		return client.captureCalls.Load()
	case domain.OperationVoid:
		return client.voidCalls.Load()
	case domain.OperationRefund:
		return client.refundCalls.Load()
	default:
		return 0
	}
}

func TestShardFailureCategoryPreservesBoundedTicketIssuancePhase(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		err  error
		want string
	}{
		{err: errors.Join(shard.ErrTicketPlanUnavailable, shard.ErrShardPaymentUnavailable), want: "shard_ticket_plan_failed"},
		{err: errors.Join(shard.ErrTicketClaimUnavailable, shard.ErrTicketClaimReadFailed), want: "shard_ticket_claim_read_failed"},
		{err: errors.Join(shard.ErrTicketClaimUnavailable, shard.ErrTicketClaimUnauthorized), want: "shard_ticket_claim_unauthorized"},
		{err: errors.Join(shard.ErrTicketClaimUnavailable, shard.ErrTicketClaimConflict), want: "shard_ticket_claim_conflict"},
		{err: errors.Join(shard.ErrTicketClaimUnavailable, shard.ErrTicketClaimCommitFailed), want: "shard_ticket_claim_commit_failed"},
		{err: errors.Join(shard.ErrTicketClaimUnavailable, shard.ErrShardPaymentUnavailable), want: "shard_ticket_claim_failed"},
		{err: errors.Join(shard.ErrTicketIssueUnavailable, shard.ErrShardPaymentUnavailable), want: "shard_ticket_issue_failed"},
		{err: shard.ErrShardPaymentUnavailable, want: "shard_command_failed"},
	} {
		if got := shardFailureCategory(test.err); got != test.want {
			t.Fatalf("shardFailureCategory(%v) = %q, want %q", test.err, got, test.want)
		}
	}
}

func newWorkerSandbox(t *testing.T, faults sandbox.FaultPlan) *sandbox.Service {
	t.Helper()
	key := sha256.Sum256([]byte("worker-sandbox-test-key"))
	service, err := sandbox.New(sandbox.Config{
		Environment: "test", Now: func() time.Time { return fixedNow },
		WebhookKeys: map[string][]byte{"test": key[:]}, IssueKeyID: "test", Faults: faults,
	})
	if err != nil {
		t.Fatalf("new sandbox: %v", err)
	}
	return service
}

type shardFake struct {
	issueErr      error
	issueReceipt  shard.IssueTicketsReceipt
	lastIssue     shard.IssueTicketsCommand
	issueCalls    int
	cancelErr     error
	cancelReceipt shard.CancelVoidedReservationReceipt
	lastCancel    shard.CancelVoidedReservationCommand
}

func (fake *shardFake) IssueTickets(_ context.Context, command shard.IssueTicketsCommand) (shard.IssueTicketsReceipt, error) {
	fake.issueCalls++
	fake.lastIssue = command
	return fake.issueReceipt, fake.issueErr
}
func (*shardFake) MarkRefundPending(context.Context, shard.MarkRefundPendingCommand) (shard.MarkRefundPendingReceipt, error) {
	return shard.MarkRefundPendingReceipt{}, nil
}

func (fake *shardFake) CancelVoidedReservation(_ context.Context, command shard.CancelVoidedReservationCommand) (shard.CancelVoidedReservationReceipt, error) {
	fake.lastCancel = command
	return fake.cancelReceipt, fake.cancelErr
}
func (*shardFake) ApplyRefundCompensation(context.Context, shard.ApplyRefundCompensationCommand) (shard.ApplyRefundCompensationReceipt, error) {
	return shard.ApplyRefundCompensationReceipt{}, nil
}
