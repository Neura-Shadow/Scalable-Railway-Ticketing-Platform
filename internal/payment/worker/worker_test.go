package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
)

var fixedNow = time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC)

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
					AmountMinor: 2500, Currency: "TWD", CapturedMinor: 2500,
					ProviderUpdatedAt: fixedNow,
				}}
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

func TestRunOnceRetriesTicketIssuanceWithSameCommandIdentity(t *testing.T) {
	t.Parallel()
	commandID := uuid.New()
	claim := ActionClaim{
		SagaID: uuid.New(), Type: ActionIssueTickets, Provider: "sandbox", Attempts: 1,
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
		TicketOrderID: uuid.New(), TicketIDs: []uuid.UUID{uuid.New()}, IssuedAt: fixedNow,
	}
	second, err := worker.RunOnce(context.Background())
	if err != nil || second.ActionsDone != 1 || shards.lastIssue.CommandID != commandID {
		t.Fatalf("second RunOnce() = %+v, %v; command = %s", second, err, shards.lastIssue.CommandID)
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
		SagaID: uuid.New(), Type: ActionCancelVoided, Provider: "sandbox", Attempts: 1,
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
		SagaID: uuid.New(), Type: ActionIssueTickets, Provider: "sandbox", Attempts: 1,
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
		Attempts: 1, LeaseOwner: "payment-test", LeaseUntil: fixedNow.Add(time.Minute),
	}
}

func validWebhook(kind provider.EventType) WebhookClaim {
	return WebhookClaim{
		InboxID: uuid.New(), Provider: "sandbox", ProviderEventID: "evt-1", EventType: kind,
		ProviderPaymentID: "pay-1", Attempts: 1, LeaseOwner: "payment-test",
		LeaseUntil: fixedNow.Add(time.Minute),
	}
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
	completeWebhookCalls   int
	ignoreWebhookCalls     int
	completeActionCalls    int
	webhookEvidence        WebhookEvidence
	operationFailures      []Failure
	webhookFailures        []Failure
	actionFailures         []Failure
	actionEvidence         ActionEvidence
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
func (store *storeFake) CompleteOperation(context.Context, OperationClaim, OperationEvidence) error {
	store.completeOperationCalls++
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
	return append([]ActionClaim(nil), store.actions...), nil
}
func (store *storeFake) CompleteAction(_ context.Context, _ ActionClaim, evidence ActionEvidence) error {
	store.completeActionCalls++
	store.actionEvidence = evidence
	return nil
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
}

func (fake *providerFake) CreateCheckout(context.Context, provider.CreateCheckoutRequest) (provider.Checkout, error) {
	return fake.checkout, fake.err
}
func (fake *providerFake) GetPaymentStatus(context.Context, string) (provider.Payment, error) {
	fake.statusCalls++
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

type shardFake struct {
	issueErr      error
	issueReceipt  shard.IssueTicketsReceipt
	lastIssue     shard.IssueTicketsCommand
	cancelErr     error
	cancelReceipt shard.CancelVoidedReservationReceipt
	lastCancel    shard.CancelVoidedReservationCommand
}

func (fake *shardFake) IssueTickets(_ context.Context, command shard.IssueTicketsCommand) (shard.IssueTicketsReceipt, error) {
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
