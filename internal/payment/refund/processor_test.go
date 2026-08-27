package refund_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
)

func TestProcessorQueriesUncertainRefundBeforeOneSelectedShardCommand(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC)
	work := refund.RefundWork{
		SagaID: uuid.New(), RequestID: uuid.New(), OperationID: uuid.New(),
		PaymentIntentID: uuid.New(), ReservationID: uuid.New(), TicketOrderID: uuid.New(),
		TrainRunID: uuid.New(), OwnerID: uuid.New(), Provider: "stripe", ProviderPaymentID: "pi_1",
		AmountMinor: 200, CapturedMinor: 1_000, RefundedBeforeMinor: 100, Currency: "TWD",
		Step: refund.StepRefundProvider, RequestFingerprint: refund.Hash{1}, TicketIDs: []uuid.UUID{uuid.New()},
	}
	store := &refundRuntimeStoreFake{work: work}
	client := &refundProviderFake{
		refundErr: &provider.Error{Category: provider.ErrorTimeoutUnknown, Retryable: true, Uncertain: true},
		lookup: provider.RefundLookupResult{Found: true, Definitive: true, Refund: provider.OperationResult{
			ProviderPaymentID: "pi_1", ProviderOperationID: "re_exact_1", Status: provider.StatusRefunded,
			AmountMinor: 200, Currency: "TWD",
		}},
	}
	shard := &selectedRefundShardFake{}
	processor, err := refund.NewProcessor(store, refund.Providers{"stripe": client}, shard, refund.ProcessorConfig{
		WorkerID: "refund-worker-1", BatchSize: 10, MaxAttempts: 8, LeaseTTL: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute, Region: "region-a", RegionalEpoch: 4,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := processor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.work.Step != refund.StepQueryProvider || client.refundCalls != 1 || client.queryCalls != 0 || shard.calls != 0 {
		t.Fatalf("uncertain pass: step=%s refund=%d query=%d shard=%d", store.work.Step, client.refundCalls, client.queryCalls, shard.calls)
	}
	client.refundErr = nil
	if _, err := processor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.work.Step != refund.StepCompensateShard || client.refundCalls != 1 || client.queryCalls != 1 || shard.calls != 0 {
		t.Fatalf("query pass: step=%s refund=%d query=%d shard=%d", store.work.Step, client.refundCalls, client.queryCalls, shard.calls)
	}
	if _, err := processor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.work.Step != refund.StepFinalize || shard.calls != 1 || shard.command.Region != "region-a" || shard.command.RegionalEpoch != 4 {
		t.Fatalf("shard pass: step=%s shard=%d command=%+v", store.work.Step, shard.calls, shard.command)
	}
	finalResult, err := processor.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if store.work.Step != refund.StepComplete || !store.finalized || shard.calls != 1 {
		t.Fatalf("final pass: step=%s finalized=%t shard=%d", store.work.Step, store.finalized, shard.calls)
	}
	if len(finalResult.Observations) != 1 || finalResult.Observations[0].Provider != "stripe" ||
		finalResult.Observations[0].Currency != "TWD" || finalResult.Observations[0].Result != "success" {
		t.Fatalf("terminal metric observation = %+v", finalResult.Observations)
	}
}

func TestProcessorCrashAfterRefundEffectRecoversWithLookupOnly(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)
	work := refund.RefundWork{
		SagaID: uuid.New(), RequestID: uuid.New(), OperationID: uuid.New(),
		PaymentIntentID: uuid.New(), ReservationID: uuid.New(), TicketOrderID: uuid.New(),
		TrainRunID: uuid.New(), OwnerID: uuid.New(), Provider: "stripe", ProviderPaymentID: "pi_crash",
		AmountMinor: 200, CapturedMinor: 1_000, RefundedBeforeMinor: 100, Currency: "TWD",
		Step: refund.StepRefundProvider, RequestFingerprint: refund.Hash{1}, TicketIDs: []uuid.UUID{uuid.New()},
	}
	store := &refundRuntimeStoreFake{work: work}
	client := &refundProviderFake{lookup: provider.RefundLookupResult{
		Found: true, Definitive: true, Refund: provider.OperationResult{
			ProviderPaymentID: "pi_crash", ProviderOperationID: "re_crash", Status: provider.StatusRefunded,
			AmountMinor: 200, Currency: "TWD",
		},
	}}
	processor, err := refund.NewProcessor(store, refund.Providers{"stripe": client}, &selectedRefundShardFake{}, refund.ProcessorConfig{
		WorkerID: "refund-worker-crash", BatchSize: 1, MaxAttempts: 8, LeaseTTL: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute, Region: "region-a", RegionalEpoch: 1,
		Now: func() time.Time { return now },
		TestAfterExternalEffect: func(point refund.ExternalEffectPoint, operationID uuid.UUID) {
			if point != refund.ExternalEffectProviderRefundCommitted || operationID != work.OperationID {
				t.Fatalf("unexpected crash barrier: point=%s operation=%s", point, operationID)
			}
			panic("simulated worker crash")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("provider effect did not reach the crash barrier")
			}
		}()
		_, _ = processor.RunOnce(context.Background())
	}()
	if client.refundCalls != 1 || client.queryCalls != 0 {
		t.Fatalf("crash pass called provider refund=%d lookup=%d, want 1/0", client.refundCalls, client.queryCalls)
	}

	// PostgreSQL Claim performs this transition atomically when the crashed
	// processing lease expires. The next processor pass must only query.
	store.work.Step = refund.StepQueryProvider
	if _, err := processor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.refundCalls != 1 || client.queryCalls != 1 || store.work.Step != refund.StepCompensateShard {
		t.Fatalf("recovery pass step=%s refund=%d lookup=%d, want compensate_shard/1/1", store.work.Step, client.refundCalls, client.queryCalls)
	}
}

func TestProcessorPreparesSelectedTicketsBeforeProviderCanBeClaimed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC)
	work := refund.RefundWork{
		SagaID: uuid.New(), RequestID: uuid.New(), OperationID: uuid.New(), PaymentIntentID: uuid.New(),
		ReservationID: uuid.New(), TicketOrderID: uuid.New(), TrainRunID: uuid.New(), OwnerID: uuid.New(),
		Provider: "stripe", ProviderPaymentID: "pi_prepare", AmountMinor: 200, CapturedMinor: 1_000,
		Currency: "TWD", Step: refund.StepValidate, RequestFingerprint: refund.Hash{1},
		TicketIDs: []uuid.UUID{uuid.New()}, RequestedAt: now.Add(-time.Minute), EligibilityCutoffAt: now.Add(time.Hour),
	}
	store := &refundRuntimeStoreFake{work: work}
	client := &refundProviderFake{}
	shard := &selectedRefundShardFake{}
	processor, err := refund.NewProcessor(store, refund.Providers{"stripe": client}, shard, refund.ProcessorConfig{
		WorkerID: "refund-worker-prepare", BatchSize: 1, MaxAttempts: 3, LeaseTTL: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute, Region: "region-a", RegionalEpoch: 4, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := processor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if shard.prepareCalls != 1 || client.refundCalls != 0 || store.work.Step != refund.StepRefundProvider {
		t.Fatalf("prepare=%d refund=%d step=%s", shard.prepareCalls, client.refundCalls, store.work.Step)
	}
}

func TestProcessorReleasesPreparedTicketsAfterDefiniteProviderFailure(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 8, 30, 0, 0, time.UTC)
	work := refund.RefundWork{
		SagaID: uuid.New(), RequestID: uuid.New(), OperationID: uuid.New(), PaymentIntentID: uuid.New(),
		ReservationID: uuid.New(), TicketOrderID: uuid.New(), TrainRunID: uuid.New(), OwnerID: uuid.New(),
		Provider: "stripe", ProviderPaymentID: "pi_release", AmountMinor: 200, CapturedMinor: 1_000,
		Currency: "TWD", Step: refund.StepRefundProvider, RequestFingerprint: refund.Hash{1},
		TicketIDs: []uuid.UUID{uuid.New()}, PrepareReceiptID: uuid.New(),
	}
	store := &refundRuntimeStoreFake{work: work}
	client := &refundProviderFake{refundErr: &provider.Error{Category: provider.ErrorPermanentValidation}}
	shard := &selectedRefundShardFake{}
	processor, err := refund.NewProcessor(store, refund.Providers{"stripe": client}, shard, refund.ProcessorConfig{
		WorkerID: "refund-worker-release", BatchSize: 1, MaxAttempts: 3, LeaseTTL: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute, Region: "region-a", RegionalEpoch: 4, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := processor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.refundCalls != 1 || shard.releaseCalls != 1 || !store.preparationFailed || store.manualReview {
		t.Fatalf("refund=%d release=%d failed=%t manual=%t", client.refundCalls, shard.releaseCalls, store.preparationFailed, store.manualReview)
	}
	if shard.releaseCommand.PrepareReceiptID != work.PrepareReceiptID || shard.releaseCommand.RequestFingerprint != work.RequestFingerprint {
		t.Fatalf("release is not bound to prepare evidence: %+v", shard.releaseCommand)
	}
}

func TestProcessorBoundsPrepareRetriesIndependently(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 8, 45, 0, 0, time.UTC)
	work := refund.RefundWork{
		SagaID: uuid.New(), RequestID: uuid.New(), OperationID: uuid.New(), PaymentIntentID: uuid.New(),
		ReservationID: uuid.New(), TicketOrderID: uuid.New(), TrainRunID: uuid.New(), OwnerID: uuid.New(),
		Provider: "stripe", ProviderPaymentID: "pi_prepare_retry", AmountMinor: 200, CapturedMinor: 1_000,
		Currency: "TWD", Step: refund.StepValidate, RequestFingerprint: refund.Hash{1}, TicketIDs: []uuid.UUID{uuid.New()},
		RequestedAt: now.Add(-time.Minute), EligibilityCutoffAt: now.Add(time.Hour),
	}
	store := &refundRuntimeStoreFake{work: work}
	shard := &selectedRefundShardFake{prepareErr: errors.New("temporary shard outage")}
	processor, err := refund.NewProcessor(store, refund.Providers{"stripe": &refundProviderFake{}}, shard, refund.ProcessorConfig{
		WorkerID: "refund-worker-prepare-retry", BatchSize: 1, MaxAttempts: 2, LeaseTTL: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute, Region: "region-a", RegionalEpoch: 4, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := processor.RunOnce(context.Background())
	if err != nil || first.Retried != 1 || store.work.PrepareAttempts != 1 || store.work.SagaAttempts != 0 || store.manualReview {
		t.Fatalf("first=%+v err=%v prepare_attempts=%d saga_attempts=%d manual=%t", first, err, store.work.PrepareAttempts, store.work.SagaAttempts, store.manualReview)
	}
	second, err := processor.RunOnce(context.Background())
	if err != nil || second.ManualReview != 1 || !store.manualReview || shard.prepareCalls != 2 {
		t.Fatalf("second=%+v err=%v manual=%t calls=%d", second, err, store.manualReview, shard.prepareCalls)
	}
}

func TestProcessorRepairsReleaseWithoutCallingProviderAgain(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 8, 55, 0, 0, time.UTC)
	work := refund.RefundWork{
		SagaID: uuid.New(), RequestID: uuid.New(), OperationID: uuid.New(), PaymentIntentID: uuid.New(),
		ReservationID: uuid.New(), TicketOrderID: uuid.New(), TrainRunID: uuid.New(), OwnerID: uuid.New(),
		Provider: "stripe", ProviderPaymentID: "pi_release_repair", AmountMinor: 200, CapturedMinor: 1_000,
		Currency: "TWD", Step: refund.StepReleasePrepared, RequestFingerprint: refund.Hash{1},
		TicketIDs: []uuid.UUID{uuid.New()}, PrepareReceiptID: uuid.New(), AbortReason: "provider_refund_failed",
	}
	store := &refundRuntimeStoreFake{work: work}
	client := &refundProviderFake{}
	shard := &selectedRefundShardFake{releaseErr: errors.New("temporary shard outage")}
	processor, err := refund.NewProcessor(store, refund.Providers{"stripe": client}, shard, refund.ProcessorConfig{
		WorkerID: "refund-worker-release-repair", BatchSize: 1, MaxAttempts: 3, LeaseTTL: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute, Region: "region-a", RegionalEpoch: 4, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := processor.RunOnce(context.Background())
	if err != nil || first.Retried != 1 || store.work.SagaAttempts != 1 || client.refundCalls != 0 {
		t.Fatalf("first=%+v err=%v attempts=%d provider_calls=%d", first, err, store.work.SagaAttempts, client.refundCalls)
	}
	shard.releaseErr = nil
	if _, err := processor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !store.preparationFailed || client.refundCalls != 0 || shard.releaseCalls != 2 {
		t.Fatalf("failed=%t provider_calls=%d release_calls=%d", store.preparationFailed, client.refundCalls, shard.releaseCalls)
	}
}

func TestProcessorSendsMismatchedExactRefundEvidenceToManualReview(t *testing.T) {
	t.Parallel()
	work := refund.RefundWork{
		SagaID: uuid.New(), RequestID: uuid.New(), OperationID: uuid.New(), PaymentIntentID: uuid.New(),
		ReservationID: uuid.New(), TicketOrderID: uuid.New(), TrainRunID: uuid.New(), OwnerID: uuid.New(),
		Provider: "stripe", ProviderPaymentID: "pi_2", AmountMinor: 200, CapturedMinor: 1_000,
		RefundedBeforeMinor: 100, Currency: "TWD", Step: refund.StepQueryProvider,
		RequestFingerprint: refund.Hash{1}, TicketIDs: []uuid.UUID{uuid.New()},
	}
	store := &refundRuntimeStoreFake{work: work}
	client := &refundProviderFake{lookup: provider.RefundLookupResult{Found: true, Definitive: true, Refund: provider.OperationResult{
		ProviderPaymentID: "pi_2", ProviderOperationID: "re_wrong_amount", Status: provider.StatusRefunded,
		AmountMinor: 250, Currency: "TWD",
	}}}
	shard := &selectedRefundShardFake{}
	processor, err := refund.NewProcessor(store, refund.Providers{"stripe": client}, shard, refund.ProcessorConfig{
		WorkerID: "refund-worker-2", BatchSize: 1, MaxAttempts: 3, LeaseTTL: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute, Region: "region-b", RegionalEpoch: 7, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := processor.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !store.manualReview || shard.calls != 0 {
		t.Fatalf("manual_review=%t shard calls=%d", store.manualReview, shard.calls)
	}
	if len(result.Observations) != 1 || result.Observations[0].Result != "manual_review" ||
		result.Observations[0].Provider != "stripe" || result.Observations[0].Currency != "TWD" {
		t.Fatalf("manual-review metric observation = %+v", result.Observations)
	}
}

func TestProcessorDoesNotCreditTwoEqualRefundsFromOneProviderOperation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	operationA, operationB := uuid.New(), uuid.New()
	requestA, requestB := uuid.New(), uuid.New()
	client := &refundProviderFake{lookupByOperation: map[string]provider.RefundLookupResult{
		operationA.String(): {Found: true, Definitive: true, Refund: provider.OperationResult{
			ProviderPaymentID: "pi_shared", ProviderOperationID: "re_only_a", Status: provider.StatusRefunded,
			AmountMinor: 200, Currency: "TWD",
		}},
		operationB.String(): {Found: false, Definitive: true},
	}}
	newWork := func(requestID, operationID uuid.UUID) refund.RefundWork {
		return refund.RefundWork{
			SagaID: uuid.New(), RequestID: requestID, OperationID: operationID, PaymentIntentID: uuid.New(),
			ReservationID: uuid.New(), TicketOrderID: uuid.New(), TrainRunID: uuid.New(), OwnerID: uuid.New(),
			Provider: "stripe", ProviderPaymentID: "pi_shared", AmountMinor: 200, CapturedMinor: 1_000,
			RefundedBeforeMinor: 0, Currency: "TWD", Step: refund.StepQueryProvider,
			RequestFingerprint: refund.Hash{1}, TicketIDs: []uuid.UUID{uuid.New()},
		}
	}
	stores := []*refundRuntimeStoreFake{{work: newWork(requestA, operationA)}, {work: newWork(requestB, operationB)}}
	processors := make([]*refund.Processor, 0, len(stores))
	for index, store := range stores {
		processor, err := refund.NewProcessor(store, refund.Providers{"stripe": client}, &selectedRefundShardFake{}, refund.ProcessorConfig{
			WorkerID: "refund-worker-equal-" + string(rune('a'+index)), BatchSize: 1, MaxAttempts: 3,
			LeaseTTL: time.Minute, RetryBase: time.Second, RetryMax: time.Minute,
			Region: "region-a", RegionalEpoch: 9, Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		processors = append(processors, processor)
	}

	results := make(chan error, len(processors))
	for _, processor := range processors {
		processor := processor
		go func() {
			_, err := processor.RunOnce(context.Background())
			results <- err
		}()
	}
	for range processors {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if stores[0].work.Step != refund.StepCompensateShard {
		t.Fatalf("matching refund step = %s", stores[0].work.Step)
	}
	if stores[1].work.Step != refund.StepRefundProvider {
		t.Fatalf("absent refund step = %s, want refund_provider", stores[1].work.Step)
	}
}

func TestProcessorRefusesRefundWhenExactLookupCapabilityIsUnavailable(t *testing.T) {
	t.Parallel()
	work := refund.RefundWork{
		SagaID: uuid.New(), RequestID: uuid.New(), OperationID: uuid.New(), PaymentIntentID: uuid.New(),
		ReservationID: uuid.New(), TicketOrderID: uuid.New(), TrainRunID: uuid.New(), OwnerID: uuid.New(),
		Provider: "legacy", ProviderPaymentID: "pay_legacy", AmountMinor: 200, CapturedMinor: 1_000,
		Currency: "TWD", Step: refund.StepRefundProvider, RequestFingerprint: refund.Hash{1},
		TicketIDs: []uuid.UUID{uuid.New()},
	}
	store := &refundRuntimeStoreFake{work: work}
	client := &refundProviderWithoutLookup{}
	_, err := refund.NewProcessor(store, refund.Providers{"legacy": client}, &selectedRefundShardFake{}, refund.ProcessorConfig{
		WorkerID: "refund-worker-no-lookup", BatchSize: 1, MaxAttempts: 3, LeaseTTL: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute, Region: "region-a", RegionalEpoch: 9, Now: time.Now,
	})
	if !errors.Is(err, refund.ErrInvalidService) || client.refundCalls != 0 {
		t.Fatalf("NewProcessor error=%v refund_calls=%d", err, client.refundCalls)
	}
}

type refundRuntimeStoreFake struct {
	work              refund.RefundWork
	manualReview      bool
	finalized         bool
	preparationFailed bool
}

func (fake *refundRuntimeStoreFake) Claim(_ context.Context, claim refund.RefundClaim) ([]refund.RefundWork, error) {
	if fake.work.Step == refund.StepComplete || fake.manualReview {
		return nil, nil
	}
	fake.work.LeaseOwner = claim.WorkerID
	return []refund.RefundWork{fake.work}, nil
}

func (fake *refundRuntimeStoreFake) AdvanceValidation(_ context.Context, work refund.RefundWork, receipt paymentshard.SelectedTicketRefundPrepareReceipt, _ time.Time) error {
	fake.work.Step = refund.StepRefundProvider
	fake.work.PrepareReceiptID = receipt.ReceiptID
	return nil
}

func (fake *refundRuntimeStoreFake) MarkPrepareRetry(_ context.Context, _ refund.RefundWork, _ string, _ time.Time, _ time.Time) error {
	fake.work.PrepareAttempts++
	return nil
}

func (fake *refundRuntimeStoreFake) MarkPreparationFailed(_ context.Context, _ refund.RefundWork, _ paymentshard.SelectedTicketRefundReleaseReceipt, _ string, _ time.Time) error {
	fake.preparationFailed = true
	fake.work.Step = refund.StepComplete
	return nil
}

func (fake *refundRuntimeStoreFake) BeginPreparationAbort(_ context.Context, _ refund.RefundWork, reason string, _ time.Time) error {
	fake.work.Step = refund.StepReleasePrepared
	fake.work.AbortReason = reason
	return nil
}

func (fake *refundRuntimeStoreFake) MarkReleaseRetry(_ context.Context, _ refund.RefundWork, _ string, _ time.Time, _ time.Time) error {
	fake.work.SagaAttempts++
	return nil
}

func (fake *refundRuntimeStoreFake) BeginProviderAttempt(context.Context, refund.RefundWork, time.Time) error {
	return nil
}

func (fake *refundRuntimeStoreFake) MarkProviderUncertain(_ context.Context, _ refund.RefundWork, _ string, _ time.Time) error {
	fake.work.Step = refund.StepQueryProvider
	return nil
}

func (fake *refundRuntimeStoreFake) MarkProviderRetry(_ context.Context, _ refund.RefundWork, _ string, _ time.Time) error {
	return nil
}

func (fake *refundRuntimeStoreFake) MarkProviderNotApplied(_ context.Context, _ refund.RefundWork, _ refund.Hash, _ time.Time) error {
	fake.work.Step = refund.StepRefundProvider
	return nil
}

func (fake *refundRuntimeStoreFake) MarkProviderSucceeded(_ context.Context, _ refund.RefundWork, evidence refund.ProviderRefundEvidence, _ time.Time) error {
	fake.work.Step = refund.StepCompensateShard
	fake.work.ResponseFingerprint = evidence.Fingerprint
	return nil
}

func (fake *refundRuntimeStoreFake) BeginShardAttempt(context.Context, refund.RefundWork, time.Time) error {
	return nil
}

func (fake *refundRuntimeStoreFake) MarkShardRetry(_ context.Context, _ refund.RefundWork, _ string, _ time.Time) error {
	return nil
}

func (fake *refundRuntimeStoreFake) MarkShardSucceeded(_ context.Context, _ refund.RefundWork, _ paymentshard.SelectedTicketRefundReceipt, _ time.Time) error {
	fake.work.Step = refund.StepFinalize
	return nil
}

func (fake *refundRuntimeStoreFake) Finalize(_ context.Context, _ refund.RefundWork, _ time.Time) error {
	fake.work.Step = refund.StepComplete
	fake.finalized = true
	return nil
}

func (fake *refundRuntimeStoreFake) MarkManualReview(_ context.Context, _ refund.RefundWork, _ string, _ refund.Hash, _ time.Time) error {
	fake.manualReview = true
	return nil
}

type refundProviderFake struct {
	mu                      sync.Mutex
	refundErr               error
	lookup                  provider.RefundLookupResult
	lookupByOperation       map[string]provider.RefundLookupResult
	refundCalls, queryCalls int
}

func (fake *refundProviderFake) Refund(_ context.Context, request provider.RefundRequest) (provider.OperationResult, error) {
	fake.refundCalls++
	if fake.refundErr != nil {
		return provider.OperationResult{}, fake.refundErr
	}
	return provider.OperationResult{ProviderPaymentID: request.ProviderPaymentID, ProviderOperationID: "re_1", Status: provider.StatusRefunded, AmountMinor: request.AmountMinor, Currency: request.Currency}, nil
}

func (fake *refundProviderFake) LookupRefund(_ context.Context, request provider.RefundLookupRequest) (provider.RefundLookupResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.queryCalls++
	if fake.lookupByOperation != nil {
		return fake.lookupByOperation[request.Metadata["refund_operation_id"]], nil
	}
	return fake.lookup, nil
}

func (*refundProviderFake) GetPaymentStatus(context.Context, string) (provider.Payment, error) {
	return provider.Payment{}, errors.New("aggregate payment status must not authorize a partial refund")
}

type refundProviderWithoutLookup struct{ refundCalls int }

func (fake *refundProviderWithoutLookup) Refund(context.Context, provider.RefundRequest) (provider.OperationResult, error) {
	fake.refundCalls++
	return provider.OperationResult{}, nil
}

func (*refundProviderWithoutLookup) GetPaymentStatus(context.Context, string) (provider.Payment, error) {
	return provider.Payment{}, nil
}

type selectedRefundShardFake struct {
	calls          int
	prepareCalls   int
	releaseCalls   int
	prepareErr     error
	releaseErr     error
	command        paymentshard.ApplySelectedTicketRefundCommand
	releaseCommand paymentshard.ReleaseSelectedTicketRefundCommand
}

func (fake *selectedRefundShardFake) PrepareSelectedTicketRefund(_ context.Context, command paymentshard.PrepareSelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundPrepareReceipt, error) {
	fake.prepareCalls++
	if fake.prepareErr != nil {
		return paymentshard.SelectedTicketRefundPrepareReceipt{}, fake.prepareErr
	}
	return paymentshard.SelectedTicketRefundPrepareReceipt{
		ReceiptID: uuid.NewSHA1(command.RefundRequestID, []byte("prepare-receipt")), CommandID: command.CommandID,
		RefundRequestID: command.RefundRequestID, RefundOperationID: command.RefundOperationID,
		PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID,
		TicketOrderID: command.TicketOrderID, TrainRunID: command.TrainRunID,
		AmountMinor: command.AmountMinor, Currency: command.Currency,
		RequestFingerprint:  command.RequestFingerprint,
		SelectedTicketCount: len(command.TicketIDs), PreparedAt: command.PreparedAt,
	}, nil
}

func (fake *selectedRefundShardFake) ReleaseSelectedTicketRefund(_ context.Context, command paymentshard.ReleaseSelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundReleaseReceipt, error) {
	fake.releaseCalls++
	fake.releaseCommand = command
	if fake.releaseErr != nil {
		return paymentshard.SelectedTicketRefundReleaseReceipt{}, fake.releaseErr
	}
	return paymentshard.SelectedTicketRefundReleaseReceipt{
		ReceiptID:        uuid.NewSHA1(command.PrepareReceiptID, []byte("release-receipt")),
		PrepareReceiptID: command.PrepareReceiptID, CommandID: command.CommandID,
		RefundRequestID: command.RefundRequestID, RefundOperationID: command.RefundOperationID,
		TrainRunID: command.TrainRunID, RequestFingerprint: command.RequestFingerprint,
		ReleasedTicketCount: len(command.TicketIDs), ReleasedAt: command.ReleasedAt,
	}, nil
}

func (fake *selectedRefundShardFake) ApplySelectedTicketRefund(_ context.Context, command paymentshard.ApplySelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundReceipt, error) {
	fake.calls++
	fake.command = command
	if command.ProviderProofHash == [32]byte{} {
		return paymentshard.SelectedTicketRefundReceipt{}, errors.New("missing provider proof")
	}
	return paymentshard.SelectedTicketRefundReceipt{
		CommandID: command.CommandID, RefundRequestID: command.RefundRequestID,
		RefundOperationID: command.RefundOperationID, PaymentIntentID: command.PaymentIntentID,
		ReservationID: command.ReservationID, TicketOrderID: command.TicketOrderID,
		TrainRunID: command.TrainRunID, AmountMinor: command.AmountMinor, Currency: command.Currency,
		SelectedTicketCount: len(command.TicketIDs), ReleasedSeatCount: len(command.TicketIDs),
		ResultingActiveTicketCount: 1, ResultingOrderState: "partially_refunded", CommittedAt: command.RefundedAt,
	}, nil
}
