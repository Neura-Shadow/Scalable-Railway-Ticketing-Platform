package refund

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
)

type RefundStep string

const (
	StepValidate        RefundStep = "validate"
	StepRefundProvider  RefundStep = "refund_provider"
	StepQueryProvider   RefundStep = "query_provider"
	StepReleasePrepared RefundStep = "release_prepared"
	StepCompensateShard RefundStep = "compensate_shard"
	StepFinalize        RefundStep = "finalize"
	StepComplete        RefundStep = "complete"
)

type RefundWork struct {
	SagaID              uuid.UUID
	RequestID           uuid.UUID
	OperationID         uuid.UUID
	PaymentIntentID     uuid.UUID
	ReservationID       uuid.UUID
	TicketOrderID       uuid.UUID
	TrainRunID          uuid.UUID
	OwnerID             uuid.UUID
	Provider            string
	ProviderPaymentID   string
	ProviderRefundID    string
	AmountMinor         int64
	CapturedMinor       int64
	RefundedBeforeMinor int64
	Currency            string
	Step                RefundStep
	SagaAttempts        int
	PrepareAttempts     int
	OperationAttempts   int
	LeaseOwner          string
	RequestFingerprint  Hash
	ResponseFingerprint Hash
	TicketIDs           []uuid.UUID
	RequestedAt         time.Time
	EligibilityCutoffAt time.Time
	PrepareReceiptID    uuid.UUID
	AbortReason         string
}

type RefundClaim struct {
	WorkerID string
	Limit    int
	Now      time.Time
	LeaseTTL time.Duration
}

type ProviderRefundEvidence struct {
	ProviderRefundID string
	CapturedMinor    int64
	RefundedMinor    int64
	Fingerprint      Hash
}

type RuntimeStore interface {
	Claim(context.Context, RefundClaim) ([]RefundWork, error)
	AdvanceValidation(context.Context, RefundWork, paymentshard.SelectedTicketRefundPrepareReceipt, time.Time) error
	MarkPrepareRetry(context.Context, RefundWork, string, time.Time, time.Time) error
	MarkPreparationFailed(context.Context, RefundWork, paymentshard.SelectedTicketRefundReleaseReceipt, string, time.Time) error
	BeginPreparationAbort(context.Context, RefundWork, string, time.Time) error
	MarkReleaseRetry(context.Context, RefundWork, string, time.Time, time.Time) error
	BeginProviderAttempt(context.Context, RefundWork, time.Time) error
	MarkProviderUncertain(context.Context, RefundWork, string, time.Time) error
	MarkProviderRetry(context.Context, RefundWork, string, time.Time) error
	MarkProviderNotApplied(context.Context, RefundWork, Hash, time.Time) error
	MarkProviderSucceeded(context.Context, RefundWork, ProviderRefundEvidence, time.Time) error
	BeginShardAttempt(context.Context, RefundWork, time.Time) error
	MarkShardRetry(context.Context, RefundWork, string, time.Time) error
	MarkShardSucceeded(context.Context, RefundWork, paymentshard.SelectedTicketRefundReceipt, time.Time) error
	Finalize(context.Context, RefundWork, time.Time) error
	MarkManualReview(context.Context, RefundWork, string, Hash, time.Time) error
}

type RefundProvider interface {
	Refund(context.Context, provider.RefundRequest) (provider.OperationResult, error)
	GetPaymentStatus(context.Context, string) (provider.Payment, error)
}

type Providers map[string]RefundProvider

type SelectedRefundShard interface {
	PrepareSelectedTicketRefund(context.Context, paymentshard.PrepareSelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundPrepareReceipt, error)
	ReleaseSelectedTicketRefund(context.Context, paymentshard.ReleaseSelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundReleaseReceipt, error)
	ApplySelectedTicketRefund(context.Context, paymentshard.ApplySelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundReceipt, error)
}

type ProcessorConfig struct {
	WorkerID      string
	BatchSize     int
	MaxAttempts   int
	LeaseTTL      time.Duration
	RetryBase     time.Duration
	RetryMax      time.Duration
	Region        string
	RegionalEpoch int64
	Now           func() time.Time
	// TestAfterExternalEffect is only supplied by the test-gated worker binary.
	// It observes the exact gap after external I/O and before control finalization.
	TestAfterExternalEffect func(ExternalEffectPoint, uuid.UUID)
}

type ExternalEffectPoint string

const (
	ExternalEffectProviderRefundCommitted ExternalEffectPoint = "partial_refund_provider_committed"
	ExternalEffectShardRefundCommitted    ExternalEffectPoint = "partial_refund_shard_committed"
)

type ProcessorResult struct {
	Claimed      int
	Completed    int
	Retried      int
	ManualReview int
	Failures     int
	Observations []MetricObservation
}

type MetricObservation struct {
	Provider string
	Result   string
	Reason   string
	Currency string
}

type Processor struct {
	store     RuntimeStore
	providers Providers
	shard     SelectedRefundShard
	config    ProcessorConfig
}

var workerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var providerIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func NewProcessor(store RuntimeStore, providers Providers, shard SelectedRefundShard, config ProcessorConfig) (*Processor, error) {
	if store == nil || len(providers) == 0 || shard == nil || !workerIDPattern.MatchString(config.WorkerID) ||
		config.BatchSize <= 0 || config.BatchSize > 100 || config.MaxAttempts <= 0 || config.MaxAttempts > 1000 ||
		config.LeaseTTL <= 0 || config.RetryBase <= 0 || config.RetryMax < config.RetryBase ||
		(config.Region != "region-a" && config.Region != "region-b") || config.RegionalEpoch <= 0 {
		return nil, ErrInvalidService
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	for name, client := range providers {
		if name == "" || client == nil {
			return nil, ErrInvalidService
		}
		if _, ok := client.(provider.RefundLookupReader); !ok {
			return nil, ErrInvalidService
		}
	}
	return &Processor{store: store, providers: providers, shard: shard, config: config}, nil
}

func (processor *Processor) RunOnce(ctx context.Context) (ProcessorResult, error) {
	if processor == nil || processor.store == nil || ctx == nil {
		return ProcessorResult{}, ErrInvalidService
	}
	now := processor.config.Now().UTC()
	if now.IsZero() {
		return ProcessorResult{}, ErrInvalidService
	}
	work, err := processor.store.Claim(ctx, RefundClaim{
		WorkerID: processor.config.WorkerID, Limit: processor.config.BatchSize, Now: now, LeaseTTL: processor.config.LeaseTTL,
	})
	if err != nil {
		return ProcessorResult{}, err
	}
	result := ProcessorResult{Claimed: len(work)}
	var failures []error
	for _, item := range work {
		outcome, processErr := processor.process(ctx, item, now)
		switch outcome {
		case "completed":
			result.Completed++
		case "retry":
			result.Retried++
		case "manual_review":
			result.ManualReview++
		}
		if processErr != nil {
			result.Failures++
			failures = append(failures, processErr)
		}
		if processErr == nil && item.Step == StepFinalize && outcome == "completed" {
			result.Observations = append(result.Observations, MetricObservation{
				Provider: item.Provider, Result: "success", Reason: "none", Currency: item.Currency,
			})
		} else if processErr == nil && outcome == "manual_review" {
			result.Observations = append(result.Observations, MetricObservation{
				Provider: item.Provider, Result: "manual_review", Reason: "manual_review", Currency: item.Currency,
			})
		}
	}
	return result, errors.Join(failures...)
}

func (processor *Processor) process(ctx context.Context, work RefundWork, now time.Time) (string, error) {
	if err := validateRefundWork(work, processor.config.WorkerID); err != nil {
		fingerprint := boundedEvidence("invalid_work", work.RequestID, work.OperationID)
		return "manual_review", processor.store.MarkManualReview(ctx, work, "invalid_work", fingerprint, now)
	}
	switch work.Step {
	case StepValidate:
		return processor.prepareShard(ctx, work, now)
	case StepRefundProvider:
		return processor.refundProvider(ctx, work, now)
	case StepQueryProvider:
		return processor.queryProvider(ctx, work, now)
	case StepReleasePrepared:
		return processor.abortPrepared(ctx, work, work.AbortReason, now)
	case StepCompensateShard:
		return processor.compensateShard(ctx, work, now)
	case StepFinalize:
		return "completed", processor.store.Finalize(ctx, work, now)
	default:
		fingerprint := boundedEvidence("invalid_step", work.RequestID, work.OperationID)
		return "manual_review", processor.store.MarkManualReview(ctx, work, "invalid_step", fingerprint, now)
	}
}

func (processor *Processor) prepareShard(ctx context.Context, work RefundWork, now time.Time) (string, error) {
	command := paymentshard.PrepareSelectedTicketRefundCommand{
		CommandID:       uuid.NewSHA1(work.RequestID, []byte("selected-ticket-refund-prepare-command")),
		RefundRequestID: work.RequestID, RefundOperationID: work.OperationID,
		PaymentIntentID: work.PaymentIntentID, ReservationID: work.ReservationID,
		TicketOrderID: work.TicketOrderID, TrainRunID: work.TrainRunID, OwnerID: work.OwnerID,
		Region: processor.config.Region, RegionalEpoch: processor.config.RegionalEpoch,
		AmountMinor: work.AmountMinor, Currency: work.Currency, RequestFingerprint: work.RequestFingerprint,
		TicketIDs: append([]uuid.UUID(nil), work.TicketIDs...), RequestedAt: work.RequestedAt,
		EligibilityCutoffAt: work.EligibilityCutoffAt, PreparedAt: now,
	}
	receipt, err := processor.shard.PrepareSelectedTicketRefund(ctx, command)
	if err != nil {
		if work.PrepareAttempts+1 >= processor.config.MaxAttempts {
			return processor.manual(ctx, work, "prepare_attempts_exhausted", boundedEvidence("prepare_attempts_exhausted", work.RequestID, work.OperationID), now)
		}
		return "retry", processor.store.MarkPrepareRetry(ctx, work, "prepare_retryable", processor.nextAttempt(work.RequestID, work.PrepareAttempts+1, now), now)
	}
	if !validPrepareReceipt(command, receipt) {
		return processor.manual(ctx, work, "prepare_receipt_inconsistent", work.RequestFingerprint, now)
	}
	return "completed", processor.store.AdvanceValidation(ctx, work, receipt, now)
}

func (processor *Processor) refundProvider(ctx context.Context, work RefundWork, now time.Time) (string, error) {
	client, ok := processor.providers[work.Provider]
	if !ok {
		return processor.scheduleAbortPrepared(ctx, work, "provider_unavailable", now)
	}
	if work.OperationAttempts >= processor.config.MaxAttempts {
		return processor.scheduleAbortPrepared(ctx, work, "provider_attempts_exhausted", now)
	}
	if err := processor.store.BeginProviderAttempt(ctx, work, now); err != nil {
		return "", err
	}
	providerKey := "ticket-refund-" + work.OperationID.String()
	metadata := provider.Metadata{
		"refund_request_id":      work.RequestID.String(),
		"refund_operation_id":    work.OperationID.String(),
		"refund_idempotency_key": providerKey,
	}
	result, err := client.Refund(ctx, provider.RefundRequest{
		PaymentIntentID: work.PaymentIntentID.String(), ProviderPaymentID: work.ProviderPaymentID,
		AmountMinor: work.AmountMinor, Currency: work.Currency, IdempotencyKey: providerKey,
		Metadata: metadata,
	})
	if err != nil {
		var providerErr *provider.Error
		if errors.As(err, &providerErr) && providerErr.Uncertain {
			return "retry", processor.store.MarkProviderUncertain(ctx, work, boundedCategory(providerErr.Category), now)
		}
		if errors.As(err, &providerErr) && providerErr.Retryable && work.OperationAttempts+1 < processor.config.MaxAttempts {
			return "retry", processor.store.MarkProviderRetry(ctx, work, boundedCategory(providerErr.Category), processor.nextAttempt(work.OperationID, work.OperationAttempts+1, now))
		}
		return processor.scheduleAbortPrepared(ctx, work, "provider_refund_failed", now)
	}
	if result.ProviderPaymentID != work.ProviderPaymentID || !providerIdentityPattern.MatchString(result.ProviderOperationID) ||
		result.Status != provider.StatusRefunded || provider.EvaluateFinancialObservation(
		provider.FinancialExpectation{AmountMinor: work.AmountMinor, Currency: work.Currency},
		provider.FinancialObservation{Status: provider.StatusRefunded, AmountMinor: result.AmountMinor,
			Currency: result.Currency, CapturedMinor: result.AmountMinor, RefundedMinor: result.AmountMinor},
	) != nil {
		return processor.manual(ctx, work, "provider_evidence_inconsistent", hashOperationResult(result, work), now)
	}
	expected, ok := checkedAdd(work.RefundedBeforeMinor, work.AmountMinor)
	if !ok || expected > work.CapturedMinor {
		return processor.manual(ctx, work, "refund_total_inconsistent", hashOperationResult(result, work), now)
	}
	evidence := ProviderRefundEvidence{
		ProviderRefundID: result.ProviderOperationID, CapturedMinor: work.CapturedMinor,
		RefundedMinor: expected, Fingerprint: hashOperationResult(result, work),
	}
	if processor.config.TestAfterExternalEffect != nil {
		processor.config.TestAfterExternalEffect(ExternalEffectProviderRefundCommitted, work.OperationID)
	}
	return "completed", processor.store.MarkProviderSucceeded(ctx, work, evidence, now)
}

func (processor *Processor) scheduleAbortPrepared(ctx context.Context, work RefundWork, reason string, now time.Time) (string, error) {
	if err := processor.store.BeginPreparationAbort(ctx, work, reason, now); err != nil {
		return "", err
	}
	work.Step = StepReleasePrepared
	work.AbortReason = reason
	return processor.abortPrepared(ctx, work, reason, now)
}

func (processor *Processor) abortPrepared(ctx context.Context, work RefundWork, reason string, now time.Time) (string, error) {
	if work.PrepareReceiptID == uuid.Nil || reason == "" {
		return processor.manual(ctx, work, "prepare_binding_missing", boundedEvidence("prepare_binding_missing", work.RequestID, work.OperationID), now)
	}
	command := paymentshard.ReleaseSelectedTicketRefundCommand{
		CommandID:        uuid.NewSHA1(work.RequestID, []byte("selected-ticket-refund-prepare-release-command")),
		PrepareReceiptID: work.PrepareReceiptID, RefundRequestID: work.RequestID, RefundOperationID: work.OperationID,
		PaymentIntentID: work.PaymentIntentID, ReservationID: work.ReservationID, TicketOrderID: work.TicketOrderID,
		TrainRunID: work.TrainRunID, OwnerID: work.OwnerID, Region: processor.config.Region,
		RegionalEpoch: processor.config.RegionalEpoch, RequestFingerprint: work.RequestFingerprint,
		TicketIDs: append([]uuid.UUID(nil), work.TicketIDs...), ReleasedAt: now,
	}
	receipt, err := processor.shard.ReleaseSelectedTicketRefund(ctx, command)
	if err != nil {
		if work.SagaAttempts+1 >= processor.config.MaxAttempts {
			return processor.manual(ctx, work, "release_attempts_exhausted", boundedEvidence("release_attempts_exhausted", work.RequestID, work.OperationID), now)
		}
		return "retry", processor.store.MarkReleaseRetry(ctx, work, "release_retryable", processor.nextAttempt(work.RequestID, work.SagaAttempts+1, now), now)
	}
	if !validReleaseReceipt(command, receipt) {
		return processor.manual(ctx, work, "release_receipt_inconsistent", work.RequestFingerprint, now)
	}
	return "completed", processor.store.MarkPreparationFailed(ctx, work, receipt, reason, now)
}

func (processor *Processor) queryProvider(ctx context.Context, work RefundWork, now time.Time) (string, error) {
	client, ok := processor.providers[work.Provider]
	if !ok {
		return processor.manual(ctx, work, "provider_unavailable", boundedEvidence("provider_unavailable", work.RequestID, work.OperationID), now)
	}
	reader, ok := client.(provider.RefundLookupReader)
	if !ok {
		return processor.manual(ctx, work, "refund_lookup_unavailable", boundedEvidence("refund_lookup_unavailable", work.RequestID, work.OperationID), now)
	}
	if work.OperationAttempts >= processor.config.MaxAttempts {
		return processor.manual(ctx, work, "query_attempts_exhausted", boundedEvidence("query_attempts_exhausted", work.RequestID, work.OperationID), now)
	}
	if err := processor.store.BeginProviderAttempt(ctx, work, now); err != nil {
		return "", err
	}
	providerKey := "ticket-refund-" + work.OperationID.String()
	lookup, err := reader.LookupRefund(ctx, provider.RefundLookupRequest{
		PaymentIntentID: work.PaymentIntentID.String(), ProviderPaymentID: work.ProviderPaymentID,
		AmountMinor: work.AmountMinor, Currency: work.Currency, IdempotencyKey: providerKey, Limit: 100,
		Metadata: provider.Metadata{
			"refund_request_id":      work.RequestID.String(),
			"refund_operation_id":    work.OperationID.String(),
			"refund_idempotency_key": providerKey,
		},
	})
	if err != nil {
		return "retry", processor.store.MarkProviderRetry(ctx, work, "query_retryable", processor.nextAttempt(work.OperationID, work.OperationAttempts+1, now))
	}
	fingerprint := hashRefundLookup(lookup, work)
	if !lookup.Definitive {
		return "retry", processor.store.MarkProviderRetry(ctx, work, "query_incomplete", processor.nextAttempt(work.OperationID, work.OperationAttempts+1, now))
	}
	if !lookup.Found {
		return "retry", processor.store.MarkProviderNotApplied(ctx, work, fingerprint, now)
	}
	result := lookup.Refund
	if result.ProviderPaymentID != work.ProviderPaymentID || !providerIdentityPattern.MatchString(result.ProviderOperationID) ||
		result.Status != provider.StatusRefunded || result.AmountMinor != work.AmountMinor || result.Currency != work.Currency {
		return processor.manual(ctx, work, "provider_evidence_inconsistent", fingerprint, now)
	}
	expected, ok := checkedAdd(work.RefundedBeforeMinor, work.AmountMinor)
	if !ok || expected > work.CapturedMinor {
		return processor.manual(ctx, work, "refund_total_inconsistent", fingerprint, now)
	}
	evidence := ProviderRefundEvidence{
		ProviderRefundID: result.ProviderOperationID, CapturedMinor: work.CapturedMinor,
		RefundedMinor: expected, Fingerprint: fingerprint,
	}
	return "completed", processor.store.MarkProviderSucceeded(ctx, work, evidence, now)
}

func (processor *Processor) compensateShard(ctx context.Context, work RefundWork, now time.Time) (string, error) {
	if work.ResponseFingerprint == (Hash{}) {
		return processor.manual(ctx, work, "provider_proof_missing", boundedEvidence("provider_proof_missing", work.RequestID, work.OperationID), now)
	}
	if work.SagaAttempts >= processor.config.MaxAttempts {
		return processor.manual(ctx, work, "shard_attempts_exhausted", work.ResponseFingerprint, now)
	}
	if err := processor.store.BeginShardAttempt(ctx, work, now); err != nil {
		return "", err
	}
	command := paymentshard.ApplySelectedTicketRefundCommand{
		CommandID:       uuid.NewSHA1(work.RequestID, []byte("selected-ticket-refund-command")),
		RefundRequestID: work.RequestID, RefundOperationID: work.OperationID,
		PaymentIntentID: work.PaymentIntentID, ReservationID: work.ReservationID,
		TicketOrderID: work.TicketOrderID, TrainRunID: work.TrainRunID, OwnerID: work.OwnerID,
		Region: processor.config.Region, RegionalEpoch: processor.config.RegionalEpoch,
		AmountMinor: work.AmountMinor, Currency: work.Currency, ProviderProofHash: work.ResponseFingerprint,
		RequestFingerprint: work.RequestFingerprint, TicketIDs: append([]uuid.UUID(nil), work.TicketIDs...), RefundedAt: now,
	}
	receipt, err := processor.shard.ApplySelectedTicketRefund(ctx, command)
	if err != nil {
		if work.SagaAttempts+1 >= processor.config.MaxAttempts {
			return processor.manual(ctx, work, "shard_attempts_exhausted", work.ResponseFingerprint, now)
		}
		return "retry", processor.store.MarkShardRetry(ctx, work, "shard_retryable", processor.nextAttempt(work.RequestID, work.SagaAttempts+1, now))
	}
	if !validShardReceipt(command, receipt) {
		return processor.manual(ctx, work, "shard_receipt_inconsistent", work.ResponseFingerprint, now)
	}
	if processor.config.TestAfterExternalEffect != nil {
		processor.config.TestAfterExternalEffect(ExternalEffectShardRefundCommitted, work.OperationID)
	}
	return "completed", processor.store.MarkShardSucceeded(ctx, work, receipt, now)
}

func (processor *Processor) manual(ctx context.Context, work RefundWork, reason string, evidence Hash, now time.Time) (string, error) {
	return "manual_review", processor.store.MarkManualReview(ctx, work, reason, evidence, now)
}

func (processor *Processor) nextAttempt(id uuid.UUID, attempt int, now time.Time) time.Time {
	delay := processor.config.RetryBase
	for count := 1; count < attempt && delay < processor.config.RetryMax; count++ {
		if delay > processor.config.RetryMax/2 {
			delay = processor.config.RetryMax
			break
		}
		delay *= 2
	}
	if delay > processor.config.RetryMax {
		delay = processor.config.RetryMax
	}
	return now.Add(delay)
}

func validateRefundWork(work RefundWork, workerID string) error {
	if work.SagaID == uuid.Nil || work.RequestID == uuid.Nil || work.OperationID == uuid.Nil ||
		work.PaymentIntentID == uuid.Nil || work.ReservationID == uuid.Nil || work.TicketOrderID == uuid.Nil ||
		work.TrainRunID == uuid.Nil || work.OwnerID == uuid.Nil || work.LeaseOwner != workerID ||
		work.Provider == "" || !providerIdentityPattern.MatchString(work.ProviderPaymentID) ||
		work.AmountMinor <= 0 || work.CapturedMinor <= 0 || work.RefundedBeforeMinor < 0 ||
		work.RefundedBeforeMinor > work.CapturedMinor || !validCurrency(work.Currency) ||
		work.RequestFingerprint == (Hash{}) || len(work.TicketIDs) == 0 || len(work.TicketIDs) > 1000 {
		return ErrInvalidProcessorState
	}
	if work.Step == StepValidate && (work.RequestedAt.IsZero() || work.EligibilityCutoffAt.IsZero() || !work.RequestedAt.Before(work.EligibilityCutoffAt)) {
		return ErrInvalidProcessorState
	}
	for index, ticketID := range work.TicketIDs {
		if ticketID == uuid.Nil || (index > 0 && work.TicketIDs[index-1].String() >= ticketID.String()) {
			return ErrInvalidProcessorState
		}
	}
	return nil
}

func validPrepareReceipt(command paymentshard.PrepareSelectedTicketRefundCommand, receipt paymentshard.SelectedTicketRefundPrepareReceipt) bool {
	return receipt.ReceiptID != uuid.Nil && receipt.CommandID == command.CommandID &&
		receipt.RefundRequestID == command.RefundRequestID && receipt.RefundOperationID == command.RefundOperationID &&
		receipt.PaymentIntentID == command.PaymentIntentID && receipt.ReservationID == command.ReservationID &&
		receipt.TicketOrderID == command.TicketOrderID && receipt.TrainRunID == command.TrainRunID &&
		receipt.AmountMinor == command.AmountMinor && receipt.Currency == command.Currency &&
		receipt.RequestFingerprint == command.RequestFingerprint && receipt.SelectedTicketCount == len(command.TicketIDs) &&
		!receipt.PreparedAt.IsZero()
}

func validReleaseReceipt(command paymentshard.ReleaseSelectedTicketRefundCommand, receipt paymentshard.SelectedTicketRefundReleaseReceipt) bool {
	return receipt.ReceiptID != uuid.Nil && receipt.PrepareReceiptID == command.PrepareReceiptID &&
		receipt.CommandID == command.CommandID && receipt.RefundRequestID == command.RefundRequestID &&
		receipt.RefundOperationID == command.RefundOperationID && receipt.TrainRunID == command.TrainRunID &&
		receipt.RequestFingerprint == command.RequestFingerprint && receipt.ReleasedTicketCount == len(command.TicketIDs) &&
		!receipt.ReleasedAt.IsZero()
}

func validShardReceipt(command paymentshard.ApplySelectedTicketRefundCommand, receipt paymentshard.SelectedTicketRefundReceipt) bool {
	return receipt.CommandID == command.CommandID && receipt.RefundRequestID == command.RefundRequestID &&
		receipt.RefundOperationID == command.RefundOperationID && receipt.PaymentIntentID == command.PaymentIntentID &&
		receipt.ReservationID == command.ReservationID && receipt.TicketOrderID == command.TicketOrderID &&
		receipt.TrainRunID == command.TrainRunID && receipt.AmountMinor == command.AmountMinor &&
		receipt.Currency == command.Currency && receipt.SelectedTicketCount == len(command.TicketIDs) &&
		receipt.ReleasedSeatCount == len(command.TicketIDs) && receipt.ResultingActiveTicketCount >= 0 &&
		((receipt.ResultingOrderState == "refunded" && receipt.ResultingActiveTicketCount == 0) ||
			(receipt.ResultingOrderState == "partially_refunded" && receipt.ResultingActiveTicketCount > 0)) &&
		!receipt.CommittedAt.IsZero()
}

func hashOperationResult(result provider.OperationResult, work RefundWork) Hash {
	return marshalEvidence(struct {
		Kind, ProviderPaymentID, ProviderOperationID, Status, Currency string
		AmountMinor                                                    int64
		RequestID, OperationID                                         uuid.UUID
	}{"refund", result.ProviderPaymentID, result.ProviderOperationID, string(result.Status), result.Currency,
		result.AmountMinor, work.RequestID, work.OperationID})
}

func hashRefundLookup(lookup provider.RefundLookupResult, work RefundWork) Hash {
	return marshalEvidence(struct {
		Kind, ProviderPaymentID, ProviderRefundID, Status, Currency string
		AmountMinor                                                 int64
		Found, Definitive                                           bool
		RequestID, OperationID                                      uuid.UUID
	}{"refund_lookup", lookup.Refund.ProviderPaymentID, lookup.Refund.ProviderOperationID,
		string(lookup.Refund.Status), lookup.Refund.Currency, lookup.Refund.AmountMinor,
		lookup.Found, lookup.Definitive, work.RequestID, work.OperationID})
}

func boundedEvidence(reason string, ids ...uuid.UUID) Hash {
	return marshalEvidence(struct {
		Reason string
		IDs    []uuid.UUID
	}{reason, ids})
}

func marshalEvidence(value any) Hash {
	encoded, err := json.Marshal(value)
	if err != nil {
		return sha256.Sum256([]byte("refund-evidence-encoding-failed"))
	}
	return sha256.Sum256(encoded)
}

func boundedCategory(category provider.ErrorCategory) string {
	value := string(category)
	if value == "" || len(value) > 64 {
		return "provider_error"
	}
	return value
}

func checkedAdd(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right {
		return 0, false
	}
	return left + right, true
}
