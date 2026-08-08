package worker

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/google/uuid"
)

const maximumBatchSize = 1000

var workerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type Worker struct {
	store     Store
	providers ProviderRegistry
	shards    ShardGateway
	metrics   Metrics
	config    Config
}

func New(store Store, providers ProviderRegistry, shards ShardGateway, metrics Metrics, config Config) (*Worker, error) {
	if store == nil || providers == nil || shards == nil || !validConfig(config) {
		return nil, ErrInvalidConfiguration
	}
	return &Worker{store: store, providers: providers, shards: shards, metrics: metrics, config: config}, nil
}

func validConfig(config Config) bool {
	return workerIDPattern.MatchString(config.WorkerID) && config.BatchSize > 0 &&
		config.BatchSize <= maximumBatchSize && config.MaxAttempts > 0 &&
		config.MaxAttempts <= 1000 && config.LeaseTTL > 0 &&
		config.RetryBase > 0 && config.RetryMax >= config.RetryBase &&
		config.Interval > 0 && config.Now != nil
}

// RunOnce is deterministic for a fixed Store response and Config.Now. Each
// claim method must finish its short transaction before any external call is
// made; each item is then finalized in its own short transaction.
func (worker *Worker) RunOnce(ctx context.Context) (Result, error) {
	if worker == nil || ctx == nil {
		return Result{}, ErrInvalidConfiguration
	}
	now := worker.config.Now().UTC()
	if now.IsZero() {
		return Result{}, ErrInvalidConfiguration
	}
	options := ClaimOptions{
		WorkerID: worker.config.WorkerID, BatchSize: worker.config.BatchSize,
		MaxAttempts: worker.config.MaxAttempts, LeaseTTL: worker.config.LeaseTTL, Now: now,
	}
	var result Result
	var failures []error

	operations, err := worker.store.ClaimOperations(ctx, options)
	if err != nil {
		failures = append(failures, fmt.Errorf("claim payment operations: %w", err))
		worker.record("operation", "claim", "failure", "database")
	} else {
		result.OperationsClaimed = len(operations)
		for _, claim := range operations {
			if err := worker.processOperation(ctx, now, claim, &result); err != nil {
				failures = append(failures, err)
			}
		}
	}

	webhooks, err := worker.store.ClaimWebhooks(ctx, options)
	if err != nil {
		failures = append(failures, fmt.Errorf("claim payment webhooks: %w", err))
		worker.record("webhook", "claim", "failure", "database")
	} else {
		result.WebhooksClaimed = len(webhooks)
		for _, claim := range webhooks {
			if err := worker.processWebhook(ctx, now, claim, &result); err != nil {
				failures = append(failures, err)
			}
		}
	}

	actions, err := worker.store.ClaimActions(ctx, options)
	if err != nil {
		failures = append(failures, fmt.Errorf("claim payment actions: %w", err))
		worker.record("action", "claim", "failure", "database")
	} else {
		result.ActionsClaimed = len(actions)
		for _, claim := range actions {
			if err := worker.processAction(ctx, now, claim, &result); err != nil {
				failures = append(failures, err)
			}
		}
	}
	result.Failures = len(failures)
	return result, errors.Join(failures...)
}

func (worker *Worker) processOperation(ctx context.Context, now time.Time, claim OperationClaim, result *Result) error {
	if err := validateOperationClaim(claim, worker.config.WorkerID); err != nil {
		return worker.failInvalidOperation(ctx, now, claim, result)
	}
	var (
		evidence OperationEvidence
		err      error
	)
	if claim.PreviousState != domain.OperationUncertain {
		if err = worker.store.BeginOperation(ctx, claim); err != nil {
			worker.record("operation", string(claim.Type), "failure", "lease")
			return fmt.Errorf("begin payment operation: %w", err)
		}
	}
	client, ok := worker.providers.Provider(claim.Provider)
	if !ok {
		return worker.failOperation(ctx, now, claim, "provider_unavailable", true, result)
	}
	if claim.PreviousState == domain.OperationUncertain {
		// An uncertain side effect is never issued again until a read-only
		// provider query proves whether the original request took effect.
		evidence, err = queryUncertain(ctx, client, claim)
	} else {
		evidence, err = invokeProvider(ctx, client, claim)
	}
	if err != nil {
		category, retryable, uncertain := classifyProviderError(err)
		if uncertain {
			category = "provider_outcome_unknown"
		}
		return worker.failOperationWithUncertainty(ctx, now, claim, category, !retryable || claim.Attempts >= worker.config.MaxAttempts, uncertain, result)
	}
	if capturedInsteadOfVoided(claim, evidence) {
		if err := worker.store.SupersedeVoidWithRefund(ctx, claim, evidence); err != nil {
			worker.record("operation", string(claim.Type), "failure", "database")
			return fmt.Errorf("supersede void with refund: %w", err)
		}
		result.OperationsDone++
		worker.record("operation", string(claim.Type), "superseded", "captured")
		return nil
	}
	if claim.Type == domain.OperationVoid && claim.PreviousState == domain.OperationUncertain &&
		evidence.Status == provider.StatusUnknown {
		return worker.failOperation(ctx, now, claim, "provider_outcome_unknown", true, result)
	}
	switch evidence.Disposition {
	case DispositionApplied:
		if err := worker.store.CompleteOperation(ctx, claim, evidence); err != nil {
			worker.record("operation", string(claim.Type), "failure", "database")
			return fmt.Errorf("complete payment operation: %w", err)
		}
		result.OperationsDone++
		worker.record("operation", string(claim.Type), "success", "none")
		return nil
	case DispositionNotApplied:
		return worker.failOperation(ctx, now, claim, "provider_not_applied", claim.Attempts >= worker.config.MaxAttempts, result)
	case DispositionConflict:
		return worker.failOperation(ctx, now, claim, "provider_state_conflict", true, result)
	default:
		return worker.failOperation(ctx, now, claim, "provider_outcome_unknown", claim.Attempts >= worker.config.MaxAttempts, result)
	}
}

func capturedInsteadOfVoided(claim OperationClaim, evidence OperationEvidence) bool {
	return claim.Type == domain.OperationVoid && claim.PreviousState == domain.OperationUncertain &&
		evidence.Status == provider.StatusCaptured && evidence.ProviderPaymentID != "" &&
		claim.AmountMinor > 0 && evidence.AmountMinor == claim.AmountMinor && evidence.Currency == claim.Currency &&
		evidence.CapturedMinor == claim.AmountMinor && evidence.RefundedMinor == 0 &&
		evidence.ResponseFingerprint != [sha256.Size]byte{}
}

func (worker *Worker) failInvalidOperation(ctx context.Context, now time.Time, claim OperationClaim, result *Result) error {
	if claim.OperationID == uuid.Nil || claim.LeaseOwner == "" {
		return errors.New("invalid claimed payment operation")
	}
	if claim.PreviousState != domain.OperationUncertain {
		if err := worker.store.BeginOperation(ctx, claim); err != nil {
			return fmt.Errorf("begin invalid payment operation: %w", err)
		}
	}
	return worker.failOperation(ctx, now, claim, "invalid_claim", true, result)
}

func (worker *Worker) failOperation(ctx context.Context, now time.Time, claim OperationClaim, category string, review bool, result *Result) error {
	return worker.failOperationWithUncertainty(ctx, now, claim, category, review, false, result)
}

func (worker *Worker) failOperationWithUncertainty(ctx context.Context, now time.Time, claim OperationClaim, category string, review, uncertain bool, result *Result) error {
	failure := Failure{Category: boundedCategory(category), ManualReview: review, Uncertain: uncertain}
	if !review {
		failure.NextAttemptAt = now.Add(retryDelay(claim.OperationID, claim.Attempts, worker.config.RetryBase, worker.config.RetryMax))
	}
	if err := worker.store.FailOperation(ctx, claim, failure); err != nil {
		worker.record("operation", string(claim.Type), "failure", "database")
		return fmt.Errorf("fail payment operation: %w", err)
	}
	if review {
		result.ManualReview++
		worker.record("operation", string(claim.Type), "manual_review", failure.Category)
	} else {
		result.Retried++
		worker.record("operation", string(claim.Type), "retry", failure.Category)
	}
	return nil
}

func (worker *Worker) processWebhook(ctx context.Context, now time.Time, claim WebhookClaim, result *Result) error {
	if !validWebhookClaim(claim, worker.config.WorkerID) {
		return worker.failWebhook(ctx, now, claim, "invalid_claim", true, result)
	}
	if claim.EventType == provider.EventUnknown || !knownEvent(claim.EventType) {
		if err := worker.store.IgnoreWebhook(ctx, claim); err != nil {
			return fmt.Errorf("ignore payment webhook: %w", err)
		}
		result.WebhooksDone++
		worker.record("webhook", string(claim.EventType), "ignored", "unknown_event")
		return nil
	}
	client, ok := worker.providers.Provider(claim.Provider)
	if !ok {
		return worker.failWebhook(ctx, now, claim, "provider_unavailable", true, result)
	}
	// Event time and event order are not authoritative. A current provider
	// read confirms every recognized event before durable state advancement.
	payment, err := client.GetPaymentStatus(ctx, claim.ProviderPaymentID)
	if err != nil {
		category, retryable, uncertain := classifyProviderError(err)
		return worker.failWebhook(ctx, now, claim, category, !retryable && !uncertain || claim.Attempts >= worker.config.MaxAttempts, result)
	}
	evidence := WebhookEvidence{
		Status: payment.Status, AmountMinor: payment.AmountMinor, Currency: payment.Currency,
		CapturedMinor: payment.CapturedMinor, RefundedMinor: payment.RefundedMinor,
		ProviderUpdated: payment.ProviderUpdatedAt.UTC(),
	}
	if err := worker.store.CompleteWebhook(ctx, claim, evidence); err != nil {
		worker.record("webhook", string(claim.EventType), "failure", "database")
		failureErr := worker.failWebhook(ctx, now, claim, "database_finalize_failed", claim.Attempts >= worker.config.MaxAttempts, result)
		return errors.Join(fmt.Errorf("complete payment webhook: %w", err), failureErr)
	}
	result.WebhooksDone++
	worker.record("webhook", string(claim.EventType), "success", "provider_confirmed")
	return nil
}

func (worker *Worker) failWebhook(ctx context.Context, now time.Time, claim WebhookClaim, category string, review bool, result *Result) error {
	if claim.InboxID == uuid.Nil || claim.LeaseOwner == "" {
		return errors.New("invalid claimed payment webhook")
	}
	failure := Failure{Category: boundedCategory(category), ManualReview: review}
	if !review {
		failure.NextAttemptAt = now.Add(retryDelay(claim.InboxID, claim.Attempts, worker.config.RetryBase, worker.config.RetryMax))
	}
	if err := worker.store.FailWebhook(ctx, claim, failure); err != nil {
		return fmt.Errorf("fail payment webhook: %w", err)
	}
	if review {
		result.ManualReview++
	} else {
		result.Retried++
	}
	return nil
}

func (worker *Worker) processAction(ctx context.Context, now time.Time, claim ActionClaim, result *Result) error {
	if !validActionClaim(claim, worker.config.WorkerID) {
		return worker.failAction(ctx, now, claim, "invalid_claim", true, result)
	}
	var (
		evidence ActionEvidence
		err      error
	)
	switch claim.Type {
	case ActionIssueTickets:
		evidence.Issue, err = worker.shards.IssueTickets(ctx, claim.Issue)
	case ActionMarkRefundPending:
		evidence.MarkRefund, err = worker.shards.MarkRefundPending(ctx, claim.MarkRefund)
	case ActionCancelVoided:
		evidence.CancelVoided, err = worker.shards.CancelVoidedReservation(ctx, claim.CancelVoided)
	case ActionCompensate:
		evidence.Compensation, err = worker.shards.ApplyRefundCompensation(ctx, claim.Compensation)
	default:
		return worker.failAction(ctx, now, claim, "invalid_action", true, result)
	}
	if err != nil {
		review := claim.Attempts >= worker.config.MaxAttempts || permanentShardError(err)
		return worker.failAction(ctx, now, claim, "shard_command_failed", review, result)
	}
	if !validActionEvidence(claim, evidence) {
		return worker.failAction(ctx, now, claim, "shard_receipt_conflict", true, result)
	}
	if err := worker.store.CompleteAction(ctx, claim, evidence); err != nil {
		worker.record("action", string(claim.Type), "failure", "database")
		failureErr := worker.failAction(ctx, now, claim, "database_finalize_failed", claim.Attempts >= worker.config.MaxAttempts, result)
		return errors.Join(fmt.Errorf("complete payment action: %w", err), failureErr)
	}
	result.ActionsDone++
	worker.record("action", string(claim.Type), "success", "none")
	return nil
}

func (worker *Worker) failAction(ctx context.Context, now time.Time, claim ActionClaim, category string, review bool, result *Result) error {
	if claim.SagaID == uuid.Nil || claim.LeaseOwner == "" {
		return errors.New("invalid claimed payment action")
	}
	compensate := review && claim.Type == ActionIssueTickets
	failure := Failure{Category: boundedCategory(category), ManualReview: review && !compensate, Compensate: compensate}
	if !review {
		failure.NextAttemptAt = now.Add(retryDelay(claim.SagaID, claim.Attempts, worker.config.RetryBase, worker.config.RetryMax))
	}
	if err := worker.store.FailAction(ctx, claim, failure); err != nil {
		return fmt.Errorf("fail payment action: %w", err)
	}
	if compensate {
		result.Compensating++
	} else if review {
		result.ManualReview++
	} else {
		result.Retried++
	}
	return nil
}

func invokeProvider(ctx context.Context, client provider.Client, claim OperationClaim) (OperationEvidence, error) {
	metadata := provider.Metadata{
		"payment_intent_id": claim.PaymentIntentID.String(),
		"operation_id":      claim.OperationID.String(),
	}
	var evidence OperationEvidence
	switch claim.Type {
	case domain.OperationCreateCheckout:
		value, err := client.CreateCheckout(ctx, provider.CreateCheckoutRequest{
			PaymentIntentID: claim.PaymentIntentID.String(), MerchantReference: claim.PaymentIntentID.String(),
			AmountMinor: claim.AmountMinor, Currency: claim.Currency,
			IdempotencyKey: claim.ProviderIdempotencyKey, Metadata: metadata,
		})
		if err != nil {
			return evidence, err
		}
		evidence = OperationEvidence{
			ProviderPaymentID: value.ProviderPaymentID, HostedSessionRef: value.HostedReference,
			Status: value.Status, AmountMinor: value.AmountMinor, Currency: value.Currency,
		}
	case domain.OperationQueryStatus:
		return queryStatus(ctx, client, claim)
	case domain.OperationAuthorize:
		value, err := client.Authorize(ctx, provider.AuthorizeRequest{
			PaymentIntentID: claim.PaymentIntentID.String(), ProviderPaymentID: claim.ProviderPaymentID,
			SyntheticToken: claim.ProviderActionToken, AmountMinor: claim.AmountMinor, Currency: claim.Currency,
			IdempotencyKey: claim.ProviderIdempotencyKey, Metadata: metadata,
		})
		if err != nil {
			return evidence, err
		}
		evidence = operationResultEvidence(value)
	case domain.OperationCapture:
		value, err := client.Capture(ctx, provider.CaptureRequest{
			PaymentIntentID: claim.PaymentIntentID.String(), ProviderPaymentID: claim.ProviderPaymentID,
			AmountMinor: claim.AmountMinor, Currency: claim.Currency,
			IdempotencyKey: claim.ProviderIdempotencyKey, Metadata: metadata,
		})
		if err != nil {
			return evidence, err
		}
		evidence = operationResultEvidence(value)
	case domain.OperationVoid:
		value, err := client.Void(ctx, provider.VoidRequest{
			PaymentIntentID: claim.PaymentIntentID.String(), ProviderPaymentID: claim.ProviderPaymentID,
			IdempotencyKey: claim.ProviderIdempotencyKey, Metadata: metadata,
		})
		if err != nil {
			return evidence, err
		}
		evidence = operationResultEvidence(value)
	case domain.OperationRefund:
		value, err := client.Refund(ctx, provider.RefundRequest{
			PaymentIntentID: claim.PaymentIntentID.String(), ProviderPaymentID: claim.ProviderPaymentID,
			AmountMinor: claim.AmountMinor, Currency: claim.Currency,
			IdempotencyKey: claim.ProviderIdempotencyKey, Metadata: metadata,
		})
		if err != nil {
			return evidence, err
		}
		evidence = operationResultEvidence(value)
	default:
		return evidence, &provider.Error{Category: provider.ErrorPermanentValidation, Operation: "unknown", Message: "payment operation is invalid"}
	}
	evidence.Disposition = expectedDisposition(claim.Type, evidence.Status)
	evidence.ResponseFingerprint = fingerprintEvidence(evidence)
	return evidence, nil
}

func queryUncertain(ctx context.Context, client provider.Client, claim OperationClaim) (OperationEvidence, error) {
	if claim.ProviderPaymentID == "" {
		return OperationEvidence{Disposition: DispositionUnknown}, nil
	}
	evidence, err := queryStatus(ctx, client, claim)
	if err != nil {
		return evidence, err
	}
	evidence.Disposition = uncertainDisposition(claim.Type, evidence.Status)
	return evidence, nil
}

func queryStatus(ctx context.Context, client provider.Client, claim OperationClaim) (OperationEvidence, error) {
	value, err := client.GetPaymentStatus(ctx, claim.ProviderPaymentID)
	if err != nil {
		return OperationEvidence{}, err
	}
	evidence := OperationEvidence{
		Disposition: DispositionApplied, ProviderPaymentID: value.ProviderPaymentID,
		Status: value.Status, AmountMinor: value.AmountMinor, Currency: value.Currency,
		CapturedMinor: value.CapturedMinor, RefundedMinor: value.RefundedMinor,
		ProviderObservedTime: value.ProviderUpdatedAt.UTC(),
	}
	evidence.ResponseFingerprint = fingerprintEvidence(evidence)
	return evidence, nil
}

func operationResultEvidence(value provider.OperationResult) OperationEvidence {
	return OperationEvidence{
		ProviderPaymentID: value.ProviderPaymentID, ProviderOperationID: value.ProviderOperationID,
		Status: value.Status, AmountMinor: value.AmountMinor, Currency: value.Currency,
	}
}

func expectedDisposition(kind domain.OperationType, status provider.Status) OperationDisposition {
	switch kind {
	case domain.OperationCreateCheckout:
		if status == provider.StatusCreated || status == provider.StatusRequiresCustomerAction {
			return DispositionApplied
		}
	case domain.OperationQueryStatus:
		return DispositionApplied
	case domain.OperationAuthorize:
		if status == provider.StatusAuthorized {
			return DispositionApplied
		}
	case domain.OperationCapture:
		if status == provider.StatusCaptured {
			return DispositionApplied
		}
	case domain.OperationVoid:
		if status == provider.StatusVoided {
			return DispositionApplied
		}
	case domain.OperationRefund:
		if status == provider.StatusRefunded {
			return DispositionApplied
		}
	}
	if status == provider.StatusUnknown {
		return DispositionUnknown
	}
	return DispositionConflict
}

func uncertainDisposition(kind domain.OperationType, status provider.Status) OperationDisposition {
	if expectedDisposition(kind, status) == DispositionApplied {
		return DispositionApplied
	}
	switch kind {
	case domain.OperationAuthorize:
		if status == provider.StatusCreated || status == provider.StatusRequiresCustomerAction {
			return DispositionNotApplied
		}
	case domain.OperationCapture:
		if status == provider.StatusAuthorized {
			return DispositionNotApplied
		}
	case domain.OperationVoid:
		if status == provider.StatusCreated || status == provider.StatusRequiresCustomerAction || status == provider.StatusAuthorized {
			return DispositionNotApplied
		}
	case domain.OperationRefund:
		if status == provider.StatusCaptured {
			return DispositionNotApplied
		}
	case domain.OperationCreateCheckout:
		// Without a provider payment identity there is no safe status query;
		// with one, any non-created terminal state is conflicting evidence.
	}
	if status == provider.StatusUnknown {
		return DispositionUnknown
	}
	return DispositionConflict
}

func classifyProviderError(err error) (string, bool, bool) {
	var paymentError *provider.Error
	if !errors.As(err, &paymentError) {
		return "provider_unavailable", true, false
	}
	category := boundedCategory(string(paymentError.Category))
	return category, paymentError.Retryable, paymentError.Uncertain
}

func fingerprintEvidence(evidence OperationEvidence) [sha256.Size]byte {
	digest := sha256.New()
	for _, value := range []string{
		"payment-evidence-v1", evidence.ProviderPaymentID, evidence.ProviderOperationID,
		string(evidence.Status), evidence.Currency,
	} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = digest.Write(size[:])
		_, _ = digest.Write([]byte(value))
	}
	for _, value := range []int64{evidence.AmountMinor, evidence.CapturedMinor, evidence.RefundedMinor} {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(value))
		_, _ = digest.Write(encoded[:])
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func validateOperationClaim(claim OperationClaim, workerID string) error {
	if claim.OperationID == uuid.Nil || claim.PaymentIntentID == uuid.Nil || claim.ReservationID == uuid.Nil ||
		claim.TrainRunID == uuid.Nil || claim.OwnerID == uuid.Nil || claim.Provider == "" ||
		claim.ProviderIdempotencyKey == "" || claim.AmountMinor < 0 || len(claim.Currency) != 3 ||
		claim.Attempts < 1 || claim.LeaseOwner != workerID || claim.LeaseUntil.IsZero() {
		return errors.New("invalid claimed payment operation")
	}
	return nil
}

func validWebhookClaim(claim WebhookClaim, workerID string) bool {
	validIdentity := claim.InboxID != uuid.Nil && claim.Provider != "" && claim.ProviderEventID != "" &&
		claim.Attempts > 0 && claim.LeaseOwner == workerID && !claim.LeaseUntil.IsZero()
	return validIdentity && (!knownEvent(claim.EventType) || claim.ProviderPaymentID != "")
}

func validActionClaim(claim ActionClaim, workerID string) bool {
	return claim.SagaID != uuid.Nil && claim.Provider != "" && claim.Attempts > 0 && claim.LeaseOwner == workerID && !claim.LeaseUntil.IsZero()
}

func validActionEvidence(claim ActionClaim, evidence ActionEvidence) bool {
	switch claim.Type {
	case ActionIssueTickets:
		if evidence.Issue.TicketOrderID == uuid.Nil || len(evidence.Issue.TicketIDs) == 0 || evidence.Issue.IssuedAt.IsZero() {
			return false
		}
		seen := make(map[uuid.UUID]struct{}, len(evidence.Issue.TicketIDs))
		for _, ticketID := range evidence.Issue.TicketIDs {
			if ticketID == uuid.Nil {
				return false
			}
			if _, duplicate := seen[ticketID]; duplicate {
				return false
			}
			seen[ticketID] = struct{}{}
		}
		return evidence.Issue.CommandID == claim.Issue.CommandID &&
			evidence.Issue.IssuanceID == claim.Issue.IssuanceID &&
			evidence.Issue.PaymentIntentID == claim.Issue.PaymentIntentID &&
			evidence.Issue.ReservationID == claim.Issue.ReservationID &&
			evidence.Issue.AmountMinor == claim.Issue.AmountMinor &&
			evidence.Issue.Currency == claim.Issue.Currency
	case ActionMarkRefundPending:
		return evidence.MarkRefund.CommandID == claim.MarkRefund.CommandID &&
			evidence.MarkRefund.PaymentIntentID == claim.MarkRefund.PaymentIntentID &&
			evidence.MarkRefund.ReservationID == claim.MarkRefund.ReservationID
	case ActionCancelVoided:
		return evidence.CancelVoided.TicketOrderID != uuid.Nil &&
			evidence.CancelVoided.ReleasedSeatCount > 0 &&
			!evidence.CancelVoided.CancelledAt.IsZero() &&
			evidence.CancelVoided.CommandID == claim.CancelVoided.CommandID &&
			evidence.CancelVoided.VoidOperationID == claim.CancelVoided.VoidOperationID &&
			evidence.CancelVoided.PaymentIntentID == claim.CancelVoided.PaymentIntentID &&
			evidence.CancelVoided.ReservationID == claim.CancelVoided.ReservationID
	case ActionCompensate:
		return evidence.Compensation.TicketOrderID != uuid.Nil &&
			evidence.Compensation.CommandID == claim.Compensation.CommandID &&
			evidence.Compensation.CompensationID == claim.Compensation.CompensationID &&
			evidence.Compensation.PaymentIntentID == claim.Compensation.PaymentIntentID &&
			evidence.Compensation.ReservationID == claim.Compensation.ReservationID
	default:
		return false
	}
}

func knownEvent(eventType provider.EventType) bool {
	switch eventType {
	case provider.EventCheckoutCreated, provider.EventAuthorized, provider.EventCaptured, provider.EventVoided, provider.EventRefunded:
		return true
	default:
		return false
	}
}

func permanentShardError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "conflict") || strings.Contains(strings.ToLower(err.Error()), "not payable")
}

func boundedCategory(value string) string {
	value = strings.ToLower(value)
	var result strings.Builder
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' {
			result.WriteRune(character)
		} else if result.Len() > 0 && result.String()[result.Len()-1] != '_' {
			result.WriteByte('_')
		}
		if result.Len() == 64 {
			break
		}
	}
	bounded := strings.Trim(result.String(), "_")
	if bounded == "" || bounded[0] < 'a' || bounded[0] > 'z' {
		return "worker_failure"
	}
	return bounded
}

func (worker *Worker) record(lane, operation, result, reason string) {
	if worker.metrics != nil {
		worker.metrics.RecordPaymentWorker(lane, operation, result, boundedCategory(reason))
	}
}
