// Package worker advances durable payment operations, verified webhook inbox
// rows, and shard-local ticket actions. Store calls are deliberately split
// into claim, I/O, and finalize phases so external provider and shard calls can
// never run inside a control-database transaction owned by this package.
package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
)

var (
	ErrInvalidConfiguration = errors.New("payment worker configuration is invalid")
	ErrStoreUnavailable     = errors.New("payment worker store unavailable")
	ErrLeaseLost            = errors.New("payment worker lease lost")
	ErrProviderUnavailable  = errors.New("payment provider unavailable")
)

type Config struct {
	WorkerID     string
	BatchSize    int
	MaxAttempts  int
	LeaseTTL     time.Duration
	RetryBase    time.Duration
	RetryMax     time.Duration
	MaxUncertain time.Duration
	Interval     time.Duration
	Now          func() time.Time
	// TestAfterExternalEffect is a deterministic test-only crash barrier. The
	// production binary only supplies it when APP_ENV=test and an exact target
	// identity is configured; it must run after external I/O and before control
	// finalization.
	TestAfterExternalEffect func(ExternalEffectPoint, uuid.UUID)
}

type ExternalEffectPoint string

const (
	ExternalEffectCaptureCommitted      ExternalEffectPoint = "capture_provider_committed"
	ExternalEffectTicketsCommitted      ExternalEffectPoint = "ticket_issue_shard_committed"
	ExternalEffectRefundCommitted       ExternalEffectPoint = "refund_provider_committed"
	ExternalEffectCompensationCommitted ExternalEffectPoint = "refund_compensation_shard_committed"
)

type ClaimOptions struct {
	WorkerID    string
	BatchSize   int
	MaxAttempts int
	LeaseTTL    time.Duration
	Now         time.Time
}

// OperationClaim contains only server-derived financial identity and bounded
// provider correlation values. ProviderIdempotencyKey is stable, opaque, and
// must not contain customer data or secret material.
type OperationClaim struct {
	OperationID            uuid.UUID
	PaymentIntentID        uuid.UUID
	ReservationID          uuid.UUID
	TrainRunID             uuid.UUID
	OwnerID                uuid.UUID
	Provider               string
	Type                   domain.OperationType
	PreviousState          domain.OperationState
	ProviderPaymentID      string
	HostedSessionReference string
	ProviderActionToken    string
	ProviderIdempotencyKey string
	AmountMinor            int64
	Currency               string
	Attempts               int
	CreatedAt              time.Time
	LeaseOwner             string
	LeaseUntil             time.Time
}

type OperationDisposition string

const (
	DispositionApplied    OperationDisposition = "applied"
	DispositionNotApplied OperationDisposition = "not_applied"
	DispositionUnknown    OperationDisposition = "unknown"
	DispositionConflict   OperationDisposition = "conflict"
)

type OperationEvidence struct {
	Disposition          OperationDisposition
	ProviderPaymentID    string
	ProviderOperationID  string
	HostedSessionRef     string
	Status               provider.Status
	AmountMinor          int64
	Currency             string
	CapturedMinor        int64
	RefundedMinor        int64
	ResponseFingerprint  [sha256.Size]byte
	ProviderObservedTime time.Time
}

type Failure struct {
	Category      string
	NextAttemptAt time.Time
	ManualReview  bool
	Uncertain     bool
	Compensate    bool
}

type FailureLane string

const (
	FailureLaneClaimOperations  FailureLane = "claim_operations"
	FailureLaneProcessOperation FailureLane = "process_operation"
	FailureLaneClaimWebhooks    FailureLane = "claim_webhooks"
	FailureLaneProcessWebhook   FailureLane = "process_webhook"
	FailureLaneClaimActions     FailureLane = "claim_actions"
	FailureLaneProcessAction    FailureLane = "process_action"
)

type FailureReason string

const (
	FailureReasonStoreUnavailable             FailureReason = "store_unavailable"
	FailureReasonLeaseLost                    FailureReason = "lease_lost"
	FailureReasonInvalidClaim                 FailureReason = "invalid_claim"
	FailureReasonProviderUnavailable          FailureReason = "provider_unavailable"
	FailureReasonProviderOutcomeUnknown       FailureReason = "provider_outcome_unknown"
	FailureReasonDatabaseFinalizeFailed       FailureReason = "database_finalize_failed"
	FailureReasonShardUnavailable             FailureReason = "shard_unavailable"
	FailureReasonRegionalAuthorityUnavailable FailureReason = "regional_authority_unavailable"
	FailureReasonConstraintRejected           FailureReason = "constraint_rejected"
	FailureReasonTimeout                      FailureReason = "timeout"
	FailureReasonUnknown                      FailureReason = "unknown"
)

// FailureSummary is safe for production logs and metrics. It deliberately
// retains no joined error, identifier, query, DSN, or provider payload.
type FailureSummary struct {
	Lane   FailureLane   `json:"lane"`
	Reason FailureReason `json:"reason"`
}

type WebhookClaim struct {
	InboxID           uuid.UUID
	Provider          string
	ProviderEventID   string
	EventType         provider.EventType
	ProviderPaymentID string
	EventCreatedAt    time.Time
	Attempts          int
	LeaseOwner        string
	LeaseUntil        time.Time
}

type WebhookEvidence struct {
	Status          provider.Status
	AmountMinor     int64
	Currency        string
	CapturedMinor   int64
	RefundedMinor   int64
	ProviderUpdated time.Time
}

type ActionType string

const (
	ActionIssueTickets      ActionType = "issue_tickets"
	ActionMarkRefundPending ActionType = "mark_refund_pending"
	ActionCancelVoided      ActionType = "cancel_voided_reservation"
	ActionCompensate        ActionType = "compensate"
)

type ActionClaim struct {
	ActionID     uuid.UUID
	SagaID       uuid.UUID
	Type         ActionType
	Provider     string
	Attempts     int
	LeaseOwner   string
	LeaseUntil   time.Time
	Issue        shard.IssueTicketsCommand
	MarkRefund   shard.MarkRefundPendingCommand
	CancelVoided shard.CancelVoidedReservationCommand
	Compensation shard.ApplyRefundCompensationCommand
}

type ActionEvidence struct {
	Issue        shard.IssueTicketsReceipt
	MarkRefund   shard.MarkRefundPendingReceipt
	CancelVoided shard.CancelVoidedReservationReceipt
	Compensation shard.ApplyRefundCompensationReceipt
}

// Store owns short control-plane transactions. Implementations must commit a
// claim before returning and must use compare-and-set lease ownership during
// every begin/finalize call.
type Store interface {
	ClaimOperations(context.Context, ClaimOptions) ([]OperationClaim, error)
	BeginOperation(context.Context, OperationClaim) error
	CompleteOperation(context.Context, OperationClaim, OperationEvidence) error
	SupersedeVoidWithRefund(context.Context, OperationClaim, OperationEvidence) error
	FailOperation(context.Context, OperationClaim, Failure) error

	ClaimWebhooks(context.Context, ClaimOptions) ([]WebhookClaim, error)
	CompleteWebhook(context.Context, WebhookClaim, WebhookEvidence) error
	IgnoreWebhook(context.Context, WebhookClaim) error
	FailWebhook(context.Context, WebhookClaim, Failure) error

	ClaimActions(context.Context, ClaimOptions) ([]ActionClaim, error)
	CompleteAction(context.Context, ActionClaim, ActionEvidence) error
	FailAction(context.Context, ActionClaim, Failure) error
}

type ProviderRegistry interface {
	Provider(string) (provider.Client, bool)
}

type ShardGateway interface {
	IssueTickets(context.Context, shard.IssueTicketsCommand) (shard.IssueTicketsReceipt, error)
	MarkRefundPending(context.Context, shard.MarkRefundPendingCommand) (shard.MarkRefundPendingReceipt, error)
	CancelVoidedReservation(context.Context, shard.CancelVoidedReservationCommand) (shard.CancelVoidedReservationReceipt, error)
	ApplyRefundCompensation(context.Context, shard.ApplyRefundCompensationCommand) (shard.ApplyRefundCompensationReceipt, error)
}

// MetricObservation contains only bounded dimensions and timing information.
// It deliberately excludes payment, reservation, provider-operation, and
// customer identifiers so metrics cannot become a correlation-data sink.
type MetricObservation struct {
	Lane, Provider, Operation, Result, Reason string
	Duration, Lag                             time.Duration
	Uncertain, Replay                         bool
}

type Metrics interface {
	RecordPaymentWorker(MetricObservation)
}

type Result struct {
	OperationsClaimed int
	OperationsDone    int
	WebhooksClaimed   int
	WebhooksDone      int
	ActionsClaimed    int
	ActionsDone       int
	Retried           int
	Compensating      int
	ManualReview      int
	Failures          int
	FailureSummaries  []FailureSummary
}
