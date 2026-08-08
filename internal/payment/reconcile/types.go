// Package reconcile provides detect-first payment reconciliation. It compares
// bounded control, shard, and provider observations without owning financial or
// seat-inventory mutations.
package reconcile

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/google/uuid"
)

const (
	MaxBatchSize         = 1000
	MaxFindingsPerIntent = 64
)

var (
	ErrInvalidConfiguration = errors.New("payment reconciler configuration invalid")
	ErrInvalidRequest       = errors.New("payment reconciliation request invalid")
	ErrRepairConfirmation   = errors.New("explicit safe repair confirmation required")
	ErrRepairUnavailable    = errors.New("safe repair replay unavailable")
)

type Scope string

const (
	ScopeIntents    Scope = "payment-intents"
	ScopeOperations Scope = "payment-operations"
	ScopeWebhooks   Scope = "payment-webhooks"
	ScopeTickets    Scope = "payment-tickets"
	ScopeProvider   Scope = "payment-provider"
	ScopeAll        Scope = "payment-all"
)

func (scope Scope) Valid() bool {
	switch scope {
	case ScopeIntents, ScopeOperations, ScopeWebhooks, ScopeTickets, ScopeProvider, ScopeAll:
		return true
	default:
		return false
	}
}

type Config struct {
	BatchSize  int
	StaleAfter time.Duration
	ReviewDue  time.Duration
	Now        func() time.Time
}

type Options struct {
	Scope         Scope
	Limit         int
	Repair        bool
	ConfirmRepair bool
}

type Intent struct {
	ID                   uuid.UUID
	ReservationID        uuid.UUID
	TrainRunID           uuid.UUID
	Provider             string
	ProviderPaymentID    string
	State                string
	AmountMinor          int64
	Currency             string
	Fingerprint          [sha256.Size]byte
	ActiveForReservation int
}

type Saga struct {
	ID          uuid.UUID
	State       string
	ActiveCount int
}

type Operation struct {
	ID                  uuid.UUID
	Type                string
	State               string
	ProviderOperationID string
	AmountMinor         int64
	Currency            string
}

type ControlSnapshot struct {
	Intent                     Intent
	Saga                       Saga
	Operations                 []Operation
	DuplicateProviderEventIDs  int
	ProviderEventHashConflicts int
	OpenManualReviewCases      int
	ActiveReconciliationCases  int
}

type ShardSnapshot struct {
	Found                    bool
	DirectoryResolved        bool
	ReservationState         string
	ReservationAmountMinor   int64
	ReservationCurrency      string
	ReservationSeatCount     int
	TicketOrderFound         bool
	TicketOrderID            uuid.UUID
	TicketOrderState         string
	TicketOrderAmountMinor   int64
	TicketOrderCurrency      string
	IssuanceReceiptFound     bool
	IssuancePaymentIntentID  uuid.UUID
	ReceiptFingerprint       [sha256.Size]byte
	ActiveTicketCount        int
	RefundPendingTicketCount int
	CancelledTicketCount     int
	DuplicateTicketCodeCount int
	RecordedCommands         []RecordedCommand
}

type RecordedCommand struct {
	ID          uuid.UUID
	Kind        string
	Fingerprint [sha256.Size]byte
}

type Finding struct {
	Code               string    `json:"code"`
	Repairable         bool      `json:"repairable"`
	CommandID          uuid.UUID `json:"command_id,omitempty"`
	commandFingerprint [sha256.Size]byte
}

type Report struct {
	PaymentIntentID uuid.UUID `json:"payment_intent_id"`
	Scope           Scope     `json:"scope"`
	RowsExamined    int       `json:"rows_examined"`
	Findings        []Finding `json:"findings"`
	ProviderQueried bool      `json:"provider_queried"`
	RepairsApplied  int       `json:"repairs_applied"`
	Truncated       bool      `json:"truncated"`
}

type Result struct {
	Scope         Scope    `json:"scope"`
	ReadOnly      bool     `json:"read_only"`
	RowsExamined  int      `json:"rows_examined"`
	MismatchCount int      `json:"mismatch_count"`
	RepairCount   int      `json:"repair_count"`
	ManualReviews int      `json:"manual_reviews"`
	Truncated     bool     `json:"truncated"`
	Reports       []Report `json:"reports"`
}

type Checkpoint struct {
	ID              uuid.UUID
	Scope           Scope
	PaymentIntentID uuid.UUID
	Repair          bool
	StartedAt       time.Time
}

type CheckpointResult struct {
	RowsExamined  int
	MismatchCount int
	RepairCount   int
	Truncated     bool
	Failed        bool
	ErrorCategory string
	CompletedAt   time.Time
}

// Store exposes only bounded reconciliation observations and durable audit
// writes. Implementations must not perform provider calls or seat mutations.
type Store interface {
	CandidateIntentIDs(context.Context, Scope, time.Time, int) ([]uuid.UUID, bool, error)
	LoadControlSnapshot(context.Context, uuid.UUID) (ControlSnapshot, error)
	LoadShardSnapshot(context.Context, uuid.UUID) (ShardSnapshot, error)
	StartCheckpoint(context.Context, Scope, uuid.UUID, bool, time.Time) (Checkpoint, error)
	FinishCheckpoint(context.Context, Checkpoint, CheckpointResult) error
	EscalateManualReview(context.Context, uuid.UUID, uuid.UUID, string, time.Time) (bool, error)
}

type StatusQuerier interface {
	GetPaymentStatus(context.Context, string) (provider.Payment, error)
}

type ProviderRegistry interface {
	Provider(string) (StatusQuerier, bool)
}

// Repairer may replay an already-recorded idempotent command only. It cannot
// accept an amount, target state, seat identity, or newly-generated command.
type Repairer interface {
	ReplayRecordedCommand(context.Context, uuid.UUID, RecordedCommand) error
}
