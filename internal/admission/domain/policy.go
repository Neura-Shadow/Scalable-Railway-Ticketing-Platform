package domain

import (
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/google/uuid"
)

var (
	ErrInvalidPolicyLimits   = errors.New("invalid admission policy limits")
	ErrInvalidHotTrainPolicy = errors.New("invalid hot train policy")
)

const (
	MaxQueueSize              = 100_000
	MaxAdmissionRatePerSecond = 10_000
	MaxInflightAdmissions     = 10_000
	MinAdmissionTokenTTL      = 6 * time.Second
	MaxAdmissionTokenTTL      = 15 * time.Minute
	MinProcessingLease        = 5 * time.Second
	MaxProcessingLease        = 2 * time.Minute
	MinQueueEntryTTL          = time.Minute
	MaxQueueEntryTTL          = 24 * time.Hour
)

type PolicyLimitsInput struct {
	MaxQueueSize           int
	AdmissionRatePerSecond int
	MaxInflightAdmissions  int
	AdmissionTokenTTL      time.Duration
	ProcessingLease        time.Duration
	QueueEntryTTL          time.Duration
}

type PolicyLimits struct {
	MaxQueueSize           int
	AdmissionRatePerSecond int
	MaxInflightAdmissions  int
	AdmissionTokenTTL      time.Duration
	ProcessingLease        time.Duration
	QueueEntryTTL          time.Duration
}

// HotTrainPolicy is the durable PostgreSQL classification for one train run
// and seat class. Redis state is intentionally absent: it is a derived,
// versioned control-plane projection, never the classification authority.
type HotTrainPolicy struct {
	ID                      uuid.UUID
	TrainRunID              uuid.UUID
	SeatClass               domain.SeatClass
	Enabled                 bool
	Version                 int64
	RedisInitializedVersion *int64
	Limits                  PolicyLimits
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

func NewPolicyLimits(input PolicyLimitsInput) (PolicyLimits, error) {
	if input.MaxQueueSize < 1 || input.MaxQueueSize > MaxQueueSize ||
		input.AdmissionRatePerSecond < 1 || input.AdmissionRatePerSecond > MaxAdmissionRatePerSecond ||
		input.MaxInflightAdmissions < 1 || input.MaxInflightAdmissions > MaxInflightAdmissions ||
		input.AdmissionTokenTTL < MinAdmissionTokenTTL || input.AdmissionTokenTTL > MaxAdmissionTokenTTL ||
		input.ProcessingLease < MinProcessingLease || input.ProcessingLease > MaxProcessingLease ||
		input.ProcessingLease >= input.AdmissionTokenTTL ||
		input.QueueEntryTTL < MinQueueEntryTTL || input.QueueEntryTTL > MaxQueueEntryTTL ||
		input.QueueEntryTTL < input.AdmissionTokenTTL+input.ProcessingLease {
		return PolicyLimits{}, ErrInvalidPolicyLimits
	}
	return PolicyLimits(input), nil
}

// NewHotTrainPolicy rejects invalid durable policy state before it reaches a
// persistence adapter. Timestamp values are intentionally normalized to UTC so
// callers never observe database-location dependent policy values.
func NewHotTrainPolicy(
	id uuid.UUID,
	trainRunID uuid.UUID,
	seatClass domain.SeatClass,
	enabled bool,
	version int64,
	redisInitializedVersion *int64,
	limits PolicyLimits,
	createdAt time.Time,
	updatedAt time.Time,
) (HotTrainPolicy, error) {
	if id == uuid.Nil || trainRunID == uuid.Nil || !seatClass.IsValid() || version < 1 ||
		createdAt.IsZero() || updatedAt.IsZero() || updatedAt.Before(createdAt) {
		return HotTrainPolicy{}, ErrInvalidHotTrainPolicy
	}
	if _, err := NewPolicyLimits(PolicyLimitsInput(limits)); err != nil {
		return HotTrainPolicy{}, err
	}
	if redisInitializedVersion != nil && (*redisInitializedVersion < 1 || *redisInitializedVersion > version) {
		return HotTrainPolicy{}, ErrInvalidHotTrainPolicy
	}
	return HotTrainPolicy{
		ID: id, TrainRunID: trainRunID, SeatClass: seatClass, Enabled: enabled,
		Version: version, RedisInitializedVersion: cloneVersion(redisInitializedVersion), Limits: limits,
		CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(),
	}, nil
}

func cloneVersion(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
