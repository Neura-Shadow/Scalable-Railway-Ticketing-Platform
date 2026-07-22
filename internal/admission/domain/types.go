package domain

import (
	"crypto/sha256"
	"errors"
	"time"
)

var ErrInvalidAdmissionState = errors.New("invalid admission state")

type EntryStatus string

const (
	EntryQueued    EntryStatus = "queued"
	EntryAdmitted  EntryStatus = "admitted"
	EntryExpired   EntryStatus = "expired"
	EntryCancelled EntryStatus = "cancelled"
)

func (s EntryStatus) IsValid() bool {
	switch s {
	case EntryQueued, EntryAdmitted, EntryExpired, EntryCancelled:
		return true
	default:
		return false
	}
}

type TokenStatus string

const (
	TokenIssued     TokenStatus = "issued"
	TokenProcessing TokenStatus = "processing"
	TokenConsumed   TokenStatus = "consumed"
	TokenExpired    TokenStatus = "expired"
	TokenCancelled  TokenStatus = "cancelled"
)

func (s TokenStatus) IsValid() bool {
	switch s {
	case TokenIssued, TokenProcessing, TokenConsumed, TokenExpired, TokenCancelled:
		return true
	default:
		return false
	}
}

type AdmissionDecision string

const (
	DecisionAcquired      AdmissionDecision = "acquired"
	DecisionRetryAllowed  AdmissionDecision = "retry_allowed"
	DecisionReplayAllowed AdmissionDecision = "replay_allowed"
)

type QueuePosition struct {
	Approximate int64
}

type WaitingRoomEntry struct {
	ID                   string
	OwnerHash            [sha256.Size]byte
	PolicyID             string
	PolicyVersion        int64
	TrainRunID           string
	FromStopIndex        int
	ToStopIndex          int
	SeatClass            string
	PassengerCount       int
	AdmissionFingerprint [sha256.Size]byte
	Sequence             int64
	Status               EntryStatus
	JoinedAt             time.Time
	ExpiresAt            time.Time
	AdmittedAt           *time.Time
	Position             QueuePosition
}

type AdmissionToken struct {
	Hash                 [sha256.Size]byte
	KeyID                string
	EntryID              string
	OwnerHash            [sha256.Size]byte
	PolicyID             string
	PolicyVersion        int64
	AdmissionFingerprint [sha256.Size]byte
	BookingFingerprint   [sha256.Size]byte
	IdempotencyKeyHash   [sha256.Size]byte
	Status               TokenStatus
	IssuedAt             time.Time
	ExpiresAt            time.Time
	LeaseOwner           string
	LeaseGeneration      int64
	LeaseExpiresAt       *time.Time
	Delivered            bool
}
