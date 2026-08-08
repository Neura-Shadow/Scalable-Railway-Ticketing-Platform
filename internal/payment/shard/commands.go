package shard

import (
	"time"

	"github.com/google/uuid"
)

type IssueTicketsCommand struct {
	CommandID          uuid.UUID
	IssuanceID         uuid.UUID
	PaymentIntentID    uuid.UUID
	PaymentOperationID uuid.UUID
	ReservationID      uuid.UUID
	TrainRunID         uuid.UUID
	OwnerID            uuid.UUID
	AmountMinor        int64
	Currency           string
	CaptureProofHash   [32]byte
	RequestFingerprint [32]byte
}

type IssueTicketsReceipt struct {
	CommandID       uuid.UUID
	IssuanceID      uuid.UUID
	PaymentIntentID uuid.UUID
	ReservationID   uuid.UUID
	TicketOrderID   uuid.UUID
	TicketIDs       []uuid.UUID
	AmountMinor     int64
	Currency        string
	IssuedAt        time.Time
}

type MarkRefundPendingCommand struct {
	CommandID          uuid.UUID
	PaymentIntentID    uuid.UUID
	ReservationID      uuid.UUID
	TrainRunID         uuid.UUID
	OwnerID            uuid.UUID
	AmountMinor        int64
	Currency           string
	CaptureProofHash   [32]byte
	RequestFingerprint [32]byte
}

type MarkRefundPendingReceipt struct {
	CommandID       uuid.UUID
	PaymentIntentID uuid.UUID
	ReservationID   uuid.UUID
	TicketOrderID   uuid.UUID
}

type ApplyRefundCompensationCommand struct {
	CommandID          uuid.UUID
	CompensationID     uuid.UUID
	RefundOperationID  uuid.UUID
	PaymentIntentID    uuid.UUID
	ReservationID      uuid.UUID
	TrainRunID         uuid.UUID
	OwnerID            uuid.UUID
	AmountMinor        int64
	Currency           string
	RefundProofHash    [32]byte
	RequestFingerprint [32]byte
	RefundedAt         time.Time
}

type ApplyRefundCompensationReceipt struct {
	CommandID            uuid.UUID
	CompensationID       uuid.UUID
	PaymentIntentID      uuid.UUID
	ReservationID        uuid.UUID
	TicketOrderID        uuid.UUID
	ReleasedSeatCount    int
	CancelledTicketCount int
}
