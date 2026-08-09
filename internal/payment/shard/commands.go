package shard

import (
	"crypto/sha256"
	"encoding/binary"
	"regexp"
	"time"

	"github.com/google/uuid"
)

var ticketCodePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)

// ValidTicketCode applies the shared opaque ticket-code boundary used by shard
// receipts, control-plane uniqueness claims, and reconciliation.
func ValidTicketCode(value string) bool { return ticketCodePattern.MatchString(value) }

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
	PlannedTicketIDs   []uuid.UUID
	PlannedTicketCodes []string
}

type TicketIdentityPlan struct {
	TicketIDs   []uuid.UUID
	TicketCodes []string
}

type IssueTicketsReceipt struct {
	CommandID       uuid.UUID
	IssuanceID      uuid.UUID
	PaymentIntentID uuid.UUID
	ReservationID   uuid.UUID
	TicketOrderID   uuid.UUID
	TicketIDs       []uuid.UUID
	TicketCodes     []string
	AmountMinor     int64
	Currency        string
	OrderCreatedAt  time.Time
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

// CancelVoidedReservationCommand is the shard-local half of a successful
// provider void. The control-plane proof is bound into RequestFingerprint;
// the shard accepts the command only while the reservation is uncaptured and
// no tickets have been issued.
type CancelVoidedReservationCommand struct {
	CommandID          uuid.UUID
	VoidOperationID    uuid.UUID
	PaymentIntentID    uuid.UUID
	ReservationID      uuid.UUID
	TrainRunID         uuid.UUID
	OwnerID            uuid.UUID
	AmountMinor        int64
	Currency           string
	VoidProofHash      [32]byte
	RequestFingerprint [32]byte
	VoidedAt           time.Time
}

type CancelVoidedReservationReceipt struct {
	CommandID         uuid.UUID
	VoidOperationID   uuid.UUID
	PaymentIntentID   uuid.UUID
	ReservationID     uuid.UUID
	TicketOrderID     uuid.UUID
	ReleasedSeatCount int
	CancelledAt       time.Time
}

// VoidCancellationFingerprint binds the command and provider-proof identities
// without persisting provider payloads. Callers cannot swap an operation,
// reservation, owner, or amount while replaying the same receipt fingerprint.
func VoidCancellationFingerprint(command CancelVoidedReservationCommand) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("payment-void-cancellation-v1"))
	for _, value := range []uuid.UUID{
		command.CommandID, command.VoidOperationID, command.PaymentIntentID,
		command.ReservationID, command.TrainRunID, command.OwnerID,
	} {
		_, _ = digest.Write(value[:])
	}
	var amount [8]byte
	binary.BigEndian.PutUint64(amount[:], uint64(command.AmountMinor))
	_, _ = digest.Write(amount[:])
	_, _ = digest.Write([]byte(command.Currency))
	_, _ = digest.Write(command.VoidProofHash[:])
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
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
