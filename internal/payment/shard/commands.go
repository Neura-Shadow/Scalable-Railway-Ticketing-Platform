package shard

import (
	"crypto/sha256"
	"encoding/binary"
	"regexp"
	"time"

	"github.com/google/uuid"
)

var (
	ticketCodePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)
	issuanceNamespace = uuid.MustParse("8fd46050-e41f-5b3c-876c-77d4f4fa2570")
)

// ValidTicketCode applies the shared opaque ticket-code boundary used by shard
// receipts, control-plane uniqueness claims, and reconciliation.
func ValidTicketCode(value string) bool { return ticketCodePattern.MatchString(value) }

// DeterministicIssuanceID is the durable identity shared by the ordinary
// worker, repair path, and historical control-plane migration. It must not be
// derived from a payment_saga_actions row because legacy action IDs are
// intentionally generated during the version-11 upgrade.
func DeterministicIssuanceID(sagaID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(issuanceNamespace, []byte(sagaID.String()+":ticket_issuance"))
}

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

// RefundOrderSnapshot is authoritative shard evidence used to derive a
// whole-ticket subset refund. It deliberately contains no provider choice or
// client-supplied money.
type RefundOrderSnapshot struct {
	TicketOrderID        uuid.UUID
	PaymentIntentID      uuid.UUID
	ReservationID        uuid.UUID
	TrainRunID           uuid.UUID
	OwnerID              uuid.UUID
	AssignmentGeneration uint64
	Status               string
	CapturedMinor        int64
	RefundedMinor        int64
	Currency             string
	Tickets              []RefundTicketSnapshot
}

type RefundTicketSnapshot struct {
	TicketID          uuid.UUID
	ReservationSeatID uuid.UUID
	State             string
	FareMinor         int64
	Currency          string
}

// PrepareSelectedTicketRefundCommand reserves an exact active ticket set
// before any provider refund is attempted. Preparing never releases inventory.
type PrepareSelectedTicketRefundCommand struct {
	CommandID           uuid.UUID
	RefundRequestID     uuid.UUID
	RefundOperationID   uuid.UUID
	PaymentIntentID     uuid.UUID
	ReservationID       uuid.UUID
	TicketOrderID       uuid.UUID
	TrainRunID          uuid.UUID
	OwnerID             uuid.UUID
	Region              string
	RegionalEpoch       int64
	AmountMinor         int64
	Currency            string
	RequestFingerprint  [32]byte
	TicketIDs           []uuid.UUID
	RequestedAt         time.Time
	EligibilityCutoffAt time.Time
	PreparedAt          time.Time
}

type SelectedTicketRefundPrepareReceipt struct {
	ReceiptID            uuid.UUID
	CommandID            uuid.UUID
	RefundRequestID      uuid.UUID
	RefundOperationID    uuid.UUID
	PaymentIntentID      uuid.UUID
	ReservationID        uuid.UUID
	TicketOrderID        uuid.UUID
	TrainRunID           uuid.UUID
	AssignmentGeneration uint64
	AmountMinor          int64
	Currency             string
	RequestFingerprint   [32]byte
	SelectedTicketCount  int
	PreparedAt           time.Time
}

// ReleaseSelectedTicketRefundCommand unwinds one exact durable prepare when
// the provider outcome is known not to have taken effect. It never changes
// inventory because prepare never releases seats.
type ReleaseSelectedTicketRefundCommand struct {
	CommandID          uuid.UUID
	PrepareReceiptID   uuid.UUID
	RefundRequestID    uuid.UUID
	RefundOperationID  uuid.UUID
	PaymentIntentID    uuid.UUID
	ReservationID      uuid.UUID
	TicketOrderID      uuid.UUID
	TrainRunID         uuid.UUID
	OwnerID            uuid.UUID
	Region             string
	RegionalEpoch      int64
	RequestFingerprint [32]byte
	TicketIDs          []uuid.UUID
	ReleasedAt         time.Time
}

type SelectedTicketRefundReleaseReceipt struct {
	ReceiptID            uuid.UUID
	PrepareReceiptID     uuid.UUID
	CommandID            uuid.UUID
	RefundRequestID      uuid.UUID
	RefundOperationID    uuid.UUID
	TrainRunID           uuid.UUID
	AssignmentGeneration uint64
	RequestFingerprint   [32]byte
	ReleasedTicketCount  int
	ReleasedAt           time.Time
}

// ApplySelectedTicketRefundCommand is accepted only after durable provider
// success. Region and epoch are explicit fencing inputs and are rechecked in
// the same transaction as every selected ticket and seat mutation.
type ApplySelectedTicketRefundCommand struct {
	CommandID          uuid.UUID
	RefundRequestID    uuid.UUID
	RefundOperationID  uuid.UUID
	PaymentIntentID    uuid.UUID
	ReservationID      uuid.UUID
	TicketOrderID      uuid.UUID
	TrainRunID         uuid.UUID
	OwnerID            uuid.UUID
	Region             string
	RegionalEpoch      int64
	AmountMinor        int64
	Currency           string
	ProviderProofHash  [32]byte
	RequestFingerprint [32]byte
	TicketIDs          []uuid.UUID
	RefundedAt         time.Time
}

type SelectedTicketRefundReceipt struct {
	ReceiptID                  uuid.UUID
	CommandID                  uuid.UUID
	RefundRequestID            uuid.UUID
	RefundOperationID          uuid.UUID
	PaymentIntentID            uuid.UUID
	ReservationID              uuid.UUID
	TicketOrderID              uuid.UUID
	TrainRunID                 uuid.UUID
	AssignmentGeneration       uint64
	AmountMinor                int64
	Currency                   string
	SelectedTicketCount        int
	ReleasedSeatCount          int
	ResultingActiveTicketCount int
	ResultingOrderState        string
	CommittedAt                time.Time
}
