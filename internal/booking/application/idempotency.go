package application

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"sort"
	"strings"
)

const MaxIdempotencyKeyBytes = 256

var (
	ErrInvalidIdempotencyKey = errors.New("invalid idempotency key")
	ErrInvalidHoldRequest    = errors.New("invalid hold request")
	ErrInvalidCommand        = errors.New("invalid idempotent command")
)

type IdempotentOperation string

const (
	OperationReservationConfirm IdempotentOperation = "reservation.confirm"
	OperationReservationCancel  IdempotentOperation = "reservation.cancel"
)

// HashIdempotencyKey returns the durable representation of a client key. Raw
// keys must never be persisted, logged, or used as metric labels.
func HashIdempotencyKey(raw string) ([sha256.Size]byte, error) {
	if raw == "" || len(raw) > MaxIdempotencyKeyBytes {
		return [sha256.Size]byte{}, ErrInvalidIdempotencyKey
	}
	return sha256.Sum256([]byte(raw)), nil
}

// HoldFingerprintInput contains only fields that determine the identity of a
// reservation-create command. Passenger identifiers are sorted into a stable
// order because seat assignment is deterministic and does not preserve caller
// list ordering.
type HoldFingerprintInput struct {
	TrainRunID      string
	OriginCode      string
	DestinationCode string
	SeatClass       string
	PassengerIDs    []string
}

// FingerprintHoldRequest hashes a length-prefixed canonical form, avoiding
// delimiter ambiguities without retaining passenger identifiers in storage.
func FingerprintHoldRequest(input HoldFingerprintInput) ([sha256.Size]byte, error) {
	trainRunID := strings.ToLower(strings.TrimSpace(input.TrainRunID))
	origin := strings.ToUpper(strings.TrimSpace(input.OriginCode))
	destination := strings.ToUpper(strings.TrimSpace(input.DestinationCode))
	seatClass := strings.ToLower(strings.TrimSpace(input.SeatClass))
	if trainRunID == "" || origin == "" || destination == "" || origin == destination || seatClass == "" || len(input.PassengerIDs) == 0 {
		return [sha256.Size]byte{}, ErrInvalidHoldRequest
	}

	seen := make(map[string]struct{}, len(input.PassengerIDs))
	passengers := make([]string, len(input.PassengerIDs))
	for index, passengerID := range input.PassengerIDs {
		passengers[index] = strings.ToLower(strings.TrimSpace(passengerID))
		if passengers[index] == "" {
			return [sha256.Size]byte{}, ErrInvalidHoldRequest
		}
		if _, exists := seen[passengers[index]]; exists {
			return [sha256.Size]byte{}, ErrInvalidHoldRequest
		}
		seen[passengers[index]] = struct{}{}
	}
	sort.Strings(passengers)

	digest := sha256.New()
	writeField(digest, "v1")
	writeField(digest, "reservation.create")
	writeField(digest, trainRunID)
	writeField(digest, origin)
	writeField(digest, destination)
	writeField(digest, seatClass)
	for _, passengerID := range passengers {
		writeField(digest, passengerID)
	}

	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func FingerprintReservationCommand(operation IdempotentOperation, reservationID string) ([sha256.Size]byte, error) {
	reservationID = strings.ToLower(strings.TrimSpace(reservationID))
	if reservationID == "" || (operation != OperationReservationConfirm && operation != OperationReservationCancel) {
		return [sha256.Size]byte{}, ErrInvalidCommand
	}
	digest := sha256.New()
	writeField(digest, "v1")
	writeField(digest, string(operation))
	writeField(digest, reservationID)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func writeField(destination hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = destination.Write(size[:])
	_, _ = destination.Write([]byte(value))
}
