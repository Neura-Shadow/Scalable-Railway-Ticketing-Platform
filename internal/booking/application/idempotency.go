package application

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"strings"
)

const MaxIdempotencyKeyBytes = 256

var (
	ErrInvalidIdempotencyKey = errors.New("invalid idempotency key")
	ErrInvalidHoldRequest    = errors.New("invalid hold request")
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
// reservation-create command. Passenger order is intentional because seats are
// assigned deterministically in that order.
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

	digest := sha256.New()
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

func writeField(destination hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = destination.Write(size[:])
	_, _ = destination.Write([]byte(value))
}
