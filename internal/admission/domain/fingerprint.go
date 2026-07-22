package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/google/uuid"
)

var ErrInvalidAdmissionRequest = errors.New("invalid admission request")

const (
	admissionFingerprintVersion = byte(1)
	MaxAdmissionPassengers      = 100
)

var admissionFingerprintDomain = []byte("railway-admission-request/v1")

type AdmissionFingerprintInput struct {
	TrainRunID     string
	FromStopIndex  int
	ToStopIndex    int
	SeatClass      string
	PassengerCount int
}

// FingerprintAdmissionRequest creates the versioned identity used at queue
// join. User identity, passenger identities, and booking idempotency are
// deliberately separate bindings.
func FingerprintAdmissionRequest(input AdmissionFingerprintInput) ([sha256.Size]byte, error) {
	runID, err := uuid.Parse(strings.TrimSpace(input.TrainRunID))
	seatClass := strings.ToLower(strings.TrimSpace(input.SeatClass))
	if err != nil || input.FromStopIndex < 0 || input.ToStopIndex <= input.FromStopIndex ||
		!validAdmissionSeatClass(seatClass) || input.PassengerCount < 1 ||
		input.PassengerCount > MaxAdmissionPassengers {
		return [sha256.Size]byte{}, ErrInvalidAdmissionRequest
	}
	var canonical bytes.Buffer
	canonical.WriteByte(admissionFingerprintVersion)
	writeLengthPrefixed(&canonical, admissionFingerprintDomain)
	writeLengthPrefixed(&canonical, []byte(runID.String()))
	_ = binary.Write(&canonical, binary.BigEndian, uint32(input.FromStopIndex))
	_ = binary.Write(&canonical, binary.BigEndian, uint32(input.ToStopIndex))
	writeLengthPrefixed(&canonical, []byte(seatClass))
	_ = binary.Write(&canonical, binary.BigEndian, uint32(input.PassengerCount))
	return sha256.Sum256(canonical.Bytes()), nil
}

func validAdmissionSeatClass(value string) bool {
	switch value {
	case "standard", "business", "first":
		return true
	default:
		return false
	}
}

func writeLengthPrefixed(buffer *bytes.Buffer, value []byte) {
	_ = binary.Write(buffer, binary.BigEndian, uint32(len(value)))
	_, _ = buffer.Write(value)
}
