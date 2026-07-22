package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestValidateCreateHoldParamsRequiresWellFormedPolicyDecision(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	valid := CreateHoldParams{
		UserID: uuid.New(), TrainRunID: uuid.New(),
		FromStopIndex: 0, ToStopIndex: 1, SeatClass: "standard",
		PassengerIDs: []uuid.UUID{uuid.New()}, HoldExpiresAt: now.Add(time.Minute),
		IdempotencyKeyHash: make([]byte, 32), RequestFingerprint: make([]byte, 32),
		IdempotencyExpiresAt: now.Add(time.Hour),
	}
	if err := validateCreateHoldParams(valid); err != nil {
		t.Fatalf("nil non-hot decision rejected: %v", err)
	}
	valid.AdmissionPolicy = &AdmissionPolicyDecision{PolicyID: uuid.Nil, Version: 1}
	if err := validateCreateHoldParams(valid); err != ErrInvalidArgument {
		t.Fatalf("nil policy ID error = %v, want %v", err, ErrInvalidArgument)
	}
	valid.AdmissionPolicy = &AdmissionPolicyDecision{PolicyID: uuid.New(), Version: 0}
	if err := validateCreateHoldParams(valid); err != ErrInvalidArgument {
		t.Fatalf("zero policy version error = %v, want %v", err, ErrInvalidArgument)
	}
	valid.AdmissionPolicy.Version = 1
	if err := validateCreateHoldParams(valid); err != nil {
		t.Fatalf("valid hot decision rejected: %v", err)
	}
}
