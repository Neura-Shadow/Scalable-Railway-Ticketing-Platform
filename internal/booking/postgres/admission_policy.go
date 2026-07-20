package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/policylock"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// AdmissionPolicyDecision is the bounded result of the application-layer
// admission flow. Nil on CreateHoldParams means the application observed no
// enabled policy and expects the transaction to confirm that classification.
type AdmissionPolicyDecision struct {
	PolicyID uuid.UUID
	Version  int64
}

// recheckAdmissionPolicy is intentionally called after completed idempotency
// replay resolution and before quota or inventory mutation. The advisory lock
// is also taken by policy mutations, including creation, because a row lock
// cannot serialize the important absent-row activation case.
func (tx *Tx) recheckAdmissionPolicy(
	ctx context.Context,
	trainRunID uuid.UUID,
	seatClass string,
	decision *AdmissionPolicyDecision,
) error {
	if tx == nil || tx.tx == nil {
		return ErrInvalidArgument
	}
	if err := policylock.AcquireBookingRead(ctx, tx.tx, trainRunID, seatClass); err != nil {
		if errors.Is(err, policylock.ErrInvalidScope) {
			return ErrInvalidArgument
		}
		return fmt.Errorf("lock admission policy scope: %w", err)
	}

	var current AdmissionPolicyDecision
	err := tx.tx.QueryRow(ctx, `
SELECT id, version
FROM hot_train_policies
WHERE train_run_id = $1
  AND seat_class = $2
  AND enabled`, trainRunID, seatClass).Scan(&current.PolicyID, &current.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		if decision == nil {
			return nil
		}
		return ErrAdmissionPolicyChanged
	}
	if err != nil {
		return fmt.Errorf("recheck admission policy: %w", err)
	}
	if decision == nil {
		return ErrAdmissionRequired
	}
	if current.PolicyID != decision.PolicyID || current.Version != decision.Version {
		return ErrAdmissionPolicyChanged
	}
	return nil
}
