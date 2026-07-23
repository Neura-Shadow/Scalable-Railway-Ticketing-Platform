package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

func (tx *Tx) insertReservationQuotaClaim(
	ctx context.Context,
	reservationID, userID, trainRunID uuid.UUID,
	passengerCount int,
) error {
	if tx == nil || tx.tx == nil || reservationID == uuid.Nil || userID == uuid.Nil ||
		trainRunID == uuid.Nil || passengerCount <= 0 {
		return ErrInvalidArgument
	}
	if tx.routed == nil {
		return nil
	}
	result, err := tx.tx.Exec(ctx, `
INSERT INTO public.reservation_quota_claims (
    reservation_id, user_id, train_run_id, passenger_count, active
)
VALUES ($1, $2, $3, $4, true)
ON CONFLICT (reservation_id) DO NOTHING`,
		reservationID, userID, trainRunID, passengerCount,
	)
	if err != nil {
		return fmt.Errorf("insert reservation quota claim: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrPersistenceInvariant
	}
	return nil
}

func (tx *Tx) closeReservationQuotaClaim(ctx context.Context, reservationID uuid.UUID) error {
	if tx == nil || tx.tx == nil || reservationID == uuid.Nil {
		return ErrInvalidArgument
	}
	if tx.routed == nil {
		return nil
	}
	if _, err := tx.tx.Exec(ctx, `
UPDATE public.reservation_quota_claims
SET active = false,
    closed_at = COALESCE(closed_at, clock_timestamp()),
    updated_at = clock_timestamp()
WHERE reservation_id = $1
  AND active`, reservationID); err != nil {
		return fmt.Errorf("close reservation quota claim: %w", err)
	}
	return nil
}
