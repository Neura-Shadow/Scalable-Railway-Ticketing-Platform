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
	if result.RowsAffected() == 1 {
		return nil
	}

	// The version-8 legacy compatibility trigger creates the same global
	// claim when the final reservation-seat row is inserted. Schema-local
	// shards have no such trigger. Accept only an exact, active claim while
	// holding its row lock; any ownership or passenger-count mismatch remains
	// a persistence invariant failure.
	var (
		claimedUserID         uuid.UUID
		claimedTrainRunID     uuid.UUID
		claimedPassengerCount int
		claimedActive         bool
	)
	if err := tx.tx.QueryRow(ctx, `
SELECT user_id, train_run_id, passenger_count, active
FROM public.reservation_quota_claims
WHERE reservation_id = $1
FOR UPDATE`, reservationID).Scan(
		&claimedUserID,
		&claimedTrainRunID,
		&claimedPassengerCount,
		&claimedActive,
	); err != nil {
		return fmt.Errorf("lock existing reservation quota claim: %w", err)
	}
	if claimedUserID != userID || claimedTrainRunID != trainRunID ||
		claimedPassengerCount != passengerCount || !claimedActive {
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
