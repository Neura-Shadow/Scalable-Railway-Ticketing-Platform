package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
)

const reservationQuotaLockNamespace = "railway/booking/reservation-quota/user/v1\x00"

type ReservationQuotaLimits struct {
	MaxActiveHoldsPerUser            int
	MaxActiveHoldsPerUserPerTrainRun int
	MaxActivePassengersPerUser       int
}

func DefaultReservationQuotaLimits() ReservationQuotaLimits {
	return ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            10,
		MaxActiveHoldsPerUserPerTrainRun: 3,
		MaxActivePassengersPerUser:       24,
	}
}

func (limits ReservationQuotaLimits) valid() bool {
	return limits.MaxActiveHoldsPerUser > 0 &&
		limits.MaxActiveHoldsPerUserPerTrainRun > 0 &&
		limits.MaxActiveHoldsPerUserPerTrainRun <= limits.MaxActiveHoldsPerUser &&
		limits.MaxActivePassengersPerUser > 0
}

// enforceReservationQuota serializes all quota decisions for one canonical
// user before any train-run, passenger, inventory, reservation, or outbox
// mutation. Counts are derived from PostgreSQL's authoritative held rows.
func (tx *Tx) enforceReservationQuota(
	ctx context.Context,
	userID, trainRunID uuid.UUID,
	passengersToAdd int,
	limits ReservationQuotaLimits,
) error {
	if tx == nil || tx.tx == nil || userID == uuid.Nil || trainRunID == uuid.Nil ||
		passengersToAdd <= 0 || !limits.valid() {
		return ErrInvalidArgument
	}

	lockKey := reservationQuotaAdvisoryKey(userID)
	if _, err := tx.tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKey); err != nil {
		return fmt.Errorf("acquire reservation quota lock: %w", err)
	}

	var activeHolds, activeRunHolds, activePassengers int64
	err := tx.tx.QueryRow(ctx, `
SELECT
    (SELECT count(*)
     FROM reservations
     WHERE user_id = $1 AND status = 'held'),
    (SELECT count(*)
     FROM reservations
     WHERE user_id = $1 AND train_run_id = $2 AND status = 'held'),
    (SELECT count(*)
     FROM reservation_seats AS rs
     JOIN reservations AS r ON r.id = rs.reservation_id
     WHERE r.user_id = $1 AND r.status = 'held')`,
		userID, trainRunID,
	).Scan(&activeHolds, &activeRunHolds, &activePassengers)
	if err != nil {
		return fmt.Errorf("count active reservation quota: %w", err)
	}

	if atOrAboveLimit(activeHolds, 1, limits.MaxActiveHoldsPerUser) ||
		atOrAboveLimit(activeRunHolds, 1, limits.MaxActiveHoldsPerUserPerTrainRun) ||
		atOrAboveLimit(activePassengers, passengersToAdd, limits.MaxActivePassengersPerUser) {
		return ErrReservationQuotaExceeded
	}
	return nil
}

func atOrAboveLimit(current int64, additional, limit int) bool {
	if current < 0 || additional < 0 || limit <= 0 || current >= int64(limit) {
		return true
	}
	return int64(additional) > int64(limit)-current
}

func reservationQuotaAdvisoryKey(userID uuid.UUID) int64 {
	digestInput := make([]byte, 0, len(reservationQuotaLockNamespace)+len(userID))
	digestInput = append(digestInput, reservationQuotaLockNamespace...)
	digestInput = append(digestInput, userID[:]...)
	digest := sha256.Sum256(digestInput)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}
