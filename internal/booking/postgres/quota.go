package postgres

import (
	"context"
	"fmt"

	bookingquota "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/quota"
	"github.com/google/uuid"
)

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

	lockKey := bookingquota.UserAdvisoryLockKey(userID)
	if _, err := tx.tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKey); err != nil {
		return fmt.Errorf("acquire reservation quota lock: %w", err)
	}

	var activeHolds, activeRunHolds, activePassengers int64
	query := `
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
     WHERE r.user_id = $1 AND r.status = 'held')`
	if tx.routed != nil {
		query = `
SELECT
    count(*) FILTER (WHERE active),
    count(*) FILTER (WHERE active AND train_run_id = $2),
    coalesce(sum(passenger_count) FILTER (WHERE active), 0)
FROM public.reservation_quota_claims
WHERE user_id = $1`
	}
	err := tx.tx.QueryRow(ctx, query, userID, trainRunID).Scan(
		&activeHolds, &activeRunHolds, &activePassengers,
	)
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
