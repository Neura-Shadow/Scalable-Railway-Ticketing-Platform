package postgres

import (
	"context"
	"fmt"
)

type ReservationQuotaReconciliation struct {
	UsersOverHoldLimit      int64 `json:"users_over_hold_limit"`
	UserRunsOverHoldLimit   int64 `json:"user_runs_over_hold_limit"`
	UsersOverPassengerLimit int64 `json:"users_over_passenger_limit"`
}

func (result ReservationQuotaReconciliation) Violations() int64 {
	return result.UsersOverHoldLimit + result.UserRunsOverHoldLimit + result.UsersOverPassengerLimit
}

// ReconcileReservationQuotas derives all checks from authoritative held rows.
// There is no counter state to mutate or repair.
func (s *Store) ReconcileReservationQuotas(
	ctx context.Context,
	limits ReservationQuotaLimits,
) (ReservationQuotaReconciliation, error) {
	if s == nil || s.pool == nil || !limits.valid() {
		return ReservationQuotaReconciliation{}, ErrInvalidArgument
	}
	var result ReservationQuotaReconciliation
	query := `
WITH held AS (
    SELECT r.id, r.user_id, r.train_run_id,
           count(rs.id)::bigint AS passenger_count
    FROM reservations AS r
    LEFT JOIN reservation_seats AS rs ON rs.reservation_id = r.id
    WHERE r.status = 'held'
    GROUP BY r.id, r.user_id, r.train_run_id
), per_user AS (
    SELECT user_id, count(*)::bigint AS holds,
           coalesce(sum(passenger_count), 0)::bigint AS passengers
    FROM held
    GROUP BY user_id
), per_user_run AS (
    SELECT user_id, train_run_id, count(*)::bigint AS holds
    FROM held
    GROUP BY user_id, train_run_id
)
SELECT
    (SELECT count(*) FROM per_user WHERE holds > $1),
    (SELECT count(*) FROM per_user_run WHERE holds > $2),
	(SELECT count(*) FROM per_user WHERE passengers > $3)`
	if s.shards != nil {
		query = `
WITH held AS (
    SELECT reservation_id AS id, user_id, train_run_id, passenger_count
    FROM public.reservation_quota_claims
    WHERE active
), per_user AS (
    SELECT user_id, count(*)::bigint AS holds,
           coalesce(sum(passenger_count), 0)::bigint AS passengers
    FROM held
    GROUP BY user_id
), per_user_run AS (
    SELECT user_id, train_run_id, count(*)::bigint AS holds
    FROM held
    GROUP BY user_id, train_run_id
)
SELECT
    (SELECT count(*) FROM per_user WHERE holds > $1),
    (SELECT count(*) FROM per_user_run WHERE holds > $2),
    (SELECT count(*) FROM per_user WHERE passengers > $3)`
	}
	err := s.pool.QueryRow(ctx, query,
		limits.MaxActiveHoldsPerUser,
		limits.MaxActiveHoldsPerUserPerTrainRun,
		limits.MaxActivePassengersPerUser,
	).Scan(
		&result.UsersOverHoldLimit,
		&result.UserRunsOverHoldLimit,
		&result.UsersOverPassengerLimit,
	)
	if err != nil {
		return ReservationQuotaReconciliation{}, fmt.Errorf("reconcile reservation quotas: %w", err)
	}
	if result.Violations() != 0 {
		return result, fmt.Errorf(
			"%w: reservation quota reconciliation found %d violations",
			ErrPersistenceInvariant, result.Violations(),
		)
	}
	return result, nil
}
