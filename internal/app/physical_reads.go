package app

import (
	"context"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type physicalRouteResolver interface {
	Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error)
}

// HybridReservationReader resolves an owner-scoped global directory entry in
// control and then reads at most one allowlisted physical shard. It never scans
// shard connections and never exposes the selected shard to the HTTP layer.
type HybridReservationReader struct {
	control controlRouteReader
	legacy  reservationReader
	router  physicalRouteResolver
}

func NewHybridReservationReader(
	control controlRouteReader,
	legacy reservationReader,
	router physicalRouteResolver,
) (*HybridReservationReader, error) {
	if control == nil || legacy == nil || router == nil {
		return nil, ErrReadNotFound
	}
	return &HybridReservationReader{control: control, legacy: legacy, router: router}, nil
}

func (reader *HybridReservationReader) GetReservationDetail(
	ctx context.Context,
	owner, reservation uuid.UUID,
) (ReservationDetail, error) {
	if reader == nil || ctx == nil || owner == uuid.Nil || reservation == uuid.Nil {
		return ReservationDetail{}, ErrReadNotFound
	}
	var trainRunID uuid.UUID
	var shardIDText, state string
	var generation int64
	err := reader.control.QueryRow(ctx, `
SELECT directory.train_run_id, directory.last_known_shard_id,
       directory.last_known_generation, directory.state
FROM public.reservation_directory AS directory
WHERE directory.reservation_id = $1
  AND directory.owner_user_id = $2`, reservation, owner).Scan(
		&trainRunID, &shardIDText, &generation, &state,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return reader.legacy.GetReservationDetail(ctx, owner, reservation)
	}
	if err != nil || trainRunID == uuid.Nil || generation <= 0 ||
		(state != "active" && state != "pending" && state != "moving") {
		return ReservationDetail{}, ErrReadNotFound
	}
	shardID, err := sharding.ParseShardID(shardIDText)
	if err != nil {
		return ReservationDetail{}, ErrReadNotFound
	}
	if shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne {
		return reader.legacy.GetReservationDetail(ctx, owner, reservation)
	}

	resolved, err := reader.router.Resolve(ctx, trainRunID, false)
	if err != nil {
		return ReservationDetail{}, sharding.ErrShardUnavailable
	}
	if resolved.Route.ShardID() != shardID || resolved.Route.Generation().Int64() != generation {
		resolved, err = reader.router.Resolve(ctx, trainRunID, true)
		if err != nil || resolved.Route.ShardID() != shardID || resolved.Route.Generation().Int64() != generation {
			return ReservationDetail{}, sharding.ErrAssignmentStale
		}
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return ReservationDetail{}, sharding.ErrShardUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var detail ReservationDetail
	var physicalTrainRun uuid.UUID
	var fromStopIndex, toStopIndex int
	var expires time.Time
	err = tx.QueryRow(ctx, `
SELECT reservation.id::text, reservation.status, reservation.train_run_id,
       reservation.from_stop_index, reservation.to_stop_index,
       reservation.seat_class, reservation.expires_at,
       COALESCE(array_agg(seat.passenger_id::text ORDER BY seat.passenger_id::text)
                FILTER (WHERE seat.passenger_id IS NOT NULL), '{}')
FROM public.reservations AS reservation
LEFT JOIN public.reservation_seats AS seat
  ON seat.reservation_id = reservation.id
WHERE reservation.id = $1
  AND reservation.user_id = $2
  AND reservation.train_run_id = $3
  AND reservation.assignment_generation = $4
GROUP BY reservation.id`, reservation, owner, trainRunID, generation).Scan(
		&detail.ID, &detail.Status, &physicalTrainRun, &fromStopIndex,
		&toStopIndex, &detail.SeatClass, &expires, &detail.PassengerIDs,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReservationDetail{}, ErrReadNotFound
	}
	if err != nil || physicalTrainRun != trainRunID {
		return ReservationDetail{}, sharding.ErrShardUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return ReservationDetail{}, sharding.ErrShardUnavailable
	}

	err = reader.control.QueryRow(ctx, `
SELECT origin_station.code, destination_station.code
FROM public.train_runs AS train_run
JOIN public.route_stops AS origin
  ON origin.route_id = train_run.route_id AND origin.stop_index = $2
JOIN public.stations AS origin_station ON origin_station.id = origin.station_id
JOIN public.route_stops AS destination
  ON destination.route_id = train_run.route_id AND destination.stop_index = $3
JOIN public.stations AS destination_station
  ON destination_station.id = destination.station_id
WHERE train_run.id = $1`, trainRunID, fromStopIndex, toStopIndex).Scan(
		&detail.OriginStationCode, &detail.DestinationStationCode,
	)
	if err != nil {
		return ReservationDetail{}, ErrReadNotFound
	}
	detail.TrainRunID = trainRunID.String()
	expires = expires.UTC()
	detail.ExpiresAt = &expires
	return detail, nil
}

var _ reservationReader = (*HybridReservationReader)(nil)
