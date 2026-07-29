package postgres

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type physicalAvailabilityRouter interface {
	Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error)
}

// PhysicalStore keeps catalog metadata in control PostgreSQL while reading
// physical availability authority from exactly one current booking shard.
type PhysicalStore struct {
	control *Store
	router  physicalAvailabilityRouter
}

func NewPhysicalStore(control *Store, router physicalAvailabilityRouter) (*PhysicalStore, error) {
	if control == nil || router == nil {
		return nil, ErrInvalidQuery
	}
	return &PhysicalStore{control: control, router: router}, nil
}

func (store *PhysicalStore) ListStations(ctx context.Context) ([]Station, error) {
	return store.control.ListStations(ctx)
}

func (store *PhysicalStore) ResolveJourney(ctx context.Context, trainRunID, origin, destination string) (Journey, error) {
	return store.control.ResolveJourney(ctx, trainRunID, origin, destination)
}

func (store *PhysicalStore) SearchTrainRuns(ctx context.Context, request SearchRequest) ([]SearchResult, error) {
	return store.control.SearchTrainRuns(ctx, request)
}

func (store *PhysicalStore) Availability(ctx context.Context, request AvailabilityRequest) (Availability, error) {
	trainRunID, err := uuid.Parse(request.TrainRunID)
	if err != nil || trainRunID == uuid.Nil {
		return Availability{}, ErrInvalidQuery
	}
	seatClass, err := domain.ParseSeatClass(request.SeatClass)
	if err != nil {
		return Availability{}, err
	}
	physical, assignmentGeneration, err := store.assignment(ctx, trainRunID)
	if err != nil {
		return Availability{}, err
	}
	if !physical {
		return store.control.Availability(ctx, request)
	}
	// All control metadata is resolved before opening the shard transaction.
	journey, err := store.control.ResolveJourney(ctx, request.TrainRunID, request.OriginCode, request.DestinationCode)
	if err != nil {
		return Availability{}, err
	}
	resolved, err := store.router.Resolve(ctx, trainRunID, false)
	if err != nil {
		return Availability{}, ErrPersistence
	}
	if resolved.Route.Generation().Int64() != assignmentGeneration {
		resolved, err = store.router.Resolve(ctx, trainRunID, true)
		if err != nil || resolved.Route.Generation().Int64() != assignmentGeneration {
			return Availability{}, ErrPersistence
		}
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return Availability{}, ErrPersistence
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	result := Availability{
		TrainRunID: journey.TrainRunID, TrainCode: journey.TrainCode,
		ScheduledDepartureAt: journey.ScheduledDepartureAt,
		FromStopIndex:        journey.FromStopIndex, ToStopIndex: journey.ToStopIndex,
		SegmentCount:                    journey.SegmentCount,
		OriginDepartureOffsetMinutes:    journey.OriginDepartureOffsetMinutes,
		DestinationArrivalOffsetMinutes: journey.DestinationArrivalOffsetMinutes,
		DepartureAt:                     journey.DepartureAt, ArrivalAt: journey.ArrivalAt,
		SeatClass: seatClass, AssignmentGeneration: resolved.Route.Generation().Int64(),
	}
	err = tx.QueryRow(ctx, `
WITH authoritative AS (
 SELECT snapshot.segment_count,
        repeat('0',$3)::bit varying
        || repeat('1',$4-$3)::bit varying
        || repeat('0',snapshot.segment_count-$4)::bit varying AS requested_mask
 FROM train_run_booking_snapshots AS snapshot
 JOIN train_run_write_fences AS fence
   ON fence.train_run_id=snapshot.train_run_id
  AND fence.assignment_generation=snapshot.assignment_generation
 WHERE snapshot.train_run_id=$1 AND snapshot.assignment_generation=$2
   AND snapshot.active AND snapshot.bookable
   AND snapshot.status IN ('scheduled','boarding')
   AND fence.state='active' AND fence.write_enabled
), priced AS (
 SELECT authoritative.*,fare.amount_minor,fare.currency
 FROM authoritative
 JOIN LATERAL (
   SELECT amount_minor,currency
   FROM booking_fare_snapshots
   WHERE train_run_id=$1 AND assignment_generation=$2
     AND from_stop_index=$3 AND to_stop_index=$4 AND seat_class=$5 AND active
   ORDER BY source_version DESC,id LIMIT 1
 ) AS fare ON true
)
SELECT priced.amount_minor,priced.currency,
       count(inventory.seat_id) FILTER (
         WHERE seat.active
           AND bit_length(inventory.occupied_segments)=priced.segment_count
           AND (inventory.occupied_segments & priced.requested_mask)
               =repeat('0',priced.segment_count)::bit varying
       )::bigint
FROM priced
LEFT JOIN seat_inventory AS inventory
  ON inventory.train_run_id=$1 AND inventory.assignment_generation=$2 AND inventory.seat_class=$5
LEFT JOIN booking_seat_catalog AS seat
  ON seat.train_run_id=inventory.train_run_id
 AND seat.assignment_generation=inventory.assignment_generation
 AND seat.seat_id=inventory.seat_id
GROUP BY priced.amount_minor,priced.currency`, trainRunID, resolved.Route.Generation().Int64(),
		journey.FromStopIndex, journey.ToStopIndex, seatClass.String()).Scan(
		&result.FareAmountMinor, &result.Currency, &result.AvailableSeats)
	if errors.Is(err, pgx.ErrNoRows) {
		return Availability{}, ErrNotFound
	}
	if err != nil || result.AvailableSeats < 0 || len(result.Currency) != 3 {
		return Availability{}, ErrPersistence
	}
	if err := tx.Commit(ctx); err != nil {
		return Availability{}, ErrPersistence
	}
	return result, nil
}

func (store *PhysicalStore) AvailabilityBatch(ctx context.Context, requests []AvailabilityRequest) ([]Availability, error) {
	if len(requests) == 0 || len(requests) > MaxPageSize {
		return nil, ErrInvalidQuery
	}
	results := make([]Availability, 0, len(requests))
	for _, request := range requests {
		result, err := store.Availability(ctx, request)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (store *PhysicalStore) AvailabilityAssignmentGeneration(ctx context.Context, rawTrainRunID string) (int64, error) {
	trainRunID, err := uuid.Parse(rawTrainRunID)
	if err != nil || trainRunID == uuid.Nil {
		return 0, ErrInvalidQuery
	}
	physical, generation, err := store.assignment(ctx, trainRunID)
	if err != nil {
		return 0, err
	}
	if !physical {
		return 0, nil
	}
	return generation, nil
}

func (store *PhysicalStore) assignment(ctx context.Context, trainRunID uuid.UUID) (bool, int64, error) {
	var storageKind, shardID, assignmentState string
	var generation int64
	err := store.control.db.QueryRow(ctx, `
SELECT shard.storage_kind,assignment.shard_id,assignment.assignment_generation,assignment.assignment_state
FROM train_run_shard_assignments AS assignment
JOIN booking_shards AS shard ON shard.shard_id=assignment.shard_id
WHERE assignment.train_run_id=$1`, trainRunID).Scan(&storageKind, &shardID, &generation, &assignmentState)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, 0, ErrPersistence
	}
	if err != nil {
		return false, 0, ErrPersistence
	}
	if storageKind != "postgres" {
		if storageKind == "legacy_schema" || storageKind == "logical_schema" {
			return false, 0, nil
		}
		return false, 0, ErrPersistence
	}
	parsed, err := sharding.ParseShardID(shardID)
	if err != nil || (parsed != sharding.ShardPhysicalZero && parsed != sharding.ShardPhysicalOne) || generation <= 0 {
		return false, 0, ErrPersistence
	}
	if assignmentState != "stable" && assignmentState != "rollback_window" {
		return false, 0, ErrPersistence
	}
	return true, generation, nil
}
