package app

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// PhysicalOperatorBookingStateReader returns only the current shard-local
// booking authority and optimistic versions. The control lookup and shard
// read are separate transactions; no transaction spans databases.
type PhysicalOperatorBookingStateReader struct {
	control controlRouteReader
	router  physicalRouteResolver
}

func NewPhysicalOperatorBookingStateReader(control controlRouteReader, router physicalRouteResolver) (*PhysicalOperatorBookingStateReader, error) {
	if control == nil || router == nil {
		return nil, httpapi.ErrUnavailable
	}
	return &PhysicalOperatorBookingStateReader{control: control, router: router}, nil
}

func (reader *PhysicalOperatorBookingStateReader) GetOperatorBookingState(ctx context.Context, query httpapi.OperatorBookingStateQuery) (httpapi.OperatorBookingStateView, error) {
	if reader == nil || ctx == nil {
		return httpapi.OperatorBookingStateView{}, httpapi.ErrUnavailable
	}
	if query.Kind != httpapi.OperatorBookingFareState && query.Kind != httpapi.OperatorBookingSeatState &&
		query.Kind != httpapi.OperatorBookingPolicyState {
		return httpapi.OperatorBookingStateView{}, httpapi.ErrInvalidInput
	}
	trainRunID, err := uuid.Parse(query.TrainRunID)
	if err != nil {
		return httpapi.OperatorBookingStateView{}, httpapi.ErrInvalidInput
	}
	resourceID := trainRunID
	if query.Kind != httpapi.OperatorBookingPolicyState {
		resourceID, err = uuid.Parse(query.ResourceID)
		if err != nil {
			return httpapi.OperatorBookingStateView{}, httpapi.ErrInvalidInput
		}
	}
	shardResourceID := resourceID
	if query.Kind == httpapi.OperatorBookingFareState {
		if err := reader.control.QueryRow(ctx,
			`SELECT public.physical_source_entity_id($1,'fare',$2)`, trainRunID, resourceID,
		).Scan(&shardResourceID); err != nil || shardResourceID == uuid.Nil {
			return httpapi.OperatorBookingStateView{}, httpapi.ErrUnavailable
		}
	}

	resolution, err := reader.router.Resolve(ctx, trainRunID, false)
	if err != nil || resolution.Route.TrainRunID() != trainRunID || resolution.Handle.Pool() == nil ||
		resolution.Handle.ShardID() != resolution.Route.ShardID() || !resolution.Handle.WriteEnabled() {
		return httpapi.OperatorBookingStateView{}, httpapi.ErrUnavailable
	}
	tx, err := resolution.Handle.Pool().BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return httpapi.OperatorBookingStateView{}, httpapi.ErrUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := requireReadableOperatorAuthority(ctx, tx, trainRunID, resolution.Route.Generation().Int64()); err != nil {
		if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
			return httpapi.OperatorBookingStateView{}, httpapi.ErrServiceTemporarilyRebalancing
		}
		return httpapi.OperatorBookingStateView{}, httpapi.ErrUnavailable
	}
	view := httpapi.OperatorBookingStateView{
		Kind: query.Kind, TrainRunID: trainRunID.String(), ResourceID: resourceID.String(),
		AssignmentGeneration: resolution.Route.Generation().Int64(),
	}
	switch query.Kind {
	case httpapi.OperatorBookingFareState:
		var active bool
		var fromStop, toStop int
		var amount int64
		if err := tx.QueryRow(ctx, `SELECT source_version,active,from_stop_index,to_stop_index,
 seat_class,amount_minor,currency FROM booking_fare_snapshots
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3`, shardResourceID, trainRunID,
			resolution.Route.Generation().Int64()).Scan(&view.SourceVersion, &active, &fromStop, &toStop,
			&view.SeatClass, &amount, &view.Currency); err != nil {
			return httpapi.OperatorBookingStateView{}, mapOperatorStateReadError(err)
		}
		view.Active, view.FromStopIndex, view.ToStopIndex, view.AmountMinor = &active, &fromStop, &toStop, &amount
	case httpapi.OperatorBookingSeatState:
		var active bool
		if err := tx.QueryRow(ctx, `SELECT source_version,active FROM booking_seat_catalog
WHERE train_run_id=$1 AND assignment_generation=$2 AND seat_id=$3`, trainRunID,
			resolution.Route.Generation().Int64(), resourceID).Scan(&view.SourceVersion, &active); err != nil {
			return httpapi.OperatorBookingStateView{}, mapOperatorStateReadError(err)
		}
		view.Active = &active
	case httpapi.OperatorBookingPolicyState:
		var policyVersion int64
		if err := tx.QueryRow(ctx, `SELECT source_version,booking_policy_version
FROM train_run_booking_snapshots WHERE train_run_id=$1 AND assignment_generation=$2 AND active`,
			trainRunID, resolution.Route.Generation().Int64()).Scan(&view.SourceVersion, &policyVersion); err != nil {
			return httpapi.OperatorBookingStateView{}, mapOperatorStateReadError(err)
		}
		view.BookingPolicyVersion = &policyVersion
	default:
		return httpapi.OperatorBookingStateView{}, httpapi.ErrInvalidInput
	}
	if view.SourceVersion < 1 {
		return httpapi.OperatorBookingStateView{}, httpapi.ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return httpapi.OperatorBookingStateView{}, httpapi.ErrUnavailable
	}
	return view, nil
}

func requireReadableOperatorAuthority(ctx context.Context, tx pgx.Tx, trainRunID uuid.UUID, generation int64) error {
	var storedGeneration int64
	var writeEnabled, active bool
	var state string
	err := tx.QueryRow(ctx, `SELECT fence.assignment_generation,fence.write_enabled,fence.state,snapshot.active
FROM train_run_write_fences AS fence JOIN train_run_booking_snapshots AS snapshot
 ON snapshot.train_run_id=fence.train_run_id AND snapshot.assignment_generation=fence.assignment_generation
WHERE fence.train_run_id=$1`, trainRunID).Scan(
		&storedGeneration, &writeEnabled, &state, &active,
	)
	if err != nil {
		return mapOperatorStateReadError(err)
	}
	if storedGeneration != generation {
		return sharding.ErrAssignmentStale
	}
	if !writeEnabled || state != "active" || !active {
		return sharding.ErrWriteFenced
	}
	return nil
}

func mapOperatorStateReadError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return httpapi.ErrNotFound
	}
	return httpapi.ErrUnavailable
}

var _ httpapi.OperatorBookingStateQueries = (*PhysicalOperatorBookingStateReader)(nil)
