package postgres

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// bookingRoutedTx keeps schema selection and fencing inside the sharding
// adapter while exposing only the established pgx transaction to booking SQL.
type bookingRoutedTx interface {
	PGXTx() pgx.Tx
	Route() sharding.ShardRoute
	Commit(context.Context) error
	Rollback(context.Context) error
}

type bookingShardRouter interface {
	ResolveTrainRun(context.Context, uuid.UUID) (sharding.ShardRoute, error)
	RefreshTrainRun(context.Context, uuid.UUID) (sharding.ShardRoute, error)
	ResolveReservation(context.Context, uuid.UUID) (sharding.ShardRoute, error)
	ResolveReservationForOwner(context.Context, uuid.UUID, uuid.UUID) (sharding.ShardRoute, error)
	BeginTrainRunWrite(context.Context, sharding.ShardRoute) (bookingRoutedTx, error)
	BeginTrainRunRead(context.Context, sharding.ShardRoute) (bookingRoutedTx, error)
	ListEnabledShards(context.Context) ([]sharding.ShardID, error)
}

type bookingRouterAdapter struct{ router *shardingpostgres.Router }

func (adapter bookingRouterAdapter) ResolveTrainRun(ctx context.Context, id uuid.UUID) (sharding.ShardRoute, error) {
	return adapter.router.ResolveTrainRun(ctx, id)
}

func (adapter bookingRouterAdapter) RefreshTrainRun(ctx context.Context, id uuid.UUID) (sharding.ShardRoute, error) {
	return adapter.router.RefreshTrainRun(ctx, id)
}

func (adapter bookingRouterAdapter) ResolveReservation(ctx context.Context, id uuid.UUID) (sharding.ShardRoute, error) {
	return adapter.router.ResolveReservation(ctx, id)
}

func (adapter bookingRouterAdapter) ResolveReservationForOwner(
	ctx context.Context,
	id, ownerUserID uuid.UUID,
) (sharding.ShardRoute, error) {
	return adapter.router.ResolveReservationForOwner(ctx, id, ownerUserID)
}

func (adapter bookingRouterAdapter) BeginTrainRunWrite(ctx context.Context, route sharding.ShardRoute) (bookingRoutedTx, error) {
	return adapter.router.BeginTrainRunWrite(ctx, route)
}

func (adapter bookingRouterAdapter) BeginTrainRunRead(ctx context.Context, route sharding.ShardRoute) (bookingRoutedTx, error) {
	return adapter.router.BeginTrainRunRead(ctx, route)
}

func (adapter bookingRouterAdapter) ListEnabledShards(ctx context.Context) ([]sharding.ShardID, error) {
	return adapter.router.ListEnabledShards(ctx)
}

// NewSharded enables explicit catalog routing for every booking command. The
// legacy constructors remain unchanged for Milestone 1-3 compatibility.
func NewSharded(pool *pgxpool.Pool, router *shardingpostgres.Router) (*Store, error) {
	return NewShardedWithReservationQuotaLimits(pool, router, DefaultReservationQuotaLimits())
}

func NewShardedWithReservationQuotaLimits(
	pool *pgxpool.Pool,
	router *shardingpostgres.Router,
	limits ReservationQuotaLimits,
) (*Store, error) {
	if pool == nil || router == nil || !limits.valid() {
		return nil, ErrInvalidArgument
	}
	return &Store{
		pool:                   pool,
		reservationQuotaLimits: limits,
		shards:                 bookingRouterAdapter{router: router},
	}, nil
}

func (s *Store) beginTrainRunWrite(ctx context.Context, trainRunID uuid.UUID) (*Tx, error) {
	if s == nil || s.shards == nil || trainRunID == uuid.Nil {
		return nil, ErrInvalidArgument
	}
	route, err := s.shards.ResolveTrainRun(ctx, trainRunID)
	if err != nil {
		return nil, err
	}
	return s.beginResolvedWrite(ctx, route)
}

func (s *Store) beginCreateHoldTransaction(ctx context.Context, trainRunID uuid.UUID) (*Tx, error) {
	if s != nil && s.shards != nil {
		return s.beginTrainRunWrite(ctx, trainRunID)
	}
	return s.Begin(ctx)
}

func (s *Store) beginReservationWrite(ctx context.Context, reservationID, ownerUserID uuid.UUID) (*Tx, error) {
	if s == nil || s.shards == nil || reservationID == uuid.Nil || ownerUserID == uuid.Nil {
		return nil, ErrInvalidArgument
	}
	route, err := s.shards.ResolveReservationForOwner(ctx, reservationID, ownerUserID)
	if err != nil {
		return nil, err
	}
	return s.beginResolvedWrite(ctx, route)
}

func (s *Store) beginReservationMaintenanceWrite(ctx context.Context, reservationID uuid.UUID) (*Tx, error) {
	if s == nil || s.shards == nil || reservationID == uuid.Nil {
		return nil, ErrInvalidArgument
	}
	route, err := s.shards.ResolveReservation(ctx, reservationID)
	if err != nil {
		return nil, err
	}
	return s.beginResolvedWrite(ctx, route)
}

func (s *Store) beginReservationCommandTransaction(
	ctx context.Context,
	reservationID, ownerUserID uuid.UUID,
) (*Tx, error) {
	if s != nil && s.shards != nil {
		return s.beginReservationWrite(ctx, reservationID, ownerUserID)
	}
	return s.Begin(ctx)
}

func (s *Store) beginResolvedWrite(ctx context.Context, route sharding.ShardRoute) (*Tx, error) {
	routed, err := s.shards.BeginTrainRunWrite(ctx, route)
	if errors.Is(err, sharding.ErrAssignmentStale) {
		route, err = s.shards.RefreshTrainRun(ctx, route.TrainRunID())
		if err != nil {
			return nil, err
		}
		routed, err = s.shards.BeginTrainRunWrite(ctx, route)
	}
	if err != nil {
		return nil, err
	}
	return newBookingTx(routed)
}

func (s *Store) beginTrainRunRead(ctx context.Context, trainRunID uuid.UUID) (*Tx, error) {
	if s == nil || s.shards == nil || trainRunID == uuid.Nil {
		return nil, ErrInvalidArgument
	}
	route, err := s.shards.ResolveTrainRun(ctx, trainRunID)
	if err != nil {
		return nil, err
	}
	return s.beginResolvedRead(ctx, route)
}

func (s *Store) beginReservationRead(ctx context.Context, reservationID, ownerUserID uuid.UUID) (*Tx, error) {
	if s == nil || s.shards == nil || reservationID == uuid.Nil || ownerUserID == uuid.Nil {
		return nil, ErrInvalidArgument
	}
	route, err := s.shards.ResolveReservationForOwner(ctx, reservationID, ownerUserID)
	if err != nil {
		return nil, err
	}
	return s.beginResolvedRead(ctx, route)
}

func (s *Store) beginResolvedRead(ctx context.Context, route sharding.ShardRoute) (*Tx, error) {
	routed, err := s.shards.BeginTrainRunRead(ctx, route)
	if errors.Is(err, sharding.ErrAssignmentStale) {
		route, err = s.shards.RefreshTrainRun(ctx, route.TrainRunID())
		if err != nil {
			return nil, err
		}
		routed, err = s.shards.BeginTrainRunRead(ctx, route)
	}
	if err != nil {
		return nil, err
	}
	return newBookingTx(routed)
}

func newBookingTx(routed bookingRoutedTx) (*Tx, error) {
	if routed == nil || routed.PGXTx() == nil || routed.Route().TrainRunID() == uuid.Nil {
		return nil, sharding.ErrShardUnavailable
	}
	return &Tx{
		tx:     routed.PGXTx(),
		route:  routed.Route(),
		routed: routed,
	}, nil
}
