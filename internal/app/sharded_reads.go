package app

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type readRoutedTx interface {
	PGXTx() pgx.Tx
	Route() sharding.ShardRoute
	Commit(context.Context) error
	Rollback(context.Context) error
}

type readShardRouter interface {
	ResolveReservationForOwner(context.Context, uuid.UUID, uuid.UUID) (sharding.ShardRoute, error)
	ResolveTicketOrderForOwner(context.Context, uuid.UUID, uuid.UUID) (sharding.ShardRoute, error)
	RefreshTrainRun(context.Context, uuid.UUID) (sharding.ShardRoute, error)
	BeginTrainRunRead(context.Context, sharding.ShardRoute) (readRoutedTx, error)
}

type readRouterAdapter struct{ router *shardingpostgres.Router }

func (adapter readRouterAdapter) ResolveReservationForOwner(
	ctx context.Context,
	id, ownerUserID uuid.UUID,
) (sharding.ShardRoute, error) {
	return adapter.router.ResolveReservationForOwner(ctx, id, ownerUserID)
}

func (adapter readRouterAdapter) ResolveTicketOrderForOwner(
	ctx context.Context,
	id, ownerUserID uuid.UUID,
) (sharding.ShardRoute, error) {
	return adapter.router.ResolveTicketOrderForOwner(ctx, id, ownerUserID)
}

func (adapter readRouterAdapter) RefreshTrainRun(ctx context.Context, id uuid.UUID) (sharding.ShardRoute, error) {
	return adapter.router.RefreshTrainRun(ctx, id)
}

func (adapter readRouterAdapter) BeginTrainRunRead(ctx context.Context, route sharding.ShardRoute) (readRoutedTx, error) {
	return adapter.router.BeginTrainRunRead(ctx, route)
}

func NewShardedPostgresReads(pool *pgxpool.Pool, router *shardingpostgres.Router) (*PostgresReads, error) {
	if pool == nil || router == nil {
		return nil, errors.New("sharded reads unavailable")
	}
	return &PostgresReads{pool: pool, shards: readRouterAdapter{router: router}}, nil
}

func (reads *PostgresReads) beginReservationRead(
	ctx context.Context,
	reservationID, ownerUserID uuid.UUID,
) (readRoutedTx, error) {
	if reads == nil || reads.shards == nil || reservationID == uuid.Nil || ownerUserID == uuid.Nil {
		return nil, sharding.ErrShardUnavailable
	}
	route, err := reads.shards.ResolveReservationForOwner(ctx, reservationID, ownerUserID)
	if err != nil {
		return nil, err
	}
	return reads.beginResolvedRead(ctx, route)
}

func (reads *PostgresReads) beginTicketOrderRead(
	ctx context.Context,
	orderID, ownerUserID uuid.UUID,
) (readRoutedTx, error) {
	if reads == nil || reads.shards == nil || orderID == uuid.Nil || ownerUserID == uuid.Nil {
		return nil, sharding.ErrShardUnavailable
	}
	route, err := reads.shards.ResolveTicketOrderForOwner(ctx, orderID, ownerUserID)
	if err != nil {
		return nil, err
	}
	return reads.beginResolvedRead(ctx, route)
}

func (reads *PostgresReads) beginResolvedRead(ctx context.Context, route sharding.ShardRoute) (readRoutedTx, error) {
	tx, err := reads.shards.BeginTrainRunRead(ctx, route)
	if errors.Is(err, sharding.ErrAssignmentStale) {
		route, err = reads.shards.RefreshTrainRun(ctx, route.TrainRunID())
		if err != nil {
			return nil, err
		}
		tx, err = reads.shards.BeginTrainRunRead(ctx, route)
	}
	return tx, err
}
