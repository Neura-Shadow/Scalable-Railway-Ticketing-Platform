package postgres

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/postgres"
	"github.com/google/uuid"
)

func NewShardedStore(db DBTX, router *shardingpostgres.Router) (*Store, error) {
	if db == nil || router == nil {
		return nil, ErrInvalidQuery
	}
	return &Store{db: db, shards: router}, nil
}

func (store *Store) beginTrainRunRead(ctx context.Context, trainRunID uuid.UUID) (*shardingpostgres.RoutedTx, error) {
	if store == nil || store.shards == nil || trainRunID == uuid.Nil {
		return nil, sharding.ErrShardUnavailable
	}
	route, err := store.shards.ResolveTrainRun(ctx, trainRunID)
	if err != nil {
		return nil, err
	}
	tx, err := store.shards.BeginTrainRunRead(ctx, route)
	if errors.Is(err, sharding.ErrAssignmentStale) {
		route, err = store.shards.RefreshTrainRun(ctx, trainRunID)
		if err != nil {
			return nil, err
		}
		tx, err = store.shards.BeginTrainRunRead(ctx, route)
	}
	return tx, err
}

// AvailabilityAssignmentGeneration returns the authoritative catalog
// generation used to validate an availability cache entry. It deliberately
// bypasses the advisory process-local route cache so a completed cutover
// invalidates old Redis payloads before the asynchronous namespace rotation
// event is consumed. Legacy mode uses generation zero.
func (store *Store) AvailabilityAssignmentGeneration(ctx context.Context, rawTrainRunID string) (int64, error) {
	if store == nil {
		return 0, ErrPersistence
	}
	if store.shards == nil {
		return 0, nil
	}
	trainRunID, err := uuid.Parse(rawTrainRunID)
	if err != nil || trainRunID == uuid.Nil {
		return 0, ErrInvalidQuery
	}
	route, err := store.shards.RefreshTrainRun(ctx, trainRunID)
	if err != nil {
		return 0, safeQueryError(err)
	}
	return route.Generation().Int64(), nil
}
