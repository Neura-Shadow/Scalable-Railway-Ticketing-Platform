package authority

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

var (
	ErrInvalidShardFence       = errors.New("invalid regional shard fence")
	ErrShardRouteMismatch      = errors.New("regional shard route mismatch")
	ErrShardGenerationMismatch = errors.New("regional shard generation mismatch")
	ErrShardWriteFenced        = errors.New("regional shard write is fenced")
)

// ShardFenceState is the database-local train-run write state.
type ShardFenceState string

const (
	ShardFenceActive   ShardFenceState = "active"
	ShardFenceDraining ShardFenceState = "draining"
	ShardFenceFenced   ShardFenceState = "fenced"
	ShardFenceRecovery ShardFenceState = "recovery"
)

func (state ShardFenceState) valid() bool {
	switch state {
	case ShardFenceActive, ShardFenceDraining, ShardFenceFenced, ShardFenceRecovery:
		return true
	default:
		return false
	}
}

// ShardFence is the local ownership row locked with the booking mutation.
type ShardFence struct {
	trainRunID    uuid.UUID
	shardID       sharding.ShardID
	generation    sharding.AssignmentGeneration
	state         ShardFenceState
	writesEnabled bool
}

func NewShardFence(
	trainRunID uuid.UUID,
	shardID sharding.ShardID,
	generation sharding.AssignmentGeneration,
	state ShardFenceState,
	writesEnabled bool,
) (ShardFence, error) {
	if trainRunID == uuid.Nil || !isPhysicalShard(shardID) || generation.Int64() <= 0 || !state.valid() {
		return ShardFence{}, ErrInvalidShardFence
	}
	return ShardFence{
		trainRunID:    trainRunID,
		shardID:       shardID,
		generation:    generation,
		state:         state,
		writesEnabled: writesEnabled,
	}, nil
}

func (fence ShardFence) TrainRunID() uuid.UUID                     { return fence.trainRunID }
func (fence ShardFence) ShardID() sharding.ShardID                 { return fence.shardID }
func (fence ShardFence) Generation() sharding.AssignmentGeneration { return fence.generation }
func (fence ShardFence) State() ShardFenceState                    { return fence.state }
func (fence ShardFence) WritesEnabled() bool                       { return fence.writesEnabled }

// ShardTransaction extends regional authority with the existing train-run
// generation fence loaded from the same physical-shard transaction.
type ShardTransaction interface {
	ControlTransaction
	TrainRunFence(context.Context, uuid.UUID) (ShardFence, error)
}

// ShardWriter validates both regional and physical-shard authority before a
// database-local booking mutation.
type ShardWriter[T ShardTransaction] struct {
	deployment Deployment
	shardID    sharding.ShardID
	db         TransactionRunner[T]
}

func NewShardWriter[T ShardTransaction](
	deployment Deployment,
	shardID sharding.ShardID,
	db TransactionRunner[T],
) (ShardWriter[T], error) {
	if !isPhysicalShard(shardID) || db == nil {
		return ShardWriter[T]{}, ErrInvalidWriter
	}
	return ShardWriter[T]{deployment: deployment, shardID: shardID, db: db}, nil
}

// Write runs one local transaction program. External or cross-database I/O
// from mutation is forbidden; it would invalidate uncertainty and lock bounds.
func (writer ShardWriter[T]) Write(
	ctx context.Context,
	route sharding.ShardRoute,
	mutation func(T) error,
) error {
	if ctx == nil || mutation == nil {
		return ErrInvalidWriter
	}
	if err := writer.deployment.allowsNormalWrite(); err != nil {
		return err
	}
	if route.ShardID() != writer.shardID {
		return ErrShardRouteMismatch
	}
	return writer.db.WithinTransaction(ctx, func(tx T) error {
		snapshot, err := tx.RegionalAuthority(ctx)
		if err != nil {
			return err
		}
		if err := writer.deployment.matches(snapshot); err != nil {
			return err
		}
		fence, err := tx.TrainRunFence(ctx, route.TrainRunID())
		if err != nil {
			return err
		}
		if fence.trainRunID != route.TrainRunID() || fence.shardID != route.ShardID() {
			return ErrShardRouteMismatch
		}
		if fence.generation != route.Generation() {
			return ErrShardGenerationMismatch
		}
		if fence.state != ShardFenceActive || !fence.writesEnabled {
			return ErrShardWriteFenced
		}
		return mutation(tx)
	})
}

func isPhysicalShard(shardID sharding.ShardID) bool {
	return shardID == sharding.ShardPhysicalZero || shardID == sharding.ShardPhysicalOne
}

// AuthorizeShard verifies regional authority and the train-run generation
// fence already locked by the caller's local shard transaction.
func (deployment Deployment) AuthorizeShard(route sharding.ShardRoute, fence ShardFence) error {
	if err := deployment.allowsNormalWrite(); err != nil {
		return err
	}
	if !isPhysicalShard(route.ShardID()) || fence.trainRunID != route.TrainRunID() || fence.shardID != route.ShardID() {
		return ErrShardRouteMismatch
	}
	if fence.generation != route.Generation() {
		return ErrShardGenerationMismatch
	}
	if fence.state != ShardFenceActive || !fence.writesEnabled {
		return ErrShardWriteFenced
	}
	return nil
}
