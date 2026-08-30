package authority_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestControlWriterRunsMutationOnlyForMatchingActiveAuthority(t *testing.T) {
	t.Parallel()

	region := mustRegion(t, "region-a")
	epoch := mustEpoch(t, 7)
	deployment, err := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	if err != nil {
		t.Fatalf("NewDeployment() error = %v", err)
	}
	tx := controlTx{snapshot: mustSnapshot(t, region, epoch, authority.StateActive, true)}
	db := &transactionRunner[controlTx]{tx: tx}
	writer, err := authority.NewControlWriter(deployment, db)
	if err != nil {
		t.Fatalf("NewControlWriter() error = %v", err)
	}

	mutations := 0
	if err := writer.Write(context.Background(), func(got controlTx) error {
		mutations++
		if got.snapshot != tx.snapshot {
			t.Fatalf("mutation transaction = %+v, want %+v", got, tx)
		}
		return nil
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if db.transactions != 1 || mutations != 1 {
		t.Fatalf("transactions/mutations = %d/%d, want 1/1", db.transactions, mutations)
	}
}

func TestControlWriterRejectsPassiveAndRecoveryBeforeOpeningTransaction(t *testing.T) {
	t.Parallel()

	region := mustRegion(t, "region-a")
	epoch := mustEpoch(t, 7)
	for _, role := range []authority.Role{authority.RolePassive, authority.RoleRecovery} {
		role := role
		t.Run(string(role), func(t *testing.T) {
			t.Parallel()
			deployment, err := authority.NewDeployment(region, role, epoch, true)
			if err != nil {
				t.Fatalf("NewDeployment() error = %v", err)
			}
			db := &transactionRunner[controlTx]{tx: controlTx{
				snapshot: mustSnapshot(t, region, epoch, authority.StateActive, true),
			}}
			writer, err := authority.NewControlWriter(deployment, db)
			if err != nil {
				t.Fatalf("NewControlWriter() error = %v", err)
			}

			mutations := 0
			err = writer.Write(context.Background(), func(controlTx) error {
				mutations++
				return nil
			})
			if !errors.Is(err, authority.ErrRoleNotActive) {
				t.Fatalf("Write() error = %v, want ErrRoleNotActive", err)
			}
			if db.transactions != 0 || mutations != 0 {
				t.Fatalf("transactions/mutations = %d/%d, want 0/0", db.transactions, mutations)
			}
		})
	}
}

func TestControlWriterRejectsStaleAndFutureDurableEpochs(t *testing.T) {
	t.Parallel()

	region := mustRegion(t, "region-a")
	configuredEpoch := mustEpoch(t, 19)
	deployment, err := authority.NewDeployment(region, authority.RoleActive, configuredEpoch, true)
	if err != nil {
		t.Fatalf("NewDeployment() error = %v", err)
	}
	for _, durableEpoch := range []authority.Epoch{mustEpoch(t, 18), mustEpoch(t, 20)} {
		durableEpoch := durableEpoch
		t.Run(strconv.FormatUint(durableEpoch.Uint64(), 10), func(t *testing.T) {
			t.Parallel()
			db := &transactionRunner[controlTx]{tx: controlTx{
				snapshot: mustSnapshot(t, region, durableEpoch, authority.StateActive, true),
			}}
			writer, err := authority.NewControlWriter(deployment, db)
			if err != nil {
				t.Fatalf("NewControlWriter() error = %v", err)
			}
			mutations := 0
			err = writer.Write(context.Background(), func(controlTx) error {
				mutations++
				return nil
			})
			if !errors.Is(err, authority.ErrEpochMismatch) {
				t.Fatalf("Write() error = %v, want ErrEpochMismatch", err)
			}
			if mutations != 0 {
				t.Fatalf("mutations = %d, want 0", mutations)
			}
		})
	}
}

func TestShardWriterRunsMutationOnlyForMatchingRegionalAndGenerationFences(t *testing.T) {
	t.Parallel()

	region := mustRegion(t, "region-b")
	epoch := mustEpoch(t, 13)
	deployment, err := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	if err != nil {
		t.Fatalf("NewDeployment() error = %v", err)
	}
	trainRunID := uuid.New()
	generation, err := sharding.NewAssignmentGeneration(17)
	if err != nil {
		t.Fatalf("NewAssignmentGeneration() error = %v", err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatalf("NewShardRoute() error = %v", err)
	}
	tx := shardTx{
		snapshot: mustSnapshot(t, region, epoch, authority.StateActive, true),
		fence: mustShardFence(
			t,
			trainRunID,
			sharding.ShardPhysicalZero,
			generation,
			authority.ShardFenceActive,
			true,
		),
	}
	db := &transactionRunner[shardTx]{tx: tx}
	writer, err := authority.NewShardWriter(deployment, sharding.ShardPhysicalZero, db)
	if err != nil {
		t.Fatalf("NewShardWriter() error = %v", err)
	}

	mutations := 0
	if err := writer.Write(context.Background(), route, func(shardTx) error {
		mutations++
		return nil
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if db.transactions != 1 || mutations != 1 {
		t.Fatalf("transactions/mutations = %d/%d, want 1/1", db.transactions, mutations)
	}
}

func TestShardWriterRejectsRouteAndGenerationMismatchWithoutMutation(t *testing.T) {
	t.Parallel()

	region := mustRegion(t, "region-b")
	epoch := mustEpoch(t, 23)
	deployment, err := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	if err != nil {
		t.Fatalf("NewDeployment() error = %v", err)
	}
	trainRunID := uuid.New()
	generation := mustGeneration(t, 29)
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatalf("NewShardRoute() error = %v", err)
	}

	t.Run("route selects another local database", func(t *testing.T) {
		db := &transactionRunner[shardTx]{}
		writer, err := authority.NewShardWriter(deployment, sharding.ShardPhysicalOne, db)
		if err != nil {
			t.Fatalf("NewShardWriter() error = %v", err)
		}
		mutations := 0
		err = writer.Write(context.Background(), route, func(shardTx) error {
			mutations++
			return nil
		})
		if !errors.Is(err, authority.ErrShardRouteMismatch) {
			t.Fatalf("Write() error = %v, want ErrShardRouteMismatch", err)
		}
		if db.transactions != 0 || mutations != 0 {
			t.Fatalf("transactions/mutations = %d/%d, want 0/0", db.transactions, mutations)
		}
	})

	t.Run("local generation differs", func(t *testing.T) {
		db := &transactionRunner[shardTx]{tx: shardTx{
			snapshot: mustSnapshot(t, region, epoch, authority.StateActive, true),
			fence: mustShardFence(
				t,
				trainRunID,
				sharding.ShardPhysicalZero,
				mustGeneration(t, 30),
				authority.ShardFenceActive,
				true,
			),
		}}
		writer, err := authority.NewShardWriter(deployment, sharding.ShardPhysicalZero, db)
		if err != nil {
			t.Fatalf("NewShardWriter() error = %v", err)
		}
		mutations := 0
		err = writer.Write(context.Background(), route, func(shardTx) error {
			mutations++
			return nil
		})
		if !errors.Is(err, authority.ErrShardGenerationMismatch) {
			t.Fatalf("Write() error = %v, want ErrShardGenerationMismatch", err)
		}
		if mutations != 0 {
			t.Fatalf("mutations = %d, want 0", mutations)
		}
	})
}

func mustRegion(t *testing.T, raw string) authority.Region {
	t.Helper()
	region, err := authority.ParseRegion(raw)
	if err != nil {
		t.Fatalf("ParseRegion(%q) error = %v", raw, err)
	}
	return region
}

func mustEpoch(t *testing.T, raw uint64) authority.Epoch {
	t.Helper()
	epoch, err := authority.NewEpoch(raw)
	if err != nil {
		t.Fatalf("NewEpoch(%d) error = %v", raw, err)
	}
	return epoch
}

func mustGeneration(t *testing.T, raw int64) sharding.AssignmentGeneration {
	t.Helper()
	generation, err := sharding.NewAssignmentGeneration(raw)
	if err != nil {
		t.Fatalf("NewAssignmentGeneration(%d) error = %v", raw, err)
	}
	return generation
}

func mustSnapshot(
	t *testing.T,
	region authority.Region,
	epoch authority.Epoch,
	state authority.State,
	writesEnabled bool,
) authority.Snapshot {
	t.Helper()
	snapshot, err := authority.NewSnapshot(region, epoch, state, writesEnabled)
	if err != nil {
		t.Fatalf("NewSnapshot() error = %v", err)
	}
	return snapshot
}

type controlTx struct {
	snapshot authority.Snapshot
}

func mustShardFence(
	t *testing.T,
	trainRunID uuid.UUID,
	shardID sharding.ShardID,
	generation sharding.AssignmentGeneration,
	state authority.ShardFenceState,
	writesEnabled bool,
) authority.ShardFence {
	t.Helper()
	fence, err := authority.NewShardFence(trainRunID, shardID, generation, state, writesEnabled)
	if err != nil {
		t.Fatalf("NewShardFence() error = %v", err)
	}
	return fence
}

type shardTx struct {
	snapshot authority.Snapshot
	fence    authority.ShardFence
}

func (tx shardTx) RegionalAuthority(context.Context) (authority.Snapshot, error) {
	return tx.snapshot, nil
}

func (tx shardTx) TrainRunFence(context.Context, uuid.UUID) (authority.ShardFence, error) {
	return tx.fence, nil
}

func (tx controlTx) RegionalAuthority(context.Context) (authority.Snapshot, error) {
	return tx.snapshot, nil
}

type transactionRunner[T any] struct {
	tx           T
	transactions int
}

func (runner *transactionRunner[T]) WithinTransaction(_ context.Context, fn func(T) error) error {
	runner.transactions++
	return fn(runner.tx)
}
