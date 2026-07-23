package sharding_test

import (
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestParseShardIDAcceptsOnlyFixedMilestoneFourTopology(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"legacy", "shard-0", "shard-1"} {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			got, err := sharding.ParseShardID(raw)
			if err != nil {
				t.Fatalf("ParseShardID(%q) returned error: %v", raw, err)
			}
			if got.String() != raw {
				t.Fatalf("ParseShardID(%q) = %q", raw, got.String())
			}
		})
	}

	for _, raw := range []string{"", "public", "booking_shard_0", "shard-2", "shard-0,public", "SHARD-0"} {
		raw := raw
		t.Run("reject_"+raw, func(t *testing.T) {
			t.Parallel()
			if _, err := sharding.ParseShardID(raw); err == nil {
				t.Fatalf("ParseShardID(%q) unexpectedly succeeded", raw)
			}
		})
	}
}

func TestAssignmentGenerationMustBePositive(t *testing.T) {
	t.Parallel()

	generation, err := sharding.NewAssignmentGeneration(7)
	if err != nil {
		t.Fatalf("NewAssignmentGeneration(7) returned error: %v", err)
	}
	if generation.Int64() != 7 {
		t.Fatalf("generation = %d, want 7", generation.Int64())
	}

	for _, value := range []int64{0, -1} {
		if _, err := sharding.NewAssignmentGeneration(value); err == nil {
			t.Fatalf("NewAssignmentGeneration(%d) unexpectedly succeeded", value)
		}
	}
}

func TestShardRouteRejectsUnknownOrIncompleteAuthority(t *testing.T) {
	t.Parallel()

	trainRunID := uuid.New()
	generation, err := sharding.NewAssignmentGeneration(3)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardZero, generation)
	if err != nil {
		t.Fatalf("NewShardRoute returned error: %v", err)
	}
	if route.TrainRunID() != trainRunID || route.ShardID() != sharding.ShardZero || route.Generation() != generation {
		t.Fatalf("route did not preserve validated authority: %+v", route)
	}

	for name, input := range map[string]struct {
		trainRunID uuid.UUID
		shardID    sharding.ShardID
		generation sharding.AssignmentGeneration
	}{
		"missing train run": {shardID: sharding.ShardZero, generation: generation},
		"unknown shard":     {trainRunID: trainRunID, shardID: sharding.ShardID("booking_shard_0"), generation: generation},
		"zero generation":   {trainRunID: trainRunID, shardID: sharding.ShardZero},
	} {
		input := input
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := sharding.NewShardRoute(input.trainRunID, input.shardID, input.generation); err == nil {
				t.Fatal("NewShardRoute unexpectedly succeeded")
			}
		})
	}
}
