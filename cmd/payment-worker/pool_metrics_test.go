package main

import (
	"context"
	"testing"
	"time"

	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
)

func TestRecordDatabasePoolSnapshotsIncludesControlAndAllowlistedShards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	control, err := postgresx.NewBoundedPool(ctx, "postgres://synthetic@127.0.0.1:1/railway?connect_timeout=1", 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(control.Close)
	shards, err := shardphysical.NewRegistry(ctx, shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "postgres://synthetic@127.0.0.1:1/railway?connect_timeout=1"},
		},
		MaxCount: 1,
		Limits: shardphysical.PoolLimits{
			MaxOpenConns: 7, MaxIdleConns: 2,
			StatementTimeout: time.Second, LockTimeout: time.Second,
		},
	}, shardphysical.OpenPGXPool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shards.Close)

	recorder := &poolSnapshotRecorder{}
	recordDatabasePoolSnapshots(recorder, control, shards)
	if len(recorder.observations) != 2 {
		t.Fatalf("pool observations = %d, want 2", len(recorder.observations))
	}
	if recorder.observations[0].role != "control" || recorder.observations[0].shard != "none" || recorder.observations[0].snapshot.MaxConnections != 5 {
		t.Fatalf("control observation = %+v", recorder.observations[0])
	}
	if recorder.observations[1].role != "booking_shard" || recorder.observations[1].shard != "physical-shard-0" || recorder.observations[1].snapshot.MaxConnections != 7 {
		t.Fatalf("shard observation = %+v", recorder.observations[1])
	}
}

type poolObservation struct {
	role     string
	shard    string
	snapshot platformmetrics.DatabasePoolSnapshot
}

type poolSnapshotRecorder struct{ observations []poolObservation }

func (recorder *poolSnapshotRecorder) RecordDatabasePoolSnapshot(role, shard string, snapshot platformmetrics.DatabasePoolSnapshot) {
	recorder.observations = append(recorder.observations, poolObservation{role: role, shard: shard, snapshot: snapshot})
}
