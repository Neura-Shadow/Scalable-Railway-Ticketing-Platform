package main

import (
	"context"
	"sort"
	"time"

	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/jackc/pgx/v5/pgxpool"
)

type databasePoolMetrics interface {
	RecordDatabasePoolSnapshot(databaseRole, shardID string, snapshot platformmetrics.DatabasePoolSnapshot)
}

func recordDatabasePoolSnapshots(recorder databasePoolMetrics, control *pgxpool.Pool, shards *shardphysical.Registry) {
	if recorder == nil || control == nil {
		return
	}
	stat := control.Stat()
	recorder.RecordDatabasePoolSnapshot("control", "none", platformmetrics.DatabasePoolSnapshot{
		TotalConnections: stat.TotalConns(), AcquiredConnections: stat.AcquiredConns(),
		IdleConnections: stat.IdleConns(), MaxConnections: stat.MaxConns(),
		AcquireCount: stat.AcquireCount(), AcquireDuration: stat.AcquireDuration(),
		EmptyAcquireCount: stat.EmptyAcquireCount(), CancelledAcquireCount: stat.CanceledAcquireCount(),
	})
	if shards == nil {
		return
	}
	snapshots := shards.PoolSnapshots()
	ids := make([]sharding.ShardID, 0, len(snapshots))
	for id := range snapshots {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		snapshot := snapshots[id]
		recorder.RecordDatabasePoolSnapshot("booking_shard", id.String(), platformmetrics.DatabasePoolSnapshot{
			TotalConnections: snapshot.TotalConnections, AcquiredConnections: snapshot.AcquiredConnections,
			IdleConnections: snapshot.IdleConnections, MaxConnections: snapshot.MaxConnections,
			AcquireCount: snapshot.AcquireCount, AcquireDuration: snapshot.AcquireDuration,
			EmptyAcquireCount: snapshot.EmptyAcquireCount, CancelledAcquireCount: snapshot.CancelledAcquireCount,
		})
	}
}

func startDatabasePoolObserver(parent context.Context, recorder databasePoolMetrics, control *pgxpool.Pool, shards *shardphysical.Registry, interval time.Duration) func() {
	if parent == nil || recorder == nil || control == nil || interval <= 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		recordDatabasePoolSnapshots(recorder, control, shards)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				recordDatabasePoolSnapshots(recorder, control, shards)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
