package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

func TestOutboxSchemaVersionMatchesCurrentControlMigration(t *testing.T) {
	t.Parallel()
	if outboxSchemaVersion != 11 {
		t.Fatalf("outboxSchemaVersion = %d, want 11", outboxSchemaVersion)
	}
}

func TestAllReadyChecksEverySelectedPublisherDependency(t *testing.T) {
	t.Parallel()

	databaseCalls := 0
	redisCalls := 0
	check := allReady(
		func(context.Context) error {
			databaseCalls++
			return nil
		},
		func(context.Context) error {
			redisCalls++
			return errors.New("redis unavailable")
		},
	)

	if err := check(context.Background()); err == nil {
		t.Fatal("allReady() error = nil, want Redis dependency failure")
	}
	if databaseCalls != 1 || redisCalls != 1 {
		t.Fatalf("dependency calls = database %d redis %d, want one each", databaseCalls, redisCalls)
	}
}

func TestOutboxReadinessTimeoutCoversRedisPublisher(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.OutboxPublisher = "redis_stream"
	cfg.DatabaseTimeout = time.Second
	cfg.RedisTimeout = 3 * time.Second
	if got := outboxReadinessTimeout(cfg); got != 3*time.Second {
		t.Fatalf("outboxReadinessTimeout() = %s, want 3s", got)
	}
}

func TestPhysicalOutboxWorkerConfigKeepsBatchAsGlobalPassLimit(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.OutboxBatchSize = 101
	cfg.PhysicalWorkerShardTimeout = 4 * time.Second
	cfg.WorkerShardConcurrency = 1

	got := physicalOutboxWorkerConfig(cfg, 2)
	if got.MaxConcurrency != 1 || got.PerShardLimit != 101 || got.PassLimit != 101 || got.ShardTimeout != 4*time.Second {
		t.Fatalf("physicalOutboxWorkerConfig() = %+v", got)
	}

	cfg.WorkerShardConcurrency = 3
	if got := physicalOutboxWorkerConfig(cfg, 2); got.MaxConcurrency != 2 {
		t.Fatalf("physicalOutboxWorkerConfig() capped concurrency = %d, want 2", got.MaxConcurrency)
	}
}
