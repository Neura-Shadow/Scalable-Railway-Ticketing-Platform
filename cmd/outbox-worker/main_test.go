package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

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
