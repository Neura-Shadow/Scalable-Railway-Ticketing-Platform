package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

func TestInitialExpirationPassHonorsDisabledConfiguration(t *testing.T) {
	t.Parallel()

	calls := 0
	runInitialExpirationPass(false, func() { calls++ })
	if calls != 0 {
		t.Fatalf("disabled initial expiration pass calls = %d, want 0", calls)
	}

	runInitialExpirationPass(true, func() { calls++ })
	if calls != 1 {
		t.Fatalf("enabled initial expiration pass calls = %d, want 1", calls)
	}
}

func TestPhysicalWorkerConfigKeepsBatchAsGlobalPassLimit(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.HoldExpirerBatchSize = 51
	cfg.PhysicalWorkerShardTimeout = 3 * time.Second

	got := physicalWorkerConfig(cfg, 2)
	if got.MaxConcurrency != 2 || got.PerShardLimit != 51 || got.PassLimit != 51 || got.ShardTimeout != 3*time.Second {
		t.Fatalf("physicalWorkerConfig() = %+v", got)
	}
}

func TestDisabledHoldExpirerWaitsForShutdownOrServerFailure(t *testing.T) {
	t.Parallel()

	t.Run("shutdown", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := waitForDisabledHoldExpirer(ctx, make(chan error)); err != nil {
			t.Fatalf("waitForDisabledHoldExpirer() error = %v", err)
		}
	})

	t.Run("server closed", func(t *testing.T) {
		t.Parallel()
		serverErrors := make(chan error, 1)
		serverErrors <- http.ErrServerClosed
		if err := waitForDisabledHoldExpirer(context.Background(), serverErrors); err != nil {
			t.Fatalf("waitForDisabledHoldExpirer() error = %v", err)
		}
	})

	t.Run("server failure", func(t *testing.T) {
		t.Parallel()
		want := errors.New("listener failed")
		serverErrors := make(chan error, 1)
		serverErrors <- want
		if err := waitForDisabledHoldExpirer(context.Background(), serverErrors); !errors.Is(err, want) {
			t.Fatalf("waitForDisabledHoldExpirer() error = %v, want %v", err, want)
		}
	})
}
