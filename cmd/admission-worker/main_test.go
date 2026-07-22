package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

func TestInitialAdmissionPassHonorsDisabledConfiguration(t *testing.T) {
	t.Parallel()
	calls := 0
	runInitialAdmissionPass(false, func() { calls++ })
	if calls != 0 {
		t.Fatalf("disabled initial pass calls = %d, want 0", calls)
	}
	runInitialAdmissionPass(true, func() { calls++ })
	if calls != 1 {
		t.Fatalf("enabled initial pass calls = %d, want 1", calls)
	}
}

func TestDisabledAdmissionWorkerWaitsForShutdownOrServerFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForDisabledAdmissionWorker(ctx, make(chan error)); err != nil {
		t.Fatalf("shutdown wait error = %v", err)
	}
	closed := make(chan error, 1)
	closed <- http.ErrServerClosed
	if err := waitForDisabledAdmissionWorker(context.Background(), closed); err != nil {
		t.Fatalf("server-close wait error = %v", err)
	}
	failed := make(chan error, 1)
	failed <- errors.New("listener secret detail")
	if err := waitForDisabledAdmissionWorker(context.Background(), failed); err == nil ||
		err.Error() != "health server failed" {
		t.Fatalf("server-failure wait error = %v", err)
	}
}

func TestAdmissionReadinessTimeoutIsBounded(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.DatabaseTimeout = time.Second
	cfg.RedisTimeout = 3 * time.Second
	if got := admissionReadinessTimeout(cfg); got != 3*time.Second {
		t.Fatalf("admissionReadinessTimeout() = %v, want 3s", got)
	}
	cfg.RedisTimeout = 30 * time.Second
	if got := admissionReadinessTimeout(cfg); got != 2*time.Second {
		t.Fatalf("bounded admissionReadinessTimeout() = %v, want 2s", got)
	}
}

func TestPublicWorkerReasonRedactsUnexpectedDetails(t *testing.T) {
	t.Parallel()
	if got := publicWorkerReason(errors.New("redis://:secret@example")); got != "worker failure" {
		t.Fatalf("publicWorkerReason() = %q", got)
	}
}
