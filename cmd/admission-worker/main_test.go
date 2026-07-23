package main

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

func TestAdmissionSchemaVersionMatchesLatestMigration(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("glob migrations: %v", err)
	}
	latest := 0
	for _, path := range paths {
		prefix, _, ok := strings.Cut(filepath.Base(path), "_")
		if !ok {
			continue
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			t.Fatalf("parse migration version from %s: %v", filepath.Base(path), err)
		}
		if version > latest {
			latest = version
		}
	}
	if latest == 0 {
		t.Fatal("no versioned up migrations found")
	}
	if admissionSchemaVersion != latest {
		t.Fatalf("admission schema version = %d, latest migration = %d", admissionSchemaVersion, latest)
	}
}

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
