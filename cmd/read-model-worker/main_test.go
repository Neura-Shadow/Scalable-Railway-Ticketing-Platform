package main

import (
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

func TestReadModelReadinessTimeoutUsesSlowerOwnedDependency(t *testing.T) {
	cfg := config.Defaults()
	cfg.DatabaseTimeout = 2 * time.Second
	cfg.RedisTimeout = 3 * time.Second
	if got := readModelReadinessTimeout(cfg); got != 3*time.Second {
		t.Fatalf("readModelReadinessTimeout() = %s, want 3s", got)
	}
}

func TestReadModelWorkerDefaultsDisabledSoNoInitialPassRuns(t *testing.T) {
	if config.Defaults().ReadModelWorkerEnabled {
		t.Fatal("read-model worker default enabled, want explicit opt-in")
	}
}
