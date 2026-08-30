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

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile"
)

func TestControlSchemaVersionMatchesLatestMigration(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	latest := 0
	for _, path := range paths {
		prefix, _, ok := strings.Cut(filepath.Base(path), "_")
		if !ok {
			continue
		}
		version, err := strconv.Atoi(prefix)
		if err == nil && version > latest {
			latest = version
		}
	}
	if controlSchemaVersion != latest {
		t.Fatalf("control schema = %d, latest = %d", controlSchemaVersion, latest)
	}
}

func TestLoadConfigUsesBoundedIndependentConnections(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"DATABASE_URL":                          "postgres://control/db",
		"BOOKING_SHARD_0_DATABASE_URL":          "postgres://shard-0/db",
		"BOOKING_SHARD_1_DATABASE_URL":          "postgres://shard-1/db",
		"DEPLOYMENT_REGION":                     "region-a",
		"DEPLOYMENT_ROLE":                       "active",
		"REGION_EPOCH":                          "1",
		"REGIONAL_WRITES_ENABLED":               "true",
		"BOOKING_COMMAND_RECONCILER_ID":         "reconciler-1",
		"BOOKING_COMMAND_RECONCILER_BATCH_SIZE": "25",
		"BOOKING_COMMAND_RECONCILER_LEASE_TTL":  "45s",
		"CONTROL_DATABASE_MAX_OPEN_CONNS":       "5",
		"PHYSICAL_SHARD_MAX_OPEN_CONNS":         "3",
		"PHYSICAL_SHARD_QUERY_TIMEOUT":          "1500ms",
	}
	cfg, err := loadConfig(mapLookup(values))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.batchSize != 25 || cfg.leaseTTL != 45*time.Second || cfg.workerID != "reconciler-1" ||
		cfg.shardZeroURL == cfg.shardOneURL || cfg.controlMaxConns != 5 || cfg.shardMaxConns != 3 ||
		cfg.queryTimeout != 1500*time.Millisecond {
		t.Fatalf("loadConfig() = %+v", cfg)
	}
}

func TestLoadConfigRejectsUnboundedOrAliasedShardSettings(t *testing.T) {
	t.Parallel()
	base := map[string]string{
		"DATABASE_URL": "postgres://control/db", "BOOKING_SHARD_0_DATABASE_URL": "postgres://same/db",
		"BOOKING_SHARD_1_DATABASE_URL": "postgres://same/db",
		"DEPLOYMENT_REGION":            "region-a", "DEPLOYMENT_ROLE": "active", "REGION_EPOCH": "1",
		"REGIONAL_WRITES_ENABLED": "true",
	}
	if _, err := loadConfig(mapLookup(base)); err == nil {
		t.Fatal("loadConfig() accepted aliased shard connections")
	}
	base["BOOKING_SHARD_1_DATABASE_URL"] = "postgres://shard-1/db"
	base["BOOKING_COMMAND_RECONCILER_BATCH_SIZE"] = "501"
	if _, err := loadConfig(mapLookup(base)); err == nil {
		t.Fatal("loadConfig() accepted unbounded batch")
	}
	base["BOOKING_COMMAND_RECONCILER_BATCH_SIZE"] = "25"
	base["PHYSICAL_SHARD_QUERY_TIMEOUT"] = "31s"
	if _, err := loadConfig(mapLookup(base)); err == nil {
		t.Fatal("loadConfig() accepted unbounded physical query timeout")
	}
}

func TestMetricsUseOnlyBoundedOutcomeLabels(t *testing.T) {
	t.Parallel()
	metrics, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	metrics.record(reconcile.Result{Finalized: 1, Deferred: 2}, nil, time.Second)
	families, err := metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 4 {
		t.Fatalf("metric family count = %d, want 4", len(families))
	}
}

func TestWorkerHelpersRedactAndStop(t *testing.T) {
	t.Parallel()
	if got := publicReason(errors.New("postgres://user:secret@example/db")); got != "worker failure" {
		t.Fatalf("publicReason() = %q", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForShutdown(ctx, make(chan error)); err != nil {
		t.Fatalf("waitForShutdown(cancelled) = %v", err)
	}
	serverErrors := make(chan error, 1)
	serverErrors <- http.ErrServerClosed
	if err := waitForShutdown(context.Background(), serverErrors); err != nil {
		t.Fatalf("waitForShutdown(closed) = %v", err)
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
