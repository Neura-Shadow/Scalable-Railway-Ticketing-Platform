package controlsource

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLiveReverseV3PreflightQueriesMatchInstalledSchemas(t *testing.T) {
	controlURL := strings.TrimSpace(os.Getenv("CONTROL_TARGET_TEST_DATABASE_URL"))
	sourceURL := strings.TrimSpace(os.Getenv("CONTROL_TARGET_TEST_SOURCE_DATABASE_URL"))
	if controlURL == "" || sourceURL == "" {
		t.Skip("reverse control-target PostgreSQL integration variables are not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	controlDB, err := pgxpool.New(ctx, controlURL)
	if err != nil {
		t.Fatalf("open control database: %v", err)
	}
	defer controlDB.Close()
	sourceDB, err := pgxpool.New(ctx, sourceURL)
	if err != nil {
		t.Fatalf("open physical source: %v", err)
	}
	defer sourceDB.Close()

	var ready bool
	if err := sourceDB.QueryRow(ctx, reverseSourcePreflightSQL, uuid.New(), int64(1)).Scan(&ready); err != nil {
		t.Fatalf("physical schema-v3 preflight query failed: %v", err)
	}
	if ready {
		t.Fatal("unassigned physical source unexpectedly passed preflight")
	}
	err = controlDB.QueryRow(ctx, reverseTargetPreflightSQL, uuid.New(), "physical-shard-0",
		int64(1), uuid.New(), SourceLegacy).Scan(&ready)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("control schema-v10 preflight query error = %v, want no matching assignment", err)
	}
}

func TestLiveReverseControlTargetHasEquivalentAuthoritativeRows(t *testing.T) {
	controlURL := strings.TrimSpace(os.Getenv("CONTROL_TARGET_TEST_DATABASE_URL"))
	sourceURL := strings.TrimSpace(os.Getenv("CONTROL_TARGET_TEST_SOURCE_DATABASE_URL"))
	migrationText := strings.TrimSpace(os.Getenv("CONTROL_TARGET_TEST_MIGRATION_ID"))
	if controlURL == "" || sourceURL == "" || migrationText == "" {
		t.Skip("reverse control-target PostgreSQL integration variables are not set")
	}
	migrationID, err := uuid.Parse(migrationText)
	if err != nil || migrationID == uuid.Nil {
		t.Fatal("CONTROL_TARGET_TEST_MIGRATION_ID must be a non-zero UUID")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	controlDB, err := pgxpool.New(ctx, controlURL)
	if err != nil {
		t.Fatalf("open control database: %v", err)
	}
	defer controlDB.Close()
	sourceDB, err := pgxpool.New(ctx, sourceURL)
	if err != nil {
		t.Fatalf("open physical source: %v", err)
	}
	defer sourceDB.Close()
	store, err := physicalpostgres.NewControl(controlDB)
	if err != nil {
		t.Fatalf("new control store: %v", err)
	}
	record, err := store.Load(ctx, migrationID)
	if err != nil {
		t.Fatalf("load migration: %v", err)
	}
	adapter, err := NewReverse(controlDB, sourceDB, record.TargetShardID)
	if err != nil {
		t.Fatalf("new reverse adapter: %v", err)
	}
	for _, table := range tableOrder {
		sourceRows, err := adapter.reverseValidationRows(ctx, sourceDB,
			reverseSourceValidationSQL(table), record, table, 100001, true)
		if err != nil {
			t.Fatalf("read source %s: %v", table, err)
		}
		var targetRows []canonicalRow
		if reverseIgnoredTable(table) {
			targetRows, err = adapter.reverseDerivedTargetRows(ctx, record, table, 100001)
		} else {
			targetRows, err = adapter.reverseValidationRows(ctx, controlDB,
				reverseTargetValidationSQL(record.TargetShardID, table), record, table, 100001, false)
		}
		if err != nil {
			t.Fatalf("read target %s: %v", table, err)
		}
		if digestRows(sourceRows) != digestRows(targetRows) {
			t.Fatalf("%s digest mismatch (source rows %d, target rows %d)", table, len(sourceRows), len(targetRows))
		}
	}
}
