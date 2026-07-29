package controlsource_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/controlsource"
	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLiveControlSourceNextBaseBatchAppliesToAPhysicalShard(t *testing.T) {
	controlURL := strings.TrimSpace(os.Getenv("CONTROL_SOURCE_TEST_DATABASE_URL"))
	targetURL := strings.TrimSpace(os.Getenv("CONTROL_SOURCE_TEST_TARGET_DATABASE_URL"))
	migrationText := strings.TrimSpace(os.Getenv("CONTROL_SOURCE_TEST_MIGRATION_ID"))
	if controlURL == "" || targetURL == "" || migrationText == "" {
		t.Skip("control-source PostgreSQL integration variables are not set")
	}
	migrationID, err := uuid.Parse(migrationText)
	if err != nil || migrationID == uuid.Nil {
		t.Fatal("CONTROL_SOURCE_TEST_MIGRATION_ID must be a non-zero UUID")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	controlDB, err := pgxpool.New(ctx, controlURL)
	if err != nil {
		t.Fatalf("open control database: %v", err)
	}
	defer controlDB.Close()
	targetDB, err := pgxpool.New(ctx, targetURL)
	if err != nil {
		t.Fatalf("open target database: %v", err)
	}
	defer targetDB.Close()
	store, err := physicalpostgres.NewControl(controlDB)
	if err != nil {
		t.Fatalf("new control store: %v", err)
	}
	record, err := store.Load(ctx, migrationID)
	if err != nil {
		t.Fatalf("load migration: %v", err)
	}
	adapter, err := controlsource.New(controlDB, targetDB, record.SourceShardID)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	batch, err := adapter.ReadBaseBatch(ctx, physicalmigration.BaseCopyRequest{
		Migration: record, Cursor: record.BaseCopyCursor, Limit: 500,
	})
	if err != nil {
		t.Fatalf("read next base batch after %q: %v", record.BaseCopyCursor, err)
	}
	if err := adapter.ApplyBaseBatch(ctx, record, batch); err != nil {
		t.Fatalf("apply %s base batch: %v", batch.ObjectName, err)
	}
	journal, err := adapter.ReadJournal(ctx, physicalmigration.JournalRequest{
		Migration: record, AfterSequence: record.LastReplayedSequence, Limit: 500,
	})
	if err != nil {
		t.Fatalf("read journal after %d: %v", record.LastReplayedSequence, err)
	}
	if journal.SourceSequence < record.LastReplayedSequence {
		t.Fatalf("journal source sequence = %d", journal.SourceSequence)
	}
	if err := adapter.CaptureOutbox(ctx, record, 100000); err != nil {
		t.Fatalf("capture outbox: %v", err)
	}
	result, err := adapter.Validate(ctx, physicalmigration.ValidationRequest{
		Migration: record, MaxRows: 100000, MaxTables: 64,
	})
	if err != nil {
		t.Fatalf("validate migrated control source: %v", err)
	}
	if !result.Passed || result.Truncated {
		t.Fatalf("validation result = %+v", result)
	}
}
