package controlsource

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
	for _, table := range []string{"seat_inventory", "reservations", "reservation_seats", "ticket_orders", "tickets", "idempotency_records", "outbox_events"} {
		sourceRows, err := adapter.reverseValidationRows(ctx, sourceDB,
			reverseSourceValidationSQL(table), record, table, 100001, true)
		if err != nil {
			t.Fatalf("read source %s: %v", table, err)
		}
		targetRows, err := adapter.reverseValidationRows(ctx, controlDB,
			reverseTargetValidationSQL(record.TargetShardID, table), record, table, 100001, false)
		if err != nil {
			t.Fatalf("read target %s: %v", table, err)
		}
		if digestRows(sourceRows) != digestRows(targetRows) {
			t.Fatalf("%s digest mismatch (source rows %d, target rows %d)", table, len(sourceRows), len(targetRows))
		}
	}
}
