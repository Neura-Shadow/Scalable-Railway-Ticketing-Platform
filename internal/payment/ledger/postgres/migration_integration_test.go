package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	ledgerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestAppendInTxExactlyReplaysPopulatedV11MigrationRow(t *testing.T) {
	databaseURL := os.Getenv("M7_LEDGER_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("M7_LEDGER_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect populated v11 database: %v", err)
	}
	defer connection.Close(context.Background())
	tx, err := connection.Begin(ctx)
	if err != nil {
		t.Fatalf("begin replay transaction: %v", err)
	}
	defer tx.Rollback(context.Background())

	candidate, err := ledger.PrepareAppend(ledger.TicketIssuanceAppendRequest(
		uuid.MustParse("75000000-0000-4000-8000-000000000001"),
		uuid.MustParse("ddb62b09-9c50-526a-adb4-e32a16aa7c66"),
		12_500,
		"TWD",
	), time.Date(2026, 1, 2, 0, 1, 1, 0, time.UTC))
	if err != nil {
		t.Fatalf("prepare canonical issuance: %v", err)
	}
	stored, created, err := ledgerpostgres.AppendInTx(ctx, tx, candidate)
	if err != nil {
		t.Fatalf("AppendInTx() exact migrated replay error = %v", err)
	}
	if created || stored.ID != candidate.ID || stored.Fingerprint != candidate.Fingerprint ||
		stored.EventID != candidate.EventID || stored.Correlation != candidate.Correlation {
		t.Fatalf("AppendInTx() replay = (%+v, created=%v), want canonical %+v", stored, created, candidate)
	}
}
