package reconcile

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresInspectorRunsDetectOnlyAgainstMigrationEight(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	var topologyExists bool
	if err := pool.QueryRow(ctx, `
SELECT to_regclass('public.train_run_shard_assignments') IS NOT NULL
   AND to_regclass('public.reservation_shard_locators') IS NOT NULL`).Scan(&topologyExists); err != nil {
		t.Fatalf("inspect topology: %v", err)
	}
	if !topologyExists {
		t.Skip("migration 8 topology is not installed")
	}
	before := reconciliationRowCount(t, ctx, pool)
	inspector, err := New(pool)
	if err != nil {
		t.Fatal(err)
	}
	limits := Limits{PageSize: 100, MaxPages: 10_000, MaxRows: 1_000_000}
	assignmentReport, assignmentErr := inspector.Assignments(ctx, limits)
	assertCompleteInspection(t, "assignments", assignmentReport, assignmentErr)
	locatorReport, locatorErr := inspector.Locators(ctx, LocatorFilter{}, limits)
	assertCompleteInspection(t, "locators", locatorReport, locatorErr)

	var migrationID uuid.UUID
	err = pool.QueryRow(ctx, `SELECT id FROM public.train_run_shard_migrations ORDER BY id LIMIT 1`).Scan(&migrationID)
	if err == nil {
		migrationReport, migrationErr := inspector.Migration(ctx, migrationID, limits)
		assertCompleteInspection(t, "migration", migrationReport, migrationErr)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("select migration fixture: %v", err)
	}
	after := reconciliationRowCount(t, ctx, pool)
	if after != before {
		t.Fatalf("detect-only reconciliation changed durable row count: before=%d after=%d", before, after)
	}
}

func assertCompleteInspection(t *testing.T, name string, report Report, err error) {
	t.Helper()
	if errors.Is(err, ErrPartial) || errors.Is(err, ErrUnavailable) || errors.Is(err, ErrLimitReached) {
		t.Fatalf("%s reconciliation incomplete: report=%+v error=%v", name, report, err)
	}
	if err != nil && !errors.Is(err, ErrViolations) {
		t.Fatalf("%s reconciliation failed: %v", name, err)
	}
	if report.Completeness != CompletenessComplete || !report.ReadOnly {
		t.Fatalf("%s reconciliation report = %+v", name, report)
	}
}

func reconciliationRowCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var count int64
	err := pool.QueryRow(ctx, `
SELECT
      (SELECT count(*) FROM public.train_run_shard_assignments)
    + (SELECT count(*) FROM public.train_run_shard_migrations)
    + (SELECT count(*) FROM public.reservation_shard_locators)
    + (SELECT count(*) FROM public.ticket_order_shard_locators)
    + (SELECT count(*) FROM public.ticket_shard_locators)
    + (SELECT count(*) FROM public.booking_idempotency_key_claims)
    + (SELECT count(*) FROM public.reservation_quota_claims)
    + (SELECT count(*) FROM public.outbox_events)`).Scan(&count)
	if err != nil {
		t.Fatalf("count reconciliation tables: %v", err)
	}
	return count
}
