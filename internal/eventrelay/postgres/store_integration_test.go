package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStoreClaimsAndFinalizesWithoutHoldingPublishTransaction(t *testing.T) {
	pool := openEventRelayTestPool(t)
	ctx := context.Background()

	aggregateID := uuid.New()
	eventID := uuid.New()
	now := time.Now().UTC()
	_, err := pool.Exec(ctx, `
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, next_attempt_at)
VALUES ($1, 'reservation', $2, 'reservation.held', '{"status":"held"}', $3)`,
		eventID, aggregateID, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	store, err := NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.Claim(ctx, "integration-worker", 10, now, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != eventID || events[0].Attempts != 1 || events[0].EventType != "reservation.held" {
		t.Fatalf("claimed events = %#v", events)
	}
	if err := store.MarkPublished(ctx, eventID, "integration-worker", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var status string
	var lockedBy *string
	if err := pool.QueryRow(ctx, `SELECT status, locked_by FROM outbox_events WHERE id = $1`, eventID).Scan(&status, &lockedBy); err != nil {
		t.Fatal(err)
	}
	if status != "published" || lockedBy != nil {
		t.Fatalf("status=%q locked_by=%v", status, lockedBy)
	}
}

func TestStoreReclaimsStaleLeaseRejectsOldOwnerAndPersistsDeadLetter(t *testing.T) {
	pool := openEventRelayTestPool(t)
	ctx := context.Background()
	now, eventID := time.Now().UTC(), uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, event_type, payload, status,
			attempts, next_attempt_at, locked_at, locked_by
		) VALUES ($1, 'reservation', $2, 'reservation.held', '{}', 'processing',
		          1, $3, $3, 'old-worker')
	`, eventID, uuid.New(), now.Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	store, _ := NewStore(pool)
	events, err := store.Claim(ctx, "new-worker", 1, now, now.Add(-time.Minute))
	if err != nil || len(events) != 1 || events[0].Attempts != 2 {
		t.Fatalf("stale claim = %#v, %v", events, err)
	}
	if err := store.MarkPublished(ctx, eventID, "old-worker", now); err == nil {
		t.Fatal("obsolete worker finalized reclaimed event")
	}
	next := now.Add(time.Minute).Truncate(time.Microsecond)
	if err := store.MarkFailed(ctx, eventID, "new-worker", next, true); err != nil {
		t.Fatal(err)
	}
	var status string
	var attempts int
	var nextAttempt time.Time
	if err := pool.QueryRow(ctx, `SELECT status, attempts, next_attempt_at FROM outbox_events WHERE id = $1`, eventID).Scan(&status, &attempts, &nextAttempt); err != nil {
		t.Fatal(err)
	}
	if status != "dead_letter" || attempts != 2 || !nextAttempt.Equal(next) {
		t.Fatalf("dead-letter state: status=%s attempts=%d next=%s", status, attempts, nextAttempt)
	}
}

func TestConcurrentWorkersClaimOneEventAtMostOnce(t *testing.T) {
	pool := openEventRelayTestPool(t)
	ctx := context.Background()
	now, eventID := time.Now().UTC(), uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, next_attempt_at)
		VALUES ($1, 'reservation', $2, 'reservation.confirmed', '{}', $3)
	`, eventID, uuid.New(), now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	store, _ := NewStore(pool)
	start := make(chan struct{})
	results := make(chan []uuid.UUID, 2)
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for _, worker := range []string{"worker-a", "worker-b"} {
		wait.Add(1)
		go func(worker string) {
			defer wait.Done()
			<-start
			events, err := store.Claim(ctx, worker, 1, now, now.Add(-time.Minute))
			if err != nil {
				errorsFound <- err
				return
			}
			ids := make([]uuid.UUID, len(events))
			for index := range events {
				ids[index] = events[index].ID
			}
			results <- ids
		}(worker)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	if err := errors.Join(<-errorsFound, <-errorsFound); err != nil {
		t.Fatal(err)
	}
	claimed := 0
	for ids := range results {
		for _, id := range ids {
			if id != eventID {
				t.Fatalf("claimed unexpected event %s", id)
			}
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("event claimed %d times, want 1", claimed)
	}
}

func openEventRelayTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := "eventrelay_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	for _, name := range []string{
		"000001_accounts.up.sql",
		"000002_railway_offering.up.sql",
		"000003_booking.up.sql",
		"000004_idempotency_outbox.up.sql",
		"000005_inventory_and_route_integrity.up.sql",
	} {
		migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", name))
		if err != nil {
			pool.Close()
			admin.Close()
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			pool.Close()
			admin.Close()
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
	})
	return pool
}
