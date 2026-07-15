package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStoreClaimsAndFinalizesWithoutHoldingPublishTransaction(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	aggregateID := uuid.New()
	eventID := uuid.New()
	now := time.Now().UTC()
	_, err = pool.Exec(ctx, `
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, next_attempt_at)
VALUES ($1, 'reservation', $2, 'reservation.held', '{"status":"held"}', $3)`,
		eventID, aggregateID, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM outbox_events WHERE id = $1`, eventID) })

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
