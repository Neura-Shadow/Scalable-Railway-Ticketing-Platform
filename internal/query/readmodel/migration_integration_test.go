package readmodel_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMigrationSevenCreatesProjectionAndReceiptSchema(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	ctx := context.Background()

	wantConstraints := []string{
		"read_model_event_progress_pkey",
		"read_model_event_progress_aggregate_type_check",
		"read_model_event_progress_consumer_name_check",
		"read_model_event_progress_event_type_check",
		"read_model_event_progress_phase_check",
		"read_model_event_progress_processed_check",
		"read_model_event_receipts_pkey",
		"read_model_event_receipts_aggregate_type_check",
		"read_model_event_receipts_consumer_name_check",
		"read_model_event_receipts_event_type_check",
		"read_model_projection_state_pkey",
		"read_model_projection_state_name_check",
		"read_model_projection_state_cursor_check",
		"train_run_journey_read_model_pkey",
		"train_run_journey_read_model_seat_class_check",
		"train_run_journey_read_model_currency_check",
		"train_run_journey_read_model_fare_amount_minor_check",
		"train_run_journey_read_model_journey_order_check",
		"train_run_journey_read_model_station_codes_check",
		"train_run_journey_read_model_station_ids_check",
		"train_run_journey_read_model_status_check",
		"train_run_journey_read_model_times_check",
	}
	sort.Strings(wantConstraints)
	gotConstraints := collectStrings(t, conn, `
		SELECT constraint_name
		FROM information_schema.table_constraints AS tc
		JOIN pg_catalog.pg_namespace AS pn
		  ON pn.nspname = tc.table_schema
		JOIN pg_catalog.pg_class AS pcl
		  ON pcl.relname = tc.table_name
		 AND pcl.relnamespace = pn.oid
		JOIN pg_catalog.pg_constraint AS pc
		  ON pc.conname = tc.constraint_name
		 AND pc.conrelid = pcl.oid
		WHERE tc.table_schema = current_schema()
		  AND tc.table_name IN ('train_run_journey_read_model', 'read_model_event_receipts', 'read_model_event_progress', 'read_model_projection_state')
		  AND pc.contype IN ('p', 'c')
		ORDER BY constraint_name
	`)
	if !equalStrings(gotConstraints, wantConstraints) {
		t.Fatalf("migration 7 constraints = %v, want %v", gotConstraints, wantConstraints)
	}

	wantIndexes := []string{
		"read_model_event_progress_pkey",
		"read_model_event_progress_projection_idx",
		"read_model_event_receipts_aggregate_idx",
		"read_model_event_receipts_pkey",
		"read_model_event_receipts_processed_at_idx",
		"read_model_projection_state_pkey",
		"train_run_journey_read_model_fare_search_idx",
		"train_run_journey_read_model_pkey",
		"train_run_journey_read_model_search_idx",
		"train_run_journey_read_model_source_updated_at_idx",
	}
	gotIndexes := collectStrings(t, conn, `
		SELECT indexname
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename IN ('train_run_journey_read_model', 'read_model_event_receipts', 'read_model_event_progress', 'read_model_projection_state')
		ORDER BY indexname
	`)
	if !equalStrings(gotIndexes, wantIndexes) {
		t.Fatalf("migration 7 indexes = %v, want %v", gotIndexes, wantIndexes)
	}
	var replayIndexes, lagIndexes int
	if err := conn.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE indexname = 'outbox_events_read_model_replay_idx'),
			count(*) FILTER (WHERE indexname = 'outbox_events_read_model_lag_idx')
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'outbox_events'
	`).Scan(&replayIndexes, &lagIndexes); err != nil || replayIndexes != 1 || lagIndexes != 1 {
		t.Fatalf("outbox read-model indexes = replay %d lag %d, %v, want 1 and 1", replayIndexes, lagIndexes, err)
	}

	var projectionColumns, receiptColumns, progressColumns, stateColumns int
	if err := conn.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE table_name = 'train_run_journey_read_model'),
			count(*) FILTER (WHERE table_name = 'read_model_event_receipts'),
			count(*) FILTER (WHERE table_name = 'read_model_event_progress'),
			count(*) FILTER (WHERE table_name = 'read_model_projection_state')
		FROM information_schema.columns
		WHERE table_schema = current_schema()
	`).Scan(&projectionColumns, &receiptColumns, &progressColumns, &stateColumns); err != nil {
		t.Fatalf("inspect migration 7 columns: %v", err)
	}
	if projectionColumns != 21 || receiptColumns != 6 || progressColumns != 11 || stateColumns != 4 {
		t.Fatalf(
			"migration 7 column counts = projection %d receipt %d progress %d state %d, want 21, 6, 11, and 4",
			projectionColumns,
			receiptColumns,
			progressColumns,
			stateColumns,
		)
	}
	var ready bool
	var rebuildAfter string
	if err := conn.QueryRow(ctx, `
		SELECT ready, rebuild_after
		FROM read_model_projection_state
		WHERE projection_name = 'journey_search'
	`).Scan(&ready, &rebuildAfter); err != nil {
		t.Fatalf("inspect initial projection state: %v", err)
	}
	if ready || rebuildAfter != "" {
		t.Fatalf("initial projection state = ready %t cursor %q, want unavailable empty cursor", ready, rebuildAfter)
	}
}

func TestMigrationSevenAllowsOnlyBoundedReadModelSourceEvents(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	ctx := context.Background()
	allowed := []struct {
		aggregateType string
		eventType     string
	}{
		{"station", "station.created"},
		{"station", "station.updated"},
		{"station", "station.disabled"},
		{"route", "route.created"},
		{"route", "route.updated"},
		{"route", "route.disabled"},
		{"train", "train.updated"},
		{"coach", "coach.updated"},
		{"seat", "seat.updated"},
		{"fare", "fare.created"},
		{"fare", "fare.updated"},
		{"fare", "fare.disabled"},
		{"train_run", "trainrun.created"},
		{"train_run", "trainrun.updated"},
	}
	for _, event := range allowed {
		if _, err := conn.Exec(ctx, `
			INSERT INTO outbox_events (
				aggregate_type,
				aggregate_id,
				event_type,
				payload
			) VALUES ($1, gen_random_uuid(), $2, '{}'::jsonb)
		`, event.aggregateType, event.eventType); err != nil {
			t.Fatalf("insert allowed event %s/%s: %v", event.aggregateType, event.eventType, err)
		}
	}

	_, err := conn.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		) VALUES ('train_run', gen_random_uuid(), 'trainrun.payload_injected', '{}'::jsonb)
	`)
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) || pgError.Code != "23514" {
		t.Fatalf("unknown read-model event error = %v, want check violation", err)
	}

	_, err = conn.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		) VALUES ('station', gen_random_uuid(), 'reservation.held', '{}'::jsonb)
	`)
	if !errors.As(err, &pgError) || pgError.Code != "23514" {
		t.Fatalf("mismatched aggregate/event pair error = %v, want check violation", err)
	}
}

func TestMigrationSevenDownUpPreservesVersionSixOutboxData(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	ctx := context.Background()
	legacyEventID := uuid.New()
	readModelEventID := uuid.New()
	if _, err := conn.Exec(ctx, `
		INSERT INTO outbox_events (
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		) VALUES
			($1, 'reservation', gen_random_uuid(), 'reservation.held', '{}'::jsonb),
			($2, 'station', gen_random_uuid(), 'station.created', '{}'::jsonb)
	`, legacyEventID, readModelEventID); err != nil {
		t.Fatalf("seed migration 7 outbox data: %v", err)
	}

	applyMigrationFile(t, conn, "000007_read_model_cache.down.sql")
	var legacyRows, readModelRows int
	if err := conn.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE id = $1),
			count(*) FILTER (WHERE id = $2)
		FROM outbox_events
	`, legacyEventID, readModelEventID).Scan(&legacyRows, &readModelRows); err != nil {
		t.Fatalf("inspect outbox after migration 7 down: %v", err)
	}
	if legacyRows != 1 || readModelRows != 0 {
		t.Fatalf("outbox after migration 7 down = legacy %d read-model %d, want 1 and 0", legacyRows, readModelRows)
	}
	_, err := conn.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		) VALUES ('station', gen_random_uuid(), 'station.created', '{}'::jsonb)
	`)
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) || pgError.Code != "23514" {
		t.Fatalf("version 6 event contract error = %v, want check violation", err)
	}

	applyMigrationFile(t, conn, "000007_read_model_cache.up.sql")
	if _, err := conn.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		) VALUES ('station', gen_random_uuid(), 'station.created', '{}'::jsonb)
	`); err != nil {
		t.Fatalf("reapply migration 7 event contract: %v", err)
	}
}

func openMigrationSevenDatabase(t *testing.T) *pgx.Conn {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect migration test database: %v", err)
	}
	schema := "read_model_migration_" + randomHex(t, 12)
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("create migration test schema: %v", err)
	}
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse migration test database URL: %v", err)
	}
	config.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect isolated migration schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = conn.Close(cleanupCtx)
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop migration test schema: %v", err)
		}
		_ = admin.Close(cleanupCtx)
	})

	for _, name := range []string{
		"000001_accounts.up.sql",
		"000002_railway_offering.up.sql",
		"000003_booking.up.sql",
		"000004_idempotency_outbox.up.sql",
		"000005_inventory_and_route_integrity.up.sql",
		"000006_hot_train_admission.up.sql",
		"000007_read_model_cache.up.sql",
	} {
		applyMigrationFile(t, conn, name)
	}
	return conn
}

func applyMigrationFile(t *testing.T, conn *pgx.Conn, name string) {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve migration integration test path")
	}
	root := filepath.Join(filepath.Dir(currentFile), "..", "..", "..")
	migration, err := os.ReadFile(filepath.Join(root, "migrations", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if _, err := conn.PgConn().Exec(context.Background(), string(migration)).ReadAll(); err != nil {
		t.Fatalf("apply %s: %v", name, err)
	}
}

func collectStrings(t *testing.T, conn *pgx.Conn, query string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("query catalog: %v", err)
	}
	values, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("collect catalog values: %v", err)
	}
	sort.Strings(values)
	return values
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func randomHex(t *testing.T, byteCount int) string {
	t.Helper()
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate schema suffix: %v", err)
	}
	return hex.EncodeToString(value)
}
