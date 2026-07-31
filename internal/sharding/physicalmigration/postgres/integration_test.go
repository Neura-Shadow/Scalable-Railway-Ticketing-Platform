package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgres16PhysicalMigrationSafety(t *testing.T) {
	source, target := openBookingShardPair(t)
	t.Run("preflight reads actual schema history", func(t *testing.T) {
		record := integrationRecord(7, 8)
		defer cleanupTrainRun(t, source, record.TrainRunID)
		defer cleanupTrainRun(t, target, record.TrainRunID)
		seedSnapshotAndFence(t, source, record.TrainRunID, record.SourceGeneration, "active", true, false)
		shards, err := physicalpostgres.NewDefaultShards(source, target)
		if err != nil {
			t.Fatal(err)
		}
		if err := shards.Preflight(context.Background(), record); err != nil {
			t.Fatalf("Preflight() error = %v", err)
		}
	})

	t.Run("physical copy preserves operator receipt source version", func(t *testing.T) {
		record := integrationRecord(11, 12)
		defer cleanupTrainRun(t, source, record.TrainRunID)
		defer cleanupTrainRun(t, target, record.TrainRunID)
		seedSnapshotAndFence(t, source, record.TrainRunID, record.SourceGeneration, "active", true, false)
		commandID := uuid.New()
		resultID := uuid.New()
		if _, err := source.Exec(context.Background(), `
INSERT INTO public.booking_command_receipts(
    command_id,train_run_id,assignment_generation,command_type,
    request_fingerprint,status,result_type,result_id,result_source_version,completed_at
) VALUES($1,$2,$3,'seat.disable',decode(repeat('42',32),'hex'),
         'succeeded','seat',$4,41,clock_timestamp())`, commandID, record.TrainRunID,
			record.SourceGeneration, resultID); err != nil {
			t.Fatal(err)
		}
		shards, err := physicalpostgres.NewDefaultShards(source, target)
		if err != nil {
			t.Fatal(err)
		}
		cursor := ""
		for {
			batch, err := shards.ReadBaseBatch(context.Background(), physicalmigration.BaseCopyRequest{
				Migration: record, Cursor: cursor, Limit: 10,
			})
			if err != nil {
				t.Fatalf("ReadBaseBatch() error = %v", err)
			}
			if batch.Done {
				break
			}
			if err := shards.ApplyBaseBatch(context.Background(), record, batch); err != nil {
				t.Fatalf("ApplyBaseBatch(%s) error = %v", batch.ObjectName, err)
			}
			cursor = batch.NextCursor
		}
		var generation, sourceVersion int64
		if err := target.QueryRow(context.Background(), `
SELECT assignment_generation,result_source_version
FROM public.booking_command_receipts WHERE command_id=$1`, commandID).Scan(
			&generation, &sourceVersion); err != nil {
			t.Fatal(err)
		}
		if generation != record.TargetGeneration || sourceVersion != 41 {
			t.Fatalf("copied receipt generation/source version = %d/%d, want %d/41",
				generation, sourceVersion, record.TargetGeneration)
		}
	})

	t.Run("outbox staging promotes a normalized snapshot atomically", func(t *testing.T) {
		record := integrationRecord(13, 14)
		defer cleanupTrainRun(t, source, record.TrainRunID)
		defer cleanupTrainRun(t, target, record.TrainRunID)
		seedSnapshotAndFence(t, source, record.TrainRunID, record.SourceGeneration, "active", true, false)
		seedSnapshotAndFence(t, target, record.TrainRunID, record.TargetGeneration, "standby", false, false)
		eventID := uuid.New()
		if _, err := source.Exec(context.Background(), `
INSERT INTO public.outbox_events(
 id,train_run_id,assignment_generation,aggregate_type,aggregate_id,event_type,
 payload,status,locked_at,locked_by,lease_token
) VALUES($1,$2,$3,'train_run',$2,'train_run.updated','{}','processing',
         clock_timestamp(),'relay-test',$4)`, eventID, record.TrainRunID,
			record.SourceGeneration, uuid.New()); err != nil {
			t.Fatal(err)
		}
		shards, err := physicalpostgres.NewDefaultShards(source, target)
		if err != nil {
			t.Fatal(err)
		}
		if err := shards.CaptureOutbox(context.Background(), record, 10); err != nil {
			t.Fatalf("CaptureOutbox() error = %v", err)
		}
		var (
			generation int64
			status     string
			lockedAt   *time.Time
			lockedBy   *string
			leaseToken *uuid.UUID
		)
		if err := target.QueryRow(context.Background(), `
SELECT assignment_generation,status,locked_at,locked_by,lease_token
FROM public.outbox_events WHERE id=$1`, eventID).Scan(
			&generation, &status, &lockedAt, &lockedBy, &leaseToken); err != nil {
			t.Fatal(err)
		}
		if generation != record.TargetGeneration || status != "pending" ||
			lockedAt != nil || lockedBy != nil || leaseToken != nil {
			t.Fatalf("promoted outbox generation/status/lease = %d/%s/%v/%v/%v",
				generation, status, lockedAt, lockedBy, leaseToken)
		}
	})

	t.Run("semantic corruption fails validation", func(t *testing.T) {
		record := integrationRecord(17, 18)
		defer cleanupTrainRun(t, source, record.TrainRunID)
		defer cleanupTrainRun(t, target, record.TrainRunID)
		for _, side := range []struct {
			pool       *pgxpool.Pool
			generation int64
			state      string
			write      bool
		}{{source, 17, "active", true}, {target, 18, "standby", false}} {
			seedSnapshotAndFence(t, side.pool, record.TrainRunID, side.generation, side.state, side.write, true)
		}
		result, err := (physicalpostgres.BoundedValidator{}).Validate(context.Background(), source, target,
			physicalmigration.ValidationRequest{Migration: record, MaxRows: 100, MaxTables: 11})
		if err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if result.Passed {
			t.Fatal("semantic inventory corruption passed validation")
		}
	})

	t.Run("reverse prepare resumes after its target commit", func(t *testing.T) {
		record := integrationRecord(28, 29)
		record.ReverseMigration = true
		record.RetainedTargetGeneration = 27
		defer cleanupTrainRun(t, source, record.TrainRunID)
		defer cleanupTrainRun(t, target, record.TrainRunID)
		seedSnapshotAndFence(t, source, record.TrainRunID, 28, "active", true, false)
		seedSnapshotAndFence(t, target, record.TrainRunID, 27, "retained", false, false)
		shards, err := physicalpostgres.NewDefaultShards(source, target)
		if err != nil {
			t.Fatal(err)
		}
		if err := shards.PrepareTarget(context.Background(), record); err != nil {
			t.Fatalf("first PrepareTarget() error = %v", err)
		}
		if err := shards.PrepareTarget(context.Background(), record); err != nil {
			t.Fatalf("resumed PrepareTarget() error = %v", err)
		}
	})

	t.Run("zero write rollback rebinds every source row monotonically", func(t *testing.T) {
		record := integrationRecord(37, 38)
		defer cleanupTrainRun(t, source, record.TrainRunID)
		defer cleanupTrainRun(t, target, record.TrainRunID)
		seedSnapshotAndFence(t, source, record.TrainRunID, 37, "retained", false, false)
		if _, err := source.Exec(context.Background(), `
INSERT INTO public.migration_capture_state(
    train_run_id,migration_id,source_generation,capture_enabled,next_sequence,disabled_at
) VALUES($1,$2,$3,false,0,clock_timestamp())`, record.TrainRunID, record.MigrationID, 37); err != nil {
			t.Fatal(err)
		}
		seedSnapshotAndFence(t, target, record.TrainRunID, 38, "active", true, false)
		if _, err := target.Exec(context.Background(), `
INSERT INTO public.train_run_target_write_evidence(
    train_run_id,assignment_generation,successful_write_count,baseline_initialized,
    baseline_reservation_count,baseline_command_receipt_count,baseline_outbox_count
) VALUES($1,$2,0,true,0,0,0)`, record.TrainRunID, int64(38)); err != nil {
			t.Fatal(err)
		}
		shards, err := physicalpostgres.NewDefaultShards(source, target)
		if err != nil {
			t.Fatal(err)
		}
		if err := shards.RollbackBeforeTargetWrites(context.Background(), record, 39); err != nil {
			t.Fatalf("RollbackBeforeTargetWrites() error = %v", err)
		}
		var sourceGeneration, targetGeneration int64
		var sourceWrite, targetWrite bool
		if err := source.QueryRow(context.Background(), `SELECT assignment_generation,write_enabled FROM public.train_run_write_fences WHERE train_run_id=$1`, record.TrainRunID).Scan(&sourceGeneration, &sourceWrite); err != nil {
			t.Fatal(err)
		}
		if err := target.QueryRow(context.Background(), `SELECT assignment_generation,write_enabled FROM public.train_run_write_fences WHERE train_run_id=$1`, record.TrainRunID).Scan(&targetGeneration, &targetWrite); err != nil {
			t.Fatal(err)
		}
		if sourceGeneration != 39 || !sourceWrite || targetGeneration != 38 || targetWrite {
			t.Fatalf("source=(%d,%v) target=(%d,%v)", sourceGeneration, sourceWrite, targetGeneration, targetWrite)
		}
	})
}

func openBookingShardPair(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	sourceURL := os.Getenv("TEST_BOOKING_SHARD_0_DATABASE_URL")
	targetURL := os.Getenv("TEST_BOOKING_SHARD_1_DATABASE_URL")
	if sourceURL == "" || targetURL == "" {
		t.Skip("TEST_BOOKING_SHARD_0_DATABASE_URL and TEST_BOOKING_SHARD_1_DATABASE_URL are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	source, err := pgxpool.New(ctx, sourceURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(source.Close)
	target, err := pgxpool.New(ctx, targetURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(target.Close)
	return source, target
}

func integrationRecord(sourceGeneration, targetGeneration int64) physicalmigration.Record {
	return physicalmigration.Record{
		MigrationID: uuid.New(), TrainRunID: uuid.New(), SourceShardID: "physical-shard-0",
		TargetShardID: "physical-shard-1", SourceGeneration: sourceGeneration,
		TargetGeneration: targetGeneration, SourceProtocolVersion: 1, SourceSchemaVersion: 1,
		TargetProtocolVersion: 1, TargetSchemaVersion: 1, State: migration.PhysicalStatePreparingTarget,
	}
}

func seedSnapshotAndFence(t *testing.T, pool *pgxpool.Pool, trainRunID uuid.UUID, generation int64, state string, write, corruptInventory bool) {
	t.Helper()
	ctx := context.Background()
	snapshotID := uuid.NewSHA1(trainRunID, []byte("snapshot"))
	trainID := uuid.NewSHA1(trainRunID, []byte("train"))
	routeID := uuid.NewSHA1(trainRunID, []byte("route"))
	seatID := uuid.NewSHA1(trainRunID, []byte("seat"))
	coachID := uuid.NewSHA1(trainRunID, []byte("coach"))
	catalogID := uuid.NewSHA1(trainRunID, []byte("catalog"))
	inventoryID := uuid.NewSHA1(trainRunID, []byte("inventory"))
	if _, err := pool.Exec(ctx, `
INSERT INTO public.train_run_booking_snapshots(
    id,train_run_id,assignment_generation,train_id,route_id,service_date,segment_count,
    route_version,booking_policy_version,source_version,status,bookable,active,
    source_updated_at,created_at,updated_at
) VALUES($1,$2,$3,$4,$5,date '2026-01-01',3,1,1,1,'scheduled',true,true,
         timestamptz '2026-01-01 00:00:00Z',timestamptz '2026-01-01 00:00:00Z',
         timestamptz '2026-01-01 00:00:00Z')`, snapshotID, trainRunID, generation, trainID, routeID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO public.train_run_write_fences(train_run_id,assignment_generation,state,write_enabled) VALUES($1,$2,$3,$4)`, trainRunID, generation, state, write); err != nil {
		t.Fatal(err)
	}
	if !corruptInventory {
		return
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.booking_seat_catalog(
    id,train_run_id,assignment_generation,train_id,coach_id,seat_id,coach_order,seat_order,
    seat_class,source_version,source_updated_at,created_at,updated_at
) VALUES($1,$2,$3,$4,$5,$6,1,1,'standard',1,timestamptz '2026-01-01 00:00:00Z',
         timestamptz '2026-01-01 00:00:00Z',timestamptz '2026-01-01 00:00:00Z')`, catalogID, trainRunID, generation, trainID, coachID, seatID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.seat_inventory(id,train_run_id,assignment_generation,segment_count,seat_id,seat_class,occupied_segments,created_at,updated_at)
VALUES($1,$2,$3,3,$4,'standard',B'100',timestamptz '2026-01-01 00:00:00Z',timestamptz '2026-01-01 00:00:00Z')`, inventoryID, trainRunID, generation, seatID); err != nil {
		t.Fatal(err)
	}
}

func cleanupTrainRun(t *testing.T, pool *pgxpool.Pool, trainRunID uuid.UUID) {
	t.Helper()
	statements := []string{
		"DELETE FROM public.migration_apply_receipts WHERE train_run_id=$1",
		"DELETE FROM public.train_run_mutation_journal WHERE train_run_id=$1",
		"DELETE FROM public.migration_capture_state WHERE train_run_id=$1",
		"DELETE FROM public.train_run_target_write_evidence WHERE train_run_id=$1",
		"DELETE FROM public.train_run_write_fences WHERE train_run_id=$1",
		"DELETE FROM public.outbox_events WHERE train_run_id=$1",
		"DELETE FROM public.booking_command_receipts WHERE train_run_id=$1",
		"DELETE FROM public.idempotency_records WHERE train_run_id=$1",
		"DELETE FROM public.tickets WHERE train_run_id=$1",
		"DELETE FROM public.ticket_orders WHERE train_run_id=$1",
		"DELETE FROM public.reservation_seats WHERE train_run_id=$1",
		"DELETE FROM public.reservations WHERE train_run_id=$1",
		"DELETE FROM public.seat_inventory WHERE train_run_id=$1",
		"DELETE FROM public.booking_fare_snapshots WHERE train_run_id=$1",
		"DELETE FROM public.booking_seat_catalog WHERE train_run_id=$1",
		"DELETE FROM public.train_run_booking_snapshots WHERE train_run_id=$1",
	}
	for _, statement := range statements {
		if _, err := pool.Exec(context.Background(), statement, trainRunID); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}
}
