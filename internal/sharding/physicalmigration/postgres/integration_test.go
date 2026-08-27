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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

	t.Run("v3 partial refund copy journal replay and validation are convergent", func(t *testing.T) {
		ctx := context.Background()
		sourceTx, err := source.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sourceTx.Rollback(context.WithoutCancel(ctx)) }()
		targetTx, err := target.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = targetTx.Rollback(context.WithoutCancel(ctx)) }()

		record := integrationRecord(15, 16)
		seedSnapshotAndFence(t, sourceTx, record.TrainRunID, record.SourceGeneration, "active", true, false)
		shards, err := physicalpostgres.NewDefaultShards(nestedTxDB{sourceTx}, nestedTxDB{targetTx})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := shards.EnableCapture(ctx, record); err != nil {
			t.Fatalf("EnableCapture() error = %v", err)
		}
		fixture := seedPartialRefundFixture(t, sourceTx, record)

		cursor := ""
		for attempts := 0; attempts < 64; attempts++ {
			batch, err := shards.ReadBaseBatch(ctx, physicalmigration.BaseCopyRequest{
				Migration: record, Cursor: cursor, Limit: 32,
			})
			if err != nil {
				t.Fatalf("ReadBaseBatch(%q) error = %v", cursor, err)
			}
			if err := shards.ApplyBaseBatch(ctx, record, batch); err != nil {
				t.Fatalf("ApplyBaseBatch(%s) error = %v", batch.ObjectName, err)
			}
			// Simulate a committed apply followed by a lost control checkpoint.
			if err := shards.ApplyBaseBatch(ctx, record, batch); err != nil {
				t.Fatalf("retry ApplyBaseBatch(%s) error = %v", batch.ObjectName, err)
			}
			cursor = batch.NextCursor
			if batch.Done {
				break
			}
			if attempts == 63 {
				t.Fatal("base copy did not converge")
			}
		}
		if _, err := sourceTx.Exec(ctx, `UPDATE public.ticket_refund_prepare_receipts
SET state='applied',resolved_at=timestamptz '2026-01-01 00:10:00Z'
WHERE id=$1 AND state='prepared'`, fixture.prepareReceiptID); err != nil {
			t.Fatalf("resolve source prepare receipt: %v", err)
		}

		journal, err := shards.ReadJournal(ctx, physicalmigration.JournalRequest{
			Migration: record, AfterSequence: 0, Limit: 128,
		})
		if err != nil {
			t.Fatalf("ReadJournal() error = %v", err)
		}
		seenPrepare, seenCompensation, seenSelected := false, false, false
		for _, entry := range journal.Entries {
			switch entry.TableName {
			case "ticket_refund_prepare_receipts":
				seenPrepare = true
			case "ticket_refund_compensation_receipts":
				seenCompensation = true
			case "selected_ticket_refund_receipts":
				seenSelected = true
			}
			alreadyApplied, err := shards.ApplyJournal(ctx, record, entry)
			if err != nil || alreadyApplied {
				t.Fatalf("first ApplyJournal(%s) = (%v, %v)", entry.TableName, alreadyApplied, err)
			}
			alreadyApplied, err = shards.ApplyJournal(ctx, record, entry)
			if err != nil || !alreadyApplied {
				t.Fatalf("replay ApplyJournal(%s) = (%v, %v)", entry.TableName, alreadyApplied, err)
			}
		}
		if !seenPrepare || !seenCompensation || !seenSelected {
			t.Fatalf("v3 journal coverage prepare=%v compensation=%v selected=%v",
				seenPrepare, seenCompensation, seenSelected)
		}
		if err := shards.CaptureOutbox(ctx, record, 32); err != nil {
			t.Fatalf("CaptureOutbox() error = %v", err)
		}
		result, err := shards.Validate(ctx, physicalmigration.ValidationRequest{
			Migration: record, MaxRows: 1024, MaxTables: 18,
		})
		if err != nil || !result.Passed || result.Truncated || result.Tables != 18 {
			t.Fatalf("Validate() = (%+v, %v)", result, err)
		}

		var compensationCount, selectedCount, outboxCount int
		var reservationState, orderState, refundedTicketState, activeTicketState string
		if err := targetTx.QueryRow(ctx, `
SELECT
 (SELECT count(*) FROM public.ticket_refund_compensation_receipts WHERE refund_request_id=$1 AND assignment_generation=$2),
 (SELECT count(*) FROM public.selected_ticket_refund_receipts WHERE refund_request_id=$1 AND assignment_generation=$2),
 (SELECT count(*) FROM public.outbox_events WHERE aggregate_id=$3 AND assignment_generation=$2),
 (SELECT status FROM public.reservations WHERE id=$4),
 (SELECT status FROM public.ticket_orders WHERE id=$5),
 (SELECT status FROM public.tickets WHERE id=$3),
 (SELECT status FROM public.tickets WHERE id=$6)`, fixture.refundRequestID,
			record.TargetGeneration, fixture.refundedTicketID, fixture.reservationID,
			fixture.ticketOrderID, fixture.activeTicketID).Scan(
			&compensationCount, &selectedCount, &outboxCount, &reservationState,
			&orderState, &refundedTicketState, &activeTicketState); err != nil {
			t.Fatal(err)
		}
		if compensationCount != 1 || selectedCount != 1 || outboxCount != 1 ||
			reservationState != "partially_refunded" || orderState != "partially_refunded" ||
			refundedTicketState != "refunded" || activeTicketState != "active" {
			t.Fatalf("target partial-refund evidence=%d/%d/%d states=%s/%s/%s/%s",
				compensationCount, selectedCount, outboxCount, reservationState, orderState,
				refundedTicketState, activeTicketState)
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
			physicalmigration.ValidationRequest{Migration: record, MaxRows: 100, MaxTables: 18})
		if err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if result.Passed {
			t.Fatal("semantic inventory corruption passed validation")
		}
	})

	t.Run("reverse prepare authorizes immutable cleanup and resumes after commit", func(t *testing.T) {
		record := integrationRecord(28, 29)
		record.ReverseMigration = true
		record.RetainedTargetGeneration = 27
		defer cleanupTrainRun(t, source, record.TrainRunID)
		defer cleanupTrainRun(t, target, record.TrainRunID)
		seedSnapshotAndFence(t, source, record.TrainRunID, 28, "active", true, false)
		seedSnapshotAndFence(t, target, record.TrainRunID, 27, "retained", false, false)
		if _, err := target.Exec(context.Background(), `
INSERT INTO public.migration_capture_state(
    train_run_id,migration_id,source_generation,capture_enabled,next_sequence,enabled_at
) VALUES($1,$2,$3,true,0,clock_timestamp())`, record.TrainRunID, uuid.New(), int64(27)); err != nil {
			t.Fatal(err)
		}
		retainedRecord := record
		retainedRecord.SourceGeneration = record.RetainedTargetGeneration
		seedPartialRefundFixture(t, target, retainedRecord)
		shards, err := physicalpostgres.NewDefaultShards(source, target)
		if err != nil {
			t.Fatal(err)
		}
		if err := shards.PrepareTarget(context.Background(), record); err != nil {
			t.Fatalf("first PrepareTarget() error = %v", err)
		}
		var selectedReceipts, compensationReceipts, authorizations int
		if err := target.QueryRow(context.Background(), `
SELECT
    (SELECT count(*) FROM public.selected_ticket_refund_receipts
     WHERE train_run_id=$1 AND assignment_generation=$2),
    (SELECT count(*) FROM public.ticket_refund_compensation_receipts
     WHERE train_run_id=$1 AND assignment_generation=$2),
    (SELECT count(*) FROM public.migration_evidence_mutation_authorizations
     WHERE train_run_id=$1 AND assignment_generation=$2)`, record.TrainRunID,
			record.RetainedTargetGeneration).Scan(
			&selectedReceipts, &compensationReceipts, &authorizations); err != nil {
			t.Fatal(err)
		}
		if selectedReceipts != 0 || compensationReceipts != 0 || authorizations != 0 {
			t.Fatalf("retained evidence cleanup selected=%d compensation=%d authorizations=%d",
				selectedReceipts, compensationReceipts, authorizations)
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
		TargetGeneration: targetGeneration, SourceProtocolVersion: 1, SourceSchemaVersion: 3,
		TargetProtocolVersion: 1, TargetSchemaVersion: 3, State: migration.PhysicalStatePreparingTarget,
	}
}

type sqlExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func seedSnapshotAndFence(t *testing.T, pool sqlExecer, trainRunID uuid.UUID, generation int64, state string, write, corruptInventory bool) {
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
    id,train_run_id,assignment_generation,train_id,route_id,service_date,scheduled_departure_at,segment_count,
    route_version,booking_policy_version,source_version,status,bookable,active,
    source_updated_at,created_at,updated_at
) VALUES($1,$2,$3,$4,$5,date '2026-01-01',timestamptz '2026-01-01 12:00:00Z',3,1,1,1,'scheduled',true,true,
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

type nestedTxDB struct{ pgx.Tx }

func (db nestedTxDB) BeginTx(ctx context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	return db.Tx.Begin(ctx)
}

type partialRefundFixture struct {
	refundRequestID  uuid.UUID
	prepareReceiptID uuid.UUID
	reservationID    uuid.UUID
	ticketOrderID    uuid.UUID
	refundedTicketID uuid.UUID
	activeTicketID   uuid.UUID
}

func seedPartialRefundFixture(t *testing.T, db sqlExecer, record physicalmigration.Record) partialRefundFixture {
	t.Helper()
	ctx := context.Background()
	fixture := partialRefundFixture{
		refundRequestID:  uuid.New(),
		prepareReceiptID: uuid.New(),
		reservationID:    uuid.New(),
		ticketOrderID:    uuid.New(),
		refundedTicketID: uuid.New(),
		activeTicketID:   uuid.New(),
	}
	userID := uuid.New()
	paymentIntentID := uuid.New()
	trainID := uuid.NewSHA1(record.TrainRunID, []byte("train"))
	coachID := uuid.New()
	seatIDs := []uuid.UUID{uuid.New(), uuid.New()}
	catalogIDs := []uuid.UUID{uuid.New(), uuid.New()}
	inventoryIDs := []uuid.UUID{uuid.New(), uuid.New()}
	fareIDs := []uuid.UUID{uuid.New(), uuid.New()}
	reservationSeatIDs := []uuid.UUID{uuid.New(), uuid.New()}
	passengerIDs := []uuid.UUID{uuid.New(), uuid.New()}
	fareAmounts := []int64{4000, 6000}

	for index := range seatIDs {
		if _, err := db.Exec(ctx, `
INSERT INTO public.booking_seat_catalog(
 id,train_run_id,assignment_generation,train_id,coach_id,seat_id,coach_order,seat_order,
 seat_class,active,source_version,source_updated_at,created_at,updated_at
) VALUES($1,$2,$3,$4,$5,$6,0,$7,'standard',true,1,
 timestamptz '2026-01-01 00:00:00Z',timestamptz '2026-01-01 00:00:00Z',
 timestamptz '2026-01-01 00:00:00Z')`, catalogIDs[index], record.TrainRunID,
			record.SourceGeneration, trainID, coachID, seatIDs[index], index); err != nil {
			t.Fatal(err)
		}
		occupied := "000"
		if index == 1 {
			occupied = "111"
		}
		if _, err := db.Exec(ctx, `
INSERT INTO public.seat_inventory(
 id,train_run_id,assignment_generation,segment_count,seat_id,seat_class,
 occupied_segments,version,created_at,updated_at
) VALUES($1,$2,$3,3,$4,'standard',$5::bit varying,1,
 timestamptz '2026-01-01 00:00:00Z',timestamptz '2026-01-01 00:00:00Z')`,
			inventoryIDs[index], record.TrainRunID, record.SourceGeneration,
			seatIDs[index], occupied); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(ctx, `
INSERT INTO public.booking_fare_snapshots(
 id,train_run_id,assignment_generation,segment_count,from_stop_index,to_stop_index,
 seat_class,amount_minor,currency,source_version,active,source_updated_at,created_at,updated_at
) VALUES($1,$2,$3,3,0,3,'standard',$4,'TWD',$5,true,
 timestamptz '2026-01-01 00:00:00Z',timestamptz '2026-01-01 00:00:00Z',
 timestamptz '2026-01-01 00:00:00Z')`, fareIDs[index], record.TrainRunID,
			record.SourceGeneration, fareAmounts[index], int64(index+1)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := db.Exec(ctx, `
INSERT INTO public.reservations(
 id,user_id,train_run_id,assignment_generation,segment_count,from_stop_index,to_stop_index,
 seat_class,status,expires_at,total_amount_minor,currency,payment_intent_id,
 payment_amount_minor,payment_currency,payment_grace_expires_at,created_at,updated_at
) VALUES($1,$2,$3,$4,3,0,3,'standard','partially_refunded',
 timestamptz '2026-02-01 00:00:00Z',10000,'TWD',$5,10000,'TWD',
 timestamptz '2026-01-01 00:30:00Z',timestamptz '2026-01-01 00:00:00Z',
 timestamptz '2026-01-01 00:00:00Z')`, fixture.reservationID, userID,
		record.TrainRunID, record.SourceGeneration, paymentIntentID); err != nil {
		t.Fatal(err)
	}
	for index := range reservationSeatIDs {
		if _, err := db.Exec(ctx, `
INSERT INTO public.reservation_seats(
 id,reservation_id,train_run_id,assignment_generation,segment_count,seat_id,
 passenger_id,fare_snapshot_id,segment_mask,fare_amount_minor,currency,created_at,updated_at
) VALUES($1,$2,$3,$4,3,$5,$6,$7,B'111',$8,'TWD',
 timestamptz '2026-01-01 00:00:00Z',timestamptz '2026-01-01 00:00:00Z')`,
			reservationSeatIDs[index], fixture.reservationID, record.TrainRunID,
			record.SourceGeneration, seatIDs[index], passengerIDs[index], fareIDs[index],
			fareAmounts[index]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(ctx, `
INSERT INTO public.ticket_orders(
 id,reservation_id,user_id,train_run_id,assignment_generation,status,total_amount_minor,
 currency,payment_intent_id,payment_currency,authorized_amount_minor,captured_amount_minor,
 refunded_amount_minor,created_at,updated_at
) VALUES($1,$2,$3,$4,$5,'partially_refunded',10000,'TWD',$6,'TWD',10000,10000,4000,
 timestamptz '2026-01-01 00:00:00Z',timestamptz '2026-01-01 00:00:00Z')`,
		fixture.ticketOrderID, fixture.reservationID, userID, record.TrainRunID,
		record.SourceGeneration, paymentIntentID); err != nil {
		t.Fatal(err)
	}
	for index, ticketID := range []uuid.UUID{fixture.refundedTicketID, fixture.activeTicketID} {
		state := "refunded"
		if index == 1 {
			state = "active"
		}
		if _, err := db.Exec(ctx, `
INSERT INTO public.tickets(
 id,ticket_order_id,reservation_seat_id,train_run_id,assignment_generation,ticket_code,
 status,created_at,updated_at
) VALUES($1,$2,$3,$4,$5,$6,$7,
 timestamptz '2026-01-01 00:00:00Z',timestamptz '2026-01-01 00:00:00Z')`,
			ticketID, fixture.ticketOrderID, reservationSeatIDs[index], record.TrainRunID,
			record.SourceGeneration, "TKT-M7-PARTIAL-"+ticketID.String(), state); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(ctx, `
INSERT INTO public.ticket_refund_prepare_receipts(
 id,command_id,refund_request_id,refund_operation_id,payment_intent_id,reservation_id,
 ticket_order_id,train_run_id,assignment_generation,request_fingerprint,amount_minor,currency,
 ticket_ids,prior_order_state,prior_reservation_state,state,requested_at,eligibility_cutoff_at,
 prepared_at,resolved_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,decode(repeat('11',32),'hex'),4000,'TWD',
 ARRAY[$10::uuid],'issued','confirmed','prepared',timestamptz '2025-12-31 23:00:00Z',
 timestamptz '2026-01-01 01:00:00Z',timestamptz '2025-12-31 23:01:00Z',
	NULL)`, fixture.prepareReceiptID, uuid.New(), fixture.refundRequestID,
		uuid.New(), paymentIntentID, fixture.reservationID, fixture.ticketOrderID,
		record.TrainRunID, record.SourceGeneration, fixture.refundedTicketID); err != nil {
		t.Fatal(err)
	}
	compensationID := uuid.New()
	if _, err := db.Exec(ctx, `
INSERT INTO public.ticket_refund_compensation_receipts(
 id,command_id,refund_request_id,refund_operation_id,payment_intent_id,reservation_id,
 ticket_order_id,train_run_id,assignment_generation,request_fingerprint,provider_proof_hash,
 amount_minor,currency,selected_ticket_count,released_seat_count,
 resulting_active_ticket_count,resulting_order_state,committed_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,decode(repeat('11',32),'hex'),
 decode(repeat('22',32),'hex'),4000,'TWD',1,1,1,'partially_refunded',
 timestamptz '2026-01-01 00:10:00Z')`, compensationID, uuid.New(),
		fixture.refundRequestID, uuid.New(), paymentIntentID, fixture.reservationID,
		fixture.ticketOrderID, record.TrainRunID, record.SourceGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `
INSERT INTO public.selected_ticket_refund_receipts(
 id,compensation_receipt_id,refund_request_id,ticket_id,reservation_seat_id,
 train_run_id,assignment_generation,fare_amount_minor,currency,segment_mask_hash,released_at
) VALUES($1,$2,$3,$4,$5,$6,$7,4000,'TWD',decode(repeat('33',32),'hex'),
 timestamptz '2026-01-01 00:10:00Z')`, uuid.New(), compensationID,
		fixture.refundRequestID, fixture.refundedTicketID, reservationSeatIDs[0],
		record.TrainRunID, record.SourceGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `
INSERT INTO public.outbox_events(
 id,train_run_id,assignment_generation,aggregate_type,aggregate_id,event_type,event_version,
 payload,status,attempts,next_attempt_at,created_at,updated_at
) VALUES($1,$2,$3,'ticket',$4,'ticket.refunded',1,
 jsonb_build_object('refund_request_id',$5::text),'pending',0,
 timestamptz '2026-01-01 00:10:00Z',timestamptz '2026-01-01 00:10:00Z',
 timestamptz '2026-01-01 00:10:00Z')`, uuid.New(), record.TrainRunID,
		record.SourceGeneration, fixture.refundedTicketID, fixture.refundRequestID); err != nil {
		t.Fatal(err)
	}
	return fixture
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
		"DELETE FROM public.selected_ticket_refund_receipts WHERE train_run_id=$1",
		"DELETE FROM public.ticket_refund_compensation_receipts WHERE train_run_id=$1",
		"DELETE FROM public.ticket_refund_prepare_receipts WHERE train_run_id=$1",
		"DELETE FROM public.payment_compensation_receipts WHERE train_run_id=$1",
		"DELETE FROM public.payment_refund_receipts WHERE train_run_id=$1",
		"DELETE FROM public.ticket_issuance_receipts WHERE train_run_id=$1",
		"DELETE FROM public.payment_command_receipts WHERE train_run_id=$1",
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
