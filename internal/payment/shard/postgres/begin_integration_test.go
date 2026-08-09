package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestBeginPaymentPersistsAuthorityBeforeImmediateReceiptFK(t *testing.T) {
	dsn := os.Getenv("PAYMENT_SHARD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PAYMENT_SHARD_TEST_DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	defer admin.Close()

	registry, err := shardphysical.NewRegistry(ctx, shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			sharding.ShardPhysicalZero.String(): {ShardID: sharding.ShardPhysicalZero, DSN: dsn},
		},
		MaxCount: 1,
		Limits: shardphysical.PoolLimits{
			MaxOpenConns: 4, MaxIdleConns: 4, ConnectTimeout: 3 * time.Second,
			StatementTimeout: 10 * time.Second, LockTimeout: 10 * time.Second,
		},
	}, shardphysical.OpenPGXPool)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	defer registry.Close()
	handle, err := registry.Resolve(shardphysical.CatalogEntry{
		ShardID: sharding.ShardPhysicalZero, StorageKind: shardphysical.StoragePostgres,
		ConnectionRef: sharding.ShardPhysicalZero.String(), ProtocolVersion: shardphysical.SupportedProtocolVersion,
		SchemaVersion: shardphysical.SupportedSchemaVersion, Enabled: true, WriteEnabled: true,
		HealthState: shardphysical.HealthHealthy, State: shardphysical.StateActive,
	})
	if err != nil {
		t.Fatalf("resolve handle: %v", err)
	}

	trainRunID, trainID, routeID := uuid.New(), uuid.New(), uuid.New()
	reservationID, ownerID, seatID, fareID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	seedTx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed transaction: %v", err)
	}
	defer seedTx.Rollback(ctx)
	seed := func(query string, args ...any) {
		t.Helper()
		if _, err := seedTx.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed begin-payment fixture: %v", err)
		}
	}
	seed(`INSERT INTO public.train_run_booking_snapshots(
 id,train_run_id,assignment_generation,train_id,route_id,service_date,segment_count,
 route_version,booking_policy_version,source_version,status,bookable,source_updated_at
) VALUES($1,$2,1,$3,$4,current_date,1,1,1,1,'scheduled',true,clock_timestamp())`,
		uuid.New(), trainRunID, trainID, routeID)
	seed(`INSERT INTO public.booking_fare_snapshots(
 id,train_run_id,assignment_generation,segment_count,from_stop_index,to_stop_index,
 seat_class,amount_minor,currency,source_version,source_updated_at
) VALUES($1,$2,1,1,0,1,'standard',700,'TWD',1,clock_timestamp())`,
		fareID, trainRunID)
	seed(`INSERT INTO public.train_run_write_fences(id,train_run_id,assignment_generation,state,write_enabled)
VALUES($1,$2,1,'active',true)`, uuid.New(), trainRunID)
	seed(`INSERT INTO public.booking_seat_catalog(
 id,train_run_id,assignment_generation,train_id,coach_id,seat_id,coach_order,seat_order,
 seat_class,source_version,source_updated_at
) VALUES($1,$2,1,$3,$4,$5,0,0,'standard',1,clock_timestamp())`,
		uuid.New(), trainRunID, trainID, uuid.New(), seatID)
	seed(`INSERT INTO public.seat_inventory(
 id,train_run_id,assignment_generation,segment_count,seat_id,seat_class,occupied_segments
) VALUES($1,$2,1,1,$3,'standard',B'1')`, uuid.New(), trainRunID, seatID)
	seed(`INSERT INTO public.reservations(
 id,user_id,train_run_id,assignment_generation,segment_count,from_stop_index,to_stop_index,
 seat_class,status,expires_at,total_amount_minor,currency
) VALUES($1,$2,$3,1,1,0,1,'standard','held',clock_timestamp()+interval '5 minutes',700,'TWD')`,
		reservationID, ownerID, trainRunID)
	seed(`INSERT INTO public.reservation_seats(
 id,reservation_id,train_run_id,assignment_generation,segment_count,seat_id,passenger_id,
 fare_snapshot_id,segment_mask,fare_amount_minor,currency
) VALUES($1,$2,$3,1,1,$4,$5,$6,B'1',700,'TWD')`,
		uuid.New(), reservationID, trainRunID, seatID, uuid.New(), fareID)
	if err := seedTx.Commit(ctx); err != nil {
		t.Fatalf("commit begin-payment fixture: %v", err)
	}
	generation, _ := sharding.NewAssignmentGeneration(1)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	store, err := NewStore(staticRouteResolver{resolution: shardphysical.Resolution{Route: route, Handle: handle}})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	command := paymentapp.BeginPaymentCommand{
		CommandID: uuid.New(), PaymentIntentID: uuid.New(), ReservationID: reservationID,
		TrainRunID: trainRunID, OwnerID: ownerID, AmountMinor: 700, Currency: "TWD",
		GraceExpiresAt:     time.Now().UTC().Add(15 * time.Minute),
		RequestFingerprint: [32]byte{1},
	}
	receipt, err := store.BeginPayment(ctx, route, command)
	if err != nil {
		t.Fatalf("BeginPayment() error = %v", err)
	}
	if receipt.CommandID != command.CommandID || receipt.PaymentIntentID != command.PaymentIntentID {
		t.Fatalf("receipt = %+v", receipt)
	}
	if replay, err := store.BeginPayment(ctx, route, command); err != nil || replay != receipt {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	var status string
	var intentID uuid.UUID
	var receiptCount, orderCount int
	if err := admin.QueryRow(ctx, `
SELECT reservation.status,reservation.payment_intent_id,
       (SELECT count(*)::integer FROM public.payment_command_receipts WHERE command_id=$2),
       (SELECT count(*)::integer FROM public.ticket_orders WHERE reservation_id=$1)
FROM public.reservations AS reservation WHERE reservation.id=$1`, reservationID, command.CommandID).Scan(
		&status, &intentID, &receiptCount, &orderCount); err != nil {
		t.Fatalf("inspect begin payment: %v", err)
	}
	if status != "payment_pending" || intentID != command.PaymentIntentID || receiptCount != 1 || orderCount != 1 {
		t.Fatalf("status=%s intent=%s receipts=%d orders=%d", status, intentID, receiptCount, orderCount)
	}

	expiredReservationID := uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO public.reservations(
 id,user_id,train_run_id,assignment_generation,segment_count,from_stop_index,to_stop_index,
 seat_class,status,expires_at,total_amount_minor,currency,created_at,updated_at
) VALUES($1,$2,$3,1,1,0,1,'standard','held',clock_timestamp()-interval '1 second',700,'TWD',
         clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '2 minutes')`,
		expiredReservationID, ownerID, trainRunID); err != nil {
		t.Fatalf("seed expired reservation: %v", err)
	}
	expiredCommand := command
	expiredCommand.CommandID = uuid.New()
	expiredCommand.PaymentIntentID = uuid.New()
	expiredCommand.ReservationID = expiredReservationID
	if _, err := store.BeginPayment(ctx, route, expiredCommand); !errors.Is(err, paymentapp.ErrReservationNotPayable) {
		t.Fatalf("expired BeginPayment() error = %v, want not payable", err)
	}
	var expiredStatus string
	var expiredReceipts, expiredOrders int
	if err := admin.QueryRow(ctx, `SELECT status,
 (SELECT count(*)::integer FROM public.payment_command_receipts WHERE command_id=$2),
 (SELECT count(*)::integer FROM public.ticket_orders WHERE reservation_id=$1)
 FROM public.reservations WHERE id=$1`, expiredReservationID, expiredCommand.CommandID).Scan(
		&expiredStatus, &expiredReceipts, &expiredOrders,
	); err != nil {
		t.Fatalf("inspect expired reservation: %v", err)
	}
	if expiredStatus != "held" || expiredReceipts != 0 || expiredOrders != 0 {
		t.Fatalf("expired status=%s receipts=%d orders=%d", expiredStatus, expiredReceipts, expiredOrders)
	}
}
