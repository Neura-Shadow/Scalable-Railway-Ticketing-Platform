package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCancelVoidedReservationConcurrentReplayAndConflicts(t *testing.T) {
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
			MaxOpenConns: 20, MaxIdleConns: 20, ConnectTimeout: 3 * time.Second,
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

	fixture := seedVoidFixture(t, ctx, admin, "payment_authorized")
	store, err := NewStore(staticRouteResolver{resolution: shardphysical.Resolution{Route: fixture.route, Handle: handle}})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	command := fixture.command
	command.RequestFingerprint = paymentshard.VoidCancellationFingerprint(command)

	const replicas = 16
	receipts := make(chan paymentshard.CancelVoidedReservationReceipt, replicas)
	errorsFound := make(chan error, replicas)
	var group sync.WaitGroup
	for range replicas {
		group.Add(1)
		go func() {
			defer group.Done()
			receipt, callErr := store.CancelVoidedReservation(ctx, fixture.route, command)
			if callErr != nil {
				errorsFound <- callErr
				return
			}
			receipts <- receipt
		}()
	}
	group.Wait()
	close(receipts)
	close(errorsFound)
	for callErr := range errorsFound {
		t.Errorf("concurrent cancellation error = %v", callErr)
	}
	for receipt := range receipts {
		if receipt.CommandID != command.CommandID || receipt.VoidOperationID != command.VoidOperationID ||
			receipt.ReleasedSeatCount != 2 || receipt.CancelledAt.IsZero() {
			t.Errorf("receipt = %+v", receipt)
		}
	}
	var reservationState, orderState, occupied string
	var inventoryVersion int64
	var receiptCount int
	if err := admin.QueryRow(ctx, `
SELECT reservation.status,orders.status,
       (SELECT string_agg(inventory.occupied_segments::text,',' ORDER BY inventory.seat_id)
        FROM public.seat_inventory AS inventory WHERE inventory.train_run_id=$1),
       (SELECT sum(inventory.version)::bigint FROM public.seat_inventory AS inventory WHERE inventory.train_run_id=$1),
       (SELECT count(*)::integer FROM public.payment_command_receipts WHERE command_id=$2)
FROM public.reservations AS reservation
JOIN public.ticket_orders AS orders ON orders.reservation_id=reservation.id
WHERE reservation.id=$3
GROUP BY reservation.status,orders.status`, fixture.route.TrainRunID(), command.CommandID, command.ReservationID).Scan(
		&reservationState, &orderState, &occupied, &inventoryVersion, &receiptCount); err != nil {
		t.Fatalf("inspect cancellation: %v", err)
	}
	if reservationState != "cancelled" || orderState != "cancelled" || occupied != "00,00" ||
		inventoryVersion != 2 || receiptCount != 1 {
		t.Fatalf("state reservation=%s order=%s occupied=%s version=%d receipts=%d",
			reservationState, orderState, occupied, inventoryVersion, receiptCount)
	}
	if replay, err := store.CancelVoidedReservation(ctx, fixture.route, command); err != nil || replay.ReleasedSeatCount != 2 {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	if err := admin.QueryRow(ctx, `SELECT sum(version)::bigint FROM public.seat_inventory WHERE train_run_id=$1`, fixture.route.TrainRunID()).Scan(&inventoryVersion); err != nil || inventoryVersion != 2 {
		t.Fatalf("replay inventory version = %d, %v", inventoryVersion, err)
	}

	identityConflict := command
	identityConflict.CommandID = uuid.New()
	identityConflict.RequestFingerprint = paymentshard.VoidCancellationFingerprint(identityConflict)
	if _, err := store.CancelVoidedReservation(ctx, fixture.route, identityConflict); !errors.Is(err, paymentapp.ErrPaymentConflict) {
		t.Fatalf("different command identity error = %v, want conflict", err)
	}
	operationConflict := command
	operationConflict.VoidOperationID = uuid.New()
	// Keeping the old fingerprint proves that operation identity is bound.
	if _, err := store.CancelVoidedReservation(ctx, fixture.route, operationConflict); !errors.Is(err, paymentapp.ErrReservationNotPayable) {
		t.Fatalf("different operation identity error = %v, want invalid command", err)
	}

	for _, state := range []string{"payment_captured", "issued"} {
		t.Run("reject_"+state, func(t *testing.T) {
			conflict := seedVoidFixture(t, ctx, admin, state)
			conflictStore, err := NewStore(staticRouteResolver{resolution: shardphysical.Resolution{Route: conflict.route, Handle: handle}})
			if err != nil {
				t.Fatal(err)
			}
			conflict.command.RequestFingerprint = paymentshard.VoidCancellationFingerprint(conflict.command)
			if _, err := conflictStore.CancelVoidedReservation(ctx, conflict.route, conflict.command); !errors.Is(err, paymentapp.ErrPaymentConflict) {
				t.Fatalf("error = %v, want captured/issued conflict", err)
			}
		})
	}

	money := seedVoidFixture(t, ctx, admin, "payment_pending")
	moneyStore, _ := NewStore(staticRouteResolver{resolution: shardphysical.Resolution{Route: money.route, Handle: handle}})
	wrongMoney := money.command
	wrongMoney.AmountMinor++
	wrongMoney.RequestFingerprint = paymentshard.VoidCancellationFingerprint(wrongMoney)
	if _, err := moneyStore.CancelVoidedReservation(ctx, money.route, wrongMoney); !errors.Is(err, paymentapp.ErrReservationNotPayable) {
		t.Fatalf("money conflict error = %v", err)
	}
	wrongOwner := money.command
	wrongOwner.OwnerID = uuid.New()
	wrongOwner.RequestFingerprint = paymentshard.VoidCancellationFingerprint(wrongOwner)
	if _, err := moneyStore.CancelVoidedReservation(ctx, money.route, wrongOwner); !errors.Is(err, paymentapp.ErrPaymentNotFound) {
		t.Fatalf("owner conflict error = %v", err)
	}
}

type voidFixture struct {
	route   sharding.ShardRoute
	command paymentshard.CancelVoidedReservationCommand
}

func seedVoidFixture(t *testing.T, ctx context.Context, db *pgxpool.Pool, orderState string) voidFixture {
	t.Helper()
	trainRunID, trainID, routeID := uuid.New(), uuid.New(), uuid.New()
	ownerID, reservationID, intentID := uuid.New(), uuid.New(), uuid.New()
	generation, err := sharding.NewAssignmentGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatal(err)
	}
	fareID := uuid.New()
	if _, err := db.Exec(ctx, `INSERT INTO public.train_run_booking_snapshots(
 id,train_run_id,assignment_generation,train_id,route_id,service_date,segment_count,
 route_version,booking_policy_version,source_version,status,bookable,source_updated_at
) VALUES($1,$2,1,$3,$4,current_date,2,1,1,1,'scheduled',true,clock_timestamp())`,
		uuid.New(), trainRunID, trainID, routeID); err != nil {
		t.Fatalf("seed train run: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO public.booking_fare_snapshots(
 id,train_run_id,assignment_generation,segment_count,from_stop_index,to_stop_index,
 seat_class,amount_minor,currency,source_version,source_updated_at
) VALUES($1,$2,1,2,0,2,'standard',6250,'TWD',1,clock_timestamp())`, fareID, trainRunID); err != nil {
		t.Fatalf("seed fare: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO public.reservations(
 id,user_id,train_run_id,assignment_generation,segment_count,from_stop_index,to_stop_index,
 seat_class,status,expires_at,total_amount_minor,currency,payment_intent_id,payment_amount_minor,
 payment_currency,payment_grace_expires_at
) VALUES($1,$2,$3,1,2,0,2,'standard','payment_pending',clock_timestamp()+interval '1 hour',
 12500,'TWD',$4,12500,'TWD',clock_timestamp()+interval '30 minutes')`, reservationID, ownerID, trainRunID, intentID); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO public.ticket_orders(
 id,reservation_id,user_id,train_run_id,assignment_generation,status,total_amount_minor,currency,
 payment_intent_id,payment_currency,authorized_amount_minor,captured_amount_minor
) VALUES($1,$2,$3,$4,1,$5,12500,'TWD',$6,'TWD',
 CASE WHEN $5='payment_pending' THEN 0 ELSE 12500 END,
 CASE WHEN $5 IN('payment_captured','issued') THEN 12500 ELSE 0 END)`,
		uuid.NewSHA1(intentID, []byte("ticket-order")), reservationID, ownerID, trainRunID, orderState, intentID); err != nil {
		t.Fatalf("seed ticket order: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO public.train_run_write_fences(id,train_run_id,assignment_generation,state,write_enabled)
VALUES($1,$2,1,'active',true)`, uuid.New(), trainRunID); err != nil {
		t.Fatalf("seed write fence: %v", err)
	}
	for index := 0; index < 2; index++ {
		seatID := uuid.New()
		if _, err := db.Exec(ctx, `INSERT INTO public.booking_seat_catalog(
 id,train_run_id,assignment_generation,train_id,coach_id,seat_id,coach_order,seat_order,
 seat_class,source_version,source_updated_at
) VALUES($1,$2,1,$3,$4,$5,0,$6,'standard',1,clock_timestamp())`,
			uuid.New(), trainRunID, trainID, uuid.New(), seatID, index); err != nil {
			t.Fatalf("seed fixture seat catalog %d: %v", index, err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO public.seat_inventory(
 id,train_run_id,assignment_generation,segment_count,seat_id,seat_class,occupied_segments
) VALUES($1,$2,1,2,$3,'standard',B'11')`, uuid.New(), trainRunID, seatID); err != nil {
			t.Fatalf("seed fixture inventory %d: %v", index, err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO public.reservation_seats(
 id,reservation_id,train_run_id,assignment_generation,segment_count,seat_id,passenger_id,
 fare_snapshot_id,segment_mask,fare_amount_minor,currency
) VALUES($1,$2,$3,1,2,$4,$5,$6,B'11',6250,'TWD')`,
			uuid.New(), reservationID, trainRunID, seatID, uuid.New(), fareID); err != nil {
			t.Fatalf("seed fixture seat %d: %v", index, err)
		}
	}
	command := paymentshard.CancelVoidedReservationCommand{
		CommandID: uuid.New(), VoidOperationID: uuid.New(), PaymentIntentID: intentID,
		ReservationID: reservationID, TrainRunID: trainRunID, OwnerID: ownerID,
		AmountMinor: 12500, Currency: "TWD", VoidProofHash: [32]byte{1},
		VoidedAt: time.Now().UTC(),
	}
	return voidFixture{route: route, command: command}
}

type staticRouteResolver struct{ resolution shardphysical.Resolution }

func (resolver staticRouteResolver) Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error) {
	return resolver.resolution, nil
}
