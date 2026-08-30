package postgres

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSelectedTicketRefundPrepareReleaseAndSequentialApply(t *testing.T) {
	dsn := os.Getenv("PAYMENT_SHARD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PAYMENT_SHARD_TEST_DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsedDSN.Query()
	query.Set("options", "-c railway.deployment_region=region-a -c railway.deployment_role=active -c railway.region_epoch=1 -c railway.regional_writes_enabled=true")
	parsedDSN.RawQuery = query.Encode()
	dsn = parsedDSN.String()
	ctx := context.Background()
	adminConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	adminConfig.ConnConfig.RuntimeParams["railway.deployment_region"] = "region-a"
	adminConfig.ConnConfig.RuntimeParams["railway.deployment_role"] = "active"
	adminConfig.ConnConfig.RuntimeParams["railway.region_epoch"] = "1"
	adminConfig.ConnConfig.RuntimeParams["railway.regional_writes_enabled"] = "true"
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	registry, err := shardphysical.NewRegistry(ctx, shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			sharding.ShardPhysicalZero.String(): {ShardID: sharding.ShardPhysicalZero, DSN: dsn},
		},
		MaxCount: 1,
		Limits: shardphysical.PoolLimits{MaxOpenConns: 12, MaxIdleConns: 12,
			ConnectTimeout: 3 * time.Second, StatementTimeout: 10 * time.Second, LockTimeout: 10 * time.Second},
	}, shardphysical.OpenPGXPool)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	handle, err := registry.Resolve(shardphysical.CatalogEntry{
		ShardID: sharding.ShardPhysicalZero, StorageKind: shardphysical.StoragePostgres,
		ConnectionRef: sharding.ShardPhysicalZero.String(), ProtocolVersion: shardphysical.SupportedProtocolVersion,
		SchemaVersion: shardphysical.SupportedSchemaVersion, Enabled: true, WriteEnabled: true,
		HealthState: shardphysical.HealthHealthy, State: shardphysical.StateActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	region, _ := authority.ParseRegion("region-a")
	epoch, _ := authority.NewEpoch(1)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)

	storeFor := func(fixture voidFixture) *Store {
		store, newErr := NewStore(staticRouteResolver{resolution: shardphysical.Resolution{Route: fixture.route, Handle: handle}}, WithRegionalAuthority(deployment))
		if newErr != nil {
			t.Fatal(newErr)
		}
		return store
	}
	seedRefund := func() (voidFixture, []uuid.UUID) {
		fixture := seedVoidFixture(t, ctx, admin, "issued")
		if _, err := admin.Exec(ctx, `UPDATE public.train_run_booking_snapshots
SET scheduled_departure_at=clock_timestamp()+interval '3 hours' WHERE train_run_id=$1`, fixture.route.TrainRunID()); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, `UPDATE public.reservations SET status='confirmed' WHERE id=$1`, fixture.command.ReservationID); err != nil {
			t.Fatal(err)
		}
		rows, err := admin.Query(ctx, `SELECT id FROM public.reservation_seats WHERE reservation_id=$1 ORDER BY id`, fixture.command.ReservationID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var ticketIDs []uuid.UUID
		for rows.Next() {
			var seatID uuid.UUID
			if err := rows.Scan(&seatID); err != nil {
				t.Fatal(err)
			}
			ticketID := uuid.NewSHA1(fixture.command.ReservationID, seatID[:])
			if _, err := admin.Exec(ctx, `INSERT INTO public.tickets(
id,ticket_order_id,reservation_seat_id,train_run_id,assignment_generation,ticket_code,status)
VALUES($1,$2,$3,$4,1,$5,'active')`, ticketID, uuid.NewSHA1(fixture.command.PaymentIntentID, []byte("ticket-order")),
				seatID, fixture.route.TrainRunID(), "partial-refund-"+ticketID.String()); err != nil {
				t.Fatal(err)
			}
			ticketIDs = append(ticketIDs, ticketID)
		}
		if rows.Err() != nil || len(ticketIDs) != 2 {
			t.Fatalf("tickets=%v err=%v", ticketIDs, rows.Err())
		}
		if ticketIDs[0].String() > ticketIDs[1].String() {
			ticketIDs[0], ticketIDs[1] = ticketIDs[1], ticketIDs[0]
		}
		return fixture, ticketIDs
	}
	prepare := func(fixture voidFixture, requestID, operationID uuid.UUID, tickets []uuid.UUID) paymentshard.PrepareSelectedTicketRefundCommand {
		now := time.Now().UTC()
		return paymentshard.PrepareSelectedTicketRefundCommand{
			CommandID: uuid.NewSHA1(requestID, []byte("prepare")), RefundRequestID: requestID, RefundOperationID: operationID,
			PaymentIntentID: fixture.command.PaymentIntentID, ReservationID: fixture.command.ReservationID,
			TicketOrderID: uuid.NewSHA1(fixture.command.PaymentIntentID, []byte("ticket-order")), TrainRunID: fixture.route.TrainRunID(),
			OwnerID: fixture.command.OwnerID, Region: "region-a", RegionalEpoch: 1, AmountMinor: int64(6250 * len(tickets)),
			Currency: "TWD", RequestFingerprint: [32]byte{1}, TicketIDs: tickets,
			RequestedAt: now.Add(-time.Minute), EligibilityCutoffAt: now.Add(time.Hour), PreparedAt: now,
		}
	}

	releaseFixture, releaseTickets := seedRefund()
	releaseStore := storeFor(releaseFixture)
	releaseCommand := prepare(releaseFixture, uuid.New(), uuid.New(), releaseTickets[:1])
	prepared, err := releaseStore.PrepareSelectedTicketRefund(ctx, releaseFixture.route, releaseCommand)
	if err != nil {
		t.Fatalf("prepare for release: %v", err)
	}
	if replay, err := releaseStore.PrepareSelectedTicketRefund(ctx, releaseFixture.route, releaseCommand); err != nil || replay.ReceiptID != prepared.ReceiptID || !replay.PreparedAt.Equal(prepared.PreparedAt) {
		t.Fatalf("prepare replay=%+v err=%v", replay, err)
	}
	unwind := paymentshard.ReleaseSelectedTicketRefundCommand{
		CommandID: uuid.NewSHA1(releaseCommand.RefundRequestID, []byte("release")), PrepareReceiptID: prepared.ReceiptID,
		RefundRequestID: releaseCommand.RefundRequestID, RefundOperationID: releaseCommand.RefundOperationID,
		PaymentIntentID: releaseCommand.PaymentIntentID, ReservationID: releaseCommand.ReservationID,
		TicketOrderID: releaseCommand.TicketOrderID, TrainRunID: releaseCommand.TrainRunID, OwnerID: releaseCommand.OwnerID,
		Region: "region-a", RegionalEpoch: 1, RequestFingerprint: releaseCommand.RequestFingerprint,
		TicketIDs: releaseCommand.TicketIDs, ReleasedAt: time.Now().UTC(),
	}
	released, err := releaseStore.ReleaseSelectedTicketRefund(ctx, releaseFixture.route, unwind)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if replay, err := releaseStore.ReleaseSelectedTicketRefund(ctx, releaseFixture.route, unwind); err != nil || replay.ReleasedAt != released.ReleasedAt {
		t.Fatalf("release replay=%+v err=%v", replay, err)
	}
	var orderState, reservationState, ticketState, prepareState string
	var inventoryVersion int64
	if err := admin.QueryRow(ctx, `SELECT orders.status,reservation.status,ticket.status,prepare.state,
 (SELECT sum(version)::bigint FROM public.seat_inventory WHERE train_run_id=$1)
FROM public.ticket_orders AS orders
JOIN public.reservations AS reservation ON reservation.id=orders.reservation_id
JOIN public.tickets AS ticket ON ticket.ticket_order_id=orders.id AND ticket.id=$2
JOIN public.ticket_refund_prepare_receipts AS prepare ON prepare.refund_request_id=$3
WHERE orders.id=$4`, releaseFixture.route.TrainRunID(), releaseTickets[0], releaseCommand.RefundRequestID,
		releaseCommand.TicketOrderID).Scan(&orderState, &reservationState, &ticketState, &prepareState, &inventoryVersion); err != nil {
		t.Fatal(err)
	}
	if orderState != "issued" || reservationState != "confirmed" || ticketState != "active" || prepareState != "released" || inventoryVersion != 0 {
		t.Fatalf("unwind state order=%s reservation=%s ticket=%s prepare=%s inventory_version=%d",
			orderState, reservationState, ticketState, prepareState, inventoryVersion)
	}

	applyFixture, applyTickets := seedRefund()
	applyStore := storeFor(applyFixture)
	for index, selected := range [][]uuid.UUID{applyTickets[:1], applyTickets[1:]} {
		requestID, operationID := uuid.New(), uuid.New()
		preparedCommand := prepare(applyFixture, requestID, operationID, selected)
		if _, err := applyStore.PrepareSelectedTicketRefund(ctx, applyFixture.route, preparedCommand); err != nil {
			t.Fatalf("prepare %d: %v", index, err)
		}
		applyCommand := paymentshard.ApplySelectedTicketRefundCommand{
			CommandID: uuid.NewSHA1(requestID, []byte("apply")), RefundRequestID: requestID, RefundOperationID: operationID,
			PaymentIntentID: preparedCommand.PaymentIntentID, ReservationID: preparedCommand.ReservationID,
			TicketOrderID: preparedCommand.TicketOrderID, TrainRunID: preparedCommand.TrainRunID, OwnerID: preparedCommand.OwnerID,
			Region: "region-a", RegionalEpoch: 1, AmountMinor: preparedCommand.AmountMinor, Currency: "TWD",
			ProviderProofHash: [32]byte{2}, RequestFingerprint: preparedCommand.RequestFingerprint,
			TicketIDs: selected, RefundedAt: time.Now().UTC(),
		}
		receipt, err := applyStore.ApplySelectedTicketRefund(ctx, applyFixture.route, applyCommand)
		if err != nil {
			t.Fatalf("apply %d: %v", index, err)
		}
		wantState := "partially_refunded"
		if index == 1 {
			wantState = "refunded"
		}
		if receipt.ResultingOrderState != wantState || receipt.ReleasedSeatCount != 1 {
			t.Fatalf("apply %d receipt=%+v", index, receipt)
		}
	}
}
