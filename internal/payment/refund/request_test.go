package refund_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	"github.com/google/uuid"
)

type refundClock struct{ now time.Time }

func (clock refundClock) Now() time.Time { return clock.now }

func TestServiceNormalizesTicketSetAndDerivesMoneyOnlyFromServerSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 6, 0, 0, 0, time.UTC)
	ownerID, orderID := uuid.New(), uuid.New()
	ticketA, ticketB := uuid.New(), uuid.New()
	store := refund.NewMemoryStore()
	store.PutOrder(refund.OrderSnapshot{
		ID: orderID, OwnerID: ownerID, Version: 1, DepartureAt: now.Add(2 * time.Hour),
		PaymentIntentID: uuid.New(), ReservationID: uuid.New(), TrainRunID: uuid.New(), ProviderPaymentID: "pi_1",
		CapturedMinor: 2_000, RefundedMinor: 100, Currency: "TWD", Provider: "stripe", ShardID: "booking_0", PartialRefundSupported: true,
		Tickets: []refund.TicketSnapshot{
			{ID: ticketA, State: refund.TicketActive, FareMinor: 600, Currency: "TWD"},
			{ID: ticketB, State: refund.TicketActive, FareMinor: 900, Currency: "TWD"},
		},
	})
	service, err := refund.NewService(store, refund.Policy{
		CutoffBeforeDeparture: 30 * time.Minute, MaxTickets: 10, Clock: refundClock{now: now},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := refund.Request{TicketIDs: []uuid.UUID{ticketB, ticketA, ticketB}, IdempotencyKey: " refund-request-1 "}

	first, err := service.Request(context.Background(), ownerID, orderID, input)
	if err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	second, err := service.Request(context.Background(), ownerID, orderID, input)
	if err != nil {
		t.Fatalf("replayed Request() error = %v", err)
	}
	if first.Replayed || !second.Replayed || first.ID != second.ID || first.AmountMinor != 1_500 || first.Currency != "TWD" || len(first.TicketIDs) != 2 || first.Provider != "stripe" || first.ShardID != "booking_0" {
		t.Fatalf("server-derived request = first:%+v second:%+v", first, second)
	}

	requestType := reflect.TypeOf(refund.Request{})
	if requestType.NumField() != 2 || requestType.Field(0).Name != "TicketIDs" || requestType.Field(1).Name != "IdempotencyKey" {
		t.Fatalf("client Request fields = %v; amount/currency/provider/shard are forbidden", requestType)
	}
}

func TestServiceEnforcesOwnerCutoffTicketAndMoneyInvariants(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 7, 0, 0, 0, time.UTC)
	ownerID, orderID, ticketID := uuid.New(), uuid.New(), uuid.New()
	base := refund.OrderSnapshot{
		ID: orderID, OwnerID: ownerID, Version: 1, DepartureAt: now.Add(time.Hour),
		PaymentIntentID: uuid.New(), ReservationID: uuid.New(), TrainRunID: uuid.New(), ProviderPaymentID: "pi_2",
		CapturedMinor: 1_000, Currency: "TWD", Provider: "stripe", ShardID: "booking_0", PartialRefundSupported: true,
		Tickets: []refund.TicketSnapshot{{ID: ticketID, State: refund.TicketActive, FareMinor: 600, Currency: "TWD"}},
	}
	tests := []struct {
		name   string
		owner  uuid.UUID
		now    time.Time
		mutate func(*refund.OrderSnapshot)
		want   error
	}{
		{name: "wrong owner", owner: uuid.New(), now: now, want: refund.ErrNotFound},
		{name: "at cutoff", owner: ownerID, now: base.DepartureAt.Add(-30 * time.Minute), want: refund.ErrCutoffPassed},
		{name: "inactive ticket", owner: ownerID, now: now, mutate: func(snapshot *refund.OrderSnapshot) { snapshot.Tickets[0].State = refund.TicketRefunded }, want: refund.ErrTicketUnavailable},
		{name: "mixed currency", owner: ownerID, now: now, mutate: func(snapshot *refund.OrderSnapshot) { snapshot.Tickets[0].Currency = "USD" }, want: refund.ErrCurrencyMismatch},
		{name: "over capture", owner: ownerID, now: now, mutate: func(snapshot *refund.OrderSnapshot) { snapshot.RefundedMinor = 500 }, want: refund.ErrRefundLimit},
		{name: "capability absent", owner: ownerID, now: now, mutate: func(snapshot *refund.OrderSnapshot) { snapshot.PartialRefundSupported = false }, want: refund.ErrCapabilityUnavailable},
		{name: "overflow", owner: ownerID, now: now, mutate: func(snapshot *refund.OrderSnapshot) {
			snapshot.CapturedMinor = math.MaxInt64
			snapshot.Tickets = []refund.TicketSnapshot{
				{ID: ticketID, State: refund.TicketActive, FareMinor: math.MaxInt64, Currency: "TWD"},
				{ID: uuid.New(), State: refund.TicketActive, FareMinor: 1, Currency: "TWD"},
			}
		}, want: refund.ErrAmountOverflow},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := base
			snapshot.Tickets = append([]refund.TicketSnapshot(nil), base.Tickets...)
			if test.mutate != nil {
				test.mutate(&snapshot)
			}
			store := refund.NewMemoryStore()
			store.PutOrder(snapshot)
			service, err := refund.NewService(store, refund.Policy{CutoffBeforeDeparture: 30 * time.Minute, MaxTickets: 10, Clock: refundClock{now: test.now}})
			if err != nil {
				t.Fatal(err)
			}
			ids := []uuid.UUID{ticketID}
			if test.name == "overflow" {
				ids = []uuid.UUID{snapshot.Tickets[0].ID, snapshot.Tickets[1].ID}
			}
			if _, err := service.Request(context.Background(), test.owner, orderID, refund.Request{TicketIDs: ids, IdempotencyKey: "key"}); !errors.Is(err, test.want) {
				t.Fatalf("Request() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestServiceIdempotencyConflictsOnChangedTicketSetAndProtectsEachTicket(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ownerID, orderID, ticketA, ticketB := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	store := refund.NewMemoryStore()
	store.PutOrder(refund.OrderSnapshot{
		ID: orderID, OwnerID: ownerID, Version: 1, DepartureAt: now.Add(2 * time.Hour), CapturedMinor: 1_000,
		PaymentIntentID: uuid.New(), ReservationID: uuid.New(), TrainRunID: uuid.New(), ProviderPaymentID: "pi_3",
		Currency: "TWD", Provider: "stripe", ShardID: "booking_0", PartialRefundSupported: true,
		Tickets: []refund.TicketSnapshot{
			{ID: ticketA, State: refund.TicketActive, FareMinor: 400, Currency: "TWD"},
			{ID: ticketB, State: refund.TicketActive, FareMinor: 600, Currency: "TWD"},
		},
	})
	service, err := refund.NewService(store, refund.Policy{CutoffBeforeDeparture: time.Minute, MaxTickets: 10, Clock: refundClock{now: now}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Request(context.Background(), ownerID, orderID, refund.Request{TicketIDs: []uuid.UUID{ticketA}, IdempotencyKey: "same-key"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Request(context.Background(), ownerID, orderID, refund.Request{TicketIDs: []uuid.UUID{ticketB}, IdempotencyKey: "same-key"}); !errors.Is(err, refund.ErrIdempotencyConflict) {
		t.Fatalf("changed selection error = %v, want ErrIdempotencyConflict", err)
	}
	if _, err := service.Request(context.Background(), ownerID, orderID, refund.Request{TicketIDs: []uuid.UUID{ticketA}, IdempotencyKey: "different-key"}); !errors.Is(err, refund.ErrTicketUnavailable) {
		t.Fatalf("second logical refund error = %v, want ErrTicketUnavailable", err)
	}
}

func TestServiceConcurrentExactReplayConvergesOnOneRequest(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ownerID, orderID, ticketID := uuid.New(), uuid.New(), uuid.New()
	store := refund.NewMemoryStore()
	store.PutOrder(refund.OrderSnapshot{
		ID: orderID, OwnerID: ownerID, Version: 1, DepartureAt: now.Add(time.Hour), CapturedMinor: 500,
		PaymentIntentID: uuid.New(), ReservationID: uuid.New(), TrainRunID: uuid.New(), ProviderPaymentID: "pi_4",
		Currency: "TWD", Provider: "stripe", ShardID: "booking_0", PartialRefundSupported: true,
		Tickets: []refund.TicketSnapshot{{ID: ticketID, State: refund.TicketActive, FareMinor: 500, Currency: "TWD"}},
	})
	service, err := refund.NewService(store, refund.Policy{CutoffBeforeDeparture: time.Minute, MaxTickets: 10, Clock: refundClock{now: now}})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 100
	type result struct {
		id       uuid.UUID
		replayed bool
	}
	results := make(chan result, callers)
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			created, err := service.Request(context.Background(), ownerID, orderID, refund.Request{TicketIDs: []uuid.UUID{ticketID}, IdempotencyKey: "concurrent-key"})
			if err != nil {
				errs <- err
				return
			}
			results <- result{id: created.ID, replayed: created.Replayed}
		}()
	}
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Request() error = %v", err)
	}
	var expected uuid.UUID
	createdCount := 0
	for value := range results {
		id := value.id
		if !value.replayed {
			createdCount++
		}
		if expected == uuid.Nil {
			expected = id
		}
		if id != expected {
			t.Fatalf("concurrent IDs differ: %s and %s", expected, id)
		}
	}
	if createdCount != 1 {
		t.Fatalf("non-replayed results = %d, want exactly 1", createdCount)
	}
}

func TestServicePersistsServerOwnedRuntimeIdentityAndOwnerScopedGet(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC)
	ownerID, orderID, ticketID := uuid.New(), uuid.New(), uuid.New()
	paymentIntentID, reservationID, trainRunID := uuid.New(), uuid.New(), uuid.New()
	store := refund.NewMemoryStore()
	store.PutOrder(refund.OrderSnapshot{
		ID: orderID, OwnerID: ownerID, Version: 7, DepartureAt: now.Add(3 * time.Hour),
		PaymentIntentID: paymentIntentID, ReservationID: reservationID, TrainRunID: trainRunID,
		ProviderPaymentID: "pi_runtime_1", CapturedMinor: 700, RefundedMinor: 0,
		Currency: "TWD", Provider: "stripe", ShardID: "physical-shard-0",
		PartialRefundSupported: true,
		Tickets:                []refund.TicketSnapshot{{ID: ticketID, State: refund.TicketActive, FareMinor: 700, Currency: "TWD"}},
	})
	service, err := refund.NewService(store, refund.Policy{CutoffBeforeDeparture: time.Hour, MaxTickets: 10, Clock: refundClock{now: now}})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Request(context.Background(), ownerID, orderID, refund.Request{
		TicketIDs: []uuid.UUID{ticketID}, IdempotencyKey: "runtime-identity",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.PaymentIntentID != paymentIntentID || created.ReservationID != reservationID ||
		created.TrainRunID != trainRunID || created.ProviderPaymentID != "pi_runtime_1" ||
		created.AssignmentGeneration != 7 || created.CapturedMinor != 700 || created.RefundedBeforeMinor != 0 {
		t.Fatalf("runtime identity = %+v", created)
	}
	if len(created.Items) != 1 || created.Items[0].TicketID != ticketID || created.Items[0].FareMinor != 700 {
		t.Fatalf("server-derived items = %+v", created.Items)
	}
	got, err := service.Get(context.Background(), ownerID, created.ID)
	if err != nil || got.ID != created.ID {
		t.Fatalf("Get() = %+v, %v", got, err)
	}
	if _, err := service.Get(context.Background(), uuid.New(), created.ID); !errors.Is(err, refund.ErrNotFound) {
		t.Fatalf("cross-owner Get() error = %v", err)
	}
}
