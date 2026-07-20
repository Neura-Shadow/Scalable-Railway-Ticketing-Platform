package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestReservationQuotaRejectsWithoutMutationAndCompletedReplayStillSucceeds(t *testing.T) {
	_, fixture := newIntegrationFixture(t, 4)
	store := NewWithReservationQuotaLimits(fixture.pool, ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            1,
		MaxActiveHoldsPerUserPerTrainRun: 1,
		MaxActivePassengersPerUser:       4,
	})
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	firstParams := quotaHoldParams(fixture, 0, 0x21, 0x31)
	first, err := store.CreateHold(ctx, firstParams)
	if err != nil {
		t.Fatalf("first CreateHold() error = %v", err)
	}
	before := quotaMutationSnapshot(t, fixture)

	_, err = store.CreateHold(ctx, quotaHoldParams(fixture, 1, 0x22, 0x32))
	if !errors.Is(err, ErrReservationQuotaExceeded) {
		t.Fatalf("over-limit CreateHold() error = %v, want %v", err, ErrReservationQuotaExceeded)
	}
	after := quotaMutationSnapshot(t, fixture)
	if after != before {
		t.Fatalf("quota rejection mutated durable booking state: before=%+v after=%+v", before, after)
	}

	replay, err := store.CreateHold(ctx, firstParams)
	if err != nil {
		t.Fatalf("completed replay error = %v", err)
	}
	if !replay.Replayed || replay.ReservationID != first.ReservationID {
		t.Fatalf("completed replay = %+v, want reservation %s", replay, first.ReservationID)
	}
}

func TestConfirmationReleasesActiveHoldQuota(t *testing.T) {
	_, fixture := newIntegrationFixture(t, 4)
	store := NewWithReservationQuotaLimits(fixture.pool, ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            1,
		MaxActiveHoldsPerUserPerTrainRun: 1,
		MaxActivePassengersPerUser:       1,
	})
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	first, err := store.CreateHold(ctx, quotaHoldParams(fixture, 0, 0x41, 0x51))
	if err != nil {
		t.Fatalf("create first hold: %v", err)
	}
	_, err = store.ConfirmReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: first.ReservationID, Now: time.Now().UTC(),
		IdempotencyKeyHash: hashWithByte(0x42), RequestFingerprint: hashWithByte(0x52),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("confirm first hold: %v", err)
	}
	if _, err := store.CreateHold(ctx, quotaHoldParams(fixture, 1, 0x43, 0x53)); err != nil {
		t.Fatalf("CreateHold() after confirmation error = %v", err)
	}
}

func TestActivePassengerQuotaCountsReservationSeats(t *testing.T) {
	_, fixture := newIntegrationFixture(t, 5)
	store := NewWithReservationQuotaLimits(fixture.pool, ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            10,
		MaxActiveHoldsPerUserPerTrainRun: 10,
		MaxActivePassengersPerUser:       2,
	})
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	params := quotaHoldParams(fixture, 0, 0x61, 0x71)
	params.PassengerIDs = fixture.passengerIDs[:2]
	if _, err := store.CreateHold(ctx, params); err != nil {
		t.Fatalf("two-passenger hold: %v", err)
	}
	_, err := store.CreateHold(ctx, quotaHoldParams(fixture, 2, 0x62, 0x72))
	if !errors.Is(err, ErrReservationQuotaExceeded) {
		t.Fatalf("passenger quota error = %v, want %v", err, ErrReservationQuotaExceeded)
	}
}

func TestConcurrentDisjointRequestsCannotExceedReservationQuota(t *testing.T) {
	_, fixture := newIntegrationFixture(t, 100)
	const limit = 3
	store := NewWithReservationQuotaLimits(fixture.pool, ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            limit,
		MaxActiveHoldsPerUserPerTrainRun: limit,
		MaxActivePassengersPerUser:       limit,
	})
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	const attempts = 100
	start := make(chan struct{})
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			_, err := store.CreateHold(ctx, quotaHoldParams(fixture, index, byte(index+1), byte(200-index)))
			results <- err
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	successes, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrReservationQuotaExceeded):
			rejected++
		default:
			t.Fatalf("concurrent quota attempt error = %v", err)
		}
	}
	if successes != limit || rejected != attempts-limit {
		t.Fatalf("quota race results: successes=%d rejected=%d", successes, rejected)
	}
	var held int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM reservations WHERE user_id = $1 AND status = 'held'`, fixture.userID).Scan(&held); err != nil {
		t.Fatalf("count held reservations: %v", err)
	}
	if held != limit {
		t.Fatalf("held reservations = %d, want %d", held, limit)
	}
}

func TestLookupCompletedCreateHoldIsReadOnlyAndFingerprintBound(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	params := quotaHoldParams(fixture, 0, 0x81, 0x82)
	created, err := store.CreateHold(ctx, params)
	if err != nil {
		t.Fatalf("CreateHold() error = %v", err)
	}

	result, found, err := store.LookupCompletedCreateHold(ctx, CompletedCreateHoldLookupParams{
		UserID: fixture.userID, IdempotencyKeyHash: params.IdempotencyKeyHash,
		RequestFingerprint: params.RequestFingerprint,
	})
	if err != nil || !found {
		t.Fatalf("LookupCompletedCreateHold() found=%v error=%v", found, err)
	}
	if !result.Replayed || result.ReservationID != created.ReservationID {
		t.Fatalf("lookup result = %+v, want replay of %s", result, created.ReservationID)
	}
	_, found, err = store.LookupCompletedCreateHold(ctx, CompletedCreateHoldLookupParams{
		UserID: fixture.userID, IdempotencyKeyHash: params.IdempotencyKeyHash,
		RequestFingerprint: hashWithByte(0x83),
	})
	if found || !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed fingerprint lookup found=%v error=%v", found, err)
	}
	_, found, err = store.LookupCompletedCreateHold(ctx, CompletedCreateHoldLookupParams{
		UserID: fixture.userID, IdempotencyKeyHash: hashWithByte(0x84),
		RequestFingerprint: hashWithByte(0x85),
	})
	if err != nil || found {
		t.Fatalf("missing lookup found=%v error=%v", found, err)
	}
}

func quotaHoldParams(fixture integrationFixture, passengerIndex int, keyByte, fingerprintByte byte) CreateHoldParams {
	now := time.Now().UTC()
	return CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs:       []uuid.UUID{fixture.passengerIDs[passengerIndex]},
		HoldExpiresAt:      now.Add(10 * time.Minute),
		IdempotencyKeyHash: hashWithByte(keyByte), RequestFingerprint: hashWithByte(fingerprintByte),
		IdempotencyExpiresAt: now.Add(24 * time.Hour),
	}
}

type quotaSnapshot struct {
	reservations, seats, createIdempotency, completedIdempotency, outbox int
	occupiedMasks                                                        string
}

func quotaMutationSnapshot(t *testing.T, fixture integrationFixture) quotaSnapshot {
	t.Helper()
	var result quotaSnapshot
	err := fixture.pool.QueryRow(context.Background(), `
SELECT
    (SELECT count(*) FROM reservations WHERE user_id = $1),
    (SELECT count(*) FROM reservation_seats AS rs JOIN reservations AS r ON r.id = rs.reservation_id WHERE r.user_id = $1),
    (SELECT count(*) FROM idempotency_records WHERE user_id = $1 AND operation = 'reservation.create'),
    (SELECT count(*) FROM idempotency_records WHERE user_id = $1 AND operation = 'reservation.create' AND status = 'completed'),
    (SELECT count(*) FROM outbox_events WHERE aggregate_type = 'reservation'),
    (SELECT string_agg(occupied_segments::text, ',' ORDER BY seat_id) FROM seat_inventory WHERE train_run_id = $2)`,
		fixture.userID, fixture.trainRunID,
	).Scan(
		&result.reservations,
		&result.seats,
		&result.createIdempotency,
		&result.completedIdempotency,
		&result.outbox,
		&result.occupiedMasks,
	)
	if err != nil {
		t.Fatalf("snapshot durable booking state: %v", err)
	}
	return result
}
