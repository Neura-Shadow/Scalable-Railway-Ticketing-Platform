package postgres

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentOverlappingHoldsAllocateOneSeatOnce(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
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
			_, err := store.CreateHold(ctx, CreateHoldParams{
				UserID: fixture.userID, TrainRunID: fixture.trainRunID,
				FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
				PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
				IdempotencyKeyHash: hashWithByte(byte(index + 1)), RequestFingerprint: hashWithByte(0x91),
				IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
			})
			results <- err
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	successes := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrInsufficientInventory):
			conflicts++
		default:
			t.Fatalf("concurrent hold error = %v", err)
		}
	}
	if successes != 1 || conflicts != attempts-1 {
		t.Fatalf("concurrent results: successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestConcurrentHoldsNeverExceedSeatCapacity(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 10)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	const attempts = 50
	start := make(chan struct{})
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			_, err := store.CreateHold(ctx, CreateHoldParams{
				UserID: fixture.userID, TrainRunID: fixture.trainRunID,
				FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
				PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
				IdempotencyKeyHash: hashWithByte(byte(index + 101)), RequestFingerprint: hashWithByte(0x92),
				IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
			})
			results <- err
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	successes := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, ErrInsufficientInventory) {
			t.Fatalf("concurrent capacity error = %v", err)
		}
	}
	if successes != 10 {
		t.Fatalf("successful holds = %d, want exact seat capacity 10", successes)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile capacity race: %v", err)
	}
}

func TestConcurrentSameIdempotencyKeyReturnsOneReservation(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	params := CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash: hashWithByte(0xa1), RequestFingerprint: hashWithByte(0xa2),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	}
	const attempts = 12
	start := make(chan struct{})
	results := make(chan CreateHoldResult, attempts)
	errorsFound := make(chan error, attempts)
	var wait sync.WaitGroup
	for range attempts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := store.CreateHold(ctx, params)
			if err != nil {
				errorsFound <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("same-key hold failed: %v", err)
	}

	var reservationID uuid.UUID
	created := 0
	count := 0
	for result := range results {
		count++
		if reservationID == uuid.Nil {
			reservationID = result.ReservationID
		}
		if result.ReservationID != reservationID {
			t.Fatalf("same key returned reservation %s and %s", reservationID, result.ReservationID)
		}
		if !result.Replayed {
			created++
		}
	}
	if count != attempts || created != 1 {
		t.Fatalf("same-key results: count=%d newly-created=%d", count, created)
	}
}

func TestOneSeatSupportsNonOverlappingIntervalsButRejectsOverlap(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO fares (train_run_id, from_stop_index, to_stop_index, seat_class, amount_minor, currency)
VALUES ($1, 2, 3, 'standard', 500, 'TWD'), ($1, 1, 3, 'standard', 900, 'TWD')`, fixture.trainRunID); err != nil {
		t.Fatalf("seed interval fares: %v", err)
	}

	first, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[:1],
		HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute), IdempotencyKeyHash: hashWithByte(0xb1),
		RequestFingerprint: hashWithByte(0xb2), IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create A-C hold: %v", err)
	}
	if first.ReservationID == uuid.Nil {
		t.Fatal("A-C hold returned nil reservation")
	}
	_, err = store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 2, ToStopIndex: 3, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[:1],
		HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute), IdempotencyKeyHash: hashWithByte(0xb3),
		RequestFingerprint: hashWithByte(0xb4), IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create C-D non-overlapping hold: %v", err)
	}
	_, err = store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 1, ToStopIndex: 3, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[:1],
		HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute), IdempotencyKeyHash: hashWithByte(0xb5),
		RequestFingerprint: hashWithByte(0xb6), IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if !errors.Is(err, ErrInsufficientInventory) {
		t.Fatalf("overlapping B-D hold error = %v, want %v", err, ErrInsufficientInventory)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile non-overlapping holds: %v", err)
	}
}

func TestIdempotencyAcquisitionReplaysCompletedResourceAndRejectsChangedRequest(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	keyHash := bytes.Repeat([]byte{0x11}, 32)
	fingerprint := bytes.Repeat([]byte{0x22}, 32)
	resourceID := uuid.New()

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin owner transaction: %v", err)
	}
	acquisition, err := tx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID:             fixture.userID,
		Operation:          OperationReservationCreate,
		KeyHash:            keyHash,
		RequestFingerprint: fingerprint,
		ExpiresAt:          time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	if !acquisition.Owned || acquisition.Replayed {
		t.Fatalf("owner acquisition = %#v", acquisition)
	}
	if err := tx.CompleteIdempotency(ctx, acquisition.RecordID, resourceID); err != nil {
		t.Fatalf("complete idempotency: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit owner: %v", err)
	}

	replayTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin replay transaction: %v", err)
	}
	defer func() { _ = replayTx.Rollback(context.Background()) }()
	replay, err := replayTx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID:             fixture.userID,
		Operation:          OperationReservationCreate,
		KeyHash:            keyHash,
		RequestFingerprint: fingerprint,
		ExpiresAt:          time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("acquire replay: %v", err)
	}
	if !replay.Replayed || replay.Owned || replay.ResourceID != resourceID {
		t.Fatalf("replay acquisition = %#v, want resource %s", replay, resourceID)
	}
	_ = replayTx.Rollback(ctx)

	conflictTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin conflict transaction: %v", err)
	}
	defer func() { _ = conflictTx.Rollback(context.Background()) }()
	_, err = conflictTx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID:             fixture.userID,
		Operation:          OperationReservationCreate,
		KeyHash:            keyHash,
		RequestFingerprint: bytes.Repeat([]byte{0x33}, 32),
		ExpiresAt:          time.Now().UTC().Add(24 * time.Hour),
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed fingerprint error = %v, want %v", err, ErrIdempotencyConflict)
	}
}

func TestCreateHoldCommitsAllocationIdempotencyAndOutboxTogether(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	params := CreateHoldParams{
		UserID:               fixture.userID,
		TrainRunID:           fixture.trainRunID,
		FromStopIndex:        0,
		ToStopIndex:          2,
		SeatClass:            "STANDARD",
		PassengerIDs:         fixture.passengerIDs[:2],
		HoldExpiresAt:        time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash:   bytes.Repeat([]byte{0x41}, 32),
		RequestFingerprint:   bytes.Repeat([]byte{0x42}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	}
	created, err := store.CreateHold(ctx, params)
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	if created.Replayed || created.ReservationID == uuid.Nil || created.SeatCount != 2 {
		t.Fatalf("created hold = %#v", created)
	}
	if created.TotalAmountMinor != 2*fixture.standardFare || created.Currency != "TWD" {
		t.Fatalf("created total = %d %s, want 2500 TWD", created.TotalAmountMinor, created.Currency)
	}

	replayed, err := store.CreateHold(ctx, params)
	if err != nil {
		t.Fatalf("replay hold: %v", err)
	}
	if !replayed.Replayed || replayed.ReservationID != created.ReservationID {
		t.Fatalf("replayed hold = %#v, want reservation %s", replayed, created.ReservationID)
	}

	record, err := store.GetReservation(ctx, fixture.userID, created.ReservationID)
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}
	if record.Status != "held" || record.SeatCount != 2 || record.OutboxEventCount != 1 {
		t.Fatalf("reservation record = %#v", record)
	}
}

func TestConfirmThenCancelCreatesTicketsOnceAndReleasesExactMasks(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x51}, 32), RequestFingerprint: bytes.Repeat([]byte{0x52}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}

	confirmed, err := store.ConfirmReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x61}, 32), RequestFingerprint: bytes.Repeat([]byte{0x62}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("confirm reservation: %v", err)
	}
	if confirmed.TicketOrderID == uuid.Nil || confirmed.TicketCount != 1 || confirmed.Replayed {
		t.Fatalf("confirmed result = %#v", confirmed)
	}

	cancelled, err := store.CancelReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x71}, 32), RequestFingerprint: bytes.Repeat([]byte{0x72}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("cancel reservation: %v", err)
	}
	if cancelled.ReleasedSeatCount != 1 || cancelled.Replayed {
		t.Fatalf("cancelled result = %#v", cancelled)
	}
	confirmationReplay, err := store.ConfirmReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x61}, 32), RequestFingerprint: bytes.Repeat([]byte{0x62}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("replay confirmation after cancellation: %v", err)
	}
	if !confirmationReplay.Replayed || confirmationReplay.TicketOrderID != confirmed.TicketOrderID {
		t.Fatalf("confirmation replay = %#v, want order %s", confirmationReplay, confirmed.TicketOrderID)
	}
	cancellationReplay, err := store.CancelReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x71}, 32), RequestFingerprint: bytes.Repeat([]byte{0x72}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("replay cancellation: %v", err)
	}
	if !cancellationReplay.Replayed || cancellationReplay.ReleasedSeatCount != cancelled.ReleasedSeatCount {
		t.Fatalf("cancellation replay = %#v, want released count %d", cancellationReplay, cancelled.ReleasedSeatCount)
	}

	record, err := store.GetReservation(ctx, fixture.userID, hold.ReservationID)
	if err != nil {
		t.Fatalf("get cancelled reservation: %v", err)
	}
	if record.Status != "cancelled" || record.ActiveTicketCount != 0 || record.OutboxEventCount != 3 {
		t.Fatalf("cancelled reservation record = %#v", record)
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin inventory check: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	masks, err := tx.InventoryMasks(ctx, fixture.trainRunID)
	if err != nil {
		t.Fatalf("get inventory masks: %v", err)
	}
	if len(masks) != 1 || !masks[0].Mask.IsZero() {
		t.Fatalf("inventory after cancel = %#v, want exact mask released", masks)
	}
}

func TestExpireDueTransitionsOnceAndReleasesInventory(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute)
	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: expiresAt,
		IdempotencyKeyHash: bytes.Repeat([]byte{0x81}, 32), RequestFingerprint: bytes.Repeat([]byte{0x82}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}

	type expiryResult struct {
		ids []uuid.UUID
		err error
	}
	start := make(chan struct{})
	results := make(chan expiryResult, 2)
	for range 2 {
		go func() {
			<-start
			ids, err := store.ExpireDue(ctx, expiresAt.Add(time.Second), 10)
			results <- expiryResult{ids: ids, err: err}
		}()
	}
	close(start)
	var expired []uuid.UUID
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("expire worker: %v", result.err)
		}
		expired = append(expired, result.ids...)
	}
	if len(expired) != 1 || expired[0] != hold.ReservationID {
		t.Fatalf("expired IDs across workers = %v, want %s once", expired, hold.ReservationID)
	}
	expiredAgain, err := store.ExpireDue(ctx, expiresAt.Add(2*time.Second), 10)
	if err != nil {
		t.Fatalf("expire due again: %v", err)
	}
	if len(expiredAgain) != 0 {
		t.Fatalf("second expiry returned %v, want none", expiredAgain)
	}
}

func TestExpireDueIsolatesOneCorruptReservationAndContinues(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute)
	create := func(index int) CreateHoldResult {
		t.Helper()
		result, err := store.CreateHold(ctx, CreateHoldParams{
			UserID: fixture.userID, TrainRunID: fixture.trainRunID,
			FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
			PassengerIDs: fixture.passengerIDs[index : index+1], HoldExpiresAt: expiresAt,
			IdempotencyKeyHash: hashWithByte(byte(0xc1 + index)), RequestFingerprint: hashWithByte(byte(0xd1 + index)),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		if err != nil {
			t.Fatalf("create hold %d: %v", index, err)
		}
		return result
	}
	corrupt := create(0)
	healthy := create(1)
	if _, err := fixture.pool.Exec(ctx, `
UPDATE seat_inventory AS si
SET occupied_segments = repeat('0', bit_length(si.occupied_segments))::bit varying
FROM reservations AS r
JOIN reservation_seats AS rs ON rs.reservation_id = r.id
WHERE r.id = $1
  AND si.train_run_id = r.train_run_id
  AND si.seat_id = rs.seat_id`, corrupt.ReservationID); err != nil {
		t.Fatalf("inject inventory mismatch: %v", err)
	}

	expired, err := store.ExpireDue(ctx, expiresAt.Add(time.Second), 10)
	if !errors.Is(err, ErrPersistenceInvariant) {
		t.Fatalf("expire batch error = %v, want persistence invariant", err)
	}
	if len(expired) != 1 || expired[0] != healthy.ReservationID {
		t.Fatalf("expired IDs = %v, want only healthy %s", expired, healthy.ReservationID)
	}
	corruptRecord, err := store.GetReservation(ctx, fixture.userID, corrupt.ReservationID)
	if err != nil {
		t.Fatalf("get corrupt reservation: %v", err)
	}
	healthyRecord, err := store.GetReservation(ctx, fixture.userID, healthy.ReservationID)
	if err != nil {
		t.Fatalf("get healthy reservation: %v", err)
	}
	if corruptRecord.Status != "held" || healthyRecord.Status != "expired" {
		t.Fatalf("statuses after isolated failure: corrupt=%s healthy=%s", corruptRecord.Status, healthyRecord.Status)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); !errors.Is(err, ErrPersistenceInvariant) {
		t.Fatalf("reconciliation error = %v, want persistence invariant", err)
	}
}

func TestConfirmVersusExpireHasOneValidOutcome(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute)
	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[:1],
		HoldExpiresAt: expiresAt, IdempotencyKeyHash: hashWithByte(0xe1), RequestFingerprint: hashWithByte(0xe2),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}

	start := make(chan struct{})
	confirmResult := make(chan error, 1)
	expireResult := make(chan expiryCallResult, 1)
	go func() {
		<-start
		_, err := store.ConfirmReservation(ctx, ReservationCommandParams{
			UserID: fixture.userID, ReservationID: hold.ReservationID, Now: expiresAt.Add(-time.Second),
			IdempotencyKeyHash: hashWithByte(0xe3), RequestFingerprint: hashWithByte(0xe4),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		confirmResult <- err
	}()
	go func() {
		<-start
		ids, err := store.ExpireDue(ctx, expiresAt.Add(time.Second), 1)
		expireResult <- expiryCallResult{ids: ids, err: err}
	}()
	close(start)
	confirmErr := <-confirmResult
	expiry := <-expireResult
	if expiry.err != nil {
		t.Fatalf("expire race: %v", expiry.err)
	}
	if confirmErr != nil && !errors.Is(confirmErr, ErrInvalidState) {
		t.Fatalf("confirm race error = %v", confirmErr)
	}
	record, err := store.GetReservation(ctx, fixture.userID, hold.ReservationID)
	if err != nil {
		t.Fatalf("get raced reservation: %v", err)
	}
	switch record.Status {
	case "confirmed":
		if confirmErr != nil || len(expiry.ids) != 0 || record.ActiveTicketCount != 1 {
			t.Fatalf("confirmed outcome: confirm=%v expired=%v record=%#v", confirmErr, expiry.ids, record)
		}
	case "expired":
		if !errors.Is(confirmErr, ErrInvalidState) || len(expiry.ids) != 1 || record.ActiveTicketCount != 0 {
			t.Fatalf("expired outcome: confirm=%v expired=%v record=%#v", confirmErr, expiry.ids, record)
		}
	default:
		t.Fatalf("invalid raced status %s", record.Status)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile confirm/expire race: %v", err)
	}
}

func TestCancelVersusConfirmEndsCancelledWithoutInventoryLeak(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[:1],
		HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute), IdempotencyKeyHash: hashWithByte(0xf1),
		RequestFingerprint: hashWithByte(0xf2), IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	start := make(chan struct{})
	confirmResult := make(chan error, 1)
	cancelResult := make(chan error, 1)
	go func() {
		<-start
		_, err := store.ConfirmReservation(ctx, ReservationCommandParams{
			UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
			IdempotencyKeyHash: hashWithByte(0xf3), RequestFingerprint: hashWithByte(0xf4),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		confirmResult <- err
	}()
	go func() {
		<-start
		_, err := store.CancelReservation(ctx, ReservationCommandParams{
			UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
			IdempotencyKeyHash: hashWithByte(0xf5), RequestFingerprint: hashWithByte(0xf6),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		cancelResult <- err
	}()
	close(start)
	confirmErr := <-confirmResult
	cancelErr := <-cancelResult
	if cancelErr != nil {
		t.Fatalf("cancel race: %v", cancelErr)
	}
	if confirmErr != nil && !errors.Is(confirmErr, ErrInvalidState) {
		t.Fatalf("confirm race: %v", confirmErr)
	}
	record, err := store.GetReservation(ctx, fixture.userID, hold.ReservationID)
	if err != nil {
		t.Fatalf("get raced reservation: %v", err)
	}
	if record.Status != "cancelled" || record.ActiveTicketCount != 0 {
		t.Fatalf("cancel/confirm outcome = %#v", record)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile cancel/confirm race: %v", err)
	}
}

func TestTrainRunCancellationBecomesAuthoritativeBeforeNewHold(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin operator transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var currentStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM train_runs WHERE id = $1 FOR UPDATE`, fixture.trainRunID).Scan(&currentStatus); err != nil {
		t.Fatalf("lock train run: %v", err)
	}

	attempting := make(chan struct{})
	holdResult := make(chan error, 1)
	go func() {
		close(attempting)
		_, holdErr := store.CreateHold(ctx, CreateHoldParams{
			UserID: fixture.userID, TrainRunID: fixture.trainRunID,
			FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
			PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
			IdempotencyKeyHash: hashWithByte(0xc7), RequestFingerprint: hashWithByte(0xc8),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		holdResult <- holdErr
	}()
	<-attempting
	if _, err := tx.Exec(ctx, `UPDATE train_runs SET status = 'cancelled' WHERE id = $1`, fixture.trainRunID); err != nil {
		t.Fatalf("cancel train run: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit train-run cancellation: %v", err)
	}
	if err := <-holdResult; !errors.Is(err, ErrNotBookable) {
		t.Fatalf("hold after authoritative cancellation error = %v, want %v", err, ErrNotBookable)
	}
}

type expiryCallResult struct {
	ids []uuid.UUID
	err error
}

func TestPostgreSQLVarbitAllocationPreservesMoreThan64Segments(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := fixture.pool.Exec(ctx, `UPDATE train_runs SET segment_count = 129 WHERE id = $1`, fixture.trainRunID); err != nil {
		t.Fatalf("extend train-run segments: %v", err)
	}
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	requested, err := domain.NewSegmentMask(129, 64, 129)
	if err != nil {
		t.Fatalf("new long segment mask: %v", err)
	}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin allocation: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.AllocateSeat(ctx, fixture.trainRunID, "standard", requested); err != nil {
		t.Fatalf("allocate long segment mask: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit long segment allocation: %v", err)
	}

	checkTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin mask check: %v", err)
	}
	defer func() { _ = checkTx.Rollback(context.Background()) }()
	masks, err := checkTx.InventoryMasks(ctx, fixture.trainRunID)
	if err != nil {
		t.Fatalf("load long inventory mask: %v", err)
	}
	if len(masks) != 1 || !masks[0].Mask.Equal(requested) {
		t.Fatalf("stored long mask = %#v, want %s", masks, requested.String())
	}
}

func TestAllocateSeatsIsDeterministicAndAllOrNothing(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()

	inserted, err := store.InitializeInventory(ctx, fixture.trainRunID)
	if err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	if inserted != 2 {
		t.Fatalf("inserted inventory = %d, want 2", inserted)
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })

	requested, err := domain.NewSegmentMask(3, 0, 2)
	if err != nil {
		t.Fatalf("new mask: %v", err)
	}
	_, err = tx.AllocateSeats(ctx, fixture.trainRunID, "standard", requested, 3)
	if !errors.Is(err, ErrInsufficientInventory) {
		t.Fatalf("allocate three seats error = %v, want %v", err, ErrInsufficientInventory)
	}

	masks, err := tx.InventoryMasks(ctx, fixture.trainRunID)
	if err != nil {
		t.Fatalf("load masks after shortfall: %v", err)
	}
	for _, item := range masks {
		if !item.Mask.IsZero() {
			t.Fatalf("seat %s was partially allocated: %s", item.SeatID, item.Mask.String())
		}
	}

	allocated, err := tx.AllocateSeats(ctx, fixture.trainRunID, "standard", requested, 2)
	if err != nil {
		t.Fatalf("allocate two seats: %v", err)
	}
	if len(allocated) != 2 {
		t.Fatalf("allocated seats = %d, want 2", len(allocated))
	}
	if allocated[0].CoachNumber != "1" || allocated[0].SeatNumber != "01A" ||
		allocated[1].CoachNumber != "1" || allocated[1].SeatNumber != "02A" {
		t.Fatalf("allocation order = %#v, want coach/seat order", allocated)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

type integrationFixture struct {
	pool          *pgxpool.Pool
	userID        uuid.UUID
	passengerIDs  []uuid.UUID
	trainRunID    uuid.UUID
	segmentCount  int
	standardFare  int64
	standardClass string
}

func newIntegrationFixture(t *testing.T, seatCount int) (*Store, integrationFixture) {
	t.Helper()
	pool := newIsolatedDatabase(t)
	fixture := seedFixture(t, pool, seatCount)
	fixture.pool = pool
	return New(pool), fixture
}

func newIsolatedDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}

	random := make([]byte, 12)
	if _, err := rand.Read(random); err != nil {
		admin.Close()
		t.Fatalf("create schema suffix: %v", err)
	}
	schema := "booking_test_" + hex.EncodeToString(random)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatalf("create isolated schema: %v", err)
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatalf("parse database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
		t.Fatalf("connect isolated pool: %v", err)
	}

	applyMigrations(t, ctx, pool)
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	return pool
}

func applyMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("glob migrations: %v", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		t.Fatal("no migrations found")
	}
	for _, path := range paths {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", path, err)
		}
	}
}

func seedFixture(t *testing.T, pool *pgxpool.Pool, seatCount int) integrationFixture {
	t.Helper()
	ctx := context.Background()
	userID := uuid.New()
	stationIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	routeID, trainID, coachID, trainRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New()

	batch := &pgx.Batch{}
	batch.Queue(`INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)`, userID, userID.String()+"@example.test", "$2a$12$abcdefghijklmnopqrstuv012345678901234567890123456789")
	for index, stationID := range stationIDs {
		batch.Queue(`INSERT INTO stations (id, code, name, timezone) VALUES ($1, $2, $3, 'Asia/Taipei')`, stationID, fmt.Sprintf("S%d", index), fmt.Sprintf("Station %d", index))
	}
	batch.Queue(`INSERT INTO routes (id, code, name, operating_timezone) VALUES ($1, 'R1', 'Test Route', 'Asia/Taipei')`, routeID)
	for index, stationID := range stationIDs {
		batch.Queue(`INSERT INTO route_stops (route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes) VALUES ($1, $2, $3, $4, $4)`, routeID, stationID, index, index*10)
	}
	batch.Queue(`INSERT INTO trains (id, code, name) VALUES ($1, 'T1', 'Test Train')`, trainID)
	batch.Queue(`INSERT INTO coaches (id, train_id, coach_number, seat_class) VALUES ($1, $2, '1', 'standard')`, coachID, trainID)
	for index := 0; index < seatCount; index++ {
		batch.Queue(`INSERT INTO seats (id, coach_id, seat_number) VALUES ($1, $2, $3)`, uuid.New(), coachID, fmt.Sprintf("%02dA", index+1))
	}
	batch.Queue(`INSERT INTO train_runs (id, train_id, route_id, service_date, scheduled_departure_at, segment_count) VALUES ($1, $2, $3, CURRENT_DATE + 1, clock_timestamp() + interval '1 day', 3)`, trainRunID, trainID, routeID)
	batch.Queue(`INSERT INTO fares (train_run_id, from_stop_index, to_stop_index, seat_class, amount_minor, currency) VALUES ($1, 0, 2, 'standard', 1250, 'TWD')`, trainRunID)

	passengerIDs := make([]uuid.UUID, seatCount+1)
	for index := range passengerIDs {
		passengerIDs[index] = uuid.New()
		batch.Queue(`INSERT INTO passengers (id, user_id, display_name) VALUES ($1, $2, $3)`, passengerIDs[index], userID, fmt.Sprintf("Passenger %d", index+1))
	}

	results := pool.SendBatch(ctx, batch)
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			t.Fatalf("seed fixture: %v", err)
		}
	}
	if err := results.Close(); err != nil {
		t.Fatalf("close seed batch: %v", err)
	}

	return integrationFixture{
		userID:        userID,
		passengerIDs:  passengerIDs,
		trainRunID:    trainRunID,
		segmentCount:  3,
		standardFare:  1250,
		standardClass: "standard",
	}
}

func hashWithByte(value byte) []byte {
	return bytes.Repeat([]byte{value}, 32)
}
