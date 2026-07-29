package physical_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestOperatorFareMutationCommitsSnapshotReceiptAndOutboxOnExactShard(t *testing.T) {
	t.Parallel()
	sourceFareID, snapshotFareID, commandID := uuid.New(), uuid.New(), uuid.New()
	_, resolution := commandFixture(t)
	trainRunID := resolution.Route.TrainRunID()
	generation := resolution.Route.Generation().Int64()
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows},
		{values: []any{generation, true, "active", "scheduled", 3, int64(7), int64(1)}},
		{err: pgx.ErrNoRows},
		{values: []any{int64(4), int64(1000), "TWD", true, 0, 2, "standard"}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	result, err := executor.InstallFare(context.Background(), commandphysical.FareInstallCommand{
		CommandID: commandID, TrainRunID: trainRunID,
		SourceFareID: sourceFareID, SnapshotFareID: snapshotFareID,
		ExpectedSourceVersion: 4, FromStopIndex: 0, ToStopIndex: 2,
		SeatClass: "standard", AmountMinor: 1200, Currency: "TWD", RequestFingerprint: [32]byte{1},
	})
	if err != nil || result.ControlResourceID != sourceFareID || result.ShardResourceID != snapshotFareID ||
		result.AssignmentGeneration != generation || result.SourceVersion != 5 || result.Replayed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	joined := strings.Join(tx.execs, "\n")
	for _, fragment := range []string{"booking_command_receipts", "UPDATE booking_fare_snapshots", "UPDATE train_run_booking_snapshots", "outbox_events", "train_run_target_write_evidence"} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("missing %q mutation: %s", fragment, joined)
		}
	}
	if tx.commits != 1 {
		t.Fatalf("commits=%d", tx.commits)
	}
}

func TestOperatorSeatMutationFailsClosedWhenMigrationFenceIsNotActive(t *testing.T) {
	t.Parallel()
	_, resolution := commandFixture(t)
	trainRunID := resolution.Route.TrainRunID()
	generation := resolution.Route.Generation().Int64()
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows},
		{values: []any{generation, false, "draining", "scheduled", 3, int64(7), int64(1)}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	_, err := executor.SetSeatActive(context.Background(), commandphysical.SeatActiveCommand{
		CommandID: uuid.New(), TrainRunID: trainRunID, SeatID: uuid.New(), Active: false,
		ExpectedSourceVersion: 2, RequestFingerprint: [32]byte{2},
	})
	if !errors.Is(err, sharding.ErrWriteFenced) || len(tx.execs) != 0 || tx.commits != 0 {
		t.Fatalf("err=%v execs=%d commits=%d", err, len(tx.execs), tx.commits)
	}
}

func TestOperatorFareDuplicateCommandReturnsReceiptWithoutDuplicateOutbox(t *testing.T) {
	t.Parallel()
	_, resolution := commandFixture(t)
	trainRunID, sourceFareID, snapshotFareID, commandID := resolution.Route.TrainRunID(), uuid.New(), uuid.New(), uuid.New()
	fingerprint := [32]byte{9}
	tx := &scriptedTx{rows: []scriptedRow{
		{values: []any{"fare.install", fingerprint[:], sourceFareID, "succeeded", trainRunID,
			resolution.Route.Generation().Int64(), int64(5), pgtype.Int8{}}},
		{values: []any{int64(5), int64(1200), "TWD", true, 0, 2, "standard"}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	result, err := executor.InstallFare(context.Background(), commandphysical.FareInstallCommand{
		CommandID: commandID, TrainRunID: trainRunID,
		SourceFareID: sourceFareID, SnapshotFareID: snapshotFareID, ExpectedSourceVersion: 4,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", AmountMinor: 1200,
		Currency: "TWD", RequestFingerprint: fingerprint,
	})
	if err != nil || !result.Replayed || result.SourceVersion != 5 || len(tx.execs) != 0 || tx.commits != 1 {
		t.Fatalf("result=%+v err=%v execs=%d commits=%d", result, err, len(tx.execs), tx.commits)
	}
}

func TestOperatorFareRejectsMismatchedImmutableRangeAndClass(t *testing.T) {
	t.Parallel()
	_, resolution := commandFixture(t)
	trainRunID := resolution.Route.TrainRunID()
	generation := resolution.Route.Generation().Int64()
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows},
		{values: []any{generation, true, "active", "scheduled", 3, int64(7), int64(1)}},
		{err: pgx.ErrNoRows},
		{values: []any{int64(4), int64(1000), "TWD", true, 0, 2, "standard"}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	_, err := executor.InstallFare(context.Background(), commandphysical.FareInstallCommand{
		CommandID: uuid.New(), TrainRunID: trainRunID,
		SourceFareID: uuid.New(), SnapshotFareID: uuid.New(), ExpectedSourceVersion: 4,
		FromStopIndex: 1, ToStopIndex: 3, SeatClass: "business", AmountMinor: 1200,
		Currency: "TWD", RequestFingerprint: [32]byte{7},
	})
	if !errors.Is(err, commandphysical.ErrInvalidPayload) || len(tx.execs) != 0 || tx.commits != 0 {
		t.Fatalf("err=%v execs=%d commits=%d", err, len(tx.execs), tx.commits)
	}
}

func TestOperatorSnapshotEventIdentitySeparatesTrainRunsSharingASeat(t *testing.T) {
	t.Parallel()
	seatID := uuid.New()
	first := commandphysical.OperatorSnapshotEventID(uuid.New(), 7, seatID, "booking_snapshot.seat_disabled", 5)
	second := commandphysical.OperatorSnapshotEventID(uuid.New(), 7, seatID, "booking_snapshot.seat_disabled", 5)
	if first == uuid.Nil || second == uuid.Nil || first == second {
		t.Fatalf("event ids must be non-zero and train-run scoped: %s %s", first, second)
	}
}

func TestOperatorFareReceiptReplayReturnsHistoricalVersionAfterLaterMutation(t *testing.T) {
	t.Parallel()
	_, resolution := commandFixture(t)
	trainRunID, sourceFareID, snapshotFareID, commandID := resolution.Route.TrainRunID(), uuid.New(), uuid.New(), uuid.New()
	generation := resolution.Route.Generation().Int64()
	fingerprint := [32]byte{9}
	tx := &scriptedTx{rows: []scriptedRow{
		{values: []any{"fare.install", fingerprint[:], sourceFareID, "succeeded", trainRunID,
			generation, int64(5), pgtype.Int8{}}},
		// A later command changed the mutable fare row to version 6. Replay must
		// return command A's receipt version 5 rather than reconstructing state.
		{values: []any{int64(6), int64(1500), "TWD", true, 0, 2, "standard"}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	result, err := executor.InstallFare(context.Background(), commandphysical.FareInstallCommand{
		CommandID: commandID, TrainRunID: trainRunID,
		SourceFareID: sourceFareID, SnapshotFareID: snapshotFareID, ExpectedSourceVersion: 4,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", AmountMinor: 1200,
		Currency: "TWD", RequestFingerprint: fingerprint,
	})
	if err != nil || !result.Replayed || result.SourceVersion != 5 || result.AssignmentGeneration != generation ||
		result.ControlResourceID != sourceFareID || result.ShardResourceID != snapshotFareID || tx.commits != 1 || tx.rowIndex != 1 {
		t.Fatalf("result=%+v err=%v commits=%d rows=%d", result, err, tx.commits, tx.rowIndex)
	}
}

func TestOperatorPolicyReceiptPersistsAndReturnsBothVersions(t *testing.T) {
	t.Parallel()
	_, resolution := commandFixture(t)
	trainRunID := resolution.Route.TrainRunID()
	generation := resolution.Route.Generation().Int64()
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows},
		{values: []any{generation, true, "active", "scheduled", 3, int64(7), int64(11)}},
		{err: pgx.ErrNoRows},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	result, err := executor.BumpBookingPolicy(context.Background(), commandphysical.BookingPolicyBumpCommand{
		CommandID: uuid.New(), TrainRunID: trainRunID, ExpectedSourceVersion: 7,
		ExpectedBookingPolicyVersion: 11, RequestFingerprint: [32]byte{3},
	})
	if err != nil || result.SourceVersion != 8 || result.BookingPolicyVersion != 12 ||
		result.AssignmentGeneration != generation || result.Replayed {
		t.Fatalf("policy result=%+v err=%v", result, err)
	}
	joined := strings.Join(tx.execs, "\n")
	if !strings.Contains(joined, "result_booking_policy_version=NULLIF($5,0)") {
		t.Fatalf("policy receipt omitted booking-policy version: %s", joined)
	}
}

func TestOperatorPolicyReplayRejectsUnexpectedReceiptVersions(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name          string
		sourceVersion int64
		policyVersion int64
	}{
		{name: "source", sourceVersion: 9, policyVersion: 12},
		{name: "policy", sourceVersion: 8, policyVersion: 13},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, resolution := commandFixture(t)
			trainRunID, commandID := resolution.Route.TrainRunID(), uuid.New()
			fingerprint := [32]byte{4}
			tx := &scriptedTx{rows: []scriptedRow{{values: []any{
				"booking_policy.bump", fingerprint[:], trainRunID, "succeeded",
				trainRunID, resolution.Route.Generation().Int64(), testCase.sourceVersion,
				pgtype.Int8{Int64: testCase.policyVersion, Valid: true},
			}}}}
			resolution.Handle = handleForTx(t, tx, true)
			executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
			_, err := executor.BumpBookingPolicy(context.Background(), commandphysical.BookingPolicyBumpCommand{
				CommandID: commandID, TrainRunID: trainRunID, ExpectedSourceVersion: 7,
				ExpectedBookingPolicyVersion: 11, RequestFingerprint: fingerprint,
			})
			if !errors.Is(err, commandphysical.ErrShardPersistence) || tx.commits != 0 {
				t.Fatalf("err=%v commits=%d", err, tx.commits)
			}
		})
	}
}
