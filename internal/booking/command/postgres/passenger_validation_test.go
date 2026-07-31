package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestReserveRejectsPassengerNotOwnedByActiveCommandOwnerBeforeRouting(t *testing.T) {
	t.Parallel()

	tx := &passengerTx{rows: []passengerRow{{values: []any{true}}, {err: pgx.ErrNoRows}, {values: []any{0}}}}
	repository, err := postgres.NewRepository(&passengerDB{tx: tx}, postgres.Options{
		LeaseTTL: 10 * time.Minute, MaxActiveHoldsPerUser: 4,
		MaxActiveHoldsPerTrainRun: 2, MaxActivePassengersPerUser: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := command.ReserveRequest{
		OwnerUserID: uuid.New(), TrainRunID: uuid.New(), Operation: command.OperationCreateReservation,
		IdempotencyKeyHash: [32]byte{1}, RequestFingerprint: [32]byte{2}, PassengerCount: 1,
		Payload: command.CreateReservationPayload{
			FromStopIndex: 0, ToStopIndex: 1, SeatClass: "standard", PassengerIDs: []uuid.UUID{uuid.New()},
			HoldExpiresAt: time.Now().UTC().Add(5 * time.Minute), ExpectedSnapshotVersion: 1,
		},
	}

	_, err = repository.Reserve(context.Background(), request)
	if !errors.Is(err, postgres.ErrPassengerOwnership) {
		t.Fatalf("Reserve() error = %v, want %v", err, postgres.ErrPassengerOwnership)
	}
	if len(tx.queries) != 3 || !strings.Contains(tx.queries[2], "public.passengers") ||
		!strings.Contains(tx.queries[2], "FOR UPDATE") || len(tx.execs) != 1 ||
		!strings.Contains(tx.execs[0], "pg_advisory_xact_lock") {
		t.Fatalf("queries = %v, execs = %v", tx.queries, tx.execs)
	}
}

func TestReserveCountsLegacyAndPhysicalQuotaClaimsTogether(t *testing.T) {
	t.Parallel()

	tx := &passengerTx{rows: []passengerRow{
		{values: []any{true}},
		{err: pgx.ErrNoRows},
		{values: []any{1}},
		{values: []any{"physical-shard-0", int64(1)}},
		{values: []any{10, 2, 10}},
	}}
	repository, err := postgres.NewRepository(&passengerDB{tx: tx}, postgres.Options{
		LeaseTTL: 10 * time.Minute, MaxActiveHoldsPerUser: 10,
		MaxActiveHoldsPerTrainRun: 3, MaxActivePassengersPerUser: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := command.ReserveRequest{
		OwnerUserID: uuid.New(), TrainRunID: uuid.New(), Operation: command.OperationCreateReservation,
		IdempotencyKeyHash: [32]byte{1}, RequestFingerprint: [32]byte{2}, PassengerCount: 1,
		Payload: command.CreateReservationPayload{
			FromStopIndex: 0, ToStopIndex: 1, SeatClass: "standard", PassengerIDs: []uuid.UUID{uuid.New()},
			HoldExpiresAt: time.Now().UTC().Add(5 * time.Minute), ExpectedSnapshotVersion: 1,
		},
	}

	_, err = repository.Reserve(context.Background(), request)
	if !errors.Is(err, postgres.ErrQuotaExceeded) {
		t.Fatalf("Reserve() error = %v, want %v", err, postgres.ErrQuotaExceeded)
	}
	if len(tx.queries) != 5 ||
		!strings.Contains(tx.queries[3], "'migrating'") ||
		!strings.Contains(tx.queries[4], "public.reservation_quota_claims") ||
		!strings.Contains(tx.queries[4], "public.booking_quota_leases") ||
		len(tx.execs) != 1 || !strings.Contains(tx.execs[0], "pg_advisory_xact_lock") {
		t.Fatalf("quota query did not combine legacy and physical claims: %v", tx.queries)
	}
}

type passengerDB struct{ tx pgx.Tx }

func (db *passengerDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return db.tx, nil }

type passengerTx struct {
	pgx.Tx
	rows      []passengerRow
	rowIndex  int
	queries   []string
	execs     []string
	rollbacks int
}

func (tx *passengerTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.queries = append(tx.queries, query)
	row := tx.rows[tx.rowIndex]
	tx.rowIndex++
	return row
}
func (tx *passengerTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, query)
	return pgconn.NewCommandTag("SELECT 1"), nil
}
func (tx *passengerTx) Rollback(context.Context) error { tx.rollbacks++; return nil }

type passengerRow struct {
	values []any
	err    error
}

func (row passengerRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range dest {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
