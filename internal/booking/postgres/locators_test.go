package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type locatorPGXTx struct {
	pgx.Tx
	row        pgx.Row
	queryCalls int
}

func (tx *locatorPGXTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if !strings.Contains(sql, "ON CONFLICT") {
		return pgconn.CommandTag{}, errors.New("unexpected locator insert")
	}
	return pgconn.NewCommandTag("INSERT 0 0"), nil
}

func (tx *locatorPGXTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if !strings.Contains(sql, "FOR UPDATE") {
		return locatorRow{err: errors.New("existing locator was not locked")}
	}
	tx.queryCalls++
	return tx.row
}

type locatorRow struct {
	values []any
	err    error
}

func (row locatorRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return errors.New("unexpected locator scan width")
	}
	for index, value := range row.values {
		destination := reflect.ValueOf(destinations[index])
		if destination.Kind() != reflect.Pointer || destination.IsNil() {
			return errors.New("invalid locator scan destination")
		}
		destination.Elem().Set(reflect.ValueOf(value))
	}
	return nil
}

func locatorTestTx(t *testing.T, row pgx.Row) (*Tx, *locatorPGXTx, sharding.ShardRoute) {
	t.Helper()
	generation, err := sharding.NewAssignmentGeneration(7)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(uuid.New(), sharding.ShardLegacy, generation)
	if err != nil {
		t.Fatal(err)
	}
	underlying := &locatorPGXTx{row: row}
	routed := &fakeBookingRoutedTx{Tx: underlying, route: route}
	return &Tx{tx: underlying, route: route, routed: routed}, underlying, route
}

func TestReservationLocatorAcceptsOnlyExactLegacyTriggerRow(t *testing.T) {
	ownerID := uuid.New()
	tx, underlying, route := locatorTestTx(t, nil)
	underlying.row = locatorRow{values: []any{
		route.TrainRunID(), route.ShardID().String(), route.Generation().Int64(), ownerID,
	}}
	reservationID := uuid.New()
	if err := tx.insertReservationLocator(context.Background(), reservationID, ownerID); err != nil {
		t.Fatalf("insertReservationLocator() error = %v", err)
	}
	if underlying.queryCalls != 1 {
		t.Fatalf("existing locator lock calls = %d, want 1", underlying.queryCalls)
	}

	underlying.row = locatorRow{values: []any{
		route.TrainRunID(), route.ShardID().String(), route.Generation().Int64(), uuid.New(),
	}}
	if err := tx.insertReservationLocator(context.Background(), reservationID, ownerID); !errors.Is(err, ErrPersistenceInvariant) {
		t.Fatalf("mismatched reservation locator error = %v", err)
	}
}

func TestTicketOrderLocatorAcceptsExactLegacyTriggerRow(t *testing.T) {
	orderID, reservationID, ownerID := uuid.New(), uuid.New(), uuid.New()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	tx, underlying, route := locatorTestTx(t, nil)
	underlying.row = locatorRow{values: []any{
		reservationID,
		route.TrainRunID(),
		route.ShardID().String(),
		route.Generation().Int64(),
		ownerID,
		"confirmed",
		int64(1250),
		"TWD",
		createdAt,
	}}
	if err := tx.insertTicketOrderLocator(
		context.Background(), orderID, reservationID, ownerID,
		"confirmed", 1250, "TWD", createdAt,
	); err != nil {
		t.Fatalf("insertTicketOrderLocator() error = %v", err)
	}
	if underlying.queryCalls != 1 {
		t.Fatalf("existing ticket-order locator lock calls = %d, want 1", underlying.queryCalls)
	}
}

func TestTicketLocatorAcceptsExactLegacyTriggerRow(t *testing.T) {
	ticketID, orderID, reservationID, ownerID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	tx, underlying, route := locatorTestTx(t, nil)
	underlying.row = locatorRow{values: []any{
		orderID,
		reservationID,
		route.TrainRunID(),
		route.ShardID().String(),
		route.Generation().Int64(),
		ownerID,
	}}
	if err := tx.insertTicketLocator(
		context.Background(), ticketID, orderID, reservationID, ownerID,
	); err != nil {
		t.Fatalf("insertTicketLocator() error = %v", err)
	}
	if underlying.queryCalls != 1 {
		t.Fatalf("existing ticket locator lock calls = %d, want 1", underlying.queryCalls)
	}
}
