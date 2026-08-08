package postgres

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestLoadPhysicalRouteRequiresCurrentSchemaVersion(t *testing.T) {
	t.Parallel()

	trainRunID := uuid.New()
	tx := &schemaVersionTx{rows: []pgx.Row{schemaVersionRow{values: []any{
		sharding.ShardPhysicalZero.String(), int64(7),
	}}}}
	if _, err := loadPhysicalRoute(context.Background(), tx, trainRunID); err != nil {
		t.Fatal(err)
	}
	assertSchemaVersionArgument(t, tx.queries[0], tx.arguments[0])
}

func TestReserveLifecycleRequiresCurrentSchemaVersion(t *testing.T) {
	t.Parallel()

	trainRunID := uuid.New()
	tx := &schemaVersionTx{rows: []pgx.Row{
		schemaVersionRow{err: pgx.ErrNoRows},
		schemaVersionRow{values: []any{
			trainRunID, sharding.ShardPhysicalZero.String(), int64(9), "active", "postgres",
		}},
	}}
	repository := &Repository{db: schemaVersionDB{tx: tx}}
	_, err := repository.ReserveLifecycle(context.Background(), command.LifecycleRequest{
		OwnerUserID: uuid.New(), ReservationID: uuid.New(), Operation: command.OperationCancelReservation,
		IdempotencyKeyHash: [32]byte{1}, RequestFingerprint: [32]byte{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertSchemaVersionArgument(t, tx.queries[1], tx.arguments[1])
}

func assertSchemaVersionArgument(t *testing.T, query string, arguments []any) {
	t.Helper()
	if !strings.Contains(query, "shard.schema_version = $") {
		t.Fatalf("route query does not bind schema version: %s", query)
	}
	if len(arguments) == 0 || arguments[len(arguments)-1] != shardphysical.SupportedSchemaVersion {
		t.Fatalf("route query schema argument = %v, want %d", arguments, shardphysical.SupportedSchemaVersion)
	}
}

type schemaVersionDB struct{ tx pgx.Tx }

func (db schemaVersionDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return db.tx, nil }

type schemaVersionTx struct {
	pgx.Tx
	rows      []pgx.Row
	queries   []string
	arguments [][]any
}

func (tx *schemaVersionTx) QueryRow(_ context.Context, query string, arguments ...any) pgx.Row {
	tx.queries = append(tx.queries, query)
	tx.arguments = append(tx.arguments, arguments)
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}

func (*schemaVersionTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (*schemaVersionTx) Commit(context.Context) error   { return nil }
func (*schemaVersionTx) Rollback(context.Context) error { return nil }

type schemaVersionRow struct {
	values []any
	err    error
}

func (row schemaVersionRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range destinations {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
