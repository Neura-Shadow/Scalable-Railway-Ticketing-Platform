package physical

import (
	"context"
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestInspectorUsesCurrentShardAndValidatesHistoricalReceiptVersion(t *testing.T) {
	trainRunID := uuid.New()
	resourceID := uuid.New()
	historicalGeneration, _ := sharding.NewAssignmentGeneration(4)
	historicalRoute, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, historicalGeneration)
	command := operatorcommand.Command{ID: uuid.New(), ActorID: uuid.New(), TrainRunID: trainRunID,
		ResourceID: resourceID, Operation: operatorcommand.OperationFareInstall,
		IdempotencyKeyHash: [32]byte{1}, RequestFingerprint: [32]byte{2}, Route: historicalRoute,
		ExpectedSourceVersion: 11, State: operatorcommand.StateNeedsRepair}
	tx := &inspectionTx{row: inspectionRow{values: []any{
		command.ID, trainRunID, int64(4), operatorcommand.OperationFareInstall,
		command.RequestFingerprint[:], "succeeded", resourceID,
		pgtype.Int8{Int64: 12, Valid: true}, pgtype.Int8{},
	}}}
	pool := &inspectionPool{tx: tx}
	resolver := currentResolver(t, trainRunID, sharding.ShardPhysicalOne, 5, pool)
	inspector, err := NewInspector(resolver)
	if err != nil {
		t.Fatal(err)
	}
	receipt, found, err := inspector.Inspect(context.Background(), operatorcommand.Candidate{Command: command})
	if err != nil || !found || receipt.HistoricalShardID != sharding.ShardPhysicalZero ||
		receipt.HistoricalGeneration != 4 || receipt.ResultSourceVersion != 12 ||
		resolver.resolvedTrainRun != trainRunID || tx.queryCount != 1 || !tx.committed {
		t.Fatalf("Inspect = (%+v,%v,%v), resolver=%s queries=%d committed=%v", receipt, found, err,
			resolver.resolvedTrainRun, tx.queryCount, tx.committed)
	}
	if pool.options.AccessMode != pgx.ReadOnly || pool.options.IsoLevel != pgx.ReadCommitted {
		t.Fatalf("transaction options = %+v", pool.options)
	}
}

func TestInspectorRejectsReceiptVersionMismatch(t *testing.T) {
	trainRunID := uuid.New()
	generation, _ := sharding.NewAssignmentGeneration(2)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	command := operatorcommand.Command{ID: uuid.New(), ActorID: uuid.New(), TrainRunID: trainRunID,
		ResourceID: uuid.New(), Operation: operatorcommand.OperationSeatDisable,
		IdempotencyKeyHash: [32]byte{1}, RequestFingerprint: [32]byte{2}, Route: route,
		ExpectedSourceVersion: 7, State: operatorcommand.StateReserved}
	pool := &inspectionPool{tx: &inspectionTx{row: inspectionRow{values: []any{
		command.ID, trainRunID, int64(2), command.Operation, command.RequestFingerprint[:],
		"succeeded", command.ResourceID, pgtype.Int8{Int64: 9, Valid: true}, pgtype.Int8{},
	}}}}
	inspector, _ := NewInspector(currentResolver(t, trainRunID, sharding.ShardPhysicalZero, 2, pool))
	if _, _, err := inspector.Inspect(context.Background(), operatorcommand.Candidate{Command: command}); !errors.Is(err, operatorcommand.ErrReceiptMismatch) {
		t.Fatalf("Inspect mismatch error = %v", err)
	}
}

type resolverStub struct {
	resolution       shardphysical.Resolution
	resolvedTrainRun uuid.UUID
}

func (resolver *resolverStub) Resolve(_ context.Context, trainRunID uuid.UUID, force bool) (shardphysical.Resolution, error) {
	if force {
		return shardphysical.Resolution{}, errors.New("unexpected forced refresh")
	}
	resolver.resolvedTrainRun = trainRunID
	return resolver.resolution, nil
}

func currentResolver(t *testing.T, trainRunID uuid.UUID, shardID sharding.ShardID, generationValue int64, pool shardphysical.Pool) *resolverStub {
	t.Helper()
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{shardID.String(): {ShardID: shardID, DSN: "synthetic"}},
		MaxCount:    1, Limits: shardphysical.PoolLimits{MaxOpenConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) { return pool, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	handle, err := registry.Resolve(shardphysical.CatalogEntry{ShardID: shardID,
		StorageKind: shardphysical.StoragePostgres, ConnectionRef: shardID.String(), ProtocolVersion: 1,
		SchemaVersion: shardphysical.SupportedSchemaVersion, Enabled: true, WriteEnabled: true, HealthState: shardphysical.HealthHealthy,
		State: shardphysical.StateActive})
	if err != nil {
		t.Fatal(err)
	}
	generation, _ := sharding.NewAssignmentGeneration(generationValue)
	route, _ := sharding.NewShardRoute(trainRunID, shardID, generation)
	return &resolverStub{resolution: shardphysical.Resolution{Route: route, Handle: handle}}
}

type inspectionPool struct {
	tx      pgx.Tx
	options pgx.TxOptions
}

func (pool *inspectionPool) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	pool.options = options
	return pool.tx, nil
}
func (*inspectionPool) Close() {}

type inspectionTx struct {
	pgx.Tx
	row        pgx.Row
	queryCount int
	committed  bool
}

func (tx *inspectionTx) QueryRow(context.Context, string, ...any) pgx.Row {
	tx.queryCount++
	return tx.row
}
func (tx *inspectionTx) Commit(context.Context) error { tx.committed = true; return nil }
func (*inspectionTx) Rollback(context.Context) error  { return pgx.ErrTxClosed }
func (*inspectionTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("read-only inspector attempted mutation")
}

type inspectionRow struct {
	values []any
	err    error
}

func (row inspectionRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return errors.New("scan arity mismatch")
	}
	for index, destination := range destinations {
		switch pointer := destination.(type) {
		case *uuid.UUID:
			*pointer = row.values[index].(uuid.UUID)
		case *int64:
			*pointer = row.values[index].(int64)
		case *operatorcommand.Operation:
			*pointer = row.values[index].(operatorcommand.Operation)
		case *[]byte:
			*pointer = append([]byte(nil), row.values[index].([]byte)...)
		case *string:
			*pointer = row.values[index].(string)
		case *pgtype.Int8:
			*pointer = row.values[index].(pgtype.Int8)
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}
