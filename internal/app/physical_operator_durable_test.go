package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestDurablePhysicalOperatorMutationBuildsBoundedCommand(t *testing.T) {
	t.Parallel()
	actorID, trainRunID, fareID := uuid.New(), uuid.New(), uuid.New()
	coordinator := &operatorCommandCoordinatorFake{result: operatorcommand.Result{ResourceID: fareID}}
	mutations, err := NewDurablePhysicalOperatorSnapshotMutations(coordinator)
	if err != nil {
		t.Fatal(err)
	}
	input := OperatorFareMutation{ActorID: actorID.String(), IdempotencyKey: "durable-key-1",
		TrainRunID: trainRunID, FareID: fareID, ExpectedSourceVersion: 9,
		FromStopIndex: 1, ToStopIndex: 3, SeatClass: "BUSINESS", AmountMinor: 2500, Currency: "twd"}
	view, err := mutations.InstallFare(context.Background(), input)
	if err != nil || view.ID != fareID.String() || coordinator.calls != 1 {
		t.Fatalf("view=%+v err=%v calls=%d", view, err, coordinator.calls)
	}
	request := coordinator.request
	if request.ActorID != actorID || request.TrainRunID != trainRunID || request.ResourceID != fareID ||
		request.Operation != operatorcommand.OperationFareInstall || request.ExpectedSourceVersion != 9 ||
		request.IdempotencyKeyHash == [32]byte{} || request.RequestFingerprint == [32]byte{} ||
		request.Mutation.SeatClass != "business" || request.Mutation.Currency != "TWD" ||
		request.Mutation.AmountMinor != 2500 {
		t.Fatalf("request=%+v", request)
	}
}

func TestDurablePhysicalOperatorMutationRejectsNonUUIDActor(t *testing.T) {
	t.Parallel()
	coordinator := &operatorCommandCoordinatorFake{}
	mutations, _ := NewDurablePhysicalOperatorSnapshotMutations(coordinator)
	_, err := mutations.SetSeatActive(context.Background(), OperatorSeatMutation{ActorID: "operator",
		IdempotencyKey: "durable-key-2", TrainRunID: uuid.New(), SeatID: uuid.New(),
		ExpectedSourceVersion: 1})
	if !errors.Is(err, httpapi.ErrInvalidInput) || coordinator.calls != 0 {
		t.Fatalf("err=%v calls=%d", err, coordinator.calls)
	}
}

func TestDurableOperatorFinalizerCommitsProjectionAndLedgerTogether(t *testing.T) {
	t.Parallel()
	generation, _ := sharding.NewAssignmentGeneration(4)
	trainRunID, seatID, commandID, actorID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	fingerprint := [32]byte{1}
	command := operatorcommand.Command{ID: commandID, ActorID: actorID, TrainRunID: trainRunID,
		ResourceID: seatID, Operation: operatorcommand.OperationSeatDisable, RequestFingerprint: fingerprint,
		Route: route, ExpectedSourceVersion: 7,
		FinalizePayload: operatorcommand.BoundedFinalizePayload{SeatActive: false}, State: operatorcommand.StateReserved}
	receipt := operatorcommand.Receipt{CommandID: commandID, TrainRunID: trainRunID, ResourceID: seatID,
		Operation: command.Operation, RequestFingerprint: fingerprint, HistoricalShardID: sharding.ShardPhysicalZero,
		HistoricalGeneration: 4, ResultSourceVersion: 8}
	tx := &operatorFinalizerTx{
		tags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("UPDATE 1")},
		row:  snapshotRow{values: []any{"reserved", pgtype.Int8{}, pgtype.Int8{}}},
	}
	finalizer, err := NewPostgresDurableOperatorCommandFinalizer(&operatorFinalizerDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizer.Finalize(context.Background(), command, receipt); err != nil {
		t.Fatal(err)
	}
	if tx.execIndex != 3 || tx.queries != 1 || tx.commits != 1 {
		t.Fatalf("execs=%d queries=%d commits=%d", tx.execIndex, tx.queries, tx.commits)
	}
}

type operatorCommandCoordinatorFake struct {
	request operatorcommand.Request
	result  operatorcommand.Result
	err     error
	calls   int
}

func (fake *operatorCommandCoordinatorFake) Execute(_ context.Context, request operatorcommand.Request) (operatorcommand.Result, error) {
	fake.calls++
	fake.request = request
	return fake.result, fake.err
}

type operatorFinalizerDB struct{ tx pgx.Tx }

func (db *operatorFinalizerDB) Begin(context.Context) (pgx.Tx, error) { return db.tx, nil }

type operatorFinalizerTx struct {
	pgx.Tx
	tags      []pgconn.CommandTag
	execIndex int
	row       pgx.Row
	queries   int
	commits   int
}

func (tx *operatorFinalizerTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	tag := tx.tags[tx.execIndex]
	tx.execIndex++
	return tag, nil
}

func (tx *operatorFinalizerTx) QueryRow(context.Context, string, ...any) pgx.Row {
	tx.queries++
	return tx.row
}

func (tx *operatorFinalizerTx) Commit(context.Context) error {
	tx.commits++
	return nil
}

func (tx *operatorFinalizerTx) Rollback(context.Context) error { return nil }

type snapshotRow struct {
	values []any
	err    error
}

func (row snapshotRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	for index, value := range row.values {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(value))
	}
	return nil
}
