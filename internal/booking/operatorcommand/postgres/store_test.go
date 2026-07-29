package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestReserveBindsStablePhysicalRouteWithoutRawKeyOrPayload(t *testing.T) {
	request := reserveFixture(operatorcommand.OperationFareInstall)
	tx := &storeTx{rows: []pgx.Row{
		storeRow{err: pgx.ErrNoRows},
		storeRow{values: []any{"physical-shard-1", int64(7)}},
		storeRow{values: []any{request.ResourceID}},
	}, execTag: pgconn.NewCommandTag("INSERT 0 1")}
	store, _ := NewStore(&storeDB{tx: tx})
	command, err := store.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if command.ActorID != request.ActorID || command.Route.ShardID() != sharding.ShardPhysicalOne ||
		command.Route.Generation().Int64() != 7 || command.State != operatorcommand.StateReserved || !tx.committed {
		t.Fatalf("Reserve = %+v committed=%v", command, tx.committed)
	}
	if !strings.Contains(tx.queryLog[1], "assignment_state='stable'") ||
		!strings.Contains(tx.queryLog[1], "active_physical_migration_id IS NULL") ||
		!strings.Contains(tx.queryLog[1], "assignment_state='rollback_window'") ||
		!strings.Contains(tx.queryLog[1], "migration.state='rollback_window'") ||
		!strings.Contains(tx.queryLog[1], "migration.target_shard_id=assignment.shard_id") ||
		!strings.Contains(tx.queryLog[1], "migration.target_generation=assignment.assignment_generation") ||
		strings.Contains(tx.execQuery, "payload") || strings.Contains(tx.execQuery, "raw") {
		t.Fatalf("reserve SQL contract drifted: query=%s insert=%s", tx.queryLog[1], tx.execQuery)
	}
	if len(tx.execArgs) != 17 || tx.execArgs[3].([]byte)[0] != 1 || tx.execArgs[4].([]byte)[0] != 2 {
		t.Fatalf("insert arguments omitted durable hashes: %#v", tx.execArgs)
	}
}

func TestReserveFarePreflightFailsBeforeCommandInsert(t *testing.T) {
	request := reserveFixture(operatorcommand.OperationFareInstall)
	tx := &storeTx{rows: []pgx.Row{
		storeRow{err: pgx.ErrNoRows},
		storeRow{values: []any{"physical-shard-0", int64(4)}},
		storeRow{err: pgx.ErrNoRows},
	}}
	store, _ := NewStore(&storeDB{tx: tx})
	if _, err := store.Reserve(context.Background(), request); !errors.Is(err, ErrControlStore) {
		t.Fatalf("Reserve missing fare error = %v", err)
	}
	if tx.execQuery != "" || tx.committed {
		t.Fatal("route-level fare mismatch inserted or committed a command")
	}
	if len(tx.queryLog) != 3 || !strings.Contains(tx.queryLog[2], "FROM public.fares") ||
		!strings.Contains(tx.queryLog[2], "source_version=$3") ||
		!strings.Contains(tx.queryLog[2], "from_stop_index=$4") ||
		!strings.Contains(tx.queryLog[2], "FOR KEY SHARE") {
		t.Fatalf("fare preflight query = %v", tx.queryLog)
	}
}

func TestClaimUsesFixedRecoveryStatesSkipLockedAndBoundedLease(t *testing.T) {
	request := reserveFixture(operatorcommand.OperationBookingPolicyBump)
	leaseUntil := time.Now().Add(time.Minute).UTC()
	rows := &storeRows{values: [][]any{{
		uuid.New(), request.ActorID, request.Operation, request.IdempotencyKeyHash[:], request.RequestFingerprint[:],
		request.TrainRunID, request.ResourceID, "physical-shard-0", int64(4), request.ExpectedSourceVersion,
		pgtype.Int8{Int64: request.ExpectedBookingPolicyVersion, Valid: true},
		pgtype.Int4{}, pgtype.Int4{}, pgtype.Text{}, pgtype.Int8{}, pgtype.Text{}, pgtype.Bool{},
		pgtype.Int8{}, pgtype.Int8{},
		string(operatorcommand.StateNeedsRepair), "worker-1", leaseUntil,
	}}}
	tx := &storeTx{queryRows: rows}
	store, _ := NewStore(&storeDB{tx: tx})
	candidates, err := store.Claim(context.Background(), operatorcommand.ClaimOptions{
		WorkerID: "worker-1", BatchSize: 9, LeaseTTL: 45 * time.Second,
	})
	if err != nil || len(candidates) != 1 || !tx.committed {
		t.Fatalf("Claim = (%+v,%v), committed=%v", candidates, err, tx.committed)
	}
	query := tx.query
	for _, required := range []string{
		"state IN ('reserved','committed_on_shard','needs_repair')",
		"FOR UPDATE SKIP LOCKED", "LIMIT $1", "attempt_count=attempt_count+1",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("claim query omitted %q", required)
		}
	}
	if strings.Contains(query, "state='finalized'") || strings.Contains(query, "state='failed'") {
		t.Fatal("claim query scans terminal states")
	}
	if tx.queryArgs[0] != 9 || tx.queryArgs[1] != "worker-1" || tx.queryArgs[2] != int64(45000) {
		t.Fatalf("claim bounds = %#v", tx.queryArgs)
	}
	if candidates[0].Command.ExpectedBookingPolicyVersion != request.ExpectedBookingPolicyVersion ||
		candidates[0].LeaseOwner != "worker-1" || !candidates[0].LeaseUntil.Equal(leaseUntil) {
		t.Fatalf("candidate = %+v", candidates[0])
	}
}

func TestReserveExistingKeyRejectsChangedFingerprint(t *testing.T) {
	request := reserveFixture(operatorcommand.OperationSeatDisable)
	commandID := uuid.New()
	tx := &storeTx{rows: []pgx.Row{storeRow{values: commandValues(commandID, request, [32]byte{9})}}}
	store, _ := NewStore(&storeDB{tx: tx})
	if _, err := store.Reserve(context.Background(), request); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("Reserve changed fingerprint error = %v", err)
	}
	if tx.committed || tx.execQuery != "" {
		t.Fatal("idempotency conflict mutated control state")
	}
}

func reserveFixture(operation operatorcommand.Operation) operatorcommand.ReserveRequest {
	trainRunID := uuid.New()
	resourceID := uuid.New()
	policy := int64(0)
	payload := operatorcommand.BoundedFinalizePayload{}
	if operation == operatorcommand.OperationFareInstall {
		payload = operatorcommand.BoundedFinalizePayload{FromStopIndex: 0, ToStopIndex: 2,
			SeatClass: "standard", AmountMinor: 100, Currency: "TWD"}
	}
	if operation == operatorcommand.OperationSeatEnable {
		payload.SeatActive = true
	}
	if operation == operatorcommand.OperationBookingPolicyBump {
		resourceID = trainRunID
		policy = 3
	}
	return operatorcommand.ReserveRequest{ActorID: uuid.New(), TrainRunID: trainRunID, ResourceID: resourceID,
		Operation: operation, IdempotencyKeyHash: [32]byte{1}, RequestFingerprint: [32]byte{2},
		ExpectedSourceVersion: 6, ExpectedBookingPolicyVersion: policy, FinalizePayload: payload}
}

func commandValues(commandID uuid.UUID, request operatorcommand.ReserveRequest, fingerprint [32]byte) []any {
	policy := pgtype.Int8{}
	if request.ExpectedBookingPolicyVersion > 0 {
		policy = pgtype.Int8{Int64: request.ExpectedBookingPolicyVersion, Valid: true}
	}
	payload := payloadDatabaseValues(request.Operation, request.FinalizePayload)
	return []any{commandID, request.ActorID, request.Operation, request.IdempotencyKeyHash[:], fingerprint[:],
		request.TrainRunID, request.ResourceID, "physical-shard-0", int64(4), request.ExpectedSourceVersion,
		policy, payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
		pgtype.Int8{}, pgtype.Int8{}, string(operatorcommand.StateReserved)}
}

func payloadDatabaseValues(operation operatorcommand.Operation, payload operatorcommand.BoundedFinalizePayload) []any {
	if operation == operatorcommand.OperationFareInstall {
		return []any{pgtype.Int4{Int32: int32(payload.FromStopIndex), Valid: true},
			pgtype.Int4{Int32: int32(payload.ToStopIndex), Valid: true},
			pgtype.Text{String: payload.SeatClass, Valid: true}, pgtype.Int8{Int64: payload.AmountMinor, Valid: true},
			pgtype.Text{String: payload.Currency, Valid: true}, pgtype.Bool{}}
	}
	if operation == operatorcommand.OperationSeatDisable || operation == operatorcommand.OperationSeatEnable {
		return []any{pgtype.Int4{}, pgtype.Int4{}, pgtype.Text{}, pgtype.Int8{}, pgtype.Text{},
			pgtype.Bool{Bool: payload.SeatActive, Valid: true}}
	}
	return []any{pgtype.Int4{}, pgtype.Int4{}, pgtype.Text{}, pgtype.Int8{}, pgtype.Text{}, pgtype.Bool{}}
}

type storeDB struct{ tx pgx.Tx }

func (db *storeDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return db.tx, nil }

type storeTx struct {
	pgx.Tx
	rows      []pgx.Row
	rowIndex  int
	queryRows pgx.Rows
	queryLog  []string
	query     string
	queryArgs []any
	execQuery string
	execArgs  []any
	execTag   pgconn.CommandTag
	committed bool
}

func (tx *storeTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.queryLog = append(tx.queryLog, query)
	row := tx.rows[tx.rowIndex]
	tx.rowIndex++
	return row
}
func (tx *storeTx) Query(_ context.Context, query string, args ...any) (pgx.Rows, error) {
	tx.query = query
	tx.queryArgs = append([]any(nil), args...)
	return tx.queryRows, nil
}
func (tx *storeTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.execQuery = query
	tx.execArgs = append([]any(nil), args...)
	return tx.execTag, nil
}
func (tx *storeTx) Commit(context.Context) error { tx.committed = true; return nil }
func (*storeTx) Rollback(context.Context) error  { return pgx.ErrTxClosed }

type storeRow struct {
	values []any
	err    error
}

func (row storeRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return errors.New("scan arity mismatch")
	}
	return assign(destinations, row.values)
}

type storeRows struct {
	values [][]any
	index  int
}

func (rows *storeRows) Next() bool { return rows.index < len(rows.values) }
func (rows *storeRows) Scan(destinations ...any) error {
	values := rows.values[rows.index]
	rows.index++
	return assign(destinations, values)
}
func (*storeRows) Close()                                       {}
func (*storeRows) Err() error                                   { return nil }
func (*storeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*storeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*storeRows) Values() ([]any, error)                       { return nil, nil }
func (*storeRows) RawValues() [][]byte                          { return nil }
func (*storeRows) Conn() *pgx.Conn                              { return nil }

func assign(destinations, values []any) error {
	if len(destinations) != len(values) {
		return errors.New("scan arity mismatch")
	}
	for index, destination := range destinations {
		switch pointer := destination.(type) {
		case *uuid.UUID:
			*pointer = values[index].(uuid.UUID)
		case *operatorcommand.Operation:
			*pointer = values[index].(operatorcommand.Operation)
		case *[]byte:
			*pointer = append([]byte(nil), values[index].([]byte)...)
		case *string:
			*pointer = values[index].(string)
		case *int64:
			*pointer = values[index].(int64)
		case *pgtype.Int8:
			*pointer = values[index].(pgtype.Int8)
		case *pgtype.Int4:
			*pointer = values[index].(pgtype.Int4)
		case *pgtype.Text:
			*pointer = values[index].(pgtype.Text)
		case *pgtype.Bool:
			*pointer = values[index].(pgtype.Bool)
		case *time.Time:
			*pointer = values[index].(time.Time)
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}
