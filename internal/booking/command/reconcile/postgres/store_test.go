package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestFinalizeRepairsControlRowsAtomicallyWithoutSeatMutation(t *testing.T) {
	t.Parallel()
	candidate := controlCandidate(t)
	tx := &controlTx{row: controlRow{values: []any{
		candidate.Command.RequestFingerprint[:], candidate.Command.ReservationID, string(command.StateNeedsRepair),
	}}}
	store, err := NewStore(&controlDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	receipt := command.Receipt{
		CommandID: candidate.Command.ID, RequestFingerprint: candidate.Command.RequestFingerprint,
		ResultResourceID: candidate.Command.ReservationID, Status: command.ReceiptCommitted,
	}
	if err := store.Finalize(context.Background(), candidate, receipt); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if !tx.committed || len(tx.statements) != 5 {
		t.Fatalf("committed = %v, statements = %d", tx.committed, len(tx.statements))
	}
	joined := strings.ToLower(strings.Join(tx.statements, "\n"))
	for _, table := range []string{"booking_commands", "reservation_directory", "reservation_shard_locators", "booking_quota_leases", "outbox_events"} {
		if !strings.Contains(joined, table) {
			t.Fatalf("Finalize() did not repair %s", table)
		}
	}
	if strings.Contains(joined, "seat_inventory") || strings.Contains(joined, "reservation_seats") {
		t.Fatalf("Finalize() attempted shard inventory mutation: %s", joined)
	}
}

func TestFinalizeRejectsFingerprintMismatchBeforeTransaction(t *testing.T) {
	t.Parallel()
	candidate := controlCandidate(t)
	db := &controlDB{}
	store, err := NewStore(db)
	if err != nil {
		t.Fatal(err)
	}
	receipt := command.Receipt{
		CommandID: candidate.Command.ID, RequestFingerprint: [32]byte{99},
		ResultResourceID: candidate.Command.ReservationID, Status: command.ReceiptCommitted,
	}
	if err := store.Finalize(context.Background(), candidate, receipt); !errors.Is(err, ErrControlStore) {
		t.Fatalf("Finalize() error = %v", err)
	}
	if db.begins != 0 {
		t.Fatalf("mismatched receipt began %d transactions", db.begins)
	}
}

func TestFinalizeConfirmationRepairsLifecycleControlStateWithoutDirectoryMutation(t *testing.T) {
	t.Parallel()
	candidate := controlCandidate(t)
	candidate.Command.Operation = command.OperationConfirmReservation
	candidate.Command.State = command.StateCommittedOnShard
	tx := &controlTx{row: controlRow{values: []any{
		candidate.Command.RequestFingerprint[:], candidate.Command.ReservationID, string(candidate.Command.State),
	}}}
	store, err := NewStore(&controlDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	receipt := command.Receipt{
		CommandID: candidate.Command.ID, RequestFingerprint: candidate.Command.RequestFingerprint,
		ResultResourceID: candidate.Command.ReservationID, Status: command.ReceiptCommitted,
		TicketOrderID: uuid.New(), TicketCount: 2, TicketIDs: []uuid.UUID{uuid.New(), uuid.New()}, TotalAmountMinor: 2400,
		Currency: "TWD", OrderCreatedAt: time.Now().UTC(),
	}
	if err := store.Finalize(context.Background(), candidate, receipt); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	joined := strings.ToLower(strings.Join(tx.statements, "\n"))
	for _, table := range []string{"booking_commands", "ticket_order_shard_locators", "ticket_shard_locators", "booking_quota_leases", "outbox_events"} {
		if !strings.Contains(joined, table) {
			t.Fatalf("Finalize() did not repair %s", table)
		}
	}
	if strings.Contains(joined, "update public.reservation_directory") || strings.Contains(joined, "seat_inventory") {
		t.Fatalf("Finalize() attempted lifecycle-forbidden mutation: %s", joined)
	}
	if !tx.committed || len(tx.statements) != 6 {
		t.Fatalf("committed = %v, statements = %d", tx.committed, len(tx.statements))
	}
	if count := strings.Count(joined, "insert into public.ticket_shard_locators"); count != len(receipt.TicketIDs) {
		t.Fatalf("ticket locator writes = %d, want %d", count, len(receipt.TicketIDs))
	}
}

func controlCandidate(t *testing.T) reconcile.Candidate {
	t.Helper()
	trainRunID := uuid.New()
	generation, _ := sharding.NewAssignmentGeneration(2)
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalOne, generation)
	if err != nil {
		t.Fatal(err)
	}
	return reconcile.Candidate{Command: command.Command{
		ID: uuid.New(), Operation: command.OperationCreateReservation, OwnerUserID: uuid.New(),
		TrainRunID: trainRunID, ReservationID: uuid.New(), Route: route,
		RequestFingerprint: [32]byte{4}, State: command.StateNeedsRepair,
	}, QuotaExpiresAt: time.Now().Add(time.Minute)}
}

type controlDB struct {
	tx     pgx.Tx
	begins int
}

func (db *controlDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	db.begins++
	if db.tx == nil {
		return nil, errors.New("unexpected transaction")
	}
	return db.tx, nil
}

type controlTx struct {
	pgx.Tx
	row        pgx.Row
	statements []string
	committed  bool
}

func (tx *controlTx) QueryRow(context.Context, string, ...any) pgx.Row { return tx.row }
func (tx *controlTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	tx.statements = append(tx.statements, query)
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (tx *controlTx) Commit(context.Context) error { tx.committed = true; return nil }
func (*controlTx) Rollback(context.Context) error  { return pgx.ErrTxClosed }

type controlRow struct{ values []any }

func (row controlRow) Scan(destinations ...any) error {
	if len(destinations) != 3 || len(row.values) != 3 {
		return errors.New("scan arity mismatch")
	}
	*(destinations[0].(*[]byte)) = append([]byte(nil), row.values[0].([]byte)...)
	*(destinations[1].(*uuid.UUID)) = row.values[1].(uuid.UUID)
	*(destinations[2].(*string)) = row.values[2].(string)
	return nil
}
