package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestProviderIdempotencyKeyIsDeterministicBoundedAndHashedAtRest(t *testing.T) {
	t.Parallel()
	operationID := uuid.New()
	key, hash := ProviderIdempotencyKey(operationID)
	if key == "" || len(key) > 128 || key != "ticket-refund-"+operationID.String() {
		t.Fatalf("provider idempotency key = %q", key)
	}
	if hash != sha256.Sum256([]byte(key)) {
		t.Fatal("stored hash does not bind provider idempotency key")
	}
	second, secondHash := ProviderIdempotencyKey(operationID)
	if second != key || secondHash != hash {
		t.Fatal("provider idempotency identity is not replay stable")
	}
}

func TestCreatePartialRefundRejectsExistingFullRefundBeforeAnyInsert(t *testing.T) {
	t.Parallel()
	ownerID, orderID, intentID := uuid.New(), uuid.New(), uuid.New()
	reservationID, trainRunID, ticketID := uuid.New(), uuid.New(), uuid.New()
	selection, err := refund.SelectionFingerprint(ownerID, orderID, []uuid.UUID{ticketID})
	if err != nil {
		t.Fatal(err)
	}
	idempotency := sha256.Sum256([]byte("partial-vs-full"))
	createdAt := time.Date(2026, 8, 11, 1, 0, 0, 0, time.UTC)
	command := refund.CreateCommand{
		Lookup:          refund.Lookup{OwnerID: ownerID, OrderID: orderID, IdempotencyHash: idempotency},
		ExpectedVersion: 3, SelectionFingerprint: selection,
		Request: refund.RefundRequest{
			ID: uuid.New(), OwnerID: ownerID, OrderID: orderID, PaymentIntentID: intentID,
			ReservationID: reservationID, TrainRunID: trainRunID, Provider: "sandbox", ProviderPaymentID: "pay-1",
			AssignmentGeneration: 3, TicketIDs: []uuid.UUID{ticketID}, Items: []refund.RefundItem{{TicketID: ticketID, FareMinor: 700}},
			AmountMinor: 700, CapturedMinor: 2500, Currency: "TWD", ShardID: "booking-shard-1", CreatedAt: createdAt,
			EligibilityCutoffAt: createdAt.Add(time.Hour),
		},
	}
	tx := &refundStoreTx{rows: []pgx.Row{
		refundStoreErrorRow{err: pgx.ErrNoRows},
		refundStoreRow(func(dest []any) {
			*(dest[0].(*uuid.UUID)) = reservationID
			*(dest[1].(*uuid.UUID)) = trainRunID
			*(dest[2].(*string)) = "booking-shard-1"
			*(dest[3].(*int64)) = 3
			*(dest[4].(*uuid.UUID)) = ownerID
			*(dest[5].(*string)) = "confirmed"
			*(dest[6].(*int64)) = 2500
			*(dest[7].(*string)) = "TWD"
		}),
		refundStoreRow(func(dest []any) {
			*(dest[0].(*uuid.UUID)) = reservationID
			*(dest[1].(*uuid.UUID)) = trainRunID
			*(dest[2].(*uuid.UUID)) = ownerID
			*(dest[3].(*string)) = "sandbox"
			*(dest[4].(*string)) = "pay-1"
			*(dest[5].(*int64)) = 2500
			*(dest[6].(*string)) = "TWD"
			*(dest[7].(*string)) = "completed"
			*(dest[8].(*bool)) = true
		}),
	}}
	store, err := NewStore(&refundStoreDB{tx: tx}, refundShardStub{}, Config{PartialRefundProviders: map[string]bool{"sandbox": true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateRequest(context.Background(), command); !errors.Is(err, refund.ErrSnapshotConflict) {
		t.Fatalf("CreateRequest() error = %v, want ErrSnapshotConflict", err)
	}
	if !tx.rolledBack || tx.committed {
		t.Fatalf("rejected refund transaction committed=%v rolledBack=%v", tx.committed, tx.rolledBack)
	}
	for _, query := range tx.execs {
		if strings.Contains(query, "INSERT INTO public.ticket_refund_") {
			t.Fatalf("partial refund inserted after full-refund evidence: %s", query)
		}
	}
}

func TestLoadOrderScopesTheFirstControlLookupToOwner(t *testing.T) {
	t.Parallel()
	db := &refundStoreDB{}
	store, err := NewStore(db, refundShardStub{}, Config{PartialRefundProviders: map[string]bool{"sandbox": true}})
	if err != nil {
		t.Fatal(err)
	}
	ownerID, orderID := uuid.New(), uuid.New()
	if _, found, err := store.LoadOrder(context.Background(), ownerID, orderID); err != nil || found {
		t.Fatalf("LoadOrder() found=%t err=%v", found, err)
	}
	if !strings.Contains(db.query, "locator.ticket_order_id=$1 AND locator.owner_user_id=$2") ||
		len(db.args) != 2 || db.args[0] != orderID || db.args[1] != ownerID {
		t.Fatalf("first lookup is not owner scoped: query=%q args=%v", db.query, db.args)
	}
}

type refundShardStub struct{}

func (refundShardStub) LoadRefundOrder(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (paymentshard.RefundOrderSnapshot, error) {
	return paymentshard.RefundOrderSnapshot{}, errors.New("unexpected shard read")
}

type refundStoreDB struct {
	tx    *refundStoreTx
	query string
	args  []any
}

func (db *refundStoreDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return db.tx, nil }
func (*refundStoreDB) Query(context.Context, string, ...any) (pgx.Rows, error)   { return nil, nil }
func (db *refundStoreDB) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	db.query = query
	db.args = append([]any(nil), args...)
	return refundStoreErrorRow{err: pgx.ErrNoRows}
}

type refundStoreTx struct {
	pgx.Tx
	rows       []pgx.Row
	execs      []string
	committed  bool
	rolledBack bool
}

func (tx *refundStoreTx) QueryRow(context.Context, string, ...any) pgx.Row {
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}
func (tx *refundStoreTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, query)
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (*refundStoreTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected ticket query")
}
func (tx *refundStoreTx) Commit(context.Context) error { tx.committed = true; return nil }
func (tx *refundStoreTx) Rollback(context.Context) error {
	tx.rolledBack = true
	return nil
}

type refundStoreRow func([]any)

func (row refundStoreRow) Scan(dest ...any) error { row(dest); return nil }

type refundStoreErrorRow struct{ err error }

func (row refundStoreErrorRow) Scan(...any) error { return row.err }
