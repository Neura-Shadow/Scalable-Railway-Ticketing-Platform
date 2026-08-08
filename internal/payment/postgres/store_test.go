package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestLookupIntentByIdempotencyReplaysOnlyMatchingFingerprint(t *testing.T) {
	fixture := intentFixture()
	db := &fakeDB{rows: []pgx.Row{intentRow(fixture), intentRow(fixture)}}
	store, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	got, found, err := store.LookupIntentByIdempotency(
		context.Background(), fixture.OwnerID, sha256.Sum256([]byte("create-key")), fixtureFingerprint,
	)
	if err != nil || !found || got != fixture {
		t.Fatalf("matching lookup = (%+v, %v, %v), want (%+v, true, nil)", got, found, err, fixture)
	}

	changed := sha256.Sum256([]byte("changed-request"))
	_, found, err = store.LookupIntentByIdempotency(
		context.Background(), fixture.OwnerID, sha256.Sum256([]byte("create-key")), changed,
	)
	if !errors.Is(err, paymentapp.ErrPaymentConflict) || found {
		t.Fatalf("changed lookup = (found %v, error %v), want conflict", found, err)
	}
}

func TestReserveIntentAtomicallyCreatesIntentAndSagaWithStableBeginIdentity(t *testing.T) {
	fixture := intentFixture()
	tx := &fakeTx{rows: []pgx.Row{
		fakeRow{err: pgx.ErrNoRows},
		fakeRow{err: pgx.ErrNoRows},
		intentRow(fixture),
	}}
	store := mustStore(t, &fakeDB{transactions: []pgx.Tx{tx}})
	request := reserveRequest(fixture)
	request.BeginCommandID = uuid.MustParse("60000000-0000-0000-0000-000000000006")

	got, replayed, err := store.ReserveIntent(context.Background(), request)
	if err != nil || replayed {
		t.Fatalf("ReserveIntent() = (%+v, %v, %v), want created", got, replayed, err)
	}
	if got.BeginCommandID != beginCommandID(fixture.SagaID) || got.BeginCommandID == request.BeginCommandID {
		t.Fatalf("BeginCommandID = %s, want stable saga-derived identity", got.BeginCommandID)
	}
	if !tx.committed || len(tx.execs) != 2 ||
		!strings.Contains(tx.execs[0].query, "INSERT INTO public.payment_intents") ||
		!strings.Contains(tx.execs[1].query, "INSERT INTO public.payment_sagas") {
		t.Fatalf("transaction = committed %v, execs %+v", tx.committed, tx.execs)
	}
	if got.AmountMinor != request.AmountMinor || got.Currency != request.Currency || got.State != "reservation_securing" {
		t.Fatalf("reserved intent = %+v", got)
	}
}

func TestReserveIntentRejectsAnotherActiveIntentForReservation(t *testing.T) {
	fixture := intentFixture()
	tx := &fakeTx{rows: []pgx.Row{fakeRow{err: pgx.ErrNoRows}, intentRow(fixture)}}
	store := mustStore(t, &fakeDB{transactions: []pgx.Tx{tx}})
	request := reserveRequest(fixture)
	request.PaymentIntentID = uuid.New()
	request.SagaID = uuid.New()

	_, _, err := store.ReserveIntent(context.Background(), request)
	if !errors.Is(err, paymentapp.ErrPaymentConflict) || len(tx.execs) != 0 || tx.committed {
		t.Fatalf("ReserveIntent() error = %v, execs = %d, committed = %v", err, len(tx.execs), tx.committed)
	}
}

func TestReserveIntentReplaysWinnerAfterConcurrentSameKeyInsert(t *testing.T) {
	fixture := intentFixture()
	tx := &fakeTx{
		rows:       []pgx.Row{fakeRow{err: pgx.ErrNoRows}, fakeRow{err: pgx.ErrNoRows}},
		execErrors: []error{&pgconn.PgError{Code: "23505"}},
	}
	store := mustStore(t, &fakeDB{
		transactions: []pgx.Tx{tx},
		rows:         []pgx.Row{intentRow(fixture)},
	})

	got, replayed, err := store.ReserveIntent(context.Background(), reserveRequest(fixture))
	if err != nil || !replayed || got != fixture {
		t.Fatalf("concurrent replay = (%+v, %v, %v)", got, replayed, err)
	}
}

func TestMarkReservationSecuredRepairsFinalizationAndCreatesStableCheckout(t *testing.T) {
	securing := intentFixture()
	securing.State = "checkout_pending"
	finalized := securing
	wantOperationID, wantHash := operationIdentity(securing.ID, "create_checkout")
	tx := &fakeTx{rows: []pgx.Row{
		intentRow(securing),
		fakeRow{values: []any{wantOperationID, wantHash[:], securing.AmountMinor, securing.Currency}},
		intentRow(finalized),
	}}
	store := mustStore(t, &fakeDB{transactions: []pgx.Tx{tx}})

	got, err := store.MarkReservationSecured(
		context.Background(), securing.ID, securing.BeginCommandID, fixtureFingerprint,
	)
	if err != nil || got.State != "checkout_pending" || !tx.committed {
		t.Fatalf("MarkReservationSecured() = (%+v, %v), committed %v", got, err, tx.committed)
	}
	if len(tx.execs) != 3 || !strings.Contains(tx.execs[2].query, "ON CONFLICT DO NOTHING") {
		t.Fatalf("finalization execs = %+v", tx.execs)
	}
	if tx.execs[2].args[1] != wantOperationID || !reflect.DeepEqual(tx.execs[2].args[2], wantHash[:]) {
		t.Fatalf("checkout identity args = %#v", tx.execs[2].args)
	}
	if !strings.Contains(tx.execs[2].query, "intent.amount_minor,intent.currency") {
		t.Fatal("checkout financials are not selected from the stored intent")
	}
}

func TestGetOwnedIntentDoesNotRevealAnotherOwnersIntent(t *testing.T) {
	store := mustStore(t, &fakeDB{rows: []pgx.Row{fakeRow{err: pgx.ErrNoRows}}})
	_, err := store.GetOwnedIntent(context.Background(), uuid.New(), uuid.New())
	if !errors.Is(err, paymentapp.ErrPaymentNotFound) {
		t.Fatalf("GetOwnedIntent() error = %v, want not found", err)
	}
}

func TestGetOwnedIntentByReservationDoesNotRevealAnotherOwnersIntent(t *testing.T) {
	store := mustStore(t, &fakeDB{rows: []pgx.Row{fakeRow{err: pgx.ErrNoRows}}})
	_, err := store.GetOwnedIntentByReservation(context.Background(), uuid.New(), uuid.New())
	if !errors.Is(err, paymentapp.ErrPaymentNotFound) {
		t.Fatalf("GetOwnedIntentByReservation() error = %v, want not found", err)
	}
}

func TestRequestCancellationCreatesVoidBeforeCaptureAndRefundAfterCapture(t *testing.T) {
	for _, test := range []struct {
		name      string
		state     string
		captured  bool
		wantKind  string
		wantState string
	}{
		{name: "before capture", state: "authorized", wantKind: "void", wantState: "void_pending"},
		{name: "after capture", state: "captured", captured: true, wantKind: "refund", wantState: "refund_pending"},
		{name: "after issuance", state: "completed", captured: true, wantKind: "refund", wantState: "refund_pending"},
	} {
		t.Run(test.name, func(t *testing.T) {
			intent := intentFixture()
			intent.State = test.state
			finalized := intent
			finalized.State = test.wantState
			request := cancellationRequest(intent)
			providerHash := cancellationOperationHash(intent.OwnerID, request.IdempotencyKeyHash)
			tx := &fakeTx{rows: []pgx.Row{
				intentRow(intent), fakeRow{values: []any{test.captured}},
				fakeRow{err: pgx.ErrNoRows}, fakeRow{err: pgx.ErrNoRows},
				fakeRow{values: []any{
					intent.ID, providerHash[:],
					intent.AmountMinor, intent.Currency,
				}},
				intentRow(finalized),
			}}
			store := mustStore(t, &fakeDB{transactions: []pgx.Tx{tx}})

			got, err := store.RequestCancellation(context.Background(), request)
			if err != nil || got.State != test.wantState || !tx.committed {
				t.Fatalf("RequestCancellation() = (%+v, %v), committed %v", got, err, tx.committed)
			}
			if len(tx.execs) != 3 || tx.execs[0].args[2] != test.wantKind {
				t.Fatalf("cancellation execs = %+v, want %s", tx.execs, test.wantKind)
			}
			wantProviderHash := cancellationOperationHash(intent.OwnerID, request.IdempotencyKeyHash)
			if !reflect.DeepEqual(tx.execs[0].args[3], wantProviderHash[:]) ||
				reflect.DeepEqual(tx.execs[0].args[3], request.IdempotencyKeyHash[:]) {
				t.Fatalf("provider idempotency hash = %#v", tx.execs[0].args[3])
			}
			if !strings.Contains(tx.execs[0].query, "intent.amount_minor,intent.currency") {
				t.Fatal("cancellation financials are not selected from the stored intent")
			}
			if test.state == "completed" && (!strings.Contains(tx.execs[2].query, "'completed'") || !strings.Contains(tx.execs[2].query, "completed_at=NULL")) {
				t.Fatalf("completed saga is not reopened safely: %s", tx.execs[2].query)
			}
		})
	}
}

func TestRequestCancellationReplaysStableExistingOperation(t *testing.T) {
	intent := intentFixture()
	intent.State = "authorized"
	request := cancellationRequest(intent)
	tx := &fakeTx{rows: []pgx.Row{
		intentRow(intent), fakeRow{values: []any{false}},
		fakeRow{values: []any{intent.ID, "void"}},
	}}
	store := mustStore(t, &fakeDB{transactions: []pgx.Tx{tx}})

	got, err := store.RequestCancellation(context.Background(), request)
	if err != nil || got != intent || !tx.committed || len(tx.execs) != 0 {
		t.Fatalf("replay = (%+v, %v), committed %v, execs %d", got, err, tx.committed, len(tx.execs))
	}
}

var fixtureFingerprint = sha256.Sum256([]byte("fixture-request"))

func intentFixture() paymentapp.IntentRecord {
	intent := paymentapp.IntentRecord{
		ID:            uuid.MustParse("10000000-0000-0000-0000-000000000001"),
		SagaID:        uuid.MustParse("20000000-0000-0000-0000-000000000002"),
		ReservationID: uuid.MustParse("30000000-0000-0000-0000-000000000003"),
		TrainRunID:    uuid.MustParse("40000000-0000-0000-0000-000000000004"),
		OwnerID:       uuid.MustParse("50000000-0000-0000-0000-000000000005"),
		Provider:      "sandbox",
		AmountMinor:   2500,
		Currency:      "TWD",
		State:         "reservation_securing",
		CreatedAt:     time.Date(2026, 8, 8, 1, 2, 3, 0, time.UTC),
		UpdatedAt:     time.Date(2026, 8, 8, 1, 2, 4, 0, time.UTC),
	}
	intent.BeginCommandID = beginCommandID(intent.SagaID)
	return intent
}

func reserveRequest(intent paymentapp.IntentRecord) paymentapp.ReserveIntentRequest {
	return paymentapp.ReserveIntentRequest{
		PaymentIntentID: intent.ID, SagaID: intent.SagaID, BeginCommandID: uuid.New(),
		ReservationID: intent.ReservationID, TrainRunID: intent.TrainRunID,
		OwnerID: intent.OwnerID, Provider: intent.Provider, AmountMinor: intent.AmountMinor,
		Currency: intent.Currency, IdempotencyKeyHash: sha256.Sum256([]byte("create-key")),
		RequestFingerprint: fixtureFingerprint,
	}
}

func cancellationRequest(intent paymentapp.IntentRecord) paymentapp.CancelIntentRequest {
	return paymentapp.CancelIntentRequest{
		OwnerID: intent.OwnerID, PaymentIntentID: intent.ID, OperationID: uuid.New(),
		IdempotencyKeyHash: sha256.Sum256([]byte("cancel-key")),
		RequestFingerprint: cancellationFingerprint(intent.OwnerID, intent.ID),
	}
}

func mustStore(t *testing.T, db DB) *Store {
	t.Helper()
	store, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store
}

func intentRow(intent paymentapp.IntentRecord) pgx.Row {
	return fakeRow{values: []any{
		intent.ID, intent.SagaID, intent.ReservationID, intent.TrainRunID,
		intent.OwnerID, intent.Provider, intent.ProviderPaymentID,
		intent.HostedSessionRef, intent.AmountMinor, intent.Currency,
		intent.State, fixtureFingerprint[:], intent.CreatedAt, intent.UpdatedAt, intent.CompletedAt,
	}}
}

type fakeDB struct {
	rows         []pgx.Row
	transactions []pgx.Tx
}

func (db *fakeDB) QueryRow(context.Context, string, ...any) pgx.Row {
	if len(db.rows) == 0 {
		return fakeRow{err: errors.New("unexpected QueryRow")}
	}
	row := db.rows[0]
	db.rows = db.rows[1:]
	return row
}

func (db *fakeDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	if len(db.transactions) == 0 {
		return nil, errors.New("unexpected BeginTx")
	}
	tx := db.transactions[0]
	db.transactions = db.transactions[1:]
	return tx, nil
}

type execCall struct {
	query string
	args  []any
}

type fakeTx struct {
	pgx.Tx
	rows       []pgx.Row
	execs      []execCall
	execErrors []error
	committed  bool
}

func (tx *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row {
	if len(tx.rows) == 0 {
		return fakeRow{err: errors.New("unexpected transaction QueryRow")}
	}
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}

func (tx *fakeTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, execCall{query: query, args: args})
	if len(tx.execErrors) > 0 {
		err := tx.execErrors[0]
		tx.execErrors = tx.execErrors[1:]
		return pgconn.CommandTag{}, err
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (tx *fakeTx) Commit(context.Context) error {
	tx.committed = true
	return nil
}

func (*fakeTx) Rollback(context.Context) error { return nil }

type fakeRow struct {
	values []any
	err    error
}

func (row fakeRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(dest) != len(row.values) {
		return errors.New("unexpected scan width")
	}
	for index := range dest {
		target := reflect.ValueOf(dest[index])
		if target.Kind() != reflect.Pointer || target.IsNil() {
			return errors.New("scan destination is not a pointer")
		}
		value := reflect.ValueOf(row.values[index])
		if !value.IsValid() {
			target.Elem().SetZero()
			continue
		}
		if !value.Type().AssignableTo(target.Elem().Type()) {
			return errors.New("scan value type mismatch")
		}
		target.Elem().Set(value)
	}
	return nil
}
