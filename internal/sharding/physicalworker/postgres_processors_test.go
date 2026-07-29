package physicalworker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestOutboxProcessorUsesDatabaseLocalLeaseAndPreservesEventID(t *testing.T) {
	eventID := uuid.New()
	aggregateID := uuid.New()
	leaseToken := uuid.New()
	claim := &recordingTx{rows: &fakeRows{rows: [][]any{{
		eventID, "reservation", aggregateID, "reservation.held", 1,
		json.RawMessage(`{"status":"held"}`), 1, time.Now().UTC(), leaseToken,
	}}}}
	finalize := &recordingTx{}
	pool := &queuePool{transactions: []pgx.Tx{claim, finalize}}
	publisher := &recordingPublisher{}
	processor, err := NewOutboxProcessor(publisher, OutboxOptions{
		WorkerID: "relay-a", MaxAttempts: 5, ProcessingTimeout: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute,
		StatementTimeout: time.Second, LockTimeout: time.Second,
		Now: func() time.Time { return time.Unix(100, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewOutboxProcessor() error = %v", err)
	}

	processed, err := processor.Process(context.Background(), fakeHandle{
		id: sharding.ShardPhysicalZero, pool: pool,
	}, 10)
	if err != nil || processed != 1 {
		t.Fatalf("Process() = (%d, %v), want (1, nil)", processed, err)
	}
	if len(publisher.events) != 1 || publisher.events[0].ID != eventID {
		t.Fatalf("published events = %+v, want original globally unique ID %s", publisher.events, eventID)
	}
	claimCall := claim.findCall("UPDATE outbox_events")
	if claimCall == nil || !strings.Contains(claimCall.query, "lease_token = gen_random_uuid()") {
		t.Fatalf("claim query did not create a database-local lease: %+v", claim.calls)
	}
	if !strings.Contains(claimCall.query, "JOIN train_run_write_fences") ||
		!strings.Contains(claimCall.query, "fence.state = 'active'") ||
		!strings.Contains(claimCall.query, "fence.write_enabled") ||
		!strings.Contains(claimCall.query, "FOR UPDATE OF event SKIP LOCKED") {
		t.Fatalf("claim query did not fence pre-cutover event relay: %s", claimCall.query)
	}
	finalizeCall := finalize.findCall("status = 'published'")
	if finalizeCall == nil || !strings.Contains(finalizeCall.query, "lease_token = $3") {
		t.Fatalf("finalize query did not fence lease ownership: %+v", finalize.calls)
	}
	if got := finalizeCall.args[2]; got != leaseToken {
		t.Fatalf("finalize lease token = %v, want %v", got, leaseToken)
	}
}

func TestOutboxProcessorPublishFailureCreatesBoundedLocalRetry(t *testing.T) {
	eventID := uuid.New()
	leaseToken := uuid.New()
	claim := &recordingTx{rows: &fakeRows{rows: [][]any{{
		eventID, "reservation", uuid.New(), "reservation.held", 1,
		json.RawMessage(`{}`), 2, time.Now().UTC(), leaseToken,
	}}}}
	retry := &recordingTx{}
	pool := &queuePool{transactions: []pgx.Tx{claim, retry}}
	processor, err := NewOutboxProcessor(&recordingPublisher{err: errors.New("broker secret")}, OutboxOptions{
		WorkerID: "relay-a", MaxAttempts: 5, ProcessingTimeout: time.Minute,
		RetryBase: time.Second, RetryMax: time.Minute,
		StatementTimeout: time.Second, LockTimeout: time.Second,
		Now: func() time.Time { return time.Unix(100, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewOutboxProcessor() error = %v", err)
	}

	processed, err := processor.Process(context.Background(), fakeHandle{
		id: sharding.ShardPhysicalOne, pool: pool,
	}, 1)
	if !errors.Is(err, ErrOutboxPublish) || processed != 1 {
		t.Fatalf("Process() = (%d, %v), want handled retry and ErrOutboxPublish", processed, err)
	}
	if strings.Contains(err.Error(), "broker secret") {
		t.Fatal("publisher error escaped the adapter")
	}
	retryCall := retry.findCall("status = $4")
	if retryCall == nil || !strings.Contains(retryCall.query, "lease_token = $3") {
		t.Fatalf("retry query did not fence local lease: %+v", retry.calls)
	}
	if got := retryCall.args[3]; got != "pending" {
		t.Fatalf("retry status = %v, want pending", got)
	}
}

func TestHoldExpirationProcessorUsesCurrentLocalFenceAfterStartupMetadataWasWriteDisabled(t *testing.T) {
	reservationID := uuid.New()
	trainRunID := uuid.New()
	now := time.Unix(200, 0).UTC()
	tx := &recordingTx{
		row: fakeRow{values: []any{reservationID, trainRunID, int64(7), int64(2)}},
		rowsAffected: map[string]int64{
			"UPDATE seat_inventory":                       2,
			"UPDATE reservations":                         1,
			"INSERT INTO outbox_events":                   1,
			"INSERT INTO train_run_target_write_evidence": 1,
		},
	}
	pool := &queuePool{transactions: []pgx.Tx{tx}}
	processor, err := NewHoldExpirationProcessor(HoldExpirationOptions{
		StatementTimeout: time.Second,
		LockTimeout:      time.Second,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewHoldExpirationProcessor() error = %v", err)
	}

	processed, err := processor.Process(context.Background(), writeHandle{
		fakeHandle: fakeHandle{id: sharding.ShardPhysicalZero, pool: pool},
		write:      false,
	}, 1)
	if err != nil || processed != 1 {
		t.Fatalf("Process() = (%d, %v), want (1, nil)", processed, err)
	}
	claim := tx.findCall("FOR UPDATE OF fence, reservation SKIP LOCKED")
	if claim == nil {
		t.Fatalf("reservation was not protected by a shard-local row lease: %+v", tx.calls)
	}
	if !strings.Contains(claim.query, "fence.state = 'active'") || !strings.Contains(claim.query, "fence.write_enabled") {
		t.Fatalf("expiration did not use the current database-local write fence: %s", claim.query)
	}
	outbox := tx.findCall("INSERT INTO outbox_events")
	if outbox == nil {
		t.Fatalf("expiration did not create shard-local event: %+v", tx.calls)
	}
	wantEventID := uuid.NewSHA1(reservationID, []byte("reservation-expired"))
	if got := outbox.args[0]; got != wantEventID {
		t.Fatalf("expiration event ID = %v, want deterministic global ID %v", got, wantEventID)
	}
}

type writeHandle struct {
	fakeHandle
	write bool
}

func (handle writeHandle) WriteEnabled() bool { return handle.write }

type recordingPublisher struct {
	events []domain.Event
	err    error
}

func (publisher *recordingPublisher) Publish(_ context.Context, event domain.Event) error {
	publisher.events = append(publisher.events, event)
	return publisher.err
}

type queuePool struct {
	mu           sync.Mutex
	transactions []pgx.Tx
}

func (pool *queuePool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.transactions) == 0 {
		return nil, errors.New("unexpected transaction")
	}
	tx := pool.transactions[0]
	pool.transactions = pool.transactions[1:]
	return tx, nil
}

func (*queuePool) Close() {}

type recordedCall struct {
	query string
	args  []any
}

type recordingTx struct {
	pgx.Tx
	mu           sync.Mutex
	calls        []recordedCall
	rows         pgx.Rows
	row          pgx.Row
	rowsAffected map[string]int64
}

func (tx *recordingTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.calls = append(tx.calls, recordedCall{query: query, args: append([]any(nil), args...)})
	rows := int64(1)
	for fragment, count := range tx.rowsAffected {
		if strings.Contains(query, fragment) {
			rows = count
			break
		}
	}
	return pgconn.NewCommandTag("UPDATE " + string(rune('0'+rows))), nil
}

func (tx *recordingTx) Query(_ context.Context, query string, args ...any) (pgx.Rows, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.calls = append(tx.calls, recordedCall{query: query, args: append([]any(nil), args...)})
	return tx.rows, nil
}

func (tx *recordingTx) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.calls = append(tx.calls, recordedCall{query: query, args: append([]any(nil), args...)})
	return tx.row
}

func (*recordingTx) Commit(context.Context) error   { return nil }
func (*recordingTx) Rollback(context.Context) error { return nil }

func (tx *recordingTx) findCall(fragment string) *recordedCall {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	for index := range tx.calls {
		if strings.Contains(tx.calls[index].query, fragment) {
			call := tx.calls[index]
			return &call
		}
	}
	return nil
}

type fakeRow struct {
	values []any
	err    error
}

func (row fakeRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	return assignValues(destinations, row.values)
}

type fakeRows struct {
	rows  [][]any
	index int
}

func (rows *fakeRows) Close()                                       {}
func (rows *fakeRows) Err() error                                   { return nil }
func (rows *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (rows *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (rows *fakeRows) Next() bool {
	if rows.index >= len(rows.rows) {
		return false
	}
	rows.index++
	return true
}
func (rows *fakeRows) Scan(destinations ...any) error {
	return assignValues(destinations, rows.rows[rows.index-1])
}
func (rows *fakeRows) Values() ([]any, error) { return rows.rows[rows.index-1], nil }
func (rows *fakeRows) RawValues() [][]byte    { return nil }
func (rows *fakeRows) Conn() *pgx.Conn        { return nil }

func assignValues(destinations, values []any) error {
	if len(destinations) != len(values) {
		return errors.New("scan arity mismatch")
	}
	for index := range destinations {
		destination := reflect.ValueOf(destinations[index])
		if destination.Kind() != reflect.Pointer || destination.IsNil() {
			return errors.New("scan destination is not a pointer")
		}
		value := reflect.ValueOf(values[index])
		if !value.Type().AssignableTo(destination.Elem().Type()) {
			return errors.New("scan type mismatch")
		}
		destination.Elem().Set(value)
	}
	return nil
}

var _ physical.Pool = (*queuePool)(nil)
