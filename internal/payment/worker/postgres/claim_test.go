package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestClaimsUseShortReadCommittedSkipLockedTransactions(t *testing.T) {
	t.Parallel()
	options := worker.ClaimOptions{
		WorkerID: "payment-test", BatchSize: 17, MaxAttempts: 8,
		LeaseTTL: 30 * time.Second, Now: time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC),
	}
	for _, test := range []struct {
		name string
		run  func(*Store) error
	}{
		{name: "operations", run: func(store *Store) error {
			_, err := store.ClaimOperations(context.Background(), options)
			return err
		}},
		{name: "webhooks", run: func(store *Store) error {
			_, err := store.ClaimWebhooks(context.Background(), options)
			return err
		}},
		{name: "actions", run: func(store *Store) error {
			_, err := store.ClaimActions(context.Background(), options)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tx := &recordingTx{}
			store, err := New(&recordingDB{tx: tx})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := test.run(store); err != nil {
				t.Fatalf("claim error = %v", err)
			}
			if !tx.committed || tx.rolledBack {
				t.Fatalf("transaction = committed %v rolledBack %v", tx.committed, tx.rolledBack)
			}
			if tx.options.IsoLevel != pgx.ReadCommitted {
				t.Fatalf("isolation = %s, want read committed", tx.options.IsoLevel)
			}
			joined := strings.Join(tx.queries, "\n")
			if !strings.Contains(joined, "FOR UPDATE") || !strings.Contains(joined, "SKIP LOCKED") ||
				!strings.Contains(joined, "lease_until") {
				t.Fatalf("claim SQL does not contain lock/lease controls:\n%s", joined)
			}
		})
	}
}

func TestOperationClaimRecoversInFlightAsUncertainNotRetryable(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = store.ClaimOperations(context.Background(), worker.ClaimOptions{
		WorkerID: "payment-test", BatchSize: 1, MaxAttempts: 3,
		LeaseTTL: time.Minute, Now: time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ClaimOperations() error = %v", err)
	}
	joined := strings.Join(tx.execs, "\n")
	if !strings.Contains(joined, "state='claimed' THEN 'pending' ELSE 'uncertain'") ||
		!strings.Contains(joined, "worker_lease_expired") {
		t.Fatalf("stale recovery SQL can blind-retry an in-flight side effect:\n%s", joined)
	}
}

func TestCompleteTicketIssuanceWritesAllLocatorsInSameControlTransaction(t *testing.T) {
	t.Parallel()
	trainRunID, ownerID := uuid.New(), uuid.New()
	tx := &recordingTx{row: staticRow{values: []any{trainRunID, "physical-shard-0", int64(7), ownerID}}}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	claim := worker.ActionClaim{
		SagaID: uuid.New(), Type: worker.ActionIssueTickets, Provider: "sandbox",
		LeaseOwner: "payment-test", LeaseUntil: time.Now().Add(time.Minute),
		Issue: shard.IssueTicketsCommand{
			ReservationID: uuid.New(), TrainRunID: trainRunID, OwnerID: ownerID,
			AmountMinor: 2500, Currency: "TWD",
		},
	}
	receipt := shard.IssueTicketsReceipt{
		TicketOrderID: uuid.New(), TicketIDs: []uuid.UUID{uuid.New(), uuid.New()},
		AmountMinor: 2500, Currency: "TWD", IssuedAt: time.Now().UTC(),
	}
	if err := store.CompleteAction(context.Background(), claim, worker.ActionEvidence{Issue: receipt}); err != nil {
		t.Fatalf("CompleteAction() error = %v", err)
	}
	joined := strings.ToLower(strings.Join(tx.execs, "\n"))
	if strings.Count(joined, "insert into public.ticket_order_shard_locators") != 1 ||
		strings.Count(joined, "insert into public.ticket_shard_locators") != len(receipt.TicketIDs) ||
		!strings.Contains(joined, "update public.payment_intents") ||
		!strings.Contains(joined, "update public.payment_sagas") || !tx.committed {
		t.Fatalf("control finalize SQL missing atomic locator writes:\n%s", joined)
	}
}

func TestCompleteCompensationCancelsTicketOrderLocator(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	claim := worker.ActionClaim{
		SagaID: uuid.New(), Type: worker.ActionCompensate, Provider: "sandbox",
		LeaseOwner: "payment-test", LeaseUntil: time.Now().Add(time.Minute),
		Compensation: shard.ApplyRefundCompensationCommand{ReservationID: uuid.New(), OwnerID: uuid.New()},
	}
	evidence := worker.ActionEvidence{Compensation: shard.ApplyRefundCompensationReceipt{TicketOrderID: uuid.New()}}
	if err := store.CompleteAction(context.Background(), claim, evidence); err != nil {
		t.Fatalf("CompleteAction() error = %v", err)
	}
	if !strings.Contains(strings.ToLower(strings.Join(tx.execs, "\n")), "update public.ticket_order_shard_locators") {
		t.Fatal("compensation did not cancel ticket-order locator")
	}
}

func TestVoidActionIdentityIsStableAndBindsProviderOperation(t *testing.T) {
	t.Parallel()
	sagaID, intentID, reservationID, trainRunID, ownerID, voidID :=
		uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	proof := make([]byte, 32)
	proof[0] = 7
	voidedAt := pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	base := worker.ActionClaim{SagaID: sagaID, Provider: "sandbox", Attempts: 1, LeaseOwner: "worker", LeaseUntil: time.Now().Add(time.Minute)}
	first, err := buildActionClaim(base, "compensating", "compensate", intentID, reservationID,
		trainRunID, ownerID, 2500, "TWD", pgtype.UUID{}, nil,
		pgtype.UUID{Bytes: voidID, Valid: true}, proof, voidedAt, pgtype.UUID{}, nil, pgtype.Timestamptz{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildActionClaim(base, "compensating", "compensate", intentID, reservationID,
		trainRunID, ownerID, 2500, "TWD", pgtype.UUID{}, nil,
		pgtype.UUID{Bytes: voidID, Valid: true}, proof, voidedAt, pgtype.UUID{}, nil, pgtype.Timestamptz{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Type != worker.ActionCancelVoided || first.CancelVoided.CommandID != second.CancelVoided.CommandID ||
		first.CancelVoided.RequestFingerprint != second.CancelVoided.RequestFingerprint {
		t.Fatalf("claims are not stable: first=%+v second=%+v", first, second)
	}
	changed, err := buildActionClaim(base, "compensating", "compensate", intentID, reservationID,
		trainRunID, ownerID, 2500, "TWD", pgtype.UUID{}, nil,
		pgtype.UUID{Bytes: uuid.New(), Valid: true}, proof, voidedAt, pgtype.UUID{}, nil, pgtype.Timestamptz{})
	if err != nil {
		t.Fatal(err)
	}
	if changed.CancelVoided.CommandID != first.CancelVoided.CommandID ||
		changed.CancelVoided.RequestFingerprint == first.CancelVoided.RequestFingerprint {
		t.Fatal("provider operation identity is not bound into the stable command receipt fingerprint")
	}
}

func TestCompleteVoidCancellationFinalizesOnlyCompensatingSaga(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	claim := worker.ActionClaim{
		SagaID: uuid.New(), Type: worker.ActionCancelVoided, Provider: "sandbox",
		LeaseOwner: "payment-test", LeaseUntil: time.Now().Add(time.Minute),
	}
	if err := store.CompleteAction(context.Background(), claim, worker.ActionEvidence{
		CancelVoided: shard.CancelVoidedReservationReceipt{TicketOrderID: uuid.New()},
	}); err != nil {
		t.Fatalf("CompleteAction() error = %v", err)
	}
	joined := strings.ToLower(strings.Join(tx.execs, "\n"))
	if !strings.Contains(joined, "intent.state='voided'") ||
		!strings.Contains(joined, "state='compensating'") ||
		!strings.Contains(joined, "state='compensated'") || !tx.committed {
		t.Fatalf("void finalization SQL = %s", joined)
	}
}

func TestVoidProviderEvidenceRejectsCapturedOrRefundedMoney(t *testing.T) {
	t.Parallel()
	claim := worker.OperationClaim{Type: domain.OperationVoid, AmountMinor: 2500, Currency: "TWD"}
	base := worker.OperationEvidence{
		ProviderPaymentID: "pay-1", ProviderOperationID: "void-1",
		AmountMinor: 2500, Currency: "TWD",
	}
	if !validProviderEvidence(claim, base) {
		t.Fatal("uncaptured void evidence was rejected")
	}
	base.CapturedMinor = 2500
	if validProviderEvidence(claim, base) {
		t.Fatal("captured void evidence was accepted")
	}
	base.CapturedMinor = 0
	base.RefundedMinor = 1
	if validProviderEvidence(claim, base) {
		t.Fatal("refunded void evidence was accepted")
	}
}

func TestProviderVoidSuccessSchedulesShardCancellation(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	claim := worker.OperationClaim{
		OperationID: uuid.New(), PaymentIntentID: uuid.New(), Provider: "sandbox",
		Type: domain.OperationVoid, AmountMinor: 2500, Currency: "TWD",
	}
	if err := applyOperationSuccess(context.Background(), tx, claim, worker.OperationEvidence{}); err != nil {
		t.Fatalf("applyOperationSuccess() error = %v", err)
	}
	assertVoidSchedulesShardAction(t, tx.execs)
}

func TestVerifiedVoidWebhookSchedulesSameShardCancellation(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{row: staticRow{values: []any{uuid.New(), "pending"}}}
	if err := applyProviderConfirmation(context.Background(), tx, uuid.New(), "sandbox", "void_pending", 2500, "TWD", worker.WebhookEvidence{
		Status: provider.StatusVoided, AmountMinor: 2500, Currency: "TWD",
	}); err != nil {
		t.Fatalf("applyProviderConfirmation() error = %v", err)
	}
	assertVoidSchedulesShardAction(t, tx.execs)
}

func TestVerifiedCapturedWebhookRacingVoidConvergesToRefund(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{row: staticRow{values: []any{uuid.New(), "pending"}}}
	if err := applyProviderConfirmation(context.Background(), tx, uuid.New(), "sandbox", "void_pending", 2500, "TWD", worker.WebhookEvidence{
		Status: provider.StatusCaptured, AmountMinor: 2500, Currency: "TWD", CapturedMinor: 2500,
	}); err != nil {
		t.Fatalf("applyProviderConfirmation() error = %v", err)
	}
	joined := strings.ToLower(strings.Join(tx.execs, "\n"))
	for _, expected := range []string{
		"superseded_by_capture", "state='refund_pending'", "state='refunding'",
		"current_step='refund'", "operation_type",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("captured webhook did not converge to refund (%s missing):\n%s", expected, joined)
		}
	}
	if strings.Contains(joined, "issue_tickets") || strings.Contains(joined, "state='issuing_tickets'") {
		t.Fatalf("captured webhook regressed into issuance:\n%s", joined)
	}
}

func TestUncertainVoidCapturePivotIsOneControlTransaction(t *testing.T) {
	t.Parallel()
	leaseOwner := "payment-test"
	tx := &recordingTx{queryRows: []pgx.Row{
		staticRow{values: []any{"void_pending", "compensating", "void", leaseOwner}},
		staticRow{values: []any{uuid.New(), "pending"}},
	}}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	claim := worker.OperationClaim{
		OperationID: uuid.New(), PaymentIntentID: uuid.New(), Provider: "sandbox",
		Type: domain.OperationVoid, PreviousState: domain.OperationUncertain,
		ProviderPaymentID: "pay-1", AmountMinor: 2500, Currency: "TWD",
		LeaseOwner: leaseOwner, LeaseUntil: time.Now().Add(time.Minute),
	}
	evidence := worker.OperationEvidence{
		ProviderPaymentID: "pay-1", Status: provider.StatusCaptured,
		AmountMinor: 2500, Currency: "TWD", CapturedMinor: 2500,
		ResponseFingerprint: [32]byte{1},
	}
	if err := store.SupersedeVoidWithRefund(context.Background(), claim, evidence); err != nil {
		t.Fatalf("SupersedeVoidWithRefund() error = %v", err)
	}
	joined := strings.ToLower(strings.Join(tx.execs, "\n"))
	for _, expected := range []string{
		"superseded_by_capture", "state='failed_permanent'", "state='refund_pending'",
		"state='refunding'", "current_step='refund'", "'refund'",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("atomic pivot missing %s:\n%s", expected, joined)
		}
	}
	if strings.Contains(joined, "payment_manual_review_cases") || !tx.committed {
		t.Fatalf("pivot opened a review case or did not commit: committed=%v\n%s", tx.committed, joined)
	}
}

func assertVoidSchedulesShardAction(t *testing.T, statements []string) {
	t.Helper()
	joined := strings.ToLower(strings.Join(statements, "\n"))
	if !strings.Contains(joined, "state='voided'") ||
		!strings.Contains(joined, "current_step='compensate'") ||
		!strings.Contains(joined, "next_attempt_at=clock_timestamp()") {
		t.Fatalf("void did not schedule shard action:\n%s", joined)
	}
	if strings.Contains(joined, "state='manual_review'") || strings.Contains(joined, "payment_manual_review_cases") {
		t.Fatalf("void incorrectly entered manual review:\n%s", joined)
	}
}

type recordingDB struct{ tx *recordingTx }

func (db *recordingDB) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	db.tx.options = options
	return db.tx, nil
}

type recordingTx struct {
	pgx.Tx
	options    pgx.TxOptions
	execs      []string
	queries    []string
	committed  bool
	rolledBack bool
	row        pgx.Row
	queryRows  []pgx.Row
	rowIndex   int
}

func (tx *recordingTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, sql)
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (tx *recordingTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	tx.queries = append(tx.queries, sql)
	return &emptyRows{}, nil
}

func (tx *recordingTx) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	if tx.rowIndex < len(tx.queryRows) {
		row := tx.queryRows[tx.rowIndex]
		tx.rowIndex++
		return row
	}
	return tx.row
}

func (tx *recordingTx) Commit(context.Context) error {
	tx.committed = true
	return nil
}

func (tx *recordingTx) Rollback(context.Context) error {
	if !tx.committed {
		tx.rolledBack = true
	}
	return nil
}

type emptyRows struct{ pgx.Rows }

func (*emptyRows) Close()                                       {}
func (*emptyRows) Err() error                                   { return nil }
func (*emptyRows) Next() bool                                   { return false }
func (*emptyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*emptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*emptyRows) Values() ([]any, error)                       { return nil, nil }
func (*emptyRows) RawValues() [][]byte                          { return nil }
func (*emptyRows) Conn() *pgx.Conn                              { return nil }

type staticRow struct{ values []any }

func (row staticRow) Scan(destinations ...any) error {
	if len(destinations) != len(row.values) {
		return errors.New("unexpected scan width")
	}
	for index := range destinations {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
