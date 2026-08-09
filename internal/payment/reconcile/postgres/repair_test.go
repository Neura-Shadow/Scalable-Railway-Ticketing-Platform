package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestIssuanceRetrySafeCategoryFailsClosed(t *testing.T) {
	for _, category := range []string{"provider_outcome_unknown", "provider_permanent", "shard_receipt_conflict", "operator_requested", ""} {
		if issuanceRetrySafeCategory(category) {
			t.Fatalf("unsafe manual-review category accepted: %q", category)
		}
	}
	for _, category := range []string{"shard_command_failed", "database_finalize_failed"} {
		if !issuanceRetrySafeCategory(category) {
			t.Fatalf("issuance retry category rejected: %q", category)
		}
	}
}

func TestProviderOperationRetryRequiresWorkerClaimableSagaState(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		operation, state, step string
		want                   bool
	}{
		{"create_checkout", "reservation_secured", "create_checkout", true},
		{"authorize", "awaiting_provider", "await_provider", true},
		{"capture", "authorized", "capture", true},
		{"void", "compensating", "void", true},
		{"refund", "refunding", "refund", true},
		{"capture", "manual_review", "capture", false},
		{"create_checkout", "reservation_secured", "await_provider", false},
		{"authorize", "awaiting_provider", "capture", false},
		{"capture", "authorized", "await_provider", false},
		{"refund", "compensating", "refund", false},
		{"void", "compensating", "refund", false},
	} {
		if got := providerOperationClaimable(test.operation, test.state, test.step); got != test.want {
			t.Fatalf("providerOperationClaimable(%q,%q,%q)=%v want %v", test.operation, test.state, test.step, got, test.want)
		}
	}
}

func TestRepairReceiptValidationFailsClosed(t *testing.T) {
	t.Parallel()
	evidence := repairEvidence{IntentID: uuid.New(), ReservationID: uuid.New(), OperationID: uuid.New(), AmountMinor: 700, Currency: "TWD"}
	issueCommand := paymentshard.IssueTicketsCommand{CommandID: uuid.New(), IssuanceID: uuid.New()}
	ticketID := uuid.New()
	issue := paymentshard.IssueTicketsReceipt{
		CommandID: issueCommand.CommandID, IssuanceID: issueCommand.IssuanceID, PaymentIntentID: evidence.IntentID,
		ReservationID: evidence.ReservationID, TicketOrderID: uuid.New(), TicketIDs: []uuid.UUID{ticketID},
		TicketCodes: []string{"ticket_code_000001"},
		AmountMinor: evidence.AmountMinor, Currency: evidence.Currency, IssuedAt: time.Now(),
	}
	if !validIssueRepairReceipt(evidence, issueCommand, issue) {
		t.Fatal("valid issue receipt rejected")
	}
	issue.TicketIDs = []uuid.UUID{ticketID, ticketID}
	issue.TicketCodes = []string{"ticket_code_000001", "ticket_code_000002"}
	if validIssueRepairReceipt(evidence, issueCommand, issue) {
		t.Fatal("duplicate ticket receipt accepted")
	}

	voidCommand := paymentshard.CancelVoidedReservationCommand{CommandID: uuid.New()}
	voidReceipt := paymentshard.CancelVoidedReservationReceipt{
		CommandID: voidCommand.CommandID, VoidOperationID: evidence.OperationID, PaymentIntentID: evidence.IntentID,
		ReservationID: evidence.ReservationID, TicketOrderID: uuid.New(), ReleasedSeatCount: 1, CancelledAt: time.Now(),
	}
	if !validVoidRepairReceipt(evidence, voidCommand, voidReceipt) {
		t.Fatal("valid void receipt rejected")
	}
	voidReceipt.ReleasedSeatCount = 0
	if validVoidRepairReceipt(evidence, voidCommand, voidReceipt) {
		t.Fatal("zero-release void receipt accepted")
	}

	compensationCommand := paymentshard.ApplyRefundCompensationCommand{CommandID: uuid.New(), CompensationID: uuid.New()}
	compensation := paymentshard.ApplyRefundCompensationReceipt{
		CommandID: compensationCommand.CommandID, CompensationID: compensationCommand.CompensationID,
		PaymentIntentID: evidence.IntentID, ReservationID: evidence.ReservationID, TicketOrderID: uuid.New(), ReleasedSeatCount: 1,
	}
	if !validCompensationRepairReceipt(evidence, compensationCommand, compensation) {
		t.Fatal("valid compensation receipt rejected")
	}
	compensation.TicketOrderID = uuid.Nil
	if validCompensationRepairReceipt(evidence, compensationCommand, compensation) {
		t.Fatal("missing-order compensation receipt accepted")
	}
}

func TestDatabaseFinalizeManualReviewCanReclaimExactReceiptPath(t *testing.T) {
	t.Parallel()
	tx := &beginRepairTx{}
	repairer := &Repairer{control: &beginControl{tx: tx}}
	evidence := repairEvidence{
		SagaID: uuid.New(), IntentID: uuid.New(), IntentState: "manual_review",
		SagaState: "manual_review", Step: "issue_tickets", BoundedErrorCategory: "database_finalize_failed",
	}
	owner, err := repairer.claimRepairSaga(context.Background(), evidence, "issue_tickets")
	if err != nil || owner == "" || !tx.committed || len(tx.execs) != 3 {
		t.Fatalf("owner=%q err=%v committed=%v execs=%d", owner, err, tx.committed, len(tx.execs))
	}
	unsafe := evidence
	unsafe.BoundedErrorCategory = "provider_outcome_unknown"
	if _, err := repairer.claimRepairSaga(context.Background(), unsafe, "issue_tickets"); !errors.Is(err, paymentreconcile.ErrRepairUnavailable) {
		t.Fatalf("unsafe category error=%v", err)
	}
	markTx := &beginRepairTx{}
	markRepairer := &Repairer{control: &beginControl{tx: markTx}}
	markEvidence := repairEvidence{
		SagaID: uuid.New(), IntentID: uuid.New(), IntentState: "manual_review",
		SagaState: "manual_review", Step: "refund", BoundedErrorCategory: "database_finalize_failed",
	}
	if owner, err := markRepairer.claimRepairSaga(context.Background(), markEvidence, "mark_refund_pending"); err != nil || owner == "" || !markTx.committed || len(markTx.execs) != 3 {
		t.Fatalf("mark owner=%q err=%v committed=%v execs=%d", owner, err, markTx.committed, len(markTx.execs))
	}
}

func TestRepairSourceKeepsLeaseAndLocatorConflictGuards(t *testing.T) {
	// This assertion is deliberately narrow: these SQL predicates are the
	// fail-closed boundary between an idempotent shard replay and control-plane
	// finalization. Removing one must break the focused repair test.
	source := repairSourceForAssertion(t)
	for _, fragment := range []string{
		"lease_owner=$3 AND lease_until>=clock_timestamp()",
		"ON CONFLICT(ticket_order_id) DO UPDATE SET status=EXCLUDED.status",
		"ticket_order_shard_locators.reservation_id=EXCLUDED.reservation_id",
		"ON CONFLICT(ticket_id) DO UPDATE SET ticket_order_id=EXCLUDED.ticket_order_id",
		"ticket_shard_locators.reservation_id=EXCLUDED.reservation_id",
		"INSERT INTO public.ticket_code_directory(ticket_code,ticket_id)",
		"ticket_code_directory.ticket_id=EXCLUDED.ticket_id",
		"finalizeTerminalCompensation(ctx, e, \"voided\", \"compensating\", receipt.TicketOrderID, false, leaseOwner)",
	} {
		if !strings.Contains(source, fragment) {
			t.Fatalf("repair boundary missing %q", fragment)
		}
	}
}

func repairSourceForAssertion(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repair test source")
	}
	data, err := os.ReadFile(strings.TrimSuffix(testFile, "_test.go") + ".go")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestBeginReceiptReplayConvergesAfterControlFinalizeFailure(t *testing.T) {
	intentID, sagaID := uuid.New(), uuid.New()
	reservationID, trainRunID, ownerID := uuid.New(), uuid.New(), uuid.New()
	fingerprint := sha256.Sum256([]byte("durable begin receipt"))
	operationID, operationHash := checkoutOperationIdentity(intentID)
	tx := &beginRepairTx{rows: []pgx.Row{
		beginRow(func(dest []any) {
			*(dest[0].(*uuid.UUID)) = sagaID
			*(dest[1].(*uuid.UUID)) = reservationID
			*(dest[2].(*uuid.UUID)) = trainRunID
			*(dest[3].(*uuid.UUID)) = ownerID
			*(dest[4].(*string)) = "reservation_securing"
			*(dest[5].(*string)) = "created"
			*(dest[6].(*string)) = "secure_reservation"
			*(dest[7].(*string)) = "sandbox"
			*(dest[8].(*string)) = "TWD"
			*(dest[9].(*int64)) = 700
			*(dest[10].(*[]byte)) = append([]byte(nil), fingerprint[:]...)
			*(dest[11].(*string)) = "active"
			*(dest[12].(*bool)) = false
		}),
		beginRow(func(dest []any) {
			*(dest[0].(*uuid.UUID)) = operationID
			*(dest[1].(*[]byte)) = append([]byte(nil), operationHash[:]...)
			*(dest[2].(*int64)) = 700
			*(dest[3].(*string)) = "TWD"
			*(dest[4].(*string)) = "sandbox"
		}),
	}}
	verifier := &beginVerifier{}
	repairer := &Repairer{control: &beginControl{tx: tx}, gateway: beginGateway{}, begin: verifier}
	recorded := paymentreconcile.RecordedCommand{ID: beginPaymentCommandID(sagaID), Kind: "finalize_reservation_begin", Fingerprint: fingerprint}

	if err := repairer.ReplayRecordedCommand(context.Background(), intentID, recorded); err != nil {
		t.Fatal(err)
	}
	if !tx.committed || !verifier.called || verifier.intentID != intentID || verifier.commandID != recorded.ID || len(tx.execs) != 4 {
		t.Fatalf("committed=%v verifier=%+v execs=%d", tx.committed, verifier, len(tx.execs))
	}
	joined := strings.Join(tx.execs, "\n")
	for _, fragment := range []string{"state='checkout_pending'", "state='reservation_secured'", "'create_checkout'", "payment_reconciliation_checkpoints"} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("finalization SQL missing %q: %s", fragment, joined)
		}
	}
}

func TestBeginReceiptReplayFailsClosedWhenShardEvidenceChanges(t *testing.T) {
	verifier := &beginVerifier{err: errors.New("receipt mismatch")}
	repairer := &Repairer{control: &beginControl{}, gateway: beginGateway{}, begin: verifier}
	err := repairer.ReplayRecordedCommand(context.Background(), uuid.New(), paymentreconcile.RecordedCommand{
		ID: uuid.New(), Kind: "finalize_reservation_begin", Fingerprint: sha256.Sum256([]byte("receipt")),
	})
	if !errors.Is(err, paymentreconcile.ErrRepairUnavailable) {
		t.Fatalf("error=%v", err)
	}
}

type beginVerifier struct {
	called              bool
	intentID, commandID uuid.UUID
	err                 error
}

func (v *beginVerifier) VerifyBeginPaymentReceipt(_ context.Context, intentID, commandID uuid.UUID, _ [sha256.Size]byte) error {
	v.called, v.intentID, v.commandID = true, intentID, commandID
	return v.err
}

type beginGateway struct{}

func (beginGateway) IssueTickets(context.Context, paymentshard.IssueTicketsCommand) (paymentshard.IssueTicketsReceipt, error) {
	return paymentshard.IssueTicketsReceipt{}, errors.New("unexpected shard mutation")
}
func (beginGateway) MarkRefundPending(context.Context, paymentshard.MarkRefundPendingCommand) (paymentshard.MarkRefundPendingReceipt, error) {
	return paymentshard.MarkRefundPendingReceipt{}, errors.New("unexpected shard mutation")
}
func (beginGateway) CancelVoidedReservation(context.Context, paymentshard.CancelVoidedReservationCommand) (paymentshard.CancelVoidedReservationReceipt, error) {
	return paymentshard.CancelVoidedReservationReceipt{}, errors.New("unexpected shard mutation")
}
func (beginGateway) ApplyRefundCompensation(context.Context, paymentshard.ApplyRefundCompensationCommand) (paymentshard.ApplyRefundCompensationReceipt, error) {
	return paymentshard.ApplyRefundCompensationReceipt{}, errors.New("unexpected shard mutation")
}

type beginControl struct{ tx pgx.Tx }

func (c *beginControl) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return c.tx, nil }
func (*beginControl) Query(context.Context, string, ...any) (pgx.Rows, error)  { return nil, nil }
func (*beginControl) QueryRow(context.Context, string, ...any) pgx.Row {
	return beginRow(func([]any) {})
}

type beginRepairTx struct {
	pgx.Tx
	rows      []pgx.Row
	execs     []string
	committed bool
}

func (tx *beginRepairTx) QueryRow(context.Context, string, ...any) pgx.Row {
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}
func (tx *beginRepairTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, query)
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (tx *beginRepairTx) Commit(context.Context) error { tx.committed = true; return nil }
func (*beginRepairTx) Rollback(context.Context) error  { return nil }

type beginRow func([]any)

func (row beginRow) Scan(dest ...any) error { row(dest); return nil }
