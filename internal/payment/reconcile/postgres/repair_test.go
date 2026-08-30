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

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
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
		AmountMinor: evidence.AmountMinor, Currency: evidence.Currency,
		OrderCreatedAt: time.Now().Add(-time.Minute), IssuedAt: time.Now(),
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
	tx := &beginRepairTx{rows: []pgx.Row{beginRow(func(dest []any) { *(dest[0].(*bool)) = false })}}
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
	markTx := &beginRepairTx{rows: []pgx.Row{beginRow(func(dest []any) { *(dest[0].(*bool)) = false })}}
	markRepairer := &Repairer{control: &beginControl{tx: markTx}}
	markEvidence := repairEvidence{
		SagaID: uuid.New(), IntentID: uuid.New(), IntentState: "manual_review",
		SagaState: "manual_review", Step: "refund", BoundedErrorCategory: "database_finalize_failed",
	}
	if owner, err := markRepairer.claimRepairSaga(context.Background(), markEvidence, "mark_refund_pending"); err != nil || owner == "" || !markTx.committed || len(markTx.execs) != 3 {
		t.Fatalf("mark owner=%q err=%v committed=%v execs=%d", owner, err, markTx.committed, len(markTx.execs))
	}
}

func TestRepairAndManualReviewFailClosedOnActiveActionLease(t *testing.T) {
	t.Parallel()
	evidence := repairEvidence{
		SagaID: uuid.New(), IntentID: uuid.New(), IntentState: "ticket_issue_pending",
		SagaState: "issuing_tickets", Step: "issue_tickets",
	}
	repairTx := &beginRepairTx{rows: []pgx.Row{beginRow(func(dest []any) { *(dest[0].(*bool)) = true })}}
	repairer := &Repairer{control: &beginControl{tx: repairTx}}
	if _, err := repairer.claimRepairSaga(context.Background(), evidence, "issue_tickets"); !errors.Is(err, paymentreconcile.ErrRepairUnavailable) {
		t.Fatalf("active action repair error=%v", err)
	}
	if repairTx.committed || len(repairTx.execs) != 0 {
		t.Fatalf("active action repair mutated control: committed=%v execs=%v", repairTx.committed, repairTx.execs)
	}

	manualTx := &beginRepairTx{rows: []pgx.Row{beginRow(func(dest []any) { *(dest[0].(*bool)) = true })}}
	manual := &Repairer{control: &beginControl{tx: manualTx}}
	if err := manual.MarkManualReview(context.Background(), evidence.IntentID); !errors.Is(err, paymentreconcile.ErrRepairUnavailable) {
		t.Fatalf("active action manual-review error=%v", err)
	}
	if manualTx.committed || len(manualTx.execs) != 0 {
		t.Fatalf("active action manual review mutated control: committed=%v execs=%v", manualTx.committed, manualTx.execs)
	}
}

func TestRepairSourceKeepsLeaseAndLocatorConflictGuards(t *testing.T) {
	// This assertion is deliberately narrow: these SQL predicates are the
	// fail-closed boundary between an idempotent shard replay and control-plane
	// finalization. Removing one must break the focused repair test.
	source := repairSourceForAssertion(t)
	for _, fragment := range []string{
		"lease_owner=$3 AND lease_until>=clock_timestamp()",
		"lockSagaActionBoundary(ctx, tx",
		"active_action.state='processing'",
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

func TestRepairIssuanceCommitsLedgerWithControlFinalization(t *testing.T) {
	t.Parallel()
	evidence := repairEvidence{
		SagaID:        uuid.MustParse("79000000-0000-4000-8000-000000000001"),
		IntentID:      uuid.MustParse("75000000-0000-4000-8000-000000000001"),
		ReservationID: uuid.New(), TrainRunID: uuid.New(), OwnerID: uuid.New(),
		AmountMinor: 12500, Currency: "TWD",
	}
	command := issueCommand(evidence)
	receipt := paymentshard.IssueTicketsReceipt{
		CommandID: command.CommandID, IssuanceID: command.IssuanceID, PaymentIntentID: evidence.IntentID,
		ReservationID: evidence.ReservationID, TicketOrderID: uuid.New(), TicketIDs: []uuid.UUID{uuid.New()},
		TicketCodes: []string{"ticket_code_000001"}, AmountMinor: evidence.AmountMinor, Currency: evidence.Currency,
		OrderCreatedAt: time.Now().Add(-time.Minute), IssuedAt: time.Now(),
	}
	tx := &beginRepairTx{rows: []pgx.Row{beginRow(func(dest []any) {
		*(dest[0].(*string)) = "ticket_issue_pending"
		*(dest[1].(*string)) = "issuing_tickets"
		*(dest[2].(*string)) = "issue_tickets"
		*(dest[3].(*uuid.UUID)) = evidence.TrainRunID
		*(dest[4].(*string)) = "booking-shard-1"
		*(dest[5].(*int64)) = 4
		*(dest[6].(*uuid.UUID)) = evidence.OwnerID
		*(dest[7].(*bool)) = true
	})}}
	repairer := &Repairer{control: &beginControl{tx: tx}}
	if err := repairer.finalizeIssue(context.Background(), evidence, command, receipt, "repair-owner"); err != nil {
		t.Fatalf("finalizeIssue() error = %v", err)
	}
	if !tx.committed {
		t.Fatal("issuance control fact and ledger did not commit together")
	}
	joined := strings.Join(tx.execs, "\n")
	for _, fragment := range []string{
		"INSERT INTO public.ticket_order_shard_locators", "INSERT INTO public.ticket_shard_locators",
		"INSERT INTO public.financial_ledger_transactions", "state='completed'",
	} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("atomic issuance finalization missing %q:\n%s", fragment, joined)
		}
	}
	assertRepairPostings(t, tx.execArgs, ledger.AccountCustomerFundsPending, ledger.AccountTicketSales)
	for index, query := range tx.execs {
		if !strings.Contains(query, "INSERT INTO public.financial_ledger_transactions") {
			continue
		}
		args := tx.execArgs[index]
		if args[0] != uuid.MustParse("f020d39d-d1cb-5f81-955e-e84dc3fa6244") ||
			args[1] != "ticket_issuance:ddb62b09-9c50-526a-adb4-e32a16aa7c66" ||
			args[2] != "payment:75000000-0000-4000-8000-000000000001" {
			t.Fatalf("repair issuance ledger identity args = %+v", args)
		}
		if got := repairHex(args[5].([]byte)); got != "d9e0ae58551a9829103246f613d371b754af2a68fec8bf8c011b36d1ce459227" {
			t.Fatalf("repair issuance fingerprint = %s", got)
		}
		return
	}
	t.Fatal("repair issuance ledger insert not found")
}

func repairHex(value []byte) string {
	const digits = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, item := range value {
		encoded[index*2] = digits[item>>4]
		encoded[index*2+1] = digits[item&0x0f]
	}
	return string(encoded)
}

func TestRepairRefundCommitsLedgerWithControlFinalization(t *testing.T) {
	t.Parallel()
	evidence := repairEvidence{
		SagaID: uuid.New(), IntentID: uuid.New(), ReservationID: uuid.New(), OwnerID: uuid.New(), OperationID: uuid.New(),
		AmountMinor: 2500, Currency: "TWD", CompletedAt: time.Now(),
	}
	command := paymentshard.ApplyRefundCompensationCommand{CommandID: uuid.New(), CompensationID: uuid.New()}
	receipt := paymentshard.ApplyRefundCompensationReceipt{
		CommandID: command.CommandID, CompensationID: command.CompensationID, PaymentIntentID: evidence.IntentID,
		ReservationID: evidence.ReservationID, TicketOrderID: uuid.New(), ReleasedSeatCount: 1, CancelledTicketCount: 1,
	}
	tx := &beginRepairTx{rows: []pgx.Row{beginRow(func(dest []any) {
		*(dest[0].(*string)) = "refunded"
		*(dest[1].(*string)) = "refunding"
		*(dest[2].(*string)) = "compensate"
		*(dest[3].(*bool)) = true
	})}}
	repairer := &Repairer{control: &beginControl{tx: tx}}
	if err := repairer.finalizeCompensation(context.Background(), evidence, command, receipt, "repair-owner"); err != nil {
		t.Fatalf("finalizeCompensation() error = %v", err)
	}
	if !tx.committed {
		t.Fatal("refund control fact and ledger did not commit together")
	}
	joined := strings.Join(tx.execs, "\n")
	for _, fragment := range []string{
		"UPDATE public.ticket_order_shard_locators SET status='cancelled'",
		"INSERT INTO public.financial_ledger_transactions", "state='cancelled'", "state='compensated'",
	} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("atomic refund finalization missing %q:\n%s", fragment, joined)
		}
	}
	assertRepairPostings(t, tx.execArgs, ledger.AccountTicketSales, ledger.AccountProviderRefundReceivable)
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
	execArgs  [][]any
	committed bool
}

func (tx *beginRepairTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	if strings.Contains(query, "FROM public.financial_ledger_transactions") {
		return beginErrorRow{err: pgx.ErrNoRows}
	}
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}
func (tx *beginRepairTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, query)
	tx.execArgs = append(tx.execArgs, append([]any(nil), args...))
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (tx *beginRepairTx) Commit(context.Context) error { tx.committed = true; return nil }
func (*beginRepairTx) Rollback(context.Context) error  { return nil }

type beginRow func([]any)

func (row beginRow) Scan(dest ...any) error { row(dest); return nil }

type beginErrorRow struct{ err error }

func (row beginErrorRow) Scan(...any) error { return row.err }

func assertRepairPostings(t *testing.T, args [][]any, debit, credit ledger.Account) {
	t.Helper()
	var sawDebit, sawCredit bool
	for _, values := range args {
		if len(values) != 6 {
			continue
		}
		sawDebit = sawDebit || values[2] == debit && values[3] == ledger.Debit
		sawCredit = sawCredit || values[2] == credit && values[3] == ledger.Credit
	}
	if !sawDebit || !sawCredit {
		t.Fatalf("ledger postings missing debit=%s credit=%s: %+v", debit, credit, args)
	}
}
