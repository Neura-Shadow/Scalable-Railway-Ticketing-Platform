package reconcile

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/google/uuid"
)

func TestInspectPaymentDetectsCrossBoundaryAndProviderMismatches(t *testing.T) {
	id := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	store := healthyStore(id)
	store.control.Operations = append(store.control.Operations, Operation{
		ID: uuid.New(), Type: "capture", State: "uncertain", AmountMinor: 700, Currency: "TWD",
	})
	store.control.ActiveReconciliationCases = 0
	store.shard.TicketOrderState = "payment_captured"
	store.shard.IssuanceReceiptFound = false
	store.shard.ActiveTicketCount = 1
	client := &fakeProvider{payment: provider.Payment{
		Status: provider.StatusAuthorized, AmountMinor: 700, Currency: "TWD",
	}}
	reconciler := newTestReconciler(t, store, fakeRegistry{client: client}, nil)

	report, err := reconciler.InspectPayment(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"captured_payment_without_ticket", "completed_payment_without_issued_ticket_order",
		"duplicate_capture_operation", "provider_capture_mismatch",
		"uncertain_operation_without_reconciliation",
	}
	if got := findingCodes(report); !reflect.DeepEqual(got, want) {
		t.Fatalf("findings = %v, want %v", got, want)
	}
	if !report.ProviderQueried || client.calls != 1 {
		t.Fatalf("provider query: report=%v calls=%d", report.ProviderQueried, client.calls)
	}
	if len(store.checkpoints) != 0 || len(store.escalations) != 0 {
		t.Fatal("inspect must be read-only")
	}
}

func TestReconcileAllIsReadOnlyAndDurablyEscalatesBoundedFindings(t *testing.T) {
	id := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	store := healthyStore(id)
	store.candidates = []uuid.UUID{id, id, uuid.Nil}
	store.shard.ActiveTicketCount = 0
	reconciler := newTestReconciler(t, store, fakeRegistry{client: &fakeProvider{payment: healthyProviderPayment()}}, nil)

	result, err := reconciler.ReconcileAll(context.Background(), Options{Scope: ScopeAll, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ReadOnly || result.RowsExamined != 1 || result.MismatchCount != 1 || result.RepairCount != 0 || result.ManualReviews != 1 {
		t.Fatalf("result = %+v", result)
	}
	if len(store.checkpoints) != 1 || len(store.finished) != 1 || store.finished[0].MismatchCount != 1 {
		t.Fatalf("checkpoint evidence missing: start=%d finish=%+v", len(store.checkpoints), store.finished)
	}
	if len(store.escalations) != 1 || store.escalations[0].reason != "confirmed_reservation_without_active_ticket" {
		t.Fatalf("escalations = %+v", store.escalations)
	}
}

func TestRowsAndMismatchCountersCountIntentsNotFindings(t *testing.T) {
	id := uuid.New()
	store := healthyStore(id)
	store.candidates = []uuid.UUID{id}
	store.shard.TicketOrderFound = false
	store.shard.TicketOrderState = ""
	store.shard.IssuanceReceiptFound = false
	store.shard.ActiveTicketCount = 0
	reconciler := newTestReconciler(t, store, fakeRegistry{client: &fakeProvider{payment: healthyProviderPayment()}}, nil)

	result, err := reconciler.ReconcileAll(context.Background(), Options{Scope: ScopeAll, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Reports) != 1 || len(result.Reports[0].Findings) < 2 || result.RowsExamined != 1 || result.MismatchCount != 1 {
		t.Fatalf("result=%+v", result)
	}
}

func TestInspectPaymentDetectsPartialTicketIssuance(t *testing.T) {
	id := uuid.MustParse("23232323-2323-4232-8232-232323232323")
	store := healthyStore(id)
	store.shard.ReservationSeatCount = 2
	reconciler := newTestReconciler(t, store, fakeRegistry{client: &fakeProvider{payment: healthyProviderPayment()}}, nil)

	report, err := reconciler.InspectPayment(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got := findingCodes(report); !reflect.DeepEqual(got, []string{"ticket_count_mismatch"}) {
		t.Fatalf("findings = %v", got)
	}
}

func TestCheckShardDetectsCommittedReceiptsBeforeControlFinalization(t *testing.T) {
	intentID := uuid.New()
	cases := []struct {
		name    string
		control ControlSnapshot
		shard   ShardSnapshot
		want    string
	}{
		{
			name: "begin payment",
			control: ControlSnapshot{Intent: Intent{ID: intentID, State: "reservation_securing", AmountMinor: 700, Currency: "TWD",
				Fingerprint: sha256.Sum256([]byte("begin"))}, Saga: Saga{State: "created"}},
			shard: ShardSnapshot{Found: true, DirectoryResolved: true, ReservationState: "payment_pending", ReservationAmountMinor: 700,
				ReservationCurrency: "TWD", BeginReceiptFound: true, ReceiptFingerprint: sha256.Sum256([]byte("begin"))},
			want: "begin_payment_shard_committed_control_incomplete",
		},
		{
			name: "issuance",
			control: ControlSnapshot{Intent: Intent{ID: intentID, State: "ticket_issue_pending", AmountMinor: 700, Currency: "TWD"},
				Saga: Saga{State: "issuing_tickets", Step: "issue_tickets"}, Operations: []Operation{{Type: "capture", State: "succeeded"}}},
			shard: ShardSnapshot{Found: true, DirectoryResolved: true, ReservationState: "confirmed", ReservationAmountMinor: 700, ReservationCurrency: "TWD",
				ReservationSeatCount: 1, TicketOrderFound: true, TicketOrderState: "issued", TicketOrderAmountMinor: 700, TicketOrderCurrency: "TWD",
				IssuanceReceiptFound: true, IssuancePaymentIntentID: intentID, ActiveTicketCount: 1},
			want: "issuance_receipt_control_not_finalized",
		},
		{
			name: "refund",
			control: ControlSnapshot{Intent: Intent{ID: intentID, State: "refunded", AmountMinor: 700, Currency: "TWD"},
				Saga: Saga{State: "refunding", Step: "compensate"}, Operations: []Operation{{Type: "capture", State: "succeeded"}, {Type: "refund", State: "succeeded"}}},
			shard: ShardSnapshot{Found: true, DirectoryResolved: true, ReservationState: "cancelled", ReservationAmountMinor: 700, ReservationCurrency: "TWD",
				ReservationSeatCount: 1, TicketOrderFound: true, TicketOrderState: "cancelled", TicketOrderAmountMinor: 700, TicketOrderCurrency: "TWD",
				IssuanceReceiptFound: true, IssuancePaymentIntentID: intentID, CancelledTicketCount: 1, CancellationReceiptFound: true, CompensationReceiptFound: true},
			want: "refund_receipt_control_not_finalized",
		},
		{
			name: "void without ticket locator",
			control: ControlSnapshot{Intent: Intent{ID: intentID, State: "voided", AmountMinor: 700, Currency: "TWD"},
				Saga: Saga{State: "compensating", Step: "compensate"}, Operations: []Operation{{Type: "void", State: "succeeded"}}},
			shard: ShardSnapshot{Found: true, DirectoryResolved: true, ReservationState: "cancelled", ReservationAmountMinor: 700, ReservationCurrency: "TWD",
				ReservationSeatCount: 1, CancellationReceiptFound: true},
			want: "void_receipt_control_not_finalized",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var findings []string
			checkShard(ScopeAll, test.control, test.shard, func(code string, _ bool) { findings = append(findings, code) })
			if !containsString(findings, test.want) {
				t.Fatalf("findings=%v, want %s", findings, test.want)
			}
		})
	}
}

func TestCheckShardAcceptsHealthyRefundedTerminalStateWithRetainedReceipts(t *testing.T) {
	intentID := uuid.New()
	control := ControlSnapshot{
		Intent: Intent{ID: intentID, State: "cancelled", AmountMinor: 700, Currency: "TWD"},
		Saga:   Saga{State: "compensated", Step: "done"},
		Operations: []Operation{
			{Type: "capture", State: "succeeded", AmountMinor: 700, Currency: "TWD"},
			{Type: "refund", State: "succeeded", AmountMinor: 700, Currency: "TWD"},
		},
	}
	shard := ShardSnapshot{
		Found: true, DirectoryResolved: true, ReservationState: "cancelled",
		ReservationAmountMinor: 700, ReservationCurrency: "TWD", ReservationSeatCount: 1,
		TicketOrderFound: true, TicketOrderState: "cancelled", TicketOrderAmountMinor: 700, TicketOrderCurrency: "TWD",
		IssuanceReceiptFound: true, IssuancePaymentIntentID: intentID, RefundPendingReceiptFound: true,
		CompensationReceiptFound: true, CancellationReceiptFound: true, CancelledTicketCount: 1,
	}
	var findings []string
	checkShard(ScopeAll, control, shard, func(code string, _ bool) { findings = append(findings, code) })
	if len(findings) != 0 {
		t.Fatalf("healthy terminal refund findings=%v", findings)
	}
}

func TestControlFinalizeIncompleteRequiresExactStepOrBoundedManualReview(t *testing.T) {
	base := ControlSnapshot{Intent: Intent{State: "ticket_issue_pending"}, Saga: Saga{State: "issuing_tickets", Step: "issue_tickets"}}
	if !controlFinalizeIncomplete(base, "ticket_issue_pending", "issuing_tickets", "issue_tickets", true) {
		t.Fatal("exact pre-finalize state was not detected")
	}
	base.Saga.Step = "refund"
	if controlFinalizeIncomplete(base, "ticket_issue_pending", "issuing_tickets", "issue_tickets", true) {
		t.Fatal("wrong saga step was accepted")
	}
	base.Intent.State = "manual_review"
	base.Saga = Saga{State: "manual_review", Step: "issue_tickets", ErrorCategory: "database_finalize_failed"}
	if !controlFinalizeIncomplete(base, "ticket_issue_pending", "issuing_tickets", "issue_tickets", true) {
		t.Fatal("bounded database-finalize review was not detected")
	}
	base.Saga.ErrorCategory = "provider_outcome_unknown"
	if controlFinalizeIncomplete(base, "ticket_issue_pending", "issuing_tickets", "issue_tickets", true) {
		t.Fatal("unrelated manual review was accepted")
	}
}

func TestCapturedManualReviewWithoutTicketStillProducesFinding(t *testing.T) {
	control := ControlSnapshot{
		Intent:     Intent{ID: uuid.New(), State: "manual_review", AmountMinor: 700, Currency: "TWD"},
		Saga:       Saga{State: "manual_review", Step: "await_provider", ErrorCategory: "operator_requested"},
		Operations: []Operation{{Type: "capture", State: "succeeded", AmountMinor: 700, Currency: "TWD"}},
	}
	shard := ShardSnapshot{Found: true, DirectoryResolved: true, ReservationState: "payment_pending", ReservationAmountMinor: 700,
		ReservationCurrency: "TWD", ReservationSeatCount: 1}
	var findings []string
	checkShard(ScopeAll, control, shard, func(code string, _ bool) { findings = append(findings, code) })
	if !containsString(findings, "captured_payment_without_ticket") {
		t.Fatalf("findings=%v", findings)
	}

	control.Saga = Saga{State: "manual_review", Step: "refund", ErrorCategory: "database_finalize_failed"}
	findings = nil
	checkShard(ScopeAll, control, shard, func(code string, _ bool) { findings = append(findings, code) })
	if !containsString(findings, "captured_payment_without_ticket") {
		t.Fatalf("missing refund-pending receipt hid captured-without-ticket: %v", findings)
	}
	shard.RefundPendingReceiptFound = true
	findings = nil
	checkShard(ScopeAll, control, shard, func(code string, _ bool) { findings = append(findings, code) })
	if containsString(findings, "captured_payment_without_ticket") {
		t.Fatalf("receipt-backed mark-refund finalize window was misclassified: %v", findings)
	}
	control.Saga.State = "compensating"
	findings = nil
	checkShard(ScopeAll, control, shard, func(code string, _ bool) { findings = append(findings, code) })
	if !containsString(findings, "captured_payment_without_ticket") {
		t.Fatalf("wrong saga state hid captured-without-ticket: %v", findings)
	}
}

func TestBeginReceiptFingerprintMismatchIsNotRepairable(t *testing.T) {
	control := ControlSnapshot{Intent: Intent{ID: uuid.New(), State: "reservation_securing", AmountMinor: 700, Currency: "TWD", Fingerprint: sha256.Sum256([]byte("control"))}}
	shard := ShardSnapshot{Found: true, DirectoryResolved: true, ReservationState: "payment_pending", ReservationAmountMinor: 700,
		ReservationCurrency: "TWD", BeginReceiptFound: true, ReceiptFingerprint: sha256.Sum256([]byte("shard")),
		RecordedCommands: []RecordedCommand{{ID: uuid.New(), Kind: "finalize_reservation_begin", Fingerprint: sha256.Sum256([]byte("shard"))}}}
	var findings []Finding
	checkShard(ScopeAll, control, shard, func(code string, repairable bool) {
		findings = append(findings, Finding{Code: code, Repairable: repairable})
	})
	if len(findings) == 0 || findings[0].Code != "begin_payment_shard_receipt_mismatch" || findings[0].Repairable {
		t.Fatalf("findings=%+v", findings)
	}
}

func TestBeginReceiptFindingUsesOnlyRecordedFinalizeIdentity(t *testing.T) {
	command := RecordedCommand{ID: uuid.New(), Kind: "finalize_reservation_begin", Fingerprint: sha256.Sum256([]byte("receipt"))}
	got, ok := matchingRecordedCommand("begin_payment_shard_committed_control_incomplete", []RecordedCommand{command})
	if !ok || got != command {
		t.Fatalf("command=%+v ok=%v", got, ok)
	}
	if _, ok := matchingRecordedCommand("begin_payment_shard_receipt_mismatch", []RecordedCommand{command}); ok {
		t.Fatal("mismatched receipt became repairable")
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func TestSafeRepairRequiresConfirmationAndReplaysOnlyStoredIdentity(t *testing.T) {
	id := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	commandID := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	fingerprint := sha256.Sum256([]byte("durable receipt fingerprint"))
	store := healthyStore(id)
	store.candidates = []uuid.UUID{id}
	store.control.Intent.State = "ticket_issue_pending"
	store.control.Saga.State = "issuing_tickets"
	store.control.Saga.Step = "issue_tickets"
	store.shard.RecordedCommands = []RecordedCommand{{ID: commandID, Kind: "issue_tickets", Fingerprint: fingerprint}}
	repairer := &fakeRepairer{}
	reconciler := newTestReconciler(t, store, fakeRegistry{client: &fakeProvider{payment: healthyProviderPayment()}}, repairer)

	_, err := reconciler.ReconcileAll(context.Background(), Options{Scope: ScopeTickets, Limit: 1, Repair: true})
	if !errors.Is(err, ErrRepairConfirmation) {
		t.Fatalf("error = %v", err)
	}
	result, err := reconciler.ReconcileAll(context.Background(), Options{Scope: ScopeTickets, Limit: 1, Repair: true, ConfirmRepair: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.ReadOnly || result.RepairCount != 1 || len(repairer.commands) != 1 {
		t.Fatalf("result=%+v commands=%+v", result, repairer.commands)
	}
	if repairer.commands[0].ID != commandID || repairer.commands[0].Fingerprint != fingerprint || repairer.commands[0].Kind != "issue_tickets" {
		t.Fatalf("replayed command = %+v", repairer.commands[0])
	}
	if len(store.escalations) != 0 {
		t.Fatal("successfully repaired finding must not be escalated")
	}
}

func TestSafeReplayFailureFailsClosedIntoManualReview(t *testing.T) {
	id := uuid.New()
	commandID := uuid.New()
	store := healthyStore(id)
	store.candidates = []uuid.UUID{id}
	store.control.Intent.State = "ticket_issue_pending"
	store.control.Saga.State = "issuing_tickets"
	store.control.Saga.Step = "issue_tickets"
	store.shard.RecordedCommands = []RecordedCommand{{ID: commandID, Kind: "issue_tickets", Fingerprint: sha256.Sum256([]byte("recorded"))}}
	repairer := &fakeRepairer{err: ErrRepairUnavailable}
	reconciler := newTestReconciler(t, store, fakeRegistry{client: &fakeProvider{payment: healthyProviderPayment()}}, repairer)

	result, err := reconciler.ReconcileAll(context.Background(), Options{Scope: ScopeTickets, Limit: 1, Repair: true, ConfirmRepair: true})
	if !errors.Is(err, ErrRepairUnavailable) || result.RepairCount != 0 || result.ManualReviews != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(store.escalations) != 1 || store.escalations[0].reason != "safe_replay_failed" {
		t.Fatalf("escalations=%+v", store.escalations)
	}
}

func TestProviderErrorIsReducedToBoundedFinding(t *testing.T) {
	id := uuid.MustParse("55555555-5555-4555-8555-555555555555")
	store := healthyStore(id)
	client := &fakeProvider{err: errors.New("secret provider response body")}
	reconciler := newTestReconciler(t, store, fakeRegistry{client: client}, nil)
	report, err := reconciler.InspectPayment(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got := findingCodes(report); !reflect.DeepEqual(got, []string{"provider_status_query_failed"}) {
		t.Fatalf("findings = %v", got)
	}
}

func TestUncertainOperationQueriesProviderOutsideProviderScope(t *testing.T) {
	id := uuid.MustParse("66666666-6666-4666-8666-666666666666")
	store := healthyStore(id)
	store.control.Operations[0].State = "uncertain"
	store.control.ActiveReconciliationCases = 1
	client := &fakeProvider{payment: healthyProviderPayment()}
	reconciler := newTestReconciler(t, store, fakeRegistry{client: client}, nil)
	report, err := reconciler.inspect(context.Background(), id, ScopeOperations)
	if err != nil || !report.ProviderQueried || client.calls != 1 {
		t.Fatalf("report=%+v calls=%d err=%v", report, client.calls, err)
	}
}

func TestReconcileAllPersistsOneIntentBoundCheckpointPerCandidate(t *testing.T) {
	first := uuid.MustParse("77777777-7777-4777-8777-777777777777")
	second := uuid.MustParse("88888888-8888-4888-8888-888888888888")
	store := healthyStore(first)
	store.candidates = []uuid.UUID{second, first}
	reconciler := newTestReconciler(t, store, fakeRegistry{client: &fakeProvider{payment: healthyProviderPayment()}}, nil)
	if _, err := reconciler.ReconcileAll(context.Background(), Options{Scope: ScopeOperations, Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if len(store.checkpoints) != 2 || store.checkpoints[0].PaymentIntentID != first || store.checkpoints[1].PaymentIntentID != second {
		t.Fatalf("checkpoints=%+v", store.checkpoints)
	}
}

func TestProviderQueriesUsePerItemBudgetAndContinueBatch(t *testing.T) {
	first := uuid.MustParse("91919191-9191-4919-8919-919191919191")
	store := healthyStore(first)
	store.candidates = []uuid.UUID{first, uuid.New(), uuid.New()}
	client := &blockingProvider{}
	reconciler := newTestReconciler(t, store, fakeRegistry{client: client}, nil)

	started := time.Now()
	result, err := reconciler.ReconcileAll(context.Background(), Options{Scope: ScopeAll, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("provider batch exceeded bounded budget: %s", elapsed)
	}
	if result.RowsExamined != 3 || len(result.Reports) != 3 || client.calls != 3 {
		t.Fatalf("result=%+v provider calls=%d", result, client.calls)
	}
}

func newTestReconciler(t *testing.T, store *fakeStore, registry ProviderRegistry, repairer Repairer) *Reconciler {
	t.Helper()
	value, err := New(store, registry, repairer, Config{
		BatchSize: 10, StaleAfter: time.Minute, ReviewDue: time.Hour,
		Now: func() time.Time { return time.Date(2026, 8, 8, 6, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func healthyStore(id uuid.UUID) *fakeStore {
	fingerprint := sha256.Sum256([]byte("request"))
	return &fakeStore{
		control: ControlSnapshot{
			Intent:     Intent{ID: id, ReservationID: uuid.New(), Provider: "sandbox", ProviderPaymentID: "pay_123", State: "completed", AmountMinor: 700, Currency: "TWD", Fingerprint: fingerprint, ActiveForReservation: 1},
			Saga:       Saga{ID: uuid.New(), State: "completed", ActiveCount: 1},
			Operations: []Operation{{ID: uuid.New(), Type: "capture", State: "succeeded", ProviderOperationID: "op_capture", AmountMinor: 700, Currency: "TWD"}},
		},
		shard: ShardSnapshot{Found: true, DirectoryResolved: true, ReservationState: "confirmed", ReservationAmountMinor: 700, ReservationCurrency: "TWD", ReservationSeatCount: 1, TicketOrderFound: true, TicketOrderState: "issued", TicketOrderAmountMinor: 700, TicketOrderCurrency: "TWD", IssuanceReceiptFound: true, IssuancePaymentIntentID: id, ActiveTicketCount: 1},
	}
}

func healthyProviderPayment() provider.Payment {
	return provider.Payment{Status: provider.StatusCaptured, AmountMinor: 700, Currency: "TWD", CapturedMinor: 700}
}

func findingCodes(report Report) []string {
	result := make([]string, len(report.Findings))
	for index, finding := range report.Findings {
		result[index] = finding.Code
	}
	return result
}

type fakeStore struct {
	candidates  []uuid.UUID
	control     ControlSnapshot
	shard       ShardSnapshot
	checkpoints []Checkpoint
	finished    []CheckpointResult
	escalations []fakeEscalation
}

type fakeEscalation struct{ reason string }

func (s *fakeStore) CandidateIntentIDs(context.Context, Scope, time.Time, int) ([]uuid.UUID, bool, error) {
	return append([]uuid.UUID(nil), s.candidates...), false, nil
}
func (s *fakeStore) LoadControlSnapshot(_ context.Context, id uuid.UUID) (ControlSnapshot, error) {
	result := s.control
	result.Intent.ID = id
	return result, nil
}
func (s *fakeStore) LoadShardSnapshot(context.Context, uuid.UUID) (ShardSnapshot, error) {
	return s.shard, nil
}
func (s *fakeStore) StartCheckpoint(_ context.Context, scope Scope, id uuid.UUID, repair bool, at time.Time) (Checkpoint, error) {
	checkpoint := Checkpoint{ID: uuid.New(), Scope: scope, PaymentIntentID: id, Repair: repair, StartedAt: at}
	s.checkpoints = append(s.checkpoints, checkpoint)
	return checkpoint, nil
}
func (s *fakeStore) FinishCheckpoint(_ context.Context, _ Checkpoint, result CheckpointResult) error {
	s.finished = append(s.finished, result)
	return nil
}
func (s *fakeStore) EscalateManualReview(_ context.Context, _, _ uuid.UUID, reason string, _ time.Time) (bool, error) {
	s.escalations = append(s.escalations, fakeEscalation{reason: reason})
	return true, nil
}

type fakeRegistry struct{ client StatusQuerier }

func (r fakeRegistry) Provider(string) (StatusQuerier, bool) { return r.client, r.client != nil }

type fakeProvider struct {
	payment provider.Payment
	err     error
	calls   int
}

type blockingProvider struct{ calls int }

func (p *blockingProvider) GetPaymentStatus(ctx context.Context, _ string) (provider.Payment, error) {
	p.calls++
	<-ctx.Done()
	return provider.Payment{}, ctx.Err()
}

func (p *fakeProvider) GetPaymentStatus(context.Context, string) (provider.Payment, error) {
	p.calls++
	return p.payment, p.err
}

type fakeRepairer struct {
	commands []RecordedCommand
	err      error
}

func (r *fakeRepairer) ReplayRecordedCommand(_ context.Context, _ uuid.UUID, command RecordedCommand) error {
	r.commands = append(r.commands, command)
	return r.err
}
