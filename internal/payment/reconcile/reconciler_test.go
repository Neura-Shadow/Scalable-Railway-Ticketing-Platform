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
		"completed_saga_without_full_capture", "duplicate_capture_operation",
		"provider_capture_mismatch", "uncertain_operation_without_reconciliation",
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

func TestSafeRepairRequiresConfirmationAndReplaysOnlyStoredIdentity(t *testing.T) {
	id := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	commandID := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	fingerprint := sha256.Sum256([]byte("durable receipt fingerprint"))
	store := healthyStore(id)
	store.candidates = []uuid.UUID{id}
	store.shard.ActiveTicketCount = 0
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

func (p *fakeProvider) GetPaymentStatus(context.Context, string) (provider.Payment, error) {
	p.calls++
	return p.payment, p.err
}

type fakeRepairer struct{ commands []RecordedCommand }

func (r *fakeRepairer) ReplayRecordedCommand(_ context.Context, _ uuid.UUID, command RecordedCommand) error {
	r.commands = append(r.commands, command)
	return nil
}
