package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	paymentticketcodes "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ticketcodes"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const testIntent = "11111111-1111-4111-8111-111111111111"

func TestInspectIntentIsReadOnlyAndBounded(t *testing.T) {
	service := &fakeBackend{result: outcome{Count: 3, Items: []item{{Kind: "intent"}, {Kind: "saga"}, {Kind: "operation"}}}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"inspect-intent", "--payment-intent-id", testIntent, "--limit", "2"}, noEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), request) (backend, func(), error) {
			return service, func() {}, nil
		})
	if code != 0 || !strings.Contains(stdout.String(), `"read_only":true`) || !strings.Contains(stdout.String(), `"truncated":true`) {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if len(service.result.Items) != 3 || service.request.PaymentIntentID.String() != testIntent {
		t.Fatalf("request=%+v", service.request)
	}
}

func TestMutationRequiresExactlyOneOfDryRunOrConfirm(t *testing.T) {
	for _, args := range [][]string{
		{"mark-manual-review", "--payment-intent-id", testIntent},
		{"mark-manual-review", "--payment-intent-id", testIntent, "--confirm", "--dry-run"},
	} {
		var stdout, stderr bytes.Buffer
		code := run(context.Background(), args, noEnv, &stdout, &stderr, openBackend)
		if code != 2 || !strings.Contains(stdout.String(), "confirmation_required") {
			t.Fatalf("args=%v code=%d stdout=%q", args, code, stdout.String())
		}
	}
}

func TestRepairRequiresExplicitFlagAndConfirmation(t *testing.T) {
	req, err := parse([]string{"reconcile-intent", "--payment-intent-id", testIntent})
	if err != nil || !requestReadOnly(req) {
		t.Fatalf("detect request=%+v err=%v", req, err)
	}
	if _, err := parse([]string{"reconcile-intent", "--payment-intent-id", testIntent, "--repair"}); !errors.Is(err, errConfirmation) {
		t.Fatalf("error=%v", err)
	}
	req, err = parse([]string{"reconcile-intent", "--payment-intent-id", testIntent, "--repair", "--confirm"})
	if err != nil || requestReadOnly(req) {
		t.Fatalf("repair request=%+v err=%v", req, err)
	}
}

func TestNoAmountOrStateMutationFlagsExist(t *testing.T) {
	for _, forbidden := range []string{"--amount", "--state", "--currency", "--seat-id"} {
		_, err := parse([]string{"request-refund", "--payment-intent-id", testIntent, "--operation-id", "22222222-2222-4222-8222-222222222222", "--confirm", forbidden, "1"})
		if err == nil {
			t.Fatalf("accepted forbidden flag %s", forbidden)
		}
	}
}

func TestBackendErrorIsRedacted(t *testing.T) {
	service := &fakeBackend{err: errors.New("card data and postgres://secret")}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"inspect-intent", "--payment-intent-id", testIntent}, noEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), request) (backend, func(), error) {
			return service, func() {}, nil
		})
	if code != 1 || strings.Contains(stdout.String(), "secret") || strings.Contains(stderr.String(), "secret") || !strings.Contains(stdout.String(), "operation_failed") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestBackendItemTextIsBoundedAndRedacted(t *testing.T) {
	service := &fakeBackend{result: outcome{Items: []item{{Kind: "intent secret", State: "postgres://dsn", Code: strings.Repeat("x", 65)}}}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"inspect-intent", "--payment-intent-id", testIntent}, noEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), request) (backend, func(), error) {
			return service, func() {}, nil
		})
	if code != 0 || strings.Contains(stdout.String(), "secret") || strings.Contains(stdout.String(), "postgres") || !strings.Contains(stdout.String(), `"kind":"resource"`) {
		t.Fatalf("code=%d stdout=%q", code, stdout.String())
	}
}

func TestRuntimeBackendReplaysOnlyRecordedRefundOperation(t *testing.T) {
	backend := runtimeTestBackend()
	operationID := uuid.New()
	backend.operator.(*fakeAdminOperator).operationID = operationID
	result, err := backend.Execute(context.Background(), request{Command: "request-refund", PaymentIntentID: uuid.MustParse(testIntent), OperationID: operationID, Confirm: true})
	if err != nil || len(result.Items) != 1 || result.Items[0].State != "scheduled" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestRuntimeBackendRejectsMutationWithoutConfirmedBoundary(t *testing.T) {
	backend := runtimeTestBackend()
	_, err := backend.Execute(context.Background(), request{Command: "request-refund", PaymentIntentID: uuid.MustParse(testIntent), OperationID: uuid.New()})
	if !errors.Is(err, errConfirmation) {
		t.Fatalf("error=%v", err)
	}
}

func TestRuntimeBackendProviderAndFinancialInspectionAreBounded(t *testing.T) {
	id := uuid.MustParse(testIntent)
	backend := runtimeTestBackend()
	status, err := backend.Execute(context.Background(), request{Command: "inspect-provider-status", PaymentIntentID: id, Limit: 1})
	if err != nil || len(status.Items) != 1 || status.Items[0].State != "captured" {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	financial, err := backend.Execute(context.Background(), request{Command: "inspect-financial-operations", PaymentIntentID: id, Limit: 1})
	if err != nil || len(financial.Items) != 1 || !financial.Truncated || financial.Count != 2 {
		t.Fatalf("financial=%+v err=%v", financial, err)
	}
}

func TestRuntimeBackendIssuanceUsesCurrentShardSnapshot(t *testing.T) {
	id := uuid.MustParse(testIntent)
	backend := runtimeTestBackend()
	result, err := backend.Execute(context.Background(), request{Command: "inspect-ticket-issuance", PaymentIntentID: id, Limit: 1})
	if err != nil || len(result.Items) != 1 || result.Items[0].State != "issued" || result.Items[0].Code != "issuance_receipt_present" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestOperatorRoleIsMandatory(t *testing.T) {
	if err := requireOperatorRole(context.Background(), roleDB{allowed: false}); err == nil {
		t.Fatal("accepted non-operator database role")
	}
	if err := requireOperatorRole(context.Background(), roleDB{allowed: true}); err != nil {
		t.Fatalf("operator rejected: %v", err)
	}
}

func TestTicketCodeBackfillRequiresConfirmationAndEmitsBoundedCount(t *testing.T) {
	for _, args := range [][]string{
		{"backfill-ticket-codes"},
		{"backfill-ticket-codes", "--confirm", "--dry-run"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(context.Background(), args, noEnv, &stdout, &stderr, openBackend); code != 2 ||
			!strings.Contains(stdout.String(), "confirmation_required") {
			t.Fatalf("args=%v code=%d stdout=%q", args, code, stdout.String())
		}
	}

	backfill := &fakeTicketBackfill{backfillResult: paymentticketcodes.Result{Missing: 3, Claimed: 3, Total: 7, Ready: true}}
	service := runtimeTestBackend()
	service.ticketBackfill = backfill
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"backfill-ticket-codes", "--confirm", "--limit", "3"}, noEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), request) (backend, func(), error) {
			return service, func() {}, nil
		})
	if code != 0 || !backfill.backfillCalled || backfill.limit != 3 ||
		!strings.Contains(stdout.String(), `"read_only":false`) ||
		!strings.Contains(stdout.String(), `"count":3`) ||
		!strings.Contains(stdout.String(), `"state":"ready"`) {
		t.Fatalf("code=%d called=%v limit=%d stdout=%q stderr=%q", code, backfill.backfillCalled, backfill.limit, stdout.String(), stderr.String())
	}
}

func TestTicketCodeBackfillDryRunUsesInspectOnly(t *testing.T) {
	backfill := &fakeTicketBackfill{inspectResult: paymentticketcodes.Result{Missing: 4}}
	service := runtimeTestBackend()
	service.ticketBackfill = backfill
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"backfill-ticket-codes", "--dry-run", "--limit", "4"}, noEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), request) (backend, func(), error) {
			return service, func() {}, nil
		})
	if code != 0 || !backfill.inspectCalled || backfill.backfillCalled || backfill.limit != 4 ||
		!strings.Contains(stdout.String(), `"read_only":true`) || !strings.Contains(stdout.String(), `"count":4`) {
		t.Fatalf("code=%d inspect=%v backfill=%v stdout=%q stderr=%q", code, backfill.inspectCalled, backfill.backfillCalled, stdout.String(), stderr.String())
	}
}

func runtimeTestBackend() *runtimeBackend {
	id := uuid.MustParse(testIntent)
	store := &fakeAdminStore{
		control: paymentreconcile.ControlSnapshot{
			Intent:     paymentreconcile.Intent{ID: id, Provider: "sandbox", ProviderPaymentID: "pay_1", State: "completed"},
			Operations: []paymentreconcile.Operation{{ID: uuid.New(), Type: "capture", State: "succeeded"}, {ID: uuid.New(), Type: "refund", State: "pending"}},
		},
		shard: paymentreconcile.ShardSnapshot{TicketOrderFound: true, TicketOrderID: uuid.New(), TicketOrderState: "issued", IssuanceReceiptFound: true},
	}
	return &runtimeBackend{store: store, reconciler: fakeAdminReconciler{}, provider: fakeStatusProvider{}, operator: &fakeAdminOperator{}}
}

type fakeAdminStore struct {
	control paymentreconcile.ControlSnapshot
	shard   paymentreconcile.ShardSnapshot
}

func (s *fakeAdminStore) LoadControlSnapshot(context.Context, uuid.UUID) (paymentreconcile.ControlSnapshot, error) {
	return s.control, nil
}
func (s *fakeAdminStore) LoadShardSnapshot(context.Context, uuid.UUID) (paymentreconcile.ShardSnapshot, error) {
	return s.shard, nil
}

type fakeAdminReconciler struct{}

func (fakeAdminReconciler) InspectPayment(context.Context, uuid.UUID) (paymentreconcile.Report, error) {
	return paymentreconcile.Report{}, nil
}
func (fakeAdminReconciler) ReconcilePayment(context.Context, uuid.UUID) (paymentreconcile.Result, error) {
	return paymentreconcile.Result{}, nil
}
func (fakeAdminReconciler) RepairPayment(context.Context, uuid.UUID) (paymentreconcile.Result, error) {
	return paymentreconcile.Result{ReadOnly: false, RepairCount: 1}, nil
}

type fakeAdminOperator struct{ operationID uuid.UUID }

func (*fakeAdminOperator) RetrySaga(context.Context, uuid.UUID) error            { return nil }
func (*fakeAdminOperator) ResumeTicketIssuance(context.Context, uuid.UUID) error { return nil }
func (operator *fakeAdminOperator) RetryProviderOperation(_ context.Context, _ uuid.UUID, operationID uuid.UUID, _ string) error {
	if operator.operationID != uuid.Nil && operationID != operator.operationID {
		return errSafeReplayUnavailable
	}
	return nil
}
func (*fakeAdminOperator) MarkManualReview(context.Context, uuid.UUID) error { return nil }

type fakeStatusProvider struct{}

func (fakeStatusProvider) GetPaymentStatus(context.Context, string) (provider.Payment, error) {
	return provider.Payment{Status: provider.StatusCaptured}, nil
}

type fakeTicketBackfill struct {
	inspectResult  paymentticketcodes.Result
	backfillResult paymentticketcodes.Result
	inspectCalled  bool
	backfillCalled bool
	limit          int
}

func (fake *fakeTicketBackfill) Inspect(_ context.Context, limit int) (paymentticketcodes.Result, error) {
	fake.inspectCalled, fake.limit = true, limit
	return fake.inspectResult, nil
}

func (fake *fakeTicketBackfill) Backfill(_ context.Context, limit int) (paymentticketcodes.Result, error) {
	fake.backfillCalled, fake.limit = true, limit
	return fake.backfillResult, nil
}

type roleDB struct{ allowed bool }

func (db roleDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return roleRow(db)
}

type roleRow struct{ allowed bool }

func (row roleRow) Scan(destinations ...any) error {
	*(destinations[0].(*bool)) = row.allowed
	return nil
}

type fakeBackend struct {
	request request
	result  outcome
	err     error
}

func (b *fakeBackend) Execute(_ context.Context, req request) (outcome, error) {
	b.request = req
	return b.result, b.err
}
func noEnv(string) (string, bool) { return "", false }
