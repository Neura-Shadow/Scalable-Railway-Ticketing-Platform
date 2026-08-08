package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const testIntent = "11111111-1111-4111-8111-111111111111"

func TestInspectIntentIsReadOnlyAndBounded(t *testing.T) {
	service := &fakeBackend{result: outcome{Count: 3, Items: []item{{Kind: "intent"}, {Kind: "saga"}, {Kind: "operation"}}}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"inspect-intent", "--payment-intent-id", testIntent, "--limit", "2"}, noEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool)) (backend, func(), error) {
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
		func(context.Context, func(string) (string, bool)) (backend, func(), error) {
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
		func(context.Context, func(string) (string, bool)) (backend, func(), error) {
			return service, func() {}, nil
		})
	if code != 0 || strings.Contains(stdout.String(), "secret") || strings.Contains(stdout.String(), "postgres") || !strings.Contains(stdout.String(), `"kind":"resource"`) {
		t.Fatalf("code=%d stdout=%q", code, stdout.String())
	}
}

func TestRuntimeBackendRejectsMutationWithoutReplayer(t *testing.T) {
	backend := runtimeTestBackend()
	_, err := backend.Execute(context.Background(), request{Command: "request-refund", PaymentIntentID: uuid.MustParse(testIntent), Confirm: true})
	if !errors.Is(err, errSafeReplayUnavailable) {
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

func runtimeTestBackend() *runtimeBackend {
	id := uuid.MustParse(testIntent)
	store := &fakeAdminStore{
		control: paymentreconcile.ControlSnapshot{
			Intent:     paymentreconcile.Intent{ID: id, Provider: "sandbox", ProviderPaymentID: "pay_1", State: "completed"},
			Operations: []paymentreconcile.Operation{{ID: uuid.New(), Type: "capture", State: "succeeded"}, {ID: uuid.New(), Type: "refund", State: "pending"}},
		},
		shard: paymentreconcile.ShardSnapshot{TicketOrderFound: true, TicketOrderID: uuid.New(), TicketOrderState: "issued", IssuanceReceiptFound: true},
	}
	return &runtimeBackend{store: store, reconciler: fakeAdminReconciler{}, provider: fakeStatusProvider{}}
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

type fakeStatusProvider struct{}

func (fakeStatusProvider) GetPaymentStatus(context.Context, string) (provider.Payment, error) {
	return provider.Payment{Status: provider.StatusCaptured}, nil
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
