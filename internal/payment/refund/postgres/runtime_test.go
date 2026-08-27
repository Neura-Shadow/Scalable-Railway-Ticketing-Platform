package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMarkProviderSucceededLocksIntentAndDerivesCumulativeRefund(t *testing.T) {
	t.Parallel()
	tx := &finalizeRefundTx{}
	store, err := NewStore(&finalizeRefundDB{tx: tx}, refundShardStub{}, Config{PartialRefundProviders: map[string]bool{"sandbox": true}})
	if err != nil {
		t.Fatal(err)
	}
	work := refund.RefundWork{
		SagaID: uuid.New(), RequestID: uuid.New(), OperationID: uuid.New(), PaymentIntentID: uuid.New(),
		AmountMinor: 50, CapturedMinor: 100, RefundedBeforeMinor: 0, Currency: "TWD", LeaseOwner: "worker-1",
	}
	evidence := refund.ProviderRefundEvidence{CapturedMinor: 100, RefundedMinor: 50, Fingerprint: refund.Hash{1}}
	if err := store.MarkProviderSucceeded(context.Background(), work, evidence, time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("MarkProviderSucceeded() error = %v", err)
	}
	if !tx.committed || !tx.lockedIntent {
		t.Fatalf("committed=%v lockedIntent=%v", tx.committed, tx.lockedIntent)
	}
	for _, call := range tx.execs {
		if strings.Contains(call.query, "UPDATE public.ticket_refund_operations") {
			if len(call.args) < 6 || call.args[4] != int64(100) || call.args[5] != int64(100) {
				t.Fatalf("operation totals = %#v, want captured=100 cumulative_refunded=100", call.args)
			}
			return
		}
	}
	t.Fatal("ticket refund operation was not finalized")
}

func TestRefundClaimLeaseUsesDatabaseClockOnly(t *testing.T) {
	t.Parallel()
	if strings.Contains(claimRefundWorkSQL, "claim.Now") ||
		strings.Contains(claimRefundWorkSQL, "next_attempt_at <= $1") ||
		strings.Contains(claimRefundWorkSQL, "lease_until <= $1") {
		t.Fatalf("claim SQL trusts a replica application clock: %s", claimRefundWorkSQL)
	}
	if got := strings.Count(claimRefundWorkSQL, "clock_timestamp()"); got < 4 {
		t.Fatalf("database clock uses = %d, want eligibility, lease expiry, new lease, and update time", got)
	}
	if !strings.Contains(claimRefundWorkSQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatal("claim SQL lost its multi-replica row lock")
	}
}

func TestRefundClaimRecoversExpiredProviderEffectAsQueryOnly(t *testing.T) {
	t.Parallel()
	for _, required := range []string{
		"current_step='refund_provider'",
		"operation.state='processing'",
		"FOR UPDATE OF saga,operation SKIP LOCKED",
		"state='uncertain'",
		"state='provider_uncertain'",
		"current_step='query_provider'",
		"worker_lease_expired",
	} {
		if !strings.Contains(recoverExpiredProviderRefundSQL, required) {
			t.Fatalf("expired provider recovery omitted %q:\n%s", required, recoverExpiredProviderRefundSQL)
		}
	}
	if strings.Contains(refundProviderAttemptStates, "processing") {
		t.Fatal("refund_provider transition can still reclaim a processing operation")
	}
}

type finalizeRefundDB struct{ tx *finalizeRefundTx }

func (db *finalizeRefundDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return db.tx, nil
}
func (*finalizeRefundDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected Query")
}
func (*finalizeRefundDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return finalizeRefundRow{err: errors.New("unexpected QueryRow")}
}

type finalizeRefundExec struct {
	query string
	args  []any
}

type finalizeRefundTx struct {
	pgx.Tx
	execs        []finalizeRefundExec
	lockedIntent bool
	committed    bool
}

func (tx *finalizeRefundTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(query, "FROM public.payment_intents"):
		tx.lockedIntent = strings.Contains(query, "FOR UPDATE")
		return finalizeRefundRow{values: []any{int64(100), "TWD", "completed"}}
	case strings.Contains(query, "sum(operation.amount_minor)"):
		return finalizeRefundRow{values: []any{int64(50)}}
	case strings.Contains(query, "financial_ledger_transactions"):
		return finalizeRefundRow{err: pgx.ErrNoRows}
	default:
		return finalizeRefundRow{err: errors.New("unexpected QueryRow")}
	}
}

func (tx *finalizeRefundTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, finalizeRefundExec{query: query, args: args})
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (tx *finalizeRefundTx) Commit(context.Context) error { tx.committed = true; return nil }
func (*finalizeRefundTx) Rollback(context.Context) error  { return nil }

type finalizeRefundRow struct {
	values []any
	err    error
}

func (row finalizeRefundRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(dest) != len(row.values) {
		return errors.New("unexpected scan width")
	}
	for index, value := range row.values {
		switch target := dest[index].(type) {
		case *int64:
			*target = value.(int64)
		case *string:
			*target = value.(string)
		default:
			return errors.New("unexpected scan type")
		}
	}
	return nil
}
