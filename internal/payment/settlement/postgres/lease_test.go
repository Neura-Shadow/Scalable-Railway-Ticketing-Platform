package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestSettlementLeaseClaimBindsPageCommitAndFinishToRandomToken(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_claimed"}
	now := time.Date(2026, 8, 17, 3, 0, 0, 0, time.UTC)
	leaseUntil := now.Add(time.Minute)
	claimTx := &fakeTx{rows: []pgx.Row{fakeRow{values: []any{"cursor-before", leaseUntil}}}}
	commitTx := &fakeTx{
		rows: []pgx.Row{fakeRow{values: []any{"cursor-before"}}},
		tags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("UPDATE 1")},
	}
	finishTx := &fakeTx{tags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}}
	store := newSettlementStore(t, &fakeDB{transactions: []pgx.Tx{claimTx, commitTx, finishTx}})

	claimedStore, lease, claimed, err := store.ClaimDue(context.Background(), scope, "replica:one", now, time.Minute)
	if err != nil || !claimed || lease.Token.String() == "00000000-0000-0000-0000-000000000000" ||
		lease.Cursor != "cursor-before" || !lease.LeaseUntil.Equal(leaseUntil) {
		t.Fatalf("ClaimDue() store=%T lease=%+v claimed=%v err=%v", claimedStore, lease, claimed, err)
	}
	if !claimTx.committed || len(claimTx.execs) != 1 {
		t.Fatalf("claim was not one short committed transaction: committed=%v execs=%+v", claimTx.committed, claimTx.execs)
	}

	result, err := claimedStore.CommitPage(context.Background(), settlement.PageCommit{
		Scope: scope, ExpectedCursor: "cursor-before", NextCursor: "cursor-after",
		Records: []settlement.ImportedRecord{{
			Record: settlement.Record{
				Kind: settlement.RecordSettlementLine, ProviderID: "line_claimed", Operation: settlement.OperationSettlement,
				GrossMinor: 100, NetMinor: 100, Currency: "TWD", CreatedAt: now,
			},
			PayloadHash: settlement.PayloadHash{1}, ImportedAt: now,
		}},
	})
	if err != nil || result.Inserted != 1 || !commitTx.committed {
		t.Fatalf("claimed CommitPage() = (%+v, %v), committed=%v", result, err, commitTx.committed)
	}
	if err := store.FinishLease(context.Background(), lease, 2*time.Minute); err != nil || !finishTx.committed {
		t.Fatalf("FinishLease() err=%v committed=%v", err, finishTx.committed)
	}
	if len(finishTx.execs) != 1 || !strings.Contains(finishTx.execs[0].query, "next_attempt_at=clock_timestamp()+make_interval") ||
		finishTx.execs[0].args[4] != 120 {
		t.Fatalf("finish did not schedule from the database clock: %+v", finishTx.execs)
	}
}

func TestSettlementLeaseBusyClaimReturnsNoWorkAndLostFinishFailsClosed(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_busy"}
	now := time.Date(2026, 8, 17, 4, 0, 0, 0, time.UTC)
	busyTx := &fakeTx{rows: []pgx.Row{fakeRow{err: pgx.ErrNoRows}}}
	lostFinishTx := &fakeTx{tags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}
	store := newSettlementStore(t, &fakeDB{transactions: []pgx.Tx{busyTx, lostFinishTx}})

	claimedStore, lease, claimed, err := store.ClaimDue(context.Background(), scope, "replica:two", now, time.Minute)
	if err != nil || claimed || claimedStore != nil || lease.Token.String() != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("busy ClaimDue() store=%T lease=%+v claimed=%v err=%v", claimedStore, lease, claimed, err)
	}
	if !busyTx.committed {
		t.Fatal("busy claim did not close its short transaction")
	}

	lost := settlement.ImportLease{
		Scope: scope, Owner: "replica:stale", Token: uuid.MustParse("0198a9d3-c042-7145-b691-8a3b31ba7aac"),
		Cursor: "cursor-stale", LeaseUntil: now.Add(-time.Second),
	}
	if err := store.FinishLease(context.Background(), lost, 0); !errors.Is(err, settlement.ErrImportLeaseLost) {
		t.Fatalf("stale FinishLease() error = %v, want ErrImportLeaseLost", err)
	}
}

func TestSettlementLeaseRejectsUnboundedRetryDelayBeforeTransaction(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 5, 0, 0, 0, time.UTC)
	store := newSettlementStore(t, &fakeDB{})
	lease := settlement.ImportLease{
		Scope: settlement.AccountScope{Provider: "stripe", AccountID: "acct_delay"},
		Owner: "replica:delay", Token: uuid.MustParse("0198a9d3-c042-7145-b691-8a3b31ba7aad"),
		LeaseUntil: now.Add(time.Minute),
	}
	for _, delay := range []time.Duration{-time.Second, 24*time.Hour + time.Second} {
		if err := store.FinishLease(context.Background(), lease, delay); !errors.Is(err, settlement.ErrInvalidImportLease) {
			t.Fatalf("FinishLease(%s) error = %v, want ErrInvalidImportLease", delay, err)
		}
	}
}
