package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestClaimTicketCodesAllowsSequentialIssuanceAfterPriorLocatorFinalization(t *testing.T) {
	t.Parallel()
	for sequence := 0; sequence < 2; sequence++ {
		tx := &recordingTx{row: staticRow{values: []any{true}}}
		store, err := New(&recordingDB{tx: tx})
		if err != nil {
			t.Fatal(err)
		}
		command := shard.IssueTicketsCommand{PaymentIntentID: uuid.New(), ReservationID: uuid.New()}
		plan := shard.TicketIdentityPlan{
			TicketIDs:   []uuid.UUID{uuid.New()},
			TicketCodes: []string{"planned_ticket_code_000" + string(rune('1'+sequence))},
		}
		if err := store.ClaimTicketCodes(context.Background(), command, plan); err != nil {
			t.Fatalf("sequence %d ClaimTicketCodes() error = %v", sequence+1, err)
		}
		if !tx.committed || len(tx.execs) != 1 || len(tx.queryRowSQL) != 1 {
			t.Fatalf("sequence %d committed=%v execs=%d checks=%d", sequence+1, tx.committed, len(tx.execs), len(tx.queryRowSQL))
		}
		if len(tx.queryRowArgs) != 1 || len(tx.queryRowArgs[0]) != 2 ||
			tx.queryRowArgs[0][0] != command.PaymentIntentID || tx.queryRowArgs[0][1] != command.ReservationID {
			t.Fatalf("sequence %d authorization args = %#v, want intent and reservation", sequence+1, tx.queryRowArgs)
		}
		check := strings.ToLower(tx.queryRowSQL[0])
		if !strings.Contains(check, "saga.state='issuing_tickets'") ||
			strings.Contains(check, "claimed_ticket_count=") ||
			strings.Contains(check, "ticket_shard_locators") {
			t.Fatalf("sequence %d used stale or invalid authorization predicate:\n%s", sequence+1, check)
		}
	}
}

func TestTicketClaimReadErrorsRemainBoundedAndActionable(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		err  error
		want error
	}{
		{err: pgx.ErrNoRows, want: shard.ErrTicketClaimUnauthorized},
		{err: context.DeadlineExceeded, want: shard.ErrTicketClaimReadTimeout},
		{err: &pgconn.PgError{Code: "40001"}, want: shard.ErrTicketClaimSQLFailed},
		{err: pgx.ScanArgError{ColumnIndex: 0, Err: errors.New("scan")}, want: shard.ErrTicketClaimScanFailed},
		{err: errors.New("decode"), want: shard.ErrTicketClaimDecodeFailed},
	} {
		if got := classifyTicketClaimReadError(test.err); !errors.Is(got, test.want) {
			t.Fatalf("classifyTicketClaimReadError(%T) = %v, want %v", test.err, got, test.want)
		}
	}
}

func TestClaimTicketCodesRejectsDuplicatePlannedIdentityBeforeControlWrite(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	store, _ := New(&recordingDB{tx: &recordingTx{row: staticRow{values: []any{true}}}})
	err := store.ClaimTicketCodes(context.Background(), shard.IssueTicketsCommand{
		PaymentIntentID: uuid.New(), ReservationID: uuid.New(),
	}, shard.TicketIdentityPlan{
		TicketIDs:   []uuid.UUID{id, id},
		TicketCodes: []string{"planned_ticket_code_0001", "planned_ticket_code_0002"},
	})
	if err == nil {
		t.Fatal("duplicate planned ticket identity was accepted")
	}
}
