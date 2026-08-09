package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestCandidateQueryIsBoundedAndNeverEnumeratesPhysicalShards(t *testing.T) {
	for _, required := range []string{"payment_reconciliation_checkpoints", "ORDER BY", "LIMIT $3"} {
		if !strings.Contains(candidateIntentSQL, required) {
			t.Fatalf("candidate query missing %q", required)
		}
	}
	for _, forbidden := range []string{"physical-shard-0", "physical-shard-1", "dblink", "postgres_fdw"} {
		if strings.Contains(candidateIntentSQL, forbidden) {
			t.Fatalf("candidate query can scan an unbound shard via %q", forbidden)
		}
	}
}

func TestStoreRejectsInvalidBoundariesBeforeDatabaseUse(t *testing.T) {
	store, err := New(fakeControl{}, fakeResolver{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CandidateIntentIDs(context.Background(), paymentreconcile.ScopeAll, time.Now(), paymentreconcile.MaxBatchSize+1); err == nil {
		t.Fatal("accepted unbounded candidate request")
	}
	if err := store.FinishCheckpoint(context.Background(), paymentreconcile.Checkpoint{ID: uuid.New()}, paymentreconcile.CheckpointResult{RowsExamined: 1, MismatchCount: 2, CompletedAt: time.Now()}); err == nil {
		t.Fatal("accepted impossible checkpoint counters")
	}
}

func TestBoundedCategoryNeverEchoesUnsafeText(t *testing.T) {
	if got := boundedCategory("postgres://secret physical-shard"); got != "reconciliation_failed" {
		t.Fatalf("category=%q", got)
	}
	if got := boundedCategory("provider_capture_mismatch"); got != "provider_capture_mismatch" {
		t.Fatalf("category=%q", got)
	}
}

func TestCompareTicketCodeRowsDetectsCrossShardDirectoryDrift(t *testing.T) {
	orderID := uuid.New()
	firstID, secondID := uuid.New(), uuid.New()
	tickets := []ticketIdentity{
		{id: firstID, code: "ticket_code_000001"},
		{id: secondID, code: "ticket_code_000002"},
	}
	otherID := uuid.New()
	observed := []directoryIdentity{
		{id: firstID, code: "ticket_code_000001", orderID: orderID},
		{id: otherID, code: "ticket_code_000002", orderID: uuid.New()},
		{id: uuid.New(), code: "ticket_code_000003", orderID: orderID},
	}
	missing, conflicts, unexpected, err := compareTicketCodeRows(orderID, tickets, observed)
	if err != nil {
		t.Fatal(err)
	}
	if missing != 0 || conflicts != 1 || unexpected != 1 {
		t.Fatalf("missing=%d conflicts=%d unexpected=%d", missing, conflicts, unexpected)
	}

	missing, conflicts, unexpected, err = compareTicketCodeRows(orderID, tickets, nil)
	if err != nil || missing != 2 || conflicts != 0 || unexpected != 0 {
		t.Fatalf("empty directory: missing=%d conflicts=%d unexpected=%d err=%v", missing, conflicts, unexpected, err)
	}
}

type fakeControl struct{}

func (fakeControl) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)  { return nil, nil }
func (fakeControl) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }
func (fakeControl) QueryRow(context.Context, string, ...any) pgx.Row        { return nil }

type fakeResolver struct{}

func (fakeResolver) Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error) {
	return shardphysical.Resolution{}, nil
}
