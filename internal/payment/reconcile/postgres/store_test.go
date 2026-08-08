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

type fakeControl struct{}

func (fakeControl) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)  { return nil, nil }
func (fakeControl) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }
func (fakeControl) QueryRow(context.Context, string, ...any) pgx.Row        { return nil }

type fakeResolver struct{}

func (fakeResolver) Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error) {
	return shardphysical.Resolution{}, nil
}
