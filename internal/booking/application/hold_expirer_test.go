package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/application"
	"github.com/google/uuid"
)

func TestHoldExpirerUsesDeterministicClockAndConfiguredBatch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 15, 3, 0, 0, 0, time.FixedZone("test", 8*60*60))
	store := &expirationStoreStub{ids: []uuid.UUID{uuid.New(), uuid.New()}}
	expirer, err := application.NewHoldExpirer(store, expirationClockStub{now}, nil, 25)
	if err != nil {
		t.Fatal(err)
	}

	result, err := expirer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Expired != 2 || store.batch != 25 || !store.now.Equal(now.UTC()) {
		t.Fatalf("result=%+v batch=%d now=%s", result, store.batch, store.now)
	}
}

func TestHoldExpirerReportsStoreFailureWithoutInventingProgress(t *testing.T) {
	t.Parallel()

	store := &expirationStoreStub{err: errors.New("database offline")}
	expirer, err := application.NewHoldExpirer(store, expirationClockStub{time.Now()}, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	result, err := expirer.RunOnce(context.Background())
	if err == nil || result.Expired != 0 {
		t.Fatalf("RunOnce() result=%+v error=%v", result, err)
	}
}

type expirationClockStub struct{ now time.Time }

func (c expirationClockStub) Now() time.Time { return c.now }

type expirationStoreStub struct {
	ids   []uuid.UUID
	err   error
	now   time.Time
	batch int
}

func (s *expirationStoreStub) ExpireDue(_ context.Context, now time.Time, batchSize int) ([]uuid.UUID, error) {
	s.now = now
	s.batch = batchSize
	return append([]uuid.UUID(nil), s.ids...), s.err
}
