package routecache

import (
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestCacheReturnsStoredRouteBeforeExpiry(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	cache, err := New(Config{
		Enabled:    true,
		TTL:        time.Minute,
		MaxEntries: 1,
		Now:        func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	trainRunID := uuid.New()
	want := mustRoute(t, trainRunID, sharding.ShardZero, 7)
	if err := cache.Put(want); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got, ok := cache.Get(trainRunID)
	if !ok {
		t.Fatal("Get() found = false, want true")
	}
	if got != want {
		t.Fatalf("Get() = %#v, want %#v", got, want)
	}
}

func TestDisabledCacheAlwaysMisses(t *testing.T) {
	cache, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	trainRunID := uuid.New()
	if err := cache.Put(mustRoute(t, trainRunID, sharding.ShardOne, 3)); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if _, ok := cache.Get(trainRunID); ok {
		t.Fatal("Get() found = true for a disabled cache")
	}

	cache.Invalidate(trainRunID)
	if _, ok := cache.Get(trainRunID); ok {
		t.Fatal("Get() found = true after invalidating a disabled cache")
	}
}

func TestCacheExpiresUsingInjectedClock(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	cache, err := New(Config{
		Enabled:    true,
		TTL:        time.Second,
		MaxEntries: 1,
		Now:        func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	trainRunID := uuid.New()
	if err := cache.Put(mustRoute(t, trainRunID, sharding.ShardZero, 1)); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	now = now.Add(time.Second)
	if _, ok := cache.Get(trainRunID); ok {
		t.Fatal("Get() found = true at the expiry boundary")
	}
}

func TestEnabledCacheRejectsUnboundedConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   error
	}{
		{
			name:   "non-positive TTL",
			config: Config{Enabled: true, MaxEntries: 1},
			want:   ErrNonPositiveTTL,
		},
		{
			name:   "non-positive maximum entries",
			config: Config{Enabled: true, TTL: time.Second},
			want:   ErrNonPositiveMaxEntries,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(test.config)
			if !errors.Is(err, test.want) {
				t.Fatalf("New() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestCacheEvictsLeastRecentlyUsedRouteAtCapacity(t *testing.T) {
	cache, err := New(Config{Enabled: true, TTL: time.Minute, MaxEntries: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first, second, third := uuid.New(), uuid.New(), uuid.New()
	if err := cache.Put(mustRoute(t, first, sharding.ShardZero, 1)); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(mustRoute(t, second, sharding.ShardOne, 1)); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Get(first); !ok {
		t.Fatal("Get(first) found = false, want true")
	}
	if err := cache.Put(mustRoute(t, third, sharding.ShardZero, 2)); err != nil {
		t.Fatal(err)
	}

	if _, ok := cache.Get(second); ok {
		t.Fatal("Get(second) found = true, want eviction")
	}
	for _, trainRunID := range []uuid.UUID{first, third} {
		if _, ok := cache.Get(trainRunID); !ok {
			t.Fatalf("Get(%s) found = false, want retained route", trainRunID)
		}
	}
}

func TestPutReplacesRouteForSameTrainRun(t *testing.T) {
	cache, err := New(Config{Enabled: true, TTL: time.Minute, MaxEntries: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	trainRunID := uuid.New()
	if err := cache.Put(mustRoute(t, trainRunID, sharding.ShardZero, 1)); err != nil {
		t.Fatal(err)
	}
	want := mustRoute(t, trainRunID, sharding.ShardOne, 2)
	if err := cache.Put(want); err != nil {
		t.Fatal(err)
	}

	got, ok := cache.Get(trainRunID)
	if !ok {
		t.Fatal("Get() found = false, want true")
	}
	if got != want {
		t.Fatalf("Get() = %#v, want %#v", got, want)
	}
}

func TestInvalidateRemovesOneRoute(t *testing.T) {
	cache, err := New(Config{Enabled: true, TTL: time.Minute, MaxEntries: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	removed, retained := uuid.New(), uuid.New()
	if err := cache.Put(mustRoute(t, removed, sharding.ShardZero, 1)); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(mustRoute(t, retained, sharding.ShardOne, 1)); err != nil {
		t.Fatal(err)
	}
	cache.Invalidate(removed)

	if _, ok := cache.Get(removed); ok {
		t.Fatal("Get(removed) found = true, want false")
	}
	if _, ok := cache.Get(retained); !ok {
		t.Fatal("Get(retained) found = false, want true")
	}
}

func TestPutRejectsIncompleteOrUnknownAuthority(t *testing.T) {
	cache, err := New(Config{Enabled: true, TTL: time.Minute, MaxEntries: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := cache.Put(sharding.ShardRoute{}); !errors.Is(err, ErrInvalidRoute) {
		t.Fatalf("Put() error = %v, want %v", err, ErrInvalidRoute)
	}
}

func mustRoute(t *testing.T, trainRunID uuid.UUID, shardID sharding.ShardID, generation int64) sharding.ShardRoute {
	t.Helper()
	value, err := sharding.NewAssignmentGeneration(generation)
	if err != nil {
		t.Fatalf("NewAssignmentGeneration(%d): %v", generation, err)
	}
	route, err := sharding.NewShardRoute(trainRunID, shardID, value)
	if err != nil {
		t.Fatalf("NewShardRoute: %v", err)
	}
	return route
}
