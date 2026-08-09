package cache

import (
	"context"
	"crypto/rand"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/google/uuid"
)

func TestVersionTokensAreCollisionResistantBoundedAndKeysAreExact(t *testing.T) {
	token, err := NewVersionToken(rand.Reader)
	if err != nil {
		t.Fatalf("NewVersionToken() error = %v", err)
	}
	if !ValidVersionToken(token) || len(token) != 24 {
		t.Fatalf("version token = %q", token)
	}
	trainRunID := uuid.New()
	availabilityKey, err := AvailabilityDataKey(token, trainRunID.String(), "tpe", "khh", "business")
	if err != nil {
		t.Fatalf("AvailabilityDataKey() error = %v", err)
	}
	want := "cache:availability:" + token + ":" + trainRunID.String() + ":TPE:KHH:business"
	if availabilityKey != want {
		t.Fatalf("AvailabilityDataKey() = %q, want %q", availabilityKey, want)
	}
	if _, err := AvailabilityDataKey(token, trainRunID.String(), "TPE", "TPE", "business"); err == nil {
		t.Fatal("AvailabilityDataKey(same station) error = nil")
	}
}

func TestSearchHashUsesStableNormalizedSchemaWithoutRawQueryMaterial(t *testing.T) {
	normalized, err := querypostgres.NormalizeSearch(querypostgres.SearchRequest{
		OriginCode: " tpe ", DestinationCode: "khh", ServiceDate: time.Date(2026, 7, 22, 23, 0, 0, 0, time.FixedZone("test", 8*60*60)),
		SeatClass: "STANDARD", Page: 2, PageSize: 25, Sort: "fare_desc",
	})
	if err != nil {
		t.Fatalf("NormalizeSearch() error = %v", err)
	}
	hashA := SearchQueryHash(normalized)
	hashB := SearchQueryHash(normalized)
	if hashA != hashB || len(hashA) != 64 {
		t.Fatalf("search hashes = %q/%q", hashA, hashB)
	}
	key, err := SearchDataKey(strings.Repeat("a", 24), hashA)
	if err != nil {
		t.Fatalf("SearchDataKey() error = %v", err)
	}
	for _, forbidden := range []string{"TPE", "KHH", "standard", "fare"} {
		if strings.Contains(key, forbidden) {
			t.Fatalf("search cache key leaked %q: %q", forbidden, key)
		}
	}
}

func TestTTLPolicyReturnsOnlyBoundedPositiveJitter(t *testing.T) {
	policy, err := NewTTLPolicy(30*time.Second, 5*time.Second, rand.Reader)
	if err != nil {
		t.Fatalf("NewTTLPolicy() error = %v", err)
	}
	for attempt := 0; attempt < 100; attempt++ {
		ttl, err := policy.Next()
		if err != nil {
			t.Fatalf("TTLPolicy.Next() error = %v", err)
		}
		if ttl < 30*time.Second || ttl > 35*time.Second {
			t.Fatalf("TTLPolicy.Next() = %s, want [30s,35s]", ttl)
		}
	}
}

func TestCoalescerSharesIdenticalFillButDoesNotSerializeUnrelatedKeys(t *testing.T) {
	joined := make(chan struct{}, 32)
	coalescer := &Coalescer{onJoin: func() { joined <- struct{}{} }}
	start := make(chan struct{})
	release := make(chan struct{})
	var identicalCalls atomic.Int64
	var unrelatedCalls atomic.Int64
	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err, _ := coalescer.Do(context.Background(), "same", func(context.Context) (any, error) {
				if identicalCalls.Add(1) == 1 {
					close(start)
				}
				<-release
				return "value", nil
			})
			if err != nil {
				t.Errorf("identical Coalescer.Do() error = %v", err)
			}
		}()
	}
	for index := 0; index < 32; index++ {
		<-joined
	}
	<-start
	_, err, shared := coalescer.Do(context.Background(), "different", func(context.Context) (any, error) {
		unrelatedCalls.Add(1)
		return "other", nil
	})
	if err != nil || shared || unrelatedCalls.Load() != 1 {
		t.Fatalf("unrelated Coalescer.Do() = shared %v calls %d error %v", shared, unrelatedCalls.Load(), err)
	}
	close(release)
	wait.Wait()
	if identicalCalls.Load() != 1 {
		t.Fatalf("identical fills = %d, want 1", identicalCalls.Load())
	}
}

func TestCoalescerLeaderCancellationDoesNotAbortHealthyWaiter(t *testing.T) {
	coalescer := &Coalescer{}
	leaderContext, cancelLeader := context.WithCancel(context.Background())
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	fill := func(ctx context.Context) (any, error) {
		calls.Add(1)
		close(entered)
		select {
		case <-release:
			return "filled", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	type outcome struct {
		value any
		err   error
	}
	leader := make(chan outcome, 1)
	go func() {
		value, err, _ := coalescer.Do(leaderContext, "shared-key", fill)
		leader <- outcome{value: value, err: err}
	}()
	<-entered
	waiter := make(chan outcome, 1)
	go func() {
		value, err, _ := coalescer.Do(context.Background(), "shared-key", fill)
		waiter <- outcome{value: value, err: err}
	}()
	for attempt := 0; attempt < 1024; attempt++ {
		runtime.Gosched()
	}
	cancelLeader()
	if result := <-leader; !errors.Is(result.err, context.Canceled) {
		t.Fatalf("leader result = %+v, want context cancellation", result)
	}
	close(release)
	if result := <-waiter; result.err != nil || result.value != "filled" {
		t.Fatalf("waiter result = %+v, want shared fill", result)
	}
	if calls.Load() != 1 {
		t.Fatalf("fill calls = %d, want 1", calls.Load())
	}
}
