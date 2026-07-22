package cache

import (
	"context"
	"crypto/rand"
	"os"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestVersionManagerAtomicCreationRotationAndVersionLossRecovery(t *testing.T) {
	address := os.Getenv("TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("TEST_REDIS_ADDR is not configured")
	}
	client := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush Redis test database: %v", err)
	}
	manager, err := NewVersionManager(client, rand.Reader)
	if err != nil {
		t.Fatalf("NewVersionManager() error = %v", err)
	}

	const replicas = 16
	tokens := make(chan string, replicas)
	var wait sync.WaitGroup
	for replica := 0; replica < replicas; replica++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			token, getErr := manager.GetOrCreate(ctx, SearchVersionKey())
			if getErr != nil {
				t.Errorf("GetOrCreate() error = %v", getErr)
				return
			}
			tokens <- token
		}()
	}
	wait.Wait()
	close(tokens)
	var initial string
	for token := range tokens {
		if initial == "" {
			initial = token
		}
		if token != initial {
			t.Fatalf("atomic version tokens differ: %q/%q", initial, token)
		}
	}
	dataKey, err := SearchDataKey(initial, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("SearchDataKey() error = %v", err)
	}
	if err := client.Set(ctx, dataKey, "old-data", 0).Err(); err != nil {
		t.Fatalf("seed old namespace data: %v", err)
	}
	if err := client.Del(ctx, SearchVersionKey()).Err(); err != nil {
		t.Fatalf("delete version pointer: %v", err)
	}
	recovered, err := manager.GetOrCreate(ctx, SearchVersionKey())
	if err != nil {
		t.Fatalf("GetOrCreate(recovery) error = %v", err)
	}
	if recovered == initial || !ValidVersionToken(recovered) {
		t.Fatalf("recovered version = %q, old %q", recovered, initial)
	}
	if old, err := client.Get(ctx, dataKey).Result(); err != nil || old != "old-data" {
		t.Fatalf("old namespace data = %q, %v", old, err)
	}
	rotated, err := manager.Rotate(ctx, SearchVersionKey())
	if err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}
	if rotated == recovered {
		t.Fatalf("Rotate() reused current token %q", rotated)
	}
	if err := client.Set(ctx, SearchVersionKey(), "malformed", 0).Err(); err != nil {
		t.Fatalf("seed malformed version: %v", err)
	}
	repaired, err := manager.GetOrCreate(ctx, SearchVersionKey())
	if err != nil || !ValidVersionToken(repaired) || repaired == "malformed" {
		t.Fatalf("GetOrCreate(malformed) = %q, %v", repaired, err)
	}
}
