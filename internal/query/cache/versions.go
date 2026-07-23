package cache

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const maxVersionGenerationAttempts = 4

var ErrVersionStore = errors.New("cache version store failure")

var getOrCreateVersionScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current and string.len(current) == 24 and string.match(current, '^[A-Za-z0-9_-]+$') then
  return {current, 0}
end
redis.call('SET', KEYS[1], ARGV[1])
return {ARGV[1], 1}
`)

var rotateVersionScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
`)

type VersionManager struct {
	client redis.UniversalClient
	random io.Reader
	mu     sync.Mutex
}

func NewVersionManager(client redis.UniversalClient, random io.Reader) (*VersionManager, error) {
	if client == nil || random == nil {
		return nil, ErrVersionStore
	}
	return &VersionManager{client: client, random: random}, nil
}

func NewSecureVersionManager(client redis.UniversalClient) (*VersionManager, error) {
	return NewVersionManager(client, rand.Reader)
}

func (manager *VersionManager) Get(ctx context.Context, key string) (string, bool, error) {
	if !validVersionKey(key) {
		return "", false, ErrInvalidCacheKey
	}
	value, err := manager.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("%w: get namespace", ErrVersionStore)
	}
	if !ValidVersionToken(value) {
		return "", false, nil
	}
	return value, true, nil
}

func (manager *VersionManager) GetOrCreate(ctx context.Context, key string) (string, error) {
	value, _, err := manager.GetOrCreateWithStatus(ctx, key)
	return value, err
}

// GetOrCreateWithStatus reports whether this call installed the returned
// generation. Callers that observed no generation before reading a mutable
// source can re-read when installed=false, preventing a concurrent rotation
// from being populated with a pre-invalidation snapshot.
func (manager *VersionManager) GetOrCreateWithStatus(ctx context.Context, key string) (string, bool, error) {
	if !validVersionKey(key) {
		return "", false, ErrInvalidCacheKey
	}
	candidate, err := manager.nextToken()
	if err != nil {
		return "", false, err
	}
	result, err := getOrCreateVersionScript.Run(ctx, manager.client, []string{key}, candidate).Slice()
	if err != nil {
		return "", false, fmt.Errorf("%w: get or create namespace", ErrVersionStore)
	}
	if len(result) != 2 {
		return "", false, fmt.Errorf("%w: invalid namespace result", ErrVersionStore)
	}
	value, valueOK := result[0].(string)
	created, createdOK := result[1].(int64)
	if !ValidVersionToken(value) {
		return "", false, fmt.Errorf("%w: invalid namespace result", ErrVersionStore)
	}
	if !valueOK || !createdOK || (created != 0 && created != 1) {
		return "", false, fmt.Errorf("%w: invalid namespace result", ErrVersionStore)
	}
	return value, created == 1, nil
}

func (manager *VersionManager) Rotate(ctx context.Context, key string) (string, error) {
	if !validVersionKey(key) {
		return "", ErrInvalidCacheKey
	}
	for attempt := 0; attempt < maxVersionGenerationAttempts; attempt++ {
		candidate, err := manager.nextToken()
		if err != nil {
			return "", err
		}
		changed, err := rotateVersionScript.Run(ctx, manager.client, []string{key}, candidate).Int64()
		if err != nil {
			return "", fmt.Errorf("%w: rotate namespace", ErrVersionStore)
		}
		if changed == 1 {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("%w: version collision retry exhausted", ErrVersionStore)
}

func (manager *VersionManager) nextToken() (string, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	token, err := NewVersionToken(manager.random)
	if err != nil {
		return "", fmt.Errorf("%w: generate namespace", ErrVersionStore)
	}
	return token, nil
}

func validVersionKey(key string) bool {
	if key == StationVersionKey() || key == SearchVersionKey() {
		return true
	}
	const prefix = "cache:availability:version:"
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	trainRunID, err := uuid.Parse(strings.TrimPrefix(key, prefix))
	return err == nil && trainRunID != uuid.Nil
}
