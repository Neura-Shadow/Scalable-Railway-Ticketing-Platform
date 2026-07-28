// Package routecache provides a bounded, process-local cache for immutable
// train-run routing decisions. It is advisory only; callers must retain the
// authoritative catalog lookup as their cache-miss path.
package routecache

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

var (
	ErrNonPositiveTTL        = errors.New("route cache TTL must be positive when enabled")
	ErrNonPositiveMaxEntries = errors.New("route cache max entries must be positive when enabled")
	ErrInvalidRoute          = errors.New("route cache requires a validated route")
)

// Config controls an optional bounded cache. Now is injected for deterministic
// expiry tests; when omitted, the system clock is used.
type Config struct {
	Enabled    bool
	TTL        time.Duration
	MaxEntries int
	Now        func() time.Time
}

// Cache is safe for concurrent callers.
type Cache struct {
	enabled bool
	ttl     time.Duration
	now     func() time.Time
	max     int

	mu      sync.Mutex
	entries map[uuid.UUID]*list.Element
	lru     *list.List
}

type entry struct {
	trainRunID uuid.UUID
	route      sharding.ShardRoute
	expiresAt  time.Time
}

// New creates a cache. A disabled cache accepts zero limits and never retains
// data; this makes disabling it a safe configuration-only change.
func New(config Config) (*Cache, error) {
	if config.Enabled && config.TTL <= 0 {
		return nil, ErrNonPositiveTTL
	}
	if config.Enabled && config.MaxEntries <= 0 {
		return nil, ErrNonPositiveMaxEntries
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Cache{
		enabled: config.Enabled,
		ttl:     config.TTL,
		now:     now,
		max:     config.MaxEntries,
		entries: make(map[uuid.UUID]*list.Element),
		lru:     list.New(),
	}, nil
}

// Get returns the current cached route. An expired entry is removed before
// returning a miss.
func (c *Cache) Get(trainRunID uuid.UUID) (sharding.ShardRoute, bool) {
	if !c.enabled {
		return sharding.ShardRoute{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.entries[trainRunID]
	if !ok {
		return sharding.ShardRoute{}, false
	}
	stored := element.Value.(entry)
	if !c.now().Before(stored.expiresAt) {
		c.remove(element)
		return sharding.ShardRoute{}, false
	}
	c.lru.MoveToFront(element)
	return stored.route, true
}

// Put stores the latest route for one train run, replacing its prior cached
// generation if present. Insertion evicts the least recently used entry first.
func (c *Cache) Put(route sharding.ShardRoute) error {
	if !c.enabled {
		return nil
	}
	trainRunID := route.TrainRunID()
	if trainRunID == uuid.Nil || route.Generation().Int64() <= 0 {
		return ErrInvalidRoute
	}
	if _, err := sharding.ParseShardID(route.ShardID().String()); err != nil {
		return ErrInvalidRoute
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	value := entry{trainRunID: trainRunID, route: route, expiresAt: c.now().Add(c.ttl)}
	if element, ok := c.entries[trainRunID]; ok {
		element.Value = value
		c.lru.MoveToFront(element)
		return nil
	}
	if c.lru.Len() == c.max {
		c.remove(c.lru.Back())
	}
	c.entries[trainRunID] = c.lru.PushFront(value)
	return nil
}

// Invalidate removes a cached route, if one exists.
func (c *Cache) Invalidate(trainRunID uuid.UUID) {
	if !c.enabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[trainRunID]; ok {
		c.remove(element)
	}
}

func (c *Cache) remove(element *list.Element) {
	if element == nil {
		return
	}
	stored := element.Value.(entry)
	delete(c.entries, stored.trainRunID)
	c.lru.Remove(element)
}
