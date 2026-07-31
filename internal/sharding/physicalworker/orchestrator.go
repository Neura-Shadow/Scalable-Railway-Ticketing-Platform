// Package physicalworker coordinates bounded work across the fixed physical
// booking-shard allowlist. It does not discover shards or route business
// writes; callers must pass handles already resolved by the physical registry.
package physicalworker

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
)

const (
	maxPhysicalShards = 2
	maxWorkLimit      = 10_000
	maxShardTimeout   = 5 * time.Minute
)

var (
	ErrInvalidConfig = errors.New("invalid physical shard worker configuration")
	ErrPartial       = errors.New("physical shard worker pass partially completed")
	ErrUnavailable   = errors.New("physical shard worker pass unavailable")
)

type Status string

const (
	StatusComplete    Status = "complete"
	StatusPartial     Status = "partial"
	StatusUnavailable Status = "unavailable"
)

// Handle is the narrow registry-handle view required by a shard worker.
// physical.Handle implements it; the interface keeps worker tests independent
// from registry construction and does not accept a DSN or catalog value.
type Handle interface {
	ShardID() sharding.ShardID
	Pool() physical.Pool
}

type Processor interface {
	Process(context.Context, Handle, int) (int, error)
}

type ProcessorFunc func(context.Context, Handle, int) (int, error)

func (function ProcessorFunc) Process(ctx context.Context, handle Handle, limit int) (int, error) {
	return function(ctx, handle, limit)
}

type Config struct {
	MaxConcurrency int
	PerShardLimit  int
	PassLimit      int
	ShardTimeout   time.Duration
}

type ShardResult struct {
	ShardID   sharding.ShardID
	Limit     int
	Processed int
	TimedOut  bool
	Error     string
}

type Result struct {
	Status    Status
	Processed int
	Shards    []ShardResult
}

type Orchestrator struct {
	handles   []Handle
	processor Processor
	config    Config

	runMu  sync.Mutex
	cursor int
}

func New(handles []Handle, processor Processor, config Config) (*Orchestrator, error) {
	if len(handles) < 1 || len(handles) > maxPhysicalShards || isNil(processor) ||
		config.MaxConcurrency < 1 || config.MaxConcurrency > len(handles) ||
		config.PerShardLimit < 1 || config.PerShardLimit > maxWorkLimit ||
		config.PassLimit < 1 || config.PassLimit > maxWorkLimit*maxPhysicalShards ||
		config.ShardTimeout <= 0 || config.ShardTimeout > maxShardTimeout {
		return nil, ErrInvalidConfig
	}

	bounded := append([]Handle(nil), handles...)
	seen := make(map[sharding.ShardID]struct{}, len(bounded))
	for _, handle := range bounded {
		if isNil(handle) || isNil(handle.Pool()) ||
			(handle.ShardID() != sharding.ShardPhysicalZero && handle.ShardID() != sharding.ShardPhysicalOne) {
			return nil, ErrInvalidConfig
		}
		if _, duplicate := seen[handle.ShardID()]; duplicate {
			return nil, ErrInvalidConfig
		}
		seen[handle.ShardID()] = struct{}{}
	}
	sort.Slice(bounded, func(left, right int) bool {
		return bounded[left].ShardID().String() < bounded[right].ShardID().String()
	})
	return &Orchestrator{handles: bounded, processor: processor, config: config}, nil
}

// RunOnce gives every scheduled shard an independent deadline and error
// boundary. The starting shard rotates after every pass, including partial and
// failed passes, so a small global budget cannot permanently favor one shard.
// Processors must honor context cancellation; PostgreSQL adapters in this
// package do so by passing the deadline to every transaction and query.
func (orchestrator *Orchestrator) RunOnce(ctx context.Context) (Result, error) {
	if orchestrator == nil || ctx == nil {
		return Result{Status: StatusUnavailable}, ErrUnavailable
	}
	orchestrator.runMu.Lock()
	defer orchestrator.runMu.Unlock()

	ordered := orchestrator.rotatedHandles()
	limits := allocateLimits(len(ordered), orchestrator.config.PerShardLimit, orchestrator.config.PassLimit)
	orchestrator.cursor = (orchestrator.cursor + 1) % len(orchestrator.handles)

	results := make([]ShardResult, len(ordered))
	semaphore := make(chan struct{}, orchestrator.config.MaxConcurrency)
	var wait sync.WaitGroup
	for index, handle := range ordered {
		limit := limits[index]
		if limit == 0 {
			continue
		}
		results[index] = ShardResult{ShardID: handle.ShardID(), Limit: limit}
		wait.Add(1)
		go func(index int, handle Handle, limit int) {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				results[index].Error = "shard_work_failed"
				results[index].TimedOut = errors.Is(ctx.Err(), context.DeadlineExceeded)
				return
			}
			shardCtx, cancel := context.WithTimeout(ctx, orchestrator.config.ShardTimeout)
			processed, err := orchestrator.processor.Process(shardCtx, handle, limit)
			timedOut := errors.Is(shardCtx.Err(), context.DeadlineExceeded)
			cancel()
			if processed < 0 || processed > limit {
				processed = 0
				err = ErrInvalidConfig
			}
			results[index].Processed = processed
			if err != nil {
				results[index].Error = "shard_work_failed"
				results[index].TimedOut = timedOut || errors.Is(err, context.DeadlineExceeded)
			}
		}(index, handle, limit)
	}
	wait.Wait()

	result := Result{Status: StatusComplete, Shards: compactResults(results)}
	failures := 0
	for _, shardResult := range result.Shards {
		result.Processed += shardResult.Processed
		if shardResult.Error != "" {
			failures++
		}
	}
	switch {
	case failures == 0:
		return result, nil
	case failures == len(result.Shards):
		result.Status = StatusUnavailable
		return result, ErrUnavailable
	default:
		result.Status = StatusPartial
		return result, ErrPartial
	}
}

func (orchestrator *Orchestrator) rotatedHandles() []Handle {
	ordered := make([]Handle, 0, len(orchestrator.handles))
	for offset := range orchestrator.handles {
		ordered = append(ordered, orchestrator.handles[(orchestrator.cursor+offset)%len(orchestrator.handles)])
	}
	return ordered
}

func allocateLimits(shardCount, perShardLimit, passLimit int) []int {
	limits := make([]int, shardCount)
	remaining := min(passLimit, shardCount*perShardLimit)
	for remaining > 0 {
		progress := false
		for index := range limits {
			if remaining == 0 {
				break
			}
			if limits[index] == perShardLimit {
				continue
			}
			limits[index]++
			remaining--
			progress = true
		}
		if !progress {
			break
		}
	}
	return limits
}

func compactResults(results []ShardResult) []ShardResult {
	compacted := make([]ShardResult, 0, len(results))
	for _, result := range results {
		if result.Limit > 0 {
			compacted = append(compacted, result)
		}
	}
	return compacted
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
