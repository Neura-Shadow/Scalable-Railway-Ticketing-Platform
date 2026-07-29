package physicalworker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/jackc/pgx/v5"
)

func TestOrchestratorRotatesWhenPassBudgetIsSmallerThanTopology(t *testing.T) {
	processor := &recordingProcessor{}
	orchestrator, err := New([]Handle{
		fakeHandle{id: sharding.ShardPhysicalOne, pool: workerPool{}},
		fakeHandle{id: sharding.ShardPhysicalZero, pool: workerPool{}},
	}, processor, Config{
		MaxConcurrency: 1,
		PerShardLimit:  1,
		PassLimit:      1,
		ShardTimeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first, err := orchestrator.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("first RunOnce() error = %v", err)
	}
	second, err := orchestrator.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}

	if got, want := first.Shards[0].ShardID, sharding.ShardPhysicalZero; got != want {
		t.Fatalf("first shard = %q, want %q", got, want)
	}
	if got, want := second.Shards[0].ShardID, sharding.ShardPhysicalOne; got != want {
		t.Fatalf("second shard = %q, want %q", got, want)
	}
}

func TestOrchestratorIsolatesFailureAndBoundsConcurrency(t *testing.T) {
	processor := &recordingProcessor{
		fail:  map[sharding.ShardID]error{sharding.ShardPhysicalZero: errors.New("dsn must not escape")},
		block: make(chan struct{}),
	}
	orchestrator, err := New([]Handle{
		fakeHandle{id: sharding.ShardPhysicalZero, pool: workerPool{}},
		fakeHandle{id: sharding.ShardPhysicalOne, pool: workerPool{}},
	}, processor, Config{
		MaxConcurrency: 1,
		PerShardLimit:  2,
		PassLimit:      4,
		ShardTimeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	close(processor.block)
	result, err := orchestrator.RunOnce(context.Background())
	if !errors.Is(err, ErrPartial) {
		t.Fatalf("RunOnce() error = %v, want ErrPartial", err)
	}
	if result.Status != StatusPartial || result.Processed != 2 || len(result.Shards) != 2 {
		t.Fatalf("RunOnce() result = %+v", result)
	}
	if processor.maxActive.Load() > 1 {
		t.Fatalf("max active = %d, want <= 1", processor.maxActive.Load())
	}
	if err.Error() == "dsn must not escape" || result.Shards[0].Error == "dsn must not escape" {
		t.Fatal("raw shard failure escaped the worker boundary")
	}
}

func TestOrchestratorAllocatesGlobalBudgetFairly(t *testing.T) {
	processor := &recordingProcessor{}
	orchestrator, err := New([]Handle{
		fakeHandle{id: sharding.ShardPhysicalZero, pool: workerPool{}},
		fakeHandle{id: sharding.ShardPhysicalOne, pool: workerPool{}},
	}, processor, Config{
		MaxConcurrency: 2,
		PerShardLimit:  3,
		PassLimit:      5,
		ShardTimeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first, err := orchestrator.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("first RunOnce() error = %v", err)
	}
	second, err := orchestrator.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}

	assertBudgets(t, first, map[sharding.ShardID]int{
		sharding.ShardPhysicalZero: 3,
		sharding.ShardPhysicalOne:  2,
	})
	assertBudgets(t, second, map[sharding.ShardID]int{
		sharding.ShardPhysicalZero: 2,
		sharding.ShardPhysicalOne:  3,
	})
}

func TestOrchestratorAppliesPerShardDeadline(t *testing.T) {
	processor := ProcessorFunc(func(ctx context.Context, _ Handle, _ int) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	orchestrator, err := New([]Handle{
		fakeHandle{id: sharding.ShardPhysicalZero, pool: workerPool{}},
	}, processor, Config{
		MaxConcurrency: 1,
		PerShardLimit:  1,
		PassLimit:      1,
		ShardTimeout:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := orchestrator.RunOnce(context.Background())
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("RunOnce() error = %v, want ErrUnavailable", err)
	}
	if result.Status != StatusUnavailable || !result.Shards[0].TimedOut {
		t.Fatalf("RunOnce() result = %+v", result)
	}
}

func TestNewRejectsUnboundedOrUnapprovedTopology(t *testing.T) {
	validConfig := Config{MaxConcurrency: 1, PerShardLimit: 1, PassLimit: 1, ShardTimeout: time.Second}
	tests := []struct {
		name    string
		handles []Handle
		config  Config
	}{
		{name: "empty", config: validConfig},
		{name: "nil pool", handles: []Handle{fakeHandle{id: sharding.ShardPhysicalZero}}, config: validConfig},
		{name: "logical shard", handles: []Handle{fakeHandle{id: sharding.ShardZero, pool: workerPool{}}}, config: validConfig},
		{name: "duplicate", handles: []Handle{
			fakeHandle{id: sharding.ShardPhysicalZero, pool: workerPool{}},
			fakeHandle{id: sharding.ShardPhysicalZero, pool: workerPool{}},
		}, config: validConfig},
		{name: "invalid concurrency", handles: []Handle{fakeHandle{id: sharding.ShardPhysicalZero, pool: workerPool{}}}, config: Config{
			MaxConcurrency: 3, PerShardLimit: 1, PassLimit: 1, ShardTimeout: time.Second,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.handles, ProcessorFunc(func(context.Context, Handle, int) (int, error) {
				return 0, nil
			}), test.config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func assertBudgets(t *testing.T, result Result, want map[sharding.ShardID]int) {
	t.Helper()
	for _, shard := range result.Shards {
		if shard.Limit != want[shard.ShardID] {
			t.Fatalf("shard %q limit = %d, want %d; result=%+v", shard.ShardID, shard.Limit, want[shard.ShardID], result)
		}
	}
}

type fakeHandle struct {
	id   sharding.ShardID
	pool physical.Pool
}

func (handle fakeHandle) ShardID() sharding.ShardID { return handle.id }
func (handle fakeHandle) Pool() physical.Pool       { return handle.pool }

type workerPool struct{}

func (workerPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return nil, nil }
func (workerPool) Close()                                                 {}

type recordingProcessor struct {
	mu        sync.Mutex
	fail      map[sharding.ShardID]error
	block     chan struct{}
	active    atomic.Int32
	maxActive atomic.Int32
}

func (processor *recordingProcessor) Process(ctx context.Context, handle Handle, limit int) (int, error) {
	active := processor.active.Add(1)
	defer processor.active.Add(-1)
	for {
		maximum := processor.maxActive.Load()
		if active <= maximum || processor.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	if processor.block != nil {
		select {
		case <-processor.block:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	processor.mu.Lock()
	err := processor.fail[handle.ShardID()]
	processor.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return limit, nil
}
