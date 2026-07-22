package app

import (
	"errors"
	"sync"
)

var ErrInvalidExecutionSlotCapacity = errors.New("execution slot capacity must be positive")

// ExecutionSlots bounds in-process work without creating a hidden wait queue.
// TryAcquire is deliberately non-blocking so callers can reject overload
// immediately and provide a bounded Retry-After response.
type ExecutionSlots struct {
	slots chan struct{}
}

func NewExecutionSlots(capacity int) (*ExecutionSlots, error) {
	if capacity <= 0 {
		return nil, ErrInvalidExecutionSlotCapacity
	}
	return &ExecutionSlots{slots: make(chan struct{}, capacity)}, nil
}

// TryAcquire returns an idempotent release function when capacity is
// available. It never waits for another request to finish.
func (s *ExecutionSlots) TryAcquire() (release func(), acquired bool) {
	if s == nil || s.slots == nil {
		return nil, false
	}
	select {
	case s.slots <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-s.slots
			})
		}, true
	default:
		return nil, false
	}
}

func (s *ExecutionSlots) Inflight() int {
	if s == nil {
		return 0
	}
	return len(s.slots)
}

func (s *ExecutionSlots) Capacity() int {
	if s == nil {
		return 0
	}
	return cap(s.slots)
}
