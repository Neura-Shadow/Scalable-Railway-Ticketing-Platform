package app

import (
	"sync"
	"testing"
)

func TestExecutionSlotsRejectImmediatelyAtCapacityAndRecoverAfterRelease(t *testing.T) {
	t.Parallel()

	slots, err := NewExecutionSlots(1)
	if err != nil {
		t.Fatalf("NewExecutionSlots() error = %v", err)
	}
	release, ok := slots.TryAcquire()
	if !ok {
		t.Fatal("first TryAcquire() rejected")
	}
	if _, ok := slots.TryAcquire(); ok {
		t.Fatal("TryAcquire() accepted above capacity")
	}
	if got := slots.Inflight(); got != 1 {
		t.Fatalf("Inflight() = %d, want 1", got)
	}

	release()
	if got := slots.Inflight(); got != 0 {
		t.Fatalf("Inflight() after release = %d, want 0", got)
	}
	if releaseAgain, ok := slots.TryAcquire(); !ok {
		t.Fatal("TryAcquire() rejected after release")
	} else {
		releaseAgain()
	}
}

func TestExecutionSlotReleaseIsIdempotentUnderConcurrency(t *testing.T) {
	t.Parallel()

	slots, err := NewExecutionSlots(1)
	if err != nil {
		t.Fatalf("NewExecutionSlots() error = %v", err)
	}
	release, ok := slots.TryAcquire()
	if !ok {
		t.Fatal("TryAcquire() rejected")
	}

	var wait sync.WaitGroup
	for range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			release()
		}()
	}
	wait.Wait()

	if got := slots.Inflight(); got != 0 {
		t.Fatalf("Inflight() = %d, want 0", got)
	}
}

func TestExecutionSlotsRejectInvalidCapacity(t *testing.T) {
	t.Parallel()

	for _, capacity := range []int{-1, 0} {
		if _, err := NewExecutionSlots(capacity); err == nil {
			t.Fatalf("NewExecutionSlots(%d) error = nil", capacity)
		}
	}
}
