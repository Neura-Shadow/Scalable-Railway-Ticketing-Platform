package workerlane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerlane"
)

func TestRunDoesNotLetASlowControlLaneStarvePhysicalWork(t *testing.T) {
	t.Parallel()

	physicalCalled := make(chan struct{}, 1)
	outcome, err := workerlane.Run(context.Background(), 25*time.Millisecond, 25*time.Millisecond,
		func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
		func(context.Context) (int, error) {
			physicalCalled <- struct{}{}
			return 7, nil
		})
	if err != nil || outcome.Physical != 7 || outcome.PhysicalErr != nil ||
		!errors.Is(outcome.ControlErr, context.DeadlineExceeded) {
		t.Fatalf("Run() = %+v, %v", outcome, err)
	}
	select {
	case <-physicalCalled:
	default:
		t.Fatal("healthy physical lane was not called")
	}
}

func TestRunDoesNotLetASlowPhysicalLaneStarveControlWork(t *testing.T) {
	t.Parallel()

	outcome, err := workerlane.Run(context.Background(), 25*time.Millisecond, 25*time.Millisecond,
		func(context.Context) (int, error) { return 9, nil },
		func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})
	if err != nil || outcome.Control != 9 || outcome.ControlErr != nil ||
		!errors.Is(outcome.PhysicalErr, context.DeadlineExceeded) {
		t.Fatalf("Run() = %+v, %v", outcome, err)
	}
}

func TestRunPreservesDistinctControlAndPhysicalBudgets(t *testing.T) {
	t.Parallel()

	outcome, err := workerlane.Run(context.Background(), 100*time.Millisecond, 10*time.Millisecond,
		func(context.Context) (int, error) {
			time.Sleep(30 * time.Millisecond)
			return 11, nil
		},
		func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})
	if err != nil || outcome.Control != 11 || outcome.ControlErr != nil ||
		!errors.Is(outcome.PhysicalErr, context.DeadlineExceeded) {
		t.Fatalf("Run() = %+v, %v", outcome, err)
	}
}
