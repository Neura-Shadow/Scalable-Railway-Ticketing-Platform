package clock_test

import (
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
)

func TestDeterministicClockAdvancesExplicitly(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	deterministic := clock.NewDeterministic(start)
	var current clock.Clock = deterministic

	if got := current.Now(); !got.Equal(start) {
		t.Fatalf("Now() = %s, want %s", got, start)
	}
	deterministic.Advance(90 * time.Second)
	if got, want := current.Now(), start.Add(90*time.Second); !got.Equal(want) {
		t.Fatalf("Now() after Advance = %s, want %s", got, want)
	}
}

func TestRealClockUsesWallClock(t *testing.T) {
	before := time.Now()
	var current clock.Clock = clock.RealClock{}
	got := current.Now()
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Fatalf("RealClock.Now() = %s, want a value between %s and %s", got, before, after)
	}
}

func TestDeterministicAfterFiresOnlyWhenDue(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	deterministic := clock.NewDeterministic(start)
	ch := deterministic.After(5 * time.Minute)

	deterministic.Advance(4 * time.Minute)
	select {
	case firedAt := <-ch:
		t.Fatalf("After fired early at %s", firedAt)
	default:
	}

	deterministic.Advance(time.Minute)
	select {
	case firedAt := <-ch:
		if want := start.Add(5 * time.Minute); !firedAt.Equal(want) {
			t.Fatalf("After fired at %s, want %s", firedAt, want)
		}
	default:
		t.Fatal("After did not fire when due")
	}
}

func TestDeterministicTickerRepeatsAndStops(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	deterministic := clock.NewDeterministic(start)
	ticker := deterministic.NewTicker(time.Minute)

	deterministic.Advance(time.Minute)
	if got := <-ticker.C(); !got.Equal(start.Add(time.Minute)) {
		t.Fatalf("first tick = %s", got)
	}
	deterministic.Advance(time.Minute)
	if got := <-ticker.C(); !got.Equal(start.Add(2 * time.Minute)) {
		t.Fatalf("second tick = %s", got)
	}

	ticker.Stop()
	deterministic.Advance(time.Minute)
	select {
	case got := <-ticker.C():
		t.Fatalf("stopped ticker fired at %s", got)
	default:
	}
}

func TestDeterministicClockRejectsBackwardAdvance(t *testing.T) {
	t.Parallel()

	deterministic := clock.NewDeterministic(time.Now())
	defer func() {
		if recover() == nil {
			t.Fatal("Advance(-1) did not panic")
		}
	}()
	deterministic.Advance(-time.Nanosecond)
}
