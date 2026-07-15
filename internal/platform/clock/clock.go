// Package clock provides production and deterministic sources of time.
package clock

import (
	"sync"
	"time"
)

// Clock supplies the current time. Domain and worker code should depend on this
// interface instead of calling time.Now directly.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
	NewTicker(time.Duration) Ticker
}

// Ticker delivers periodic clock instants until stopped.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock uses the process wall clock.
type RealClock struct{}

// Now returns time.Now.
func (RealClock) Now() time.Time {
	return time.Now()
}

// After delegates to time.After.
func (RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// NewTicker creates a wall-clock ticker.
func (RealClock) NewTicker(d time.Duration) Ticker {
	return realTicker{ticker: time.NewTicker(d)}
}

type realTicker struct {
	ticker *time.Ticker
}

func (t realTicker) C() <-chan time.Time { return t.ticker.C }
func (t realTicker) Stop()               { t.ticker.Stop() }

// DeterministicClock advances only when instructed by a test.
type DeterministicClock struct {
	mu        sync.RWMutex
	now       time.Time
	scheduled map[*scheduled]struct{}
}

type scheduled struct {
	deadline time.Time
	interval time.Duration
	ch       chan time.Time
}

type deterministicTicker struct {
	clock *DeterministicClock
	event *scheduled
}

// NewDeterministic creates a deterministic clock at start.
func NewDeterministic(start time.Time) *DeterministicClock {
	return &DeterministicClock{
		now:       start,
		scheduled: make(map[*scheduled]struct{}),
	}
}

// Now returns the deterministic current time.
func (c *DeterministicClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// After returns a channel that receives when Advance reaches the deadline.
func (c *DeterministicClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.scheduled[&scheduled{deadline: c.now.Add(d), ch: ch}] = struct{}{}
	return ch
}

// NewTicker creates a ticker driven by Advance. It panics for a non-positive
// interval, matching time.NewTicker.
func (c *DeterministicClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for NewTicker")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	event := &scheduled{
		deadline: c.now.Add(d),
		interval: d,
		ch:       make(chan time.Time, 1),
	}
	c.scheduled[event] = struct{}{}
	return &deterministicTicker{clock: c, event: event}
}

func (t *deterministicTicker) C() <-chan time.Time { return t.event.ch }

func (t *deterministicTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	delete(t.clock.scheduled, t.event)
}

// Advance moves the clock forward by d.
func (c *DeterministicClock) Advance(d time.Duration) {
	if d < 0 {
		panic("clock: cannot advance backwards")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	target := c.now.Add(d)
	for event := range c.scheduled {
		if event.deadline.After(target) {
			continue
		}
		select {
		case event.ch <- event.deadline:
		default:
		}
		if event.interval == 0 {
			delete(c.scheduled, event)
			continue
		}
		elapsedIntervals := target.Sub(event.deadline)/event.interval + 1
		event.deadline = event.deadline.Add(elapsedIntervals * event.interval)
	}
	c.now = target
}
