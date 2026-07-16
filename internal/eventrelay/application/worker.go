package application

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/domain"
	"github.com/google/uuid"
)

type Store interface {
	Claim(ctx context.Context, workerID string, batchSize int, now, staleBefore time.Time) ([]domain.Event, error)
	MarkPublished(ctx context.Context, eventID uuid.UUID, workerID string, publishedAt time.Time) error
	MarkFailed(ctx context.Context, eventID uuid.UUID, workerID string, nextAttemptAt time.Time, deadLetter bool) error
}

type Publisher interface {
	Publish(ctx context.Context, event domain.Event) error
}

type Clock interface {
	Now() time.Time
}

type Metrics interface {
	RecordOutbox(operation, eventType, result, reason string)
}

type Config struct {
	WorkerID          string
	BatchSize         int
	MaxAttempts       int
	ProcessingTimeout time.Duration
	RetryBase         time.Duration
	RetryMax          time.Duration
}

type Worker struct {
	store     Store
	publisher Publisher
	clock     Clock
	metrics   Metrics
	config    Config
}

type Result struct {
	Claimed    int
	Published  int
	Retried    int
	DeadLetter int
}

func NewWorker(store Store, publisher Publisher, clock Clock, metrics Metrics, config Config) (*Worker, error) {
	if store == nil || publisher == nil || clock == nil || config.WorkerID == "" || config.BatchSize <= 0 || config.MaxAttempts <= 0 || config.ProcessingTimeout <= 0 || config.RetryBase <= 0 || config.RetryMax < config.RetryBase {
		return nil, errors.New("invalid outbox worker configuration")
	}
	return &Worker{store: store, publisher: publisher, clock: clock, metrics: metrics, config: config}, nil
}

// RunOnce claims in one short store transaction, publishes with no database
// transaction held, then finalizes every event independently.
func (w *Worker) RunOnce(ctx context.Context) (Result, error) {
	now := w.clock.Now().UTC()
	events, err := w.store.Claim(ctx, w.config.WorkerID, w.config.BatchSize, now, now.Add(-w.config.ProcessingTimeout))
	if err != nil {
		w.record("claim", "unknown", "failure", "database")
		return Result{}, fmt.Errorf("claim outbox: %w", err)
	}
	result := Result{Claimed: len(events)}
	for _, event := range events {
		w.record("claim", event.EventType, "success", "none")
	}
	var failures []error
	for _, event := range events {
		if err := w.publisher.Publish(ctx, event); err != nil {
			deadLetter := event.Attempts >= w.config.MaxAttempts
			nextAttemptAt := now.Add(w.retryDelay(event.ID, event.Attempts))
			if finalizeErr := w.store.MarkFailed(ctx, event.ID, w.config.WorkerID, nextAttemptAt, deadLetter); finalizeErr != nil {
				w.record("finalize", event.EventType, "failure", "database")
				failures = append(failures, fmt.Errorf("finalize failed event: %w", finalizeErr))
				continue
			}
			if deadLetter {
				result.DeadLetter++
				w.record("dead_letter", event.EventType, "failure", "internal")
			} else {
				result.Retried++
				w.record("publish", event.EventType, "failure", "unavailable")
			}
			continue
		}
		if err := w.store.MarkPublished(ctx, event.ID, w.config.WorkerID, w.clock.Now().UTC()); err != nil {
			w.record("finalize", event.EventType, "failure", "database")
			failures = append(failures, fmt.Errorf("finalize published event: %w", err))
			continue
		}
		result.Published++
		w.record("publish", event.EventType, "success", "none")
	}
	return result, errors.Join(failures...)
}

func (w *Worker) retryDelay(eventID uuid.UUID, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := w.config.RetryBase
	for count := 1; count < attempt && delay < w.config.RetryMax; count++ {
		if delay > w.config.RetryMax/2 {
			delay = w.config.RetryMax
			break
		}
		delay *= 2
	}
	if delay >= w.config.RetryMax {
		return w.config.RetryMax
	}
	jitterWindow := delay / 4
	if jitterWindow <= 0 {
		return delay
	}
	seed := binary.BigEndian.Uint64(eventID[:8]) ^ uint64(attempt)
	jitter := time.Duration(seed % uint64(jitterWindow+1))
	if delay > w.config.RetryMax-jitter {
		return w.config.RetryMax
	}
	return delay + jitter
}

func (w *Worker) record(operation, eventType, result, reason string) {
	if w.metrics != nil {
		w.metrics.RecordOutbox(operation, eventType, result, reason)
	}
}
