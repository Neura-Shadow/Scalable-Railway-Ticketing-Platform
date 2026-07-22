package readmodel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	DurableConsumerName = "railway-read-model"
	MaxWorkerBatchSize  = 100
	MaxWorkerAttempts   = 10
)

var ErrInvalidWorker = errors.New("read-model worker configuration invalid")

type StreamMessage struct {
	ID     string
	Values map[string]string
}

type StreamTransport interface {
	EnsureGroup(context.Context) error
	ClaimPending(context.Context, string, time.Duration, int64) ([]StreamMessage, error)
	ReadNew(context.Context, string, int64) ([]StreamMessage, error)
	DeliveryCount(context.Context, string) (int64, error)
	ContinueAndAck(context.Context, StreamMessage) error
	DeadLetterAndAck(context.Context, StreamMessage, string) error
	Ack(context.Context, string) error
}

type EventHandler interface {
	HandleEvent(context.Context, ProjectionEvent) error
}

type WorkerConfig struct {
	ConsumerName string
	BatchSize    int64
	MaxAttempts  int64
	PendingIdle  time.Duration
}

type WorkerResult struct {
	Claimed      int
	Read         int
	Processed    int
	Retried      int
	DeadLettered int
	Continued    int
	Acked        int
}

type Worker struct {
	transport StreamTransport
	handler   EventHandler
	config    WorkerConfig
}

func NewWorker(transport StreamTransport, handler EventHandler, config WorkerConfig) (*Worker, error) {
	if transport == nil || handler == nil ||
		config.ConsumerName != strings.TrimSpace(config.ConsumerName) ||
		len(config.ConsumerName) < 1 || len(config.ConsumerName) > 128 ||
		config.BatchSize < 1 || config.BatchSize > MaxWorkerBatchSize ||
		config.MaxAttempts < 1 || config.MaxAttempts > MaxWorkerAttempts ||
		config.PendingIdle <= 0 {
		return nil, ErrInvalidWorker
	}
	return &Worker{transport: transport, handler: handler, config: config}, nil
}

func (worker *Worker) RunOnce(ctx context.Context) (WorkerResult, error) {
	var result WorkerResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := worker.transport.EnsureGroup(ctx); err != nil {
		return result, fmt.Errorf("ensure read-model consumer group: %w", err)
	}
	pending, err := worker.transport.ClaimPending(
		ctx,
		worker.config.ConsumerName,
		worker.config.PendingIdle,
		worker.config.BatchSize,
	)
	if err != nil {
		return result, fmt.Errorf("claim pending read-model events: %w", err)
	}
	result.Claimed = len(pending)
	messages := append([]StreamMessage(nil), pending...)
	remaining := worker.config.BatchSize - int64(len(messages))
	if remaining > 0 {
		fresh, readErr := worker.transport.ReadNew(ctx, worker.config.ConsumerName, remaining)
		if readErr != nil {
			return result, fmt.Errorf("read new read-model events: %w", readErr)
		}
		result.Read = len(fresh)
		messages = append(messages, fresh...)
	}

	var runErrors []error
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			runErrors = append(runErrors, err)
			break
		}
		event, parseErr := parseProjectionMessage(message)
		if parseErr != nil {
			if err := worker.handleFailure(ctx, message, "invalid_event", &result); err != nil {
				runErrors = append(runErrors, err)
			}
			runErrors = append(runErrors, parseErr)
			continue
		}
		if err := worker.handler.HandleEvent(ctx, event); err != nil {
			if errors.Is(err, ErrProjectionPending) {
				if continuationErr := worker.transport.ContinueAndAck(ctx, message); continuationErr != nil {
					runErrors = append(runErrors, fmt.Errorf("continue read-model event: %w", continuationErr))
					continue
				}
				result.Continued++
				result.Acked++
				continue
			}
			if failureErr := worker.handleFailure(ctx, message, "handler_failure", &result); failureErr != nil {
				runErrors = append(runErrors, failureErr)
			}
			runErrors = append(runErrors, fmt.Errorf("handle read-model event: %w", err))
			continue
		}
		if err := worker.transport.Ack(ctx, message.ID); err != nil {
			runErrors = append(runErrors, fmt.Errorf("ack read-model event: %w", err))
			continue
		}
		result.Processed++
		result.Acked++
	}
	return result, errors.Join(runErrors...)
}

func (worker *Worker) handleFailure(
	ctx context.Context,
	message StreamMessage,
	reason string,
	result *WorkerResult,
) error {
	deliveries, err := worker.transport.DeliveryCount(ctx, message.ID)
	if err != nil {
		return fmt.Errorf("inspect read-model event attempts: %w", err)
	}
	if deliveries < worker.config.MaxAttempts {
		result.Retried++
		return nil
	}
	if err := worker.transport.DeadLetterAndAck(ctx, message, reason); err != nil {
		return fmt.Errorf("dead-letter read-model event: %w", err)
	}
	result.DeadLettered++
	result.Acked++
	return nil
}

func parseProjectionMessage(message StreamMessage) (ProjectionEvent, error) {
	if message.ID == "" || message.Values == nil {
		return ProjectionEvent{}, ErrInvalidEvent
	}
	event := ProjectionEvent{
		ConsumerName:  DurableConsumerName,
		EventID:       message.Values["event_id"],
		EventType:     message.Values["event_type"],
		AggregateType: message.Values["aggregate_type"],
		AggregateID:   message.Values["aggregate_id"],
	}
	if len(event.EventType) < 1 || len(event.EventType) > 128 ||
		len(event.AggregateType) < 1 || len(event.AggregateType) > 64 {
		return ProjectionEvent{}, ErrInvalidEvent
	}
	if eventID, err := uuid.Parse(event.EventID); err != nil || eventID == uuid.Nil {
		return ProjectionEvent{}, ErrInvalidEvent
	}
	if aggregateID, err := uuid.Parse(event.AggregateID); err != nil || aggregateID == uuid.Nil {
		return ProjectionEvent{}, ErrInvalidEvent
	}
	return event, nil
}
