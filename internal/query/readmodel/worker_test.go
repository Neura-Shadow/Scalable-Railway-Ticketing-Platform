package readmodel

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWorkerRunOnceClaimsPendingBeforeReadingNewAndStaysBounded(t *testing.T) {
	transport := &streamTransportFake{
		pending: []StreamMessage{
			validStreamMessage("1-0", uuid.NewString()),
			validStreamMessage("2-0", uuid.NewString()),
		},
		fresh: []StreamMessage{validStreamMessage("3-0", uuid.NewString())},
	}
	handler := &eventHandlerFake{}
	worker, err := NewWorker(transport, handler, WorkerConfig{
		ConsumerName: "replica-a",
		BatchSize:    2,
		MaxAttempts:  3,
		PendingIdle:  time.Minute,
	})
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Claimed != 2 || result.Read != 0 || result.Processed != 2 || result.Acked != 2 {
		t.Fatalf("RunOnce() = %+v, want bounded pending recovery", result)
	}
	if !reflect.DeepEqual(transport.calls, []string{"ensure", "claim", "ack:1-0", "ack:2-0"}) {
		t.Fatalf("transport calls = %v", transport.calls)
	}
}

func TestWorkerRunOnceLeavesTransientFailurePendingUntilAttemptLimit(t *testing.T) {
	message := validStreamMessage("1-0", uuid.NewString())
	transport := &streamTransportFake{
		pending:       []StreamMessage{message},
		deliveryCount: 1,
	}
	handler := &eventHandlerFake{err: errors.New("injected projection outage")}
	worker := mustNewWorker(t, transport, handler, 2)

	result, err := worker.RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce() error = nil, want handled transient failure")
	}
	if result.Retried != 1 || result.DeadLettered != 0 || len(transport.acked) != 0 {
		t.Fatalf("RunOnce() = %+v acked=%v, want pending retry", result, transport.acked)
	}
}

func TestWorkerRunOnceDeadLettersPoisonAfterBoundedAttemptsBeforeAck(t *testing.T) {
	message := validStreamMessage("9-0", "not-a-uuid")
	transport := &streamTransportFake{
		pending:       []StreamMessage{message},
		deliveryCount: 3,
	}
	worker := mustNewWorker(t, transport, &eventHandlerFake{}, 3)

	result, err := worker.RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce() error = nil, want poison event report")
	}
	if result.DeadLettered != 1 || result.Acked != 1 || len(transport.deadLetters) != 1 {
		t.Fatalf("RunOnce() = %+v dead_letters=%v", result, transport.deadLetters)
	}
	if !reflect.DeepEqual(transport.calls, []string{"ensure", "claim", "read", "deliveries:9-0", "dlq_ack:9-0:invalid_event"}) {
		t.Fatalf("transport calls = %v, want atomic DLQ and ack", transport.calls)
	}
}

func TestWorkerRunOnceContinuesBoundedFanoutWithoutCountingFailureAttempts(t *testing.T) {
	message := validStreamMessage("11-0", uuid.NewString())
	transport := &streamTransportFake{pending: []StreamMessage{message}}
	worker := mustNewWorker(t, transport, &eventHandlerFake{err: ErrProjectionPending}, 1)

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Continued != 1 || result.Acked != 1 || result.DeadLettered != 0 || result.Retried != 0 {
		t.Fatalf("RunOnce() = %+v, want one planned continuation", result)
	}
	if !reflect.DeepEqual(transport.calls, []string{"ensure", "claim", "read", "continue_ack:11-0"}) {
		t.Fatalf("transport calls = %v", transport.calls)
	}
}

func mustNewWorker(t *testing.T, transport StreamTransport, handler EventHandler, maxAttempts int64) *Worker {
	t.Helper()
	worker, err := NewWorker(transport, handler, WorkerConfig{
		ConsumerName: "replica-a",
		BatchSize:    10,
		MaxAttempts:  maxAttempts,
		PendingIdle:  time.Minute,
	})
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	return worker
}

func validStreamMessage(streamID, eventID string) StreamMessage {
	return StreamMessage{
		ID: streamID,
		Values: map[string]string{
			"event_id":       eventID,
			"event_type":     "trainrun.updated",
			"aggregate_type": "train_run",
			"aggregate_id":   uuid.NewString(),
		},
	}
}

type eventHandlerFake struct {
	err    error
	events []ProjectionEvent
}

func (fake *eventHandlerFake) HandleEvent(_ context.Context, event ProjectionEvent) error {
	fake.events = append(fake.events, event)
	return fake.err
}

type streamTransportFake struct {
	pending       []StreamMessage
	fresh         []StreamMessage
	deliveryCount int64
	calls         []string
	acked         []string
	deadLetters   []string
}

func (fake *streamTransportFake) EnsureGroup(context.Context) error {
	fake.calls = append(fake.calls, "ensure")
	return nil
}

func (fake *streamTransportFake) ClaimPending(_ context.Context, _ string, _ time.Duration, count int64) ([]StreamMessage, error) {
	fake.calls = append(fake.calls, "claim")
	return takeMessages(fake.pending, count), nil
}

func (fake *streamTransportFake) ReadNew(_ context.Context, _ string, count int64) ([]StreamMessage, error) {
	fake.calls = append(fake.calls, "read")
	return takeMessages(fake.fresh, count), nil
}

func (fake *streamTransportFake) DeliveryCount(_ context.Context, messageID string) (int64, error) {
	fake.calls = append(fake.calls, "deliveries:"+messageID)
	return fake.deliveryCount, nil
}

func (fake *streamTransportFake) ContinueAndAck(_ context.Context, message StreamMessage) error {
	fake.calls = append(fake.calls, "continue_ack:"+message.ID)
	fake.acked = append(fake.acked, message.ID)
	return nil
}

func (fake *streamTransportFake) DeadLetterAndAck(_ context.Context, message StreamMessage, reason string) error {
	fake.calls = append(fake.calls, "dlq_ack:"+message.ID+":"+reason)
	fake.deadLetters = append(fake.deadLetters, message.ID)
	fake.acked = append(fake.acked, message.ID)
	return nil
}

func (fake *streamTransportFake) Ack(_ context.Context, messageID string) error {
	fake.calls = append(fake.calls, "ack:"+messageID)
	fake.acked = append(fake.acked, messageID)
	return nil
}

func takeMessages(messages []StreamMessage, count int64) []StreamMessage {
	if int64(len(messages)) <= count {
		return append([]StreamMessage(nil), messages...)
	}
	return append([]StreamMessage(nil), messages[:count]...)
}
