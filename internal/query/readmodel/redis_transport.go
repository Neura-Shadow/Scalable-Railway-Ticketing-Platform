package readmodel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	MaxDeadLetterStreamLength = 10_000
	maxSafeStreamFieldLength  = 128
	invalidSafeStreamField    = "invalid"
)

var ErrInvalidStreamTransport = errors.New("read-model stream transport configuration invalid")

var continueAndAckScript = redis.NewScript(`
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then
  return ''
end
local continuation = redis.call(
  'XADD', KEYS[1], '*',
  'event_id', ARGV[3],
  'event_type', ARGV[4],
  'aggregate_type', ARGV[5],
  'aggregate_id', ARGV[6]
)
local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acked ~= 1 then
  return redis.error_reply('continuation acknowledgment conflict')
end
return continuation
`)

var deadLetterAndAckScript = redis.NewScript(`
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then
  return ''
end
local dead_letter = redis.call(
  'XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*',
  'source_stream_id', ARGV[2],
  'reason', ARGV[4],
  'event_id', ARGV[5],
  'event_type', ARGV[6],
  'aggregate_type', ARGV[7],
  'aggregate_id', ARGV[8]
)
local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acked ~= 1 then
  return redis.error_reply('dead-letter acknowledgment conflict')
end
return dead_letter
`)

type RedisStreamTransport struct {
	client        redis.UniversalClient
	stream        string
	group         string
	dlq           string
	claimMu       sync.Mutex
	claimConsumer string
	claimCursor   string
}

func NewRedisStreamTransport(
	client redis.UniversalClient,
	stream string,
	group string,
	dlq string,
) (*RedisStreamTransport, error) {
	if client == nil || !validRedisStreamName(stream) || !validRedisStreamName(group) ||
		!validRedisStreamName(dlq) || stream == dlq {
		return nil, ErrInvalidStreamTransport
	}
	return &RedisStreamTransport{client: client, stream: stream, group: group, dlq: dlq}, nil
}

func (transport *RedisStreamTransport) EnsureGroup(ctx context.Context) error {
	err := transport.client.XGroupCreateMkStream(ctx, transport.stream, transport.group, "0").Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf("create Redis consumer group: %w", err)
}

func (transport *RedisStreamTransport) ClaimPending(
	ctx context.Context,
	consumer string,
	minIdle time.Duration,
	count int64,
) ([]StreamMessage, error) {
	transport.claimMu.Lock()
	defer transport.claimMu.Unlock()
	if transport.claimConsumer != consumer {
		transport.claimConsumer = consumer
		transport.claimCursor = "0-0"
	}
	if transport.claimCursor == "" {
		transport.claimCursor = "0-0"
	}
	messages, nextCursor, err := transport.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: transport.stream, Group: transport.group, Consumer: consumer,
		MinIdle: minIdle, Start: transport.claimCursor, Count: count,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("claim Redis stream pending entries: %w", err)
	}
	transport.claimCursor = nextCursor
	if transport.claimCursor == "" {
		transport.claimCursor = "0-0"
	}
	return safeStreamMessages(messages), nil
}

func (transport *RedisStreamTransport) ReadNew(
	ctx context.Context,
	consumer string,
	count int64,
) ([]StreamMessage, error) {
	streams, err := transport.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: transport.group, Consumer: consumer,
		Streams: []string{transport.stream, ">"}, Count: count, Block: -1,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Redis stream group: %w", err)
	}
	messages := make([]StreamMessage, 0)
	for _, stream := range streams {
		messages = append(messages, safeStreamMessages(stream.Messages)...)
	}
	return messages, nil
}

func (transport *RedisStreamTransport) EnqueueEvent(ctx context.Context, event ProjectionEvent) (string, error) {
	if _, _, err := validateProjectionEvent(event); err != nil || !validEventPair(event.EventType, event.AggregateType) {
		return "", ErrInvalidEvent
	}
	messageID, err := transport.client.XAdd(ctx, &redis.XAddArgs{
		Stream: transport.stream,
		Values: map[string]any{
			"event_id":       event.EventID,
			"event_type":     event.EventType,
			"aggregate_type": event.AggregateType,
			"aggregate_id":   event.AggregateID,
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("enqueue Redis stream event: %w", err)
	}
	return messageID, nil
}

func (transport *RedisStreamTransport) DeliveryCount(ctx context.Context, messageID string) (int64, error) {
	pending, err := transport.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: transport.stream,
		Group:  transport.group,
		Start:  messageID,
		End:    messageID,
		Count:  1,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("inspect Redis pending entry: %w", err)
	}
	if len(pending) != 1 || pending[0].ID != messageID || pending[0].RetryCount < 1 {
		return 0, errors.New("redis pending entry unavailable")
	}
	return pending[0].RetryCount, nil
}

func (transport *RedisStreamTransport) ContinueAndAck(ctx context.Context, message StreamMessage) error {
	continuationID, err := continueAndAckScript.Run(
		ctx,
		transport.client,
		[]string{transport.stream},
		transport.group,
		message.ID,
		message.Values["event_id"],
		message.Values["event_type"],
		message.Values["aggregate_type"],
		message.Values["aggregate_id"],
	).Text()
	if err != nil {
		return fmt.Errorf("continue and acknowledge Redis stream event: %w", err)
	}
	if continuationID == "" {
		return errors.New("redis stream continuation conflict")
	}
	return nil
}

func (transport *RedisStreamTransport) DeadLetterAndAck(
	ctx context.Context,
	message StreamMessage,
	reason string,
) error {
	if reason != "invalid_event" && reason != "handler_failure" {
		return ErrInvalidEvent
	}
	deadLetterID, err := deadLetterAndAckScript.Run(
		ctx,
		transport.client,
		[]string{transport.stream, transport.dlq},
		transport.group,
		message.ID,
		MaxDeadLetterStreamLength,
		reason,
		message.Values["event_id"],
		message.Values["event_type"],
		message.Values["aggregate_type"],
		message.Values["aggregate_id"],
	).Text()
	if err != nil {
		return fmt.Errorf("dead-letter and acknowledge Redis stream event: %w", err)
	}
	if deadLetterID == "" {
		return errors.New("redis stream dead-letter conflict")
	}
	return nil
}

func (transport *RedisStreamTransport) Ack(ctx context.Context, messageID string) error {
	acked, err := transport.client.XAck(ctx, transport.stream, transport.group, messageID).Result()
	if err != nil {
		return fmt.Errorf("ack Redis stream event: %w", err)
	}
	if acked != 1 {
		return errors.New("redis stream event acknowledgment conflict")
	}
	return nil
}

func safeStreamMessages(messages []redis.XMessage) []StreamMessage {
	result := make([]StreamMessage, 0, len(messages))
	for _, message := range messages {
		values := make(map[string]string, 4)
		for _, name := range []string{"event_id", "event_type", "aggregate_type", "aggregate_id"} {
			if value, exists := message.Values[name]; exists {
				values[name] = boundedStreamField(fmt.Sprint(value))
			}
		}
		result = append(result, StreamMessage{ID: message.ID, Values: values})
	}
	return result
}

func boundedStreamField(value string) string {
	if len(value) > maxSafeStreamFieldLength {
		return invalidSafeStreamField
	}
	return value
}

func validRedisStreamName(value string) bool {
	return value == strings.TrimSpace(value) && len(value) >= 1 && len(value) <= 256 &&
		!strings.ContainsAny(value, "\x00\r\n")
}
