package publisher

import (
	"context"
	"errors"
	"log/slog"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/domain"
	"github.com/redis/go-redis/v9"
)

type Log struct{ logger *slog.Logger }

func NewLog(logger *slog.Logger) (*Log, error) {
	if logger == nil {
		return nil, errors.New("log publisher requires a logger")
	}
	return &Log{logger: logger}, nil
}

func (p *Log) Publish(ctx context.Context, event domain.Event) error {
	p.logger.InfoContext(ctx, "outbox event published", "event_type", event.EventType, "event_version", event.EventVersion)
	return nil
}

type RedisStream struct {
	client redis.UniversalClient
	stream string
}

func NewRedisStream(client redis.UniversalClient, stream string) (*RedisStream, error) {
	if client == nil || stream == "" {
		return nil, errors.New("redis stream publisher requires a client and stream")
	}
	return &RedisStream{client: client, stream: stream}, nil
}

func (p *RedisStream) Publish(ctx context.Context, event domain.Event) error {
	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream,
		Values: map[string]any{
			"event_id":       event.ID.String(),
			"aggregate_type": event.AggregateType,
			"aggregate_id":   event.AggregateID.String(),
			"event_type":     event.EventType,
			"event_version":  event.EventVersion,
			"payload":        string(event.Payload),
		},
	}).Err()
}
