package readmodel_test

import (
	"context"
	"os"
	"testing"

	readmodel "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/readmodel"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestRedisStreamTransportAtomicallyContinuesAndDeadLettersSafeFields(t *testing.T) {
	address := os.Getenv("TEST_REDIS_ADDR")
	if address == "" {
		address = "127.0.0.1:56379"
	}
	client := redis.NewClient(&redis.Options{Addr: address, DB: 13})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("Redis integration dependency unavailable: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush Redis test database: %v", err)
	}
	const stream = "railway:test:outbox"
	const group = "railway-read-model"
	const dlq = "railway:test:outbox:read-model:dlq"
	transport, err := readmodel.NewRedisStreamTransport(client, stream, group, dlq)
	if err != nil {
		t.Fatalf("NewRedisStreamTransport() error = %v", err)
	}
	if err := transport.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup() error = %v", err)
	}
	eventID := uuid.NewString()
	messageID, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"event_id": eventID, "event_type": "trainrun.updated",
			"aggregate_type": "train_run", "aggregate_id": uuid.NewString(),
			"payload": "must-not-enter-consumer-message-or-dlq",
		},
	}).Result()
	if err != nil {
		t.Fatalf("seed Redis stream event: %v", err)
	}
	messages, err := transport.ReadNew(ctx, "replica-a", 1)
	if err != nil || len(messages) != 1 || messages[0].ID != messageID {
		t.Fatalf("ReadNew() = %+v, %v", messages, err)
	}
	if _, leaked := messages[0].Values["payload"]; leaked {
		t.Fatal("consumer message retained outbox payload")
	}
	claimed, err := transport.ClaimPending(ctx, "replica-b", 0, 1)
	if err != nil || len(claimed) != 1 || claimed[0].ID != messageID {
		t.Fatalf("ClaimPending() = %+v, %v", claimed, err)
	}
	deliveries, err := transport.DeliveryCount(ctx, messageID)
	if err != nil || deliveries < 2 {
		t.Fatalf("DeliveryCount() = %d, %v", deliveries, err)
	}
	if err := transport.ContinueAndAck(ctx, claimed[0]); err != nil {
		t.Fatalf("ContinueAndAck() error = %v", err)
	}
	if err := transport.ContinueAndAck(ctx, claimed[0]); err == nil {
		t.Fatal("ContinueAndAck(duplicate) error = nil, want old-pending conflict")
	}
	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending after continuation+ack = %+v, %v", pending, err)
	}
	streamEntries, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil || len(streamEntries) != 2 {
		t.Fatalf("stream entries after continuation = %+v, %v", streamEntries, err)
	}
	if _, leaked := streamEntries[1].Values["payload"]; leaked {
		t.Fatal("continuation retained outbox payload")
	}
	continued, err := transport.ReadNew(ctx, "replica-c", 1)
	if err != nil || len(continued) != 1 || continued[0].Values["event_id"] != eventID {
		t.Fatalf("ReadNew(continuation) = %+v, %v", continued, err)
	}
	if err := transport.DeadLetterAndAck(ctx, continued[0], "handler_failure"); err != nil {
		t.Fatalf("DeadLetterAndAck() error = %v", err)
	}
	pending, err = client.XPending(ctx, stream, group).Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending after DLQ+ack = %+v, %v", pending, err)
	}
	dlqEntries, err := client.XRange(ctx, dlq, "-", "+").Result()
	if err != nil || len(dlqEntries) != 1 {
		t.Fatalf("DLQ entries = %+v, %v", dlqEntries, err)
	}
	if _, leaked := dlqEntries[0].Values["payload"]; leaked {
		t.Fatal("dead-letter entry retained outbox payload")
	}
}
