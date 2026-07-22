package readmodel_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	querycache "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/cache"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
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

func TestRedisContinuationDoesNotTrimMoreThanTenThousandPendingEntries(t *testing.T) {
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
	const stream = "railway:test:large-outbox"
	const group = "railway-read-model"
	transport, err := readmodel.NewRedisStreamTransport(client, stream, group, stream+":dlq")
	if err != nil {
		t.Fatalf("NewRedisStreamTransport() error = %v", err)
	}
	if err := transport.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup() error = %v", err)
	}
	eventID := uuid.NewString()
	aggregateID := uuid.NewString()
	_, err = client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index := 0; index < 10_001; index++ {
			pipe.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{
				"event_id": eventID, "event_type": "trainrun.updated",
				"aggregate_type": "train_run", "aggregate_id": aggregateID,
			}})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed large Redis stream: %v", err)
	}
	messages, err := transport.ReadNew(ctx, "replica-large", 10_001)
	if err != nil || len(messages) != 10_001 {
		t.Fatalf("ReadNew(large) count = %d, error = %v", len(messages), err)
	}
	lastPendingID := messages[len(messages)-1].ID
	if err := transport.ContinueAndAck(ctx, messages[0]); err != nil {
		t.Fatalf("ContinueAndAck(large) error = %v", err)
	}
	length, err := client.XLen(ctx, stream).Result()
	if err != nil || length != 10_002 {
		t.Fatalf("source stream length after continuation = %d, %v, want 10002 without trimming", length, err)
	}
	lastEntry, err := client.XRangeN(ctx, stream, lastPendingID, lastPendingID, 1).Result()
	if err != nil || len(lastEntry) != 1 {
		t.Fatalf("last pending source entry after continuation = %+v, %v", lastEntry, err)
	}
}

func TestDeadLetteredProgressConvergesAfterOperatorRedrive(t *testing.T) {
	conn := openMigrationSevenDatabase(t)
	trainRunID := seedProjectionSource(t, conn)
	store, err := readmodel.NewStore(conn, clock.NewDeterministic(time.Now().UTC()))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	resolver, err := readmodel.NewPostgresImpactResolver(conn)
	if err != nil {
		t.Fatalf("NewPostgresImpactResolver() error = %v", err)
	}
	rotator := &versionRotatorFake{err: errors.New("injected Redis rotation outage")}
	coordinator, err := readmodel.NewEventCoordinator(store, resolver, rotator)
	if err != nil {
		t.Fatalf("NewEventCoordinator() error = %v", err)
	}

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
	transport, err := readmodel.NewRedisStreamTransport(
		client,
		"railway:test:redrive",
		readmodel.DurableConsumerName,
		"railway:test:redrive:dlq",
	)
	if err != nil {
		t.Fatalf("NewRedisStreamTransport() error = %v", err)
	}
	worker, err := readmodel.NewWorker(transport, coordinator, readmodel.WorkerConfig{
		ConsumerName: "redrive-replica", BatchSize: 10, MaxAttempts: 1, PendingIdle: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	event := projectionTrainRunEvent(trainRunID)
	if _, err := transport.EnqueueEvent(ctx, event); err != nil {
		t.Fatalf("EnqueueEvent(initial) error = %v", err)
	}
	first, err := worker.RunOnce(ctx)
	if err == nil || first.DeadLettered != 1 || first.Acked != 1 {
		t.Fatalf("RunOnce(outage) = %+v, %v, want terminal DLQ", first, err)
	}
	var progressRows, receiptRows int
	if err := conn.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM read_model_event_progress WHERE consumer_name = $1 AND event_id = $2),
			(SELECT count(*) FROM read_model_event_receipts WHERE consumer_name = $1 AND event_id = $2)
	`, event.ConsumerName, event.EventID).Scan(&progressRows, &receiptRows); err != nil {
		t.Fatalf("inspect orphan-safe progress: %v", err)
	}
	if progressRows != 1 || receiptRows != 0 {
		t.Fatalf("outage progress/receipt rows = %d/%d, want 1/0", progressRows, receiptRows)
	}
	searchRequest := querypostgres.SearchRequest{
		OriginCode: "TPE", DestinationCode: "KHH", ServiceDate: time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC),
		SeatClass: "standard", Page: 1, PageSize: 20, Sort: "departure_asc",
	}
	if _, err := store.SearchTrainRuns(ctx, searchRequest); !errors.Is(err, readmodel.ErrProjectionUnavailable) {
		t.Fatalf("SearchTrainRuns(before redrive) error = %v", err)
	}

	rotator.err = nil
	pendingEvent, err := store.PendingEvent(ctx, event.ConsumerName, event.EventID)
	if err != nil {
		t.Fatalf("PendingEvent() error = %v", err)
	}
	if _, err := transport.EnqueueEvent(ctx, pendingEvent); err != nil {
		t.Fatalf("EnqueueEvent(redrive) error = %v", err)
	}
	second, err := worker.RunOnce(ctx)
	if err != nil || second.Processed != 1 || second.Acked != 1 {
		t.Fatalf("RunOnce(redrive) = %+v, %v", second, err)
	}
	var projectionRows int
	if err := conn.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM read_model_event_progress WHERE consumer_name = $1 AND event_id = $2),
			(SELECT count(*) FROM read_model_event_receipts WHERE consumer_name = $1 AND event_id = $2),
			(SELECT count(*) FROM train_run_journey_read_model WHERE train_run_id = $3)
	`, event.ConsumerName, event.EventID, trainRunID).Scan(&progressRows, &receiptRows, &projectionRows); err != nil {
		t.Fatalf("inspect recovered progress: %v", err)
	}
	if progressRows != 0 || receiptRows != 1 || projectionRows != 6 {
		t.Fatalf("recovered progress/receipt/projection rows = %d/%d/%d, want 0/1/6", progressRows, receiptRows, projectionRows)
	}
	availabilityKey, _ := querycache.AvailabilityVersionKey(trainRunID.String())
	if len(rotator.keys) < 2 || !reflect.DeepEqual(
		rotator.keys[len(rotator.keys)-2:],
		[]string{querycache.SearchVersionKey(), availabilityKey},
	) {
		t.Fatalf("redrive rotations = %v", rotator.keys)
	}
}
