package physicalworker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidPostgresProcessor = errors.New("invalid physical shard postgres worker configuration")
	ErrOutboxStore              = errors.New("shard-local outbox store unavailable")
	ErrOutboxPublish            = errors.New("shard-local outbox publish failed")
	ErrExpirationStore          = errors.New("shard-local expiration store unavailable")
)

type OutboxPublisher interface {
	Publish(context.Context, domain.Event) error
}

type OutboxOptions struct {
	WorkerID          string
	MaxAttempts       int
	ProcessingTimeout time.Duration
	RetryBase         time.Duration
	RetryMax          time.Duration
	StatementTimeout  time.Duration
	LockTimeout       time.Duration
	Now               func() time.Time
}

// OutboxProcessor claims and finalizes leases entirely inside the database
// represented by one configured handle. It never discovers another shard and
// never routes or fans out a write after a failure.
type OutboxProcessor struct {
	publisher OutboxPublisher
	options   OutboxOptions
}

func NewOutboxProcessor(publisher OutboxPublisher, options OutboxOptions) (*OutboxProcessor, error) {
	if isNil(publisher) || strings.TrimSpace(options.WorkerID) == "" || len(options.WorkerID) > 96 ||
		options.MaxAttempts < 1 || options.MaxAttempts > 100 ||
		options.ProcessingTimeout <= 0 || options.ProcessingTimeout > 24*time.Hour ||
		options.RetryBase <= 0 || options.RetryMax < options.RetryBase || options.RetryMax > 24*time.Hour ||
		!validDatabaseTimeout(options.StatementTimeout) || !validDatabaseTimeout(options.LockTimeout) ||
		options.Now == nil {
		return nil, ErrInvalidPostgresProcessor
	}
	return &OutboxProcessor{publisher: publisher, options: options}, nil
}

func (processor *OutboxProcessor) Process(ctx context.Context, handle Handle, limit int) (int, error) {
	if processor == nil || ctx == nil || isNil(handle) || isNil(handle.Pool()) || limit < 1 || limit > maxWorkLimit {
		return 0, ErrInvalidPostgresProcessor
	}
	now := processor.options.Now().UTC()
	events, err := processor.claim(ctx, handle, limit, now)
	if err != nil {
		return 0, err
	}

	processed := 0
	var failures []error
	for _, event := range events {
		if err := processor.publisher.Publish(ctx, event.Event); err != nil {
			deadLetter := event.Event.Attempts >= processor.options.MaxAttempts
			nextAttempt := now.Add(outboxRetryDelay(event.Event.ID, event.Event.Attempts, processor.options.RetryBase, processor.options.RetryMax))
			if markErr := processor.markFailed(ctx, handle, event, nextAttempt, deadLetter); markErr != nil {
				failures = append(failures, markErr)
				continue
			}
			processed++
			failures = append(failures, ErrOutboxPublish)
			continue
		}
		if err := processor.markPublished(ctx, handle, event, processor.options.Now().UTC()); err != nil {
			failures = append(failures, err)
			continue
		}
		processed++
	}
	return processed, errors.Join(failures...)
}

type leasedEvent struct {
	Event      domain.Event
	LeaseToken uuid.UUID
}

func (processor *OutboxProcessor) claim(
	ctx context.Context,
	handle Handle,
	limit int,
	now time.Time,
) ([]leasedEvent, error) {
	tx, err := handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil || tx == nil {
		return nil, ErrOutboxStore
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := applyLocalTimeouts(ctx, tx, processor.options.StatementTimeout, processor.options.LockTimeout); err != nil {
		return nil, ErrOutboxStore
	}

	rows, err := tx.Query(ctx, `
WITH candidates AS MATERIALIZED (
    SELECT event.id
    FROM outbox_events AS event
    JOIN train_run_write_fences AS fence
      ON fence.train_run_id = event.train_run_id
     AND fence.assignment_generation = event.assignment_generation
    WHERE fence.state = 'active'
      AND fence.write_enabled
      AND ((event.status = 'pending' AND event.next_attempt_at <= $1)
       OR (event.status = 'processing' AND event.locked_at <= $2))
    ORDER BY event.created_at, event.id
    FOR UPDATE OF event SKIP LOCKED
    LIMIT $3
), claimed AS (
    UPDATE outbox_events AS event
    SET status = 'processing',
        attempts = event.attempts + 1,
        locked_at = $1,
        locked_by = $4,
        lease_token = gen_random_uuid()
    FROM candidates
    WHERE event.id = candidates.id
    RETURNING event.id, event.aggregate_type, event.aggregate_id,
              event.event_type, event.event_version, event.payload,
              event.attempts, event.created_at, event.lease_token
)
SELECT id, aggregate_type, aggregate_id, event_type, event_version,
       payload, attempts, created_at, lease_token
FROM claimed
ORDER BY created_at, id`, now, now.Add(-processor.options.ProcessingTimeout), limit, processor.options.WorkerID)
	if err != nil {
		return nil, ErrOutboxStore
	}
	defer rows.Close()
	events := make([]leasedEvent, 0, limit)
	for rows.Next() {
		var event leasedEvent
		if err := rows.Scan(
			&event.Event.ID,
			&event.Event.AggregateType,
			&event.Event.AggregateID,
			&event.Event.EventType,
			&event.Event.EventVersion,
			&event.Event.Payload,
			&event.Event.Attempts,
			&event.Event.CreatedAt,
			&event.LeaseToken,
		); err != nil || event.Event.ID == uuid.Nil || event.LeaseToken == uuid.Nil {
			return nil, ErrOutboxStore
		}
		events = append(events, event)
	}
	if rows.Err() != nil || len(events) > limit {
		return nil, ErrOutboxStore
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, ErrOutboxStore
	}
	return events, nil
}

func (processor *OutboxProcessor) markPublished(
	ctx context.Context,
	handle Handle,
	event leasedEvent,
	publishedAt time.Time,
) error {
	return processor.finalize(ctx, handle, `
UPDATE outbox_events
SET status = 'published', published_at = $4,
    locked_at = NULL, locked_by = NULL, lease_token = NULL
WHERE id = $1
  AND status = 'processing'
  AND locked_by = $2
  AND lease_token = $3`, event.Event.ID, processor.options.WorkerID, event.LeaseToken, publishedAt)
}

func (processor *OutboxProcessor) markFailed(
	ctx context.Context,
	handle Handle,
	event leasedEvent,
	nextAttempt time.Time,
	deadLetter bool,
) error {
	status := "pending"
	if deadLetter {
		status = "dead_letter"
	}
	return processor.finalize(ctx, handle, `
UPDATE outbox_events
SET status = $4, next_attempt_at = $5,
    locked_at = NULL, locked_by = NULL, lease_token = NULL
WHERE id = $1
  AND locked_by = $2
  AND lease_token = $3
  AND status = 'processing'`, event.Event.ID, processor.options.WorkerID, event.LeaseToken, status, nextAttempt)
}

func (processor *OutboxProcessor) finalize(ctx context.Context, handle Handle, query string, arguments ...any) error {
	tx, err := handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil || tx == nil {
		return ErrOutboxStore
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := applyLocalTimeouts(ctx, tx, processor.options.StatementTimeout, processor.options.LockTimeout); err != nil {
		return ErrOutboxStore
	}
	tag, err := tx.Exec(ctx, query, arguments...)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrOutboxStore
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrOutboxStore
	}
	return nil
}

type HoldExpirationOptions struct {
	StatementTimeout time.Duration
	LockTimeout      time.Duration
	Now              func() time.Time
}

type HoldExpirationProcessor struct {
	options HoldExpirationOptions
}

func NewHoldExpirationProcessor(options HoldExpirationOptions) (*HoldExpirationProcessor, error) {
	if !validDatabaseTimeout(options.StatementTimeout) || !validDatabaseTimeout(options.LockTimeout) || options.Now == nil {
		return nil, ErrInvalidPostgresProcessor
	}
	return &HoldExpirationProcessor{options: options}, nil
}

func (processor *HoldExpirationProcessor) Process(ctx context.Context, handle Handle, limit int) (int, error) {
	if processor == nil || ctx == nil || isNil(handle) || isNil(handle.Pool()) || limit < 1 || limit > maxWorkLimit {
		return 0, ErrInvalidPostgresProcessor
	}
	now := processor.options.Now().UTC()
	processed := 0
	for processed < limit {
		expired, err := processor.expireOne(ctx, handle, now)
		if err != nil {
			return processed, err
		}
		if !expired {
			break
		}
		processed++
	}
	return processed, nil
}

func (processor *HoldExpirationProcessor) expireOne(
	ctx context.Context,
	handle Handle,
	now time.Time,
) (bool, error) {
	tx, err := handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil || tx == nil {
		return false, ErrExpirationStore
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := applyLocalTimeouts(ctx, tx, processor.options.StatementTimeout, processor.options.LockTimeout); err != nil {
		return false, ErrExpirationStore
	}

	var (
		reservationID uuid.UUID
		trainRunID    uuid.UUID
		generation    int64
		seatCount     int64
	)
	err = tx.QueryRow(ctx, `
SELECT reservation.id,
       reservation.train_run_id,
       reservation.assignment_generation,
       (SELECT count(*)
          FROM reservation_seats AS seat
         WHERE seat.reservation_id = reservation.id)
FROM reservations AS reservation
JOIN train_run_write_fences AS fence
  ON fence.train_run_id = reservation.train_run_id
 AND fence.assignment_generation = reservation.assignment_generation
WHERE reservation.status = 'held'
  AND reservation.expires_at <= $1
  AND fence.state = 'active'
  AND fence.write_enabled
ORDER BY reservation.expires_at, reservation.id
FOR UPDATE OF fence, reservation SKIP LOCKED
LIMIT 1`, now).Scan(&reservationID, &trainRunID, &generation, &seatCount)
	if errors.Is(err, pgx.ErrNoRows) {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return false, ErrExpirationStore
		}
		return false, nil
	}
	if err != nil || reservationID == uuid.Nil || trainRunID == uuid.Nil || generation <= 0 || seatCount <= 0 {
		return false, ErrExpirationStore
	}

	tag, err := tx.Exec(ctx, `
UPDATE seat_inventory AS inventory
SET occupied_segments = inventory.occupied_segments & ~seat.segment_mask,
    version = inventory.version + 1
FROM reservation_seats AS seat
WHERE seat.reservation_id = $1
  AND seat.train_run_id = $2
  AND seat.assignment_generation = $3
  AND inventory.train_run_id = seat.train_run_id
  AND inventory.assignment_generation = seat.assignment_generation
  AND inventory.seat_id = seat.seat_id
  AND (inventory.occupied_segments & seat.segment_mask) = seat.segment_mask`, reservationID, trainRunID, generation)
	if err != nil || tag.RowsAffected() != seatCount {
		return false, ErrExpirationStore
	}
	tag, err = tx.Exec(ctx, `
UPDATE reservations
SET status = 'expired'
WHERE id = $1
  AND status = 'held'
  AND expires_at <= $2`, reservationID, now)
	if err != nil || tag.RowsAffected() != 1 {
		return false, ErrExpirationStore
	}

	payload, err := json.Marshal(map[string]any{
		"reservation_id":        reservationID,
		"train_run_id":          trainRunID,
		"assignment_generation": generation,
		"status":                "expired",
		"released_seat_count":   seatCount,
	})
	if err != nil {
		return false, ErrExpirationStore
	}
	eventID := uuid.NewSHA1(reservationID, []byte("reservation-expired"))
	tag, err = tx.Exec(ctx, `
INSERT INTO outbox_events (
    id, train_run_id, assignment_generation, aggregate_type, aggregate_id,
    event_type, payload
) VALUES ($1, $2, $3, 'reservation', $4, 'reservation.expired', $5::jsonb)`,
		eventID, trainRunID, generation, reservationID, string(payload))
	if err != nil || tag.RowsAffected() != 1 {
		return false, ErrExpirationStore
	}

	evidenceID := uuid.NewSHA1(trainRunID, []byte("target-write-evidence:"+handle.ShardID().String()))
	tag, err = tx.Exec(ctx, `
INSERT INTO train_run_target_write_evidence (
    id, train_run_id, assignment_generation, successful_write_count,
    first_successful_write_at, last_successful_write_at
) VALUES ($1, $2, $3, 1, clock_timestamp(), clock_timestamp())
ON CONFLICT (train_run_id, assignment_generation) DO UPDATE
SET successful_write_count = train_run_target_write_evidence.successful_write_count + 1,
    first_successful_write_at = COALESCE(
        train_run_target_write_evidence.first_successful_write_at,
        EXCLUDED.first_successful_write_at
    ),
    last_successful_write_at = EXCLUDED.last_successful_write_at`, evidenceID, trainRunID, generation)
	if err != nil || tag.RowsAffected() != 1 {
		return false, ErrExpirationStore
	}
	if err := tx.Commit(ctx); err != nil {
		return false, ErrExpirationStore
	}
	return true, nil
}

func applyLocalTimeouts(ctx context.Context, tx pgx.Tx, statementTimeout, lockTimeout time.Duration) error {
	_, err := tx.Exec(ctx, `
SELECT set_config('statement_timeout', $1, true),
       set_config('lock_timeout', $2, true)`, durationMilliseconds(statementTimeout), durationMilliseconds(lockTimeout))
	return err
}

func validDatabaseTimeout(timeout time.Duration) bool {
	return timeout > 0 && timeout <= 5*time.Minute
}

func durationMilliseconds(duration time.Duration) string {
	milliseconds := duration.Milliseconds()
	if milliseconds < 1 {
		milliseconds = 1
	}
	return fmt.Sprintf("%dms", milliseconds)
}

func outboxRetryDelay(eventID uuid.UUID, attempt int, base, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for count := 1; count < attempt && delay < maximum; count++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay >= maximum {
		return maximum
	}
	jitterWindow := delay / 4
	if jitterWindow <= 0 {
		return delay
	}
	seed := binary.BigEndian.Uint64(eventID[:8]) ^ uint64(attempt)
	jitter := time.Duration(seed % uint64(jitterWindow+1))
	if delay > maximum-jitter {
		return maximum
	}
	return delay + jitter
}
