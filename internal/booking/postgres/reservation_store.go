package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type CreateHoldParams struct {
	UserID               uuid.UUID
	TrainRunID           uuid.UUID
	FromStopIndex        int
	ToStopIndex          int
	SeatClass            string
	PassengerIDs         []uuid.UUID
	HoldExpiresAt        time.Time
	IdempotencyKeyHash   []byte
	RequestFingerprint   []byte
	IdempotencyExpiresAt time.Time
	AdmissionPolicy      *AdmissionPolicyDecision
}

type CreateHoldResult struct {
	ReservationID    uuid.UUID
	SeatCount        int
	TotalAmountMinor int64
	Currency         string
	Replayed         bool
}

type ReservationCommandParams struct {
	UserID               uuid.UUID
	ReservationID        uuid.UUID
	Now                  time.Time
	IdempotencyKeyHash   []byte
	RequestFingerprint   []byte
	IdempotencyExpiresAt time.Time
}

type ConfirmReservationResult struct {
	ReservationID uuid.UUID
	TicketOrderID uuid.UUID
	TicketCount   int
	Replayed      bool
}

type CancelReservationResult struct {
	ReservationID     uuid.UUID
	ReleasedSeatCount int
	Replayed          bool
}

type ReservationRecord struct {
	ReservationID     uuid.UUID
	Status            string
	SeatCount         int
	TotalAmountMinor  int64
	Currency          string
	ActiveTicketCount int
	OutboxEventCount  int
}

func (s *Store) CreateHold(ctx context.Context, params CreateHoldParams) (CreateHoldResult, error) {
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		result, err := s.createHoldOnce(ctx, params)
		if err == nil || !isRetryableTransactionError(err) || attempt == maxAttempts-1 {
			return result, err
		}
		delay := time.Duration(5*(1<<attempt)+rand.IntN(5)) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return CreateHoldResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	return CreateHoldResult{}, ErrPersistenceInvariant
}

func (s *Store) createHoldOnce(ctx context.Context, params CreateHoldParams) (CreateHoldResult, error) {
	if err := validateCreateHoldParams(params); err != nil {
		return CreateHoldResult{}, err
	}
	passengerIDs := append([]uuid.UUID(nil), params.PassengerIDs...)
	seatClass := strings.ToLower(params.SeatClass)
	sort.Slice(passengerIDs, func(i, j int) bool { return passengerIDs[i].String() < passengerIDs[j].String() })
	for index := 1; index < len(passengerIDs); index++ {
		if passengerIDs[index] == passengerIDs[index-1] {
			return CreateHoldResult{}, ErrInvalidArgument
		}
	}

	tx, err := s.beginCreateHoldTransaction(ctx, params.TrainRunID)
	if err != nil {
		return CreateHoldResult{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	acquisition, err := tx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID: params.UserID, Operation: OperationReservationCreate,
		KeyHash: params.IdempotencyKeyHash, RequestFingerprint: params.RequestFingerprint,
		ExpiresAt: params.IdempotencyExpiresAt,
	})
	if err != nil {
		return CreateHoldResult{}, fmt.Errorf("acquire create-hold idempotency: %w", err)
	}
	if acquisition.Replayed {
		result, err := tx.loadCreateHoldResult(ctx, params.UserID, acquisition.ResourceID)
		if err != nil {
			return CreateHoldResult{}, err
		}
		result.Replayed = true
		if err := tx.Commit(ctx); err != nil {
			return CreateHoldResult{}, err
		}
		return result, nil
	}
	if err := tx.recheckAdmissionPolicy(ctx, params.TrainRunID, seatClass, params.AdmissionPolicy); err != nil {
		return CreateHoldResult{}, fmt.Errorf("recheck create-hold admission policy: %w", err)
	}
	if err := tx.enforceReservationQuota(ctx, params.UserID, params.TrainRunID, len(passengerIDs), s.reservationQuotaLimits); err != nil {
		return CreateHoldResult{}, fmt.Errorf("enforce create-hold quota: %w", err)
	}

	var (
		routeID      uuid.UUID
		segmentCount int
		status       string
		databaseNow  time.Time
	)
	err = tx.tx.QueryRow(ctx, `
SELECT route_id, segment_count, status, clock_timestamp()
FROM train_runs
WHERE id = $1
FOR SHARE`, params.TrainRunID).Scan(&routeID, &segmentCount, &status, &databaseNow)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CreateHoldResult{}, ErrNotFound
		}
		return CreateHoldResult{}, fmt.Errorf("lock train run: %w", err)
	}
	if status != "scheduled" {
		return CreateHoldResult{}, ErrNotBookable
	}
	if !params.HoldExpiresAt.After(databaseNow) {
		return CreateHoldResult{}, ErrInvalidArgument
	}
	requested, err := domain.NewSegmentMask(segmentCount, params.FromStopIndex, params.ToStopIndex)
	if err != nil {
		return CreateHoldResult{}, ErrInvalidArgument
	}

	ownedPassengers, err := tx.ownedPassengerIDs(ctx, params.UserID, passengerIDs)
	if err != nil {
		return CreateHoldResult{}, err
	}
	if len(ownedPassengers) != len(passengerIDs) {
		return CreateHoldResult{}, ErrNotFound
	}
	if err := tx.ensurePassengersAvailable(ctx, params.TrainRunID, ownedPassengers); err != nil {
		return CreateHoldResult{}, err
	}

	var fareAmountMinor int64
	var currency string
	err = tx.tx.QueryRow(ctx, `
SELECT amount_minor, currency
FROM fares
WHERE active
  AND from_stop_index = $3
  AND to_stop_index = $4
	  AND seat_class = $5
  AND (train_run_id = $1 OR (train_run_id IS NULL AND route_id = $2))
ORDER BY (train_run_id IS NOT NULL) DESC
LIMIT 1`, params.TrainRunID, routeID, params.FromStopIndex, params.ToStopIndex, seatClass).Scan(&fareAmountMinor, &currency)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CreateHoldResult{}, ErrNotFound
		}
		return CreateHoldResult{}, fmt.Errorf("load fare: %w", err)
	}
	fare, err := domain.NewMoney(fareAmountMinor, currency)
	if err != nil {
		return CreateHoldResult{}, ErrPersistenceInvariant
	}
	total, err := fare.Multiply(int64(len(passengerIDs)))
	if err != nil {
		return CreateHoldResult{}, err
	}

	allocated, err := tx.AllocateSeats(ctx, params.TrainRunID, seatClass, requested, len(passengerIDs))
	if err != nil {
		return CreateHoldResult{}, fmt.Errorf("allocate create-hold seats: %w", err)
	}

	reservationID := uuid.New()
	_, err = tx.tx.Exec(ctx, `
INSERT INTO reservations (
    id, user_id, train_run_id, segment_count, from_stop_index, to_stop_index,
    seat_class, status, expires_at, total_amount_minor, currency
)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'held', $8, $9, $10)`,
		reservationID, params.UserID, params.TrainRunID, segmentCount,
		params.FromStopIndex, params.ToStopIndex, seatClass,
		params.HoldExpiresAt.UTC(), total.AmountMinor(), total.Currency())
	if err != nil {
		return CreateHoldResult{}, fmt.Errorf("insert reservation: %w", err)
	}

	encodedMask := EncodeSegmentMask(requested)
	for index, seat := range allocated {
		_, err := tx.tx.Exec(ctx, `
INSERT INTO reservation_seats (
    reservation_id, train_run_id, segment_count, seat_id, passenger_id, segment_mask,
    fare_amount_minor, currency
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, reservationID, params.TrainRunID, segmentCount, seat.SeatID,
			ownedPassengers[index], encodedMask, fare.AmountMinor(), fare.Currency())
		if err != nil {
			return CreateHoldResult{}, fmt.Errorf("insert reservation seat: %w", err)
		}
	}
	if err := tx.insertReservationQuotaClaim(
		ctx, reservationID, params.UserID, params.TrainRunID, len(ownedPassengers),
	); err != nil {
		return CreateHoldResult{}, fmt.Errorf("persist create-hold quota claim: %w", err)
	}
	if err := tx.insertReservationLocator(ctx, reservationID, params.UserID); err != nil {
		return CreateHoldResult{}, fmt.Errorf("persist create-hold locator: %w", err)
	}

	if err := tx.CompleteIdempotency(ctx, acquisition.RecordID, reservationID); err != nil {
		return CreateHoldResult{}, fmt.Errorf("complete create-hold idempotency: %w", err)
	}
	if err := tx.appendReservationEvent(ctx, reservationID, "reservation.held", map[string]any{
		"reservationId": reservationID,
		"trainRunId":    params.TrainRunID,
		"status":        "held",
		"expiresAt":     params.HoldExpiresAt.UTC(),
		"seatCount":     len(allocated),
	}); err != nil {
		return CreateHoldResult{}, fmt.Errorf("append create-hold outbox event: %w", err)
	}
	if err := tx.recordSuccessfulGenerationWrite(ctx); err != nil {
		return CreateHoldResult{}, fmt.Errorf("record create-hold generation write: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return CreateHoldResult{}, err
	}
	return CreateHoldResult{
		ReservationID: reservationID, SeatCount: len(allocated),
		TotalAmountMinor: total.AmountMinor(), Currency: total.Currency(),
	}, nil
}

func (s *Store) ConfirmReservation(ctx context.Context, params ReservationCommandParams) (ConfirmReservationResult, error) {
	if err := validateReservationCommandParams(params); err != nil {
		return ConfirmReservationResult{}, err
	}
	tx, err := s.beginReservationCommandTransaction(ctx, params.ReservationID, params.UserID)
	if err != nil {
		return ConfirmReservationResult{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	acquisition, err := tx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID: params.UserID, Operation: OperationReservationConfirm,
		KeyHash: params.IdempotencyKeyHash, RequestFingerprint: params.RequestFingerprint,
		ExpiresAt: params.IdempotencyExpiresAt,
	})
	if err != nil {
		return ConfirmReservationResult{}, err
	}
	if acquisition.Replayed {
		result, err := tx.loadConfirmationResult(ctx, params.UserID, acquisition.ResourceID)
		if err != nil {
			return ConfirmReservationResult{}, err
		}
		result.Replayed = true
		if err := tx.Commit(ctx); err != nil {
			return ConfirmReservationResult{}, err
		}
		return result, nil
	}

	status, _, totalAmount, currency, err := tx.lockOwnedReservation(ctx, params.UserID, params.ReservationID)
	if err != nil {
		return ConfirmReservationResult{}, err
	}
	if status == "confirmed" {
		if err := tx.CompleteIdempotency(ctx, acquisition.RecordID, params.ReservationID); err != nil {
			return ConfirmReservationResult{}, err
		}
		result, err := tx.loadConfirmationResult(ctx, params.UserID, params.ReservationID)
		if err != nil {
			return ConfirmReservationResult{}, err
		}
		if err := tx.recordSuccessfulGenerationWrite(ctx); err != nil {
			return ConfirmReservationResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ConfirmReservationResult{}, err
		}
		return result, nil
	}
	if status != "held" {
		return ConfirmReservationResult{}, ErrInvalidState
	}
	commandTag, err := tx.tx.Exec(ctx, `
UPDATE reservations
SET status = 'confirmed'
WHERE id = $1
  AND status = 'held'
  AND expires_at > clock_timestamp()`, params.ReservationID)
	if err != nil {
		return ConfirmReservationResult{}, fmt.Errorf("confirm reservation: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ConfirmReservationResult{}, ErrReservationExpired
	}
	if err := tx.closeReservationQuotaClaim(ctx, params.ReservationID); err != nil {
		return ConfirmReservationResult{}, err
	}

	orderID := uuid.New()
	var orderCreatedAt time.Time
	err = tx.tx.QueryRow(ctx, `
INSERT INTO ticket_orders (id, reservation_id, user_id, status, total_amount_minor, currency)
VALUES ($1, $2, $3, 'confirmed', $4, $5)
RETURNING created_at`, orderID, params.ReservationID, params.UserID, totalAmount, currency).Scan(&orderCreatedAt)
	if err != nil {
		return ConfirmReservationResult{}, fmt.Errorf("insert ticket order: %w", err)
	}
	if err := tx.insertTicketOrderLocator(
		ctx, orderID, params.ReservationID, params.UserID,
		"confirmed", totalAmount, currency, orderCreatedAt,
	); err != nil {
		return ConfirmReservationResult{}, err
	}

	reservationSeatIDs, err := tx.reservationSeatIDs(ctx, params.ReservationID)
	if err != nil {
		return ConfirmReservationResult{}, err
	}
	if len(reservationSeatIDs) == 0 {
		return ConfirmReservationResult{}, ErrPersistenceInvariant
	}
	for _, reservationSeatID := range reservationSeatIDs {
		ticketID := uuid.New()
		ticketCode := "TKT" + uuid.NewString()
		_, err := tx.tx.Exec(ctx, `
INSERT INTO tickets (id, ticket_order_id, reservation_seat_id, ticket_code, status)
VALUES ($1, $2, $3, $4, 'active')`, ticketID, orderID, reservationSeatID, ticketCode)
		if err != nil {
			return ConfirmReservationResult{}, fmt.Errorf("insert ticket: %w", err)
		}
		if err := tx.insertTicketLocator(ctx, ticketID, orderID, params.ReservationID, params.UserID); err != nil {
			return ConfirmReservationResult{}, err
		}
		if err := tx.appendTicketEvent(ctx, ticketID, map[string]any{
			"ticketId": ticketID, "orderId": orderID, "reservationId": params.ReservationID,
		}); err != nil {
			return ConfirmReservationResult{}, err
		}
	}

	if err := tx.CompleteIdempotency(ctx, acquisition.RecordID, params.ReservationID); err != nil {
		return ConfirmReservationResult{}, err
	}
	if err := tx.appendReservationEvent(ctx, params.ReservationID, "reservation.confirmed", map[string]any{
		"reservationId": params.ReservationID, "ticketOrderId": orderID, "status": "confirmed",
	}); err != nil {
		return ConfirmReservationResult{}, err
	}
	if err := tx.recordSuccessfulGenerationWrite(ctx); err != nil {
		return ConfirmReservationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ConfirmReservationResult{}, err
	}
	return ConfirmReservationResult{ReservationID: params.ReservationID, TicketOrderID: orderID, TicketCount: len(reservationSeatIDs)}, nil
}

func (s *Store) CancelReservation(ctx context.Context, params ReservationCommandParams) (CancelReservationResult, error) {
	if err := validateReservationCommandParams(params); err != nil {
		return CancelReservationResult{}, err
	}
	tx, err := s.beginReservationCommandTransaction(ctx, params.ReservationID, params.UserID)
	if err != nil {
		return CancelReservationResult{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	acquisition, err := tx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID: params.UserID, Operation: OperationReservationCancel,
		KeyHash: params.IdempotencyKeyHash, RequestFingerprint: params.RequestFingerprint,
		ExpiresAt: params.IdempotencyExpiresAt,
	})
	if err != nil {
		return CancelReservationResult{}, err
	}
	if acquisition.Replayed {
		result, err := tx.loadCancellationResult(ctx, params.UserID, acquisition.ResourceID)
		if err != nil {
			return CancelReservationResult{}, err
		}
		result.Replayed = true
		if err := tx.Commit(ctx); err != nil {
			return CancelReservationResult{}, err
		}
		return result, nil
	}

	status, _, _, _, err := tx.lockOwnedReservation(ctx, params.UserID, params.ReservationID)
	if err != nil {
		return CancelReservationResult{}, err
	}
	if status == "cancelled" {
		if err := tx.CompleteIdempotency(ctx, acquisition.RecordID, params.ReservationID); err != nil {
			return CancelReservationResult{}, err
		}
		if err := tx.recordSuccessfulGenerationWrite(ctx); err != nil {
			return CancelReservationResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return CancelReservationResult{}, err
		}
		return CancelReservationResult{ReservationID: params.ReservationID}, nil
	}
	if status != "held" && status != "confirmed" {
		return CancelReservationResult{}, ErrInvalidState
	}
	commandTag, err := tx.tx.Exec(ctx, `UPDATE reservations SET status = 'cancelled' WHERE id = $1 AND status = $2`, params.ReservationID, status)
	if err != nil {
		return CancelReservationResult{}, fmt.Errorf("cancel reservation: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return CancelReservationResult{}, ErrPersistenceInvariant
	}

	released, err := tx.releaseReservationSeats(ctx, params.ReservationID)
	if err != nil {
		return CancelReservationResult{}, err
	}
	if status == "confirmed" {
		if _, err := tx.tx.Exec(ctx, `UPDATE ticket_orders SET status = 'cancelled' WHERE reservation_id = $1`, params.ReservationID); err != nil {
			return CancelReservationResult{}, fmt.Errorf("cancel ticket order: %w", err)
		}
		if _, err := tx.tx.Exec(ctx, `
UPDATE tickets
SET status = 'cancelled'
WHERE ticket_order_id = (SELECT id FROM ticket_orders WHERE reservation_id = $1)`, params.ReservationID); err != nil {
			return CancelReservationResult{}, fmt.Errorf("cancel tickets: %w", err)
		}
		if err := tx.updateTicketOrderLocatorStatus(ctx, params.ReservationID, "cancelled"); err != nil {
			return CancelReservationResult{}, err
		}
	}
	if err := tx.closeReservationQuotaClaim(ctx, params.ReservationID); err != nil {
		return CancelReservationResult{}, err
	}
	if err := tx.CompleteIdempotency(ctx, acquisition.RecordID, params.ReservationID); err != nil {
		return CancelReservationResult{}, err
	}
	if err := tx.appendReservationEvent(ctx, params.ReservationID, "reservation.cancelled", map[string]any{
		"reservationId": params.ReservationID, "status": "cancelled", "releasedSeatCount": released,
	}); err != nil {
		return CancelReservationResult{}, err
	}
	if err := tx.recordSuccessfulGenerationWrite(ctx); err != nil {
		return CancelReservationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CancelReservationResult{}, err
	}
	return CancelReservationResult{ReservationID: params.ReservationID, ReleasedSeatCount: released}, nil
}

func (s *Store) ExpireDue(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	if now.IsZero() || limit <= 0 || limit > 1000 {
		return nil, ErrInvalidArgument
	}
	if s.shards != nil {
		return s.expireDueAcrossShards(ctx, limit)
	}
	var (
		expired  []uuid.UUID
		failed   []uuid.UUID
		failures []error
	)
	for len(expired)+len(failed) < limit {
		id, found, err := s.expireOneDue(ctx, failed)
		if !found && err == nil {
			break
		}
		if err != nil {
			if id == uuid.Nil {
				return expired, errors.Join(append(failures, err)...)
			}
			failed = append(failed, id)
			failures = append(failures, fmt.Errorf("expire reservation %s: %w", id, err))
			continue
		}
		expired = append(expired, id)
	}
	if err := s.cleanupExpiredIdempotency(ctx, 1000); err != nil {
		failures = append(failures, err)
	}
	return expired, errors.Join(failures...)
}

func (s *Store) cleanupExpiredIdempotency(ctx context.Context, limit int) error {
	if limit <= 0 {
		return ErrInvalidArgument
	}
	_, err := s.pool.Exec(ctx, `
		WITH expired AS (
			SELECT id
			FROM idempotency_records
			WHERE expires_at <= clock_timestamp()
			ORDER BY expires_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		DELETE FROM idempotency_records AS record
		USING expired
		WHERE record.id = expired.id
	`, limit)
	if err != nil {
		return fmt.Errorf("clean expired idempotency records: %w", err)
	}
	return nil
}

// expireOneDue gives each claimed reservation its own transaction. A rollback
// releases the row for another worker; the caller excludes that ID only from
// its current batch so one corrupt item cannot starve the remaining batch.
func (s *Store) expireOneDue(ctx context.Context, excluded []uuid.UUID) (uuid.UUID, bool, error) {
	tx, err := s.Begin(ctx)
	if err != nil {
		return uuid.Nil, false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	excludedStrings := make([]string, len(excluded))
	for index, id := range excluded {
		excludedStrings[index] = id.String()
	}
	var id uuid.UUID
	err = tx.tx.QueryRow(ctx, `
SELECT id
FROM reservations
WHERE status = 'held'
  AND expires_at <= clock_timestamp()
  AND NOT (id = ANY($1::uuid[]))
ORDER BY expires_at, id
FOR UPDATE SKIP LOCKED
LIMIT 1`, excludedStrings).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("claim due reservation: %w", err)
	}

	commandTag, err := tx.tx.Exec(ctx, `
UPDATE reservations
SET status = 'expired'
WHERE id = $1
  AND status = 'held'
  AND expires_at <= clock_timestamp()`, id)
	if err != nil {
		return id, true, fmt.Errorf("transition to expired: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return id, true, ErrPersistenceInvariant
	}
	released, err := tx.releaseReservationSeats(ctx, id)
	if err != nil {
		return id, true, err
	}
	if err := tx.appendReservationEvent(ctx, id, "reservation.expired", map[string]any{
		"reservationId": id, "status": "expired", "releasedSeatCount": released,
	}); err != nil {
		return id, true, err
	}
	if err := tx.closeReservationQuotaClaim(ctx, id); err != nil {
		return id, true, err
	}
	if err := tx.Commit(ctx); err != nil {
		return id, true, err
	}
	return id, true, nil
}

func (s *Store) GetReservation(ctx context.Context, userID, reservationID uuid.UUID) (ReservationRecord, error) {
	if s == nil || s.pool == nil || userID == uuid.Nil || reservationID == uuid.Nil {
		return ReservationRecord{}, ErrInvalidArgument
	}
	var (
		record ReservationRecord
		row    pgx.Row
		tx     *Tx
		err    error
	)
	if s.shards != nil {
		tx, err = s.beginReservationRead(ctx, reservationID, userID)
		if err != nil {
			if errors.Is(err, sharding.ErrLocatorNotFound) {
				return ReservationRecord{}, ErrNotFound
			}
			return ReservationRecord{}, err
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		row = tx.tx.QueryRow(ctx, `
SELECT r.id, r.status, r.total_amount_minor, r.currency,
       (SELECT count(*) FROM reservation_seats AS rs WHERE rs.reservation_id = r.id),
       (SELECT count(*) FROM tickets AS t
        JOIN ticket_orders AS o ON o.id = t.ticket_order_id
        WHERE o.reservation_id = r.id AND t.status = 'active'),
       (SELECT count(*) FROM outbox_events AS e
        WHERE e.aggregate_type = 'reservation' AND e.aggregate_id = r.id)
FROM reservations AS r
WHERE r.id = $1 AND r.user_id = $2`, reservationID, userID)
	} else {
		row = s.pool.QueryRow(ctx, `
SELECT r.id, r.status, r.total_amount_minor, r.currency,
       (SELECT count(*) FROM reservation_seats AS rs WHERE rs.reservation_id = r.id),
       (SELECT count(*) FROM tickets AS t
        JOIN ticket_orders AS o ON o.id = t.ticket_order_id
        WHERE o.reservation_id = r.id AND t.status = 'active'),
       (SELECT count(*) FROM outbox_events AS e
        WHERE e.aggregate_type = 'reservation' AND e.aggregate_id = r.id)
FROM reservations AS r
WHERE r.id = $1 AND r.user_id = $2`, reservationID, userID)
	}
	err = row.Scan(
		&record.ReservationID, &record.Status, &record.TotalAmountMinor, &record.Currency,
		&record.SeatCount, &record.ActiveTicketCount, &record.OutboxEventCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ReservationRecord{}, ErrNotFound
		}
		return ReservationRecord{}, fmt.Errorf("get reservation: %w", err)
	}
	if tx != nil {
		if err := tx.Commit(ctx); err != nil {
			return ReservationRecord{}, err
		}
	}
	return record, nil
}

func (tx *Tx) releaseReservationSeats(ctx context.Context, reservationID uuid.UUID) (int, error) {
	var expected, released int
	err := tx.tx.QueryRow(ctx, `
WITH locked AS MATERIALIZED (
    SELECT si.train_run_id, si.seat_id, rs.segment_mask
    FROM reservation_seats AS rs
    JOIN reservations AS r ON r.id = rs.reservation_id
    JOIN seat_inventory AS si
      ON si.train_run_id = r.train_run_id
     AND si.seat_id = rs.seat_id
    WHERE rs.reservation_id = $1
      AND CASE
            WHEN bit_length(si.occupied_segments) = bit_length(rs.segment_mask)
            THEN (si.occupied_segments & rs.segment_mask) = rs.segment_mask
            ELSE false
          END
    ORDER BY si.seat_id
    FOR UPDATE OF rs, si
), expected AS (
    SELECT count(*)::integer AS count
    FROM reservation_seats
    WHERE reservation_id = $1
), updated AS (
    UPDATE seat_inventory AS si
    SET occupied_segments = si.occupied_segments & ~locked.segment_mask,
        version = si.version + 1
    FROM locked, expected
    WHERE si.train_run_id = locked.train_run_id
      AND si.seat_id = locked.seat_id
      AND (SELECT count(*) FROM locked) = expected.count
      AND expected.count > 0
    RETURNING si.seat_id
)
SELECT expected.count, count(updated.seat_id)::integer
FROM expected
LEFT JOIN updated ON true
GROUP BY expected.count`, reservationID).Scan(&expected, &released)
	if err != nil {
		return 0, fmt.Errorf("release reservation seats: %w", err)
	}
	if expected <= 0 || released != expected {
		return 0, ErrPersistenceInvariant
	}
	return released, nil
}

func (tx *Tx) ownedPassengerIDs(ctx context.Context, userID uuid.UUID, requested []uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]string, len(requested))
	for index, id := range requested {
		ids[index] = id.String()
	}
	rows, err := tx.tx.Query(ctx, `
SELECT id
FROM passengers
WHERE user_id = $1 AND id = ANY($2::uuid[])
ORDER BY id
FOR UPDATE`, userID, ids)
	if err != nil {
		return nil, fmt.Errorf("load owned passengers: %w", err)
	}
	defer rows.Close()
	var result []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan owned passenger: %w", err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate owned passengers: %w", err)
	}
	return result, nil
}

func (tx *Tx) ensurePassengersAvailable(ctx context.Context, trainRunID uuid.UUID, passengerIDs []uuid.UUID) error {
	ids := make([]string, len(passengerIDs))
	for index, id := range passengerIDs {
		ids[index] = id.String()
	}
	var conflicts int
	if err := tx.tx.QueryRow(ctx, `
SELECT count(*)::integer
FROM reservation_seats AS rs
JOIN reservations AS r ON r.id = rs.reservation_id
WHERE r.train_run_id = $1
  AND r.status IN ('held', 'confirmed')
  AND rs.passenger_id = ANY($2::uuid[])`, trainRunID, ids).Scan(&conflicts); err != nil {
		return fmt.Errorf("check active passenger reservations: %w", err)
	}
	if conflicts != 0 {
		return ErrPassengerConflict
	}
	return nil
}

func (tx *Tx) reservationSeatIDs(ctx context.Context, reservationID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := tx.tx.Query(ctx, `
SELECT id
FROM reservation_seats
WHERE reservation_id = $1
ORDER BY seat_id
FOR UPDATE`, reservationID)
	if err != nil {
		return nil, fmt.Errorf("load reservation seats: %w", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan reservation seat: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reservation seats: %w", err)
	}
	return ids, nil
}

func (tx *Tx) lockOwnedReservation(ctx context.Context, userID, reservationID uuid.UUID) (string, time.Time, int64, string, error) {
	var status, currency string
	var expiresAt time.Time
	var totalAmount int64
	err := tx.tx.QueryRow(ctx, `
SELECT status, expires_at, total_amount_minor, currency
FROM reservations
WHERE id = $1 AND user_id = $2
FOR UPDATE`, reservationID, userID).Scan(&status, &expiresAt, &totalAmount, &currency)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", time.Time{}, 0, "", ErrNotFound
		}
		return "", time.Time{}, 0, "", fmt.Errorf("lock owned reservation: %w", err)
	}
	return status, expiresAt, totalAmount, currency, nil
}

func (tx *Tx) loadCreateHoldResult(ctx context.Context, userID, reservationID uuid.UUID) (CreateHoldResult, error) {
	var result CreateHoldResult
	err := tx.tx.QueryRow(ctx, `
SELECT r.id, r.total_amount_minor, r.currency,
       (SELECT count(*) FROM reservation_seats AS rs WHERE rs.reservation_id = r.id)
FROM reservations AS r
WHERE r.id = $1 AND r.user_id = $2`, reservationID, userID).Scan(
		&result.ReservationID, &result.TotalAmountMinor, &result.Currency, &result.SeatCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CreateHoldResult{}, ErrPersistenceInvariant
		}
		return CreateHoldResult{}, fmt.Errorf("load replayed hold: %w", err)
	}
	return result, nil
}

func (tx *Tx) loadConfirmationResult(ctx context.Context, userID, reservationID uuid.UUID) (ConfirmReservationResult, error) {
	var result ConfirmReservationResult
	err := tx.tx.QueryRow(ctx, `
SELECT r.id, o.id, (SELECT count(*) FROM tickets WHERE ticket_order_id = o.id)
FROM reservations AS r
JOIN ticket_orders AS o ON o.reservation_id = r.id
WHERE r.id = $1 AND r.user_id = $2`, reservationID, userID).Scan(
		&result.ReservationID, &result.TicketOrderID, &result.TicketCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ConfirmReservationResult{}, ErrPersistenceInvariant
		}
		return ConfirmReservationResult{}, fmt.Errorf("load confirmation result: %w", err)
	}
	return result, nil
}

func (tx *Tx) loadCancellationResult(ctx context.Context, userID, reservationID uuid.UUID) (CancelReservationResult, error) {
	var result CancelReservationResult
	err := tx.tx.QueryRow(ctx, `
SELECT r.id, (SELECT count(*) FROM reservation_seats AS rs WHERE rs.reservation_id = r.id)
FROM reservations AS r
WHERE r.id = $1 AND r.user_id = $2 AND r.status = 'cancelled'`, reservationID, userID).Scan(
		&result.ReservationID, &result.ReleasedSeatCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CancelReservationResult{}, ErrPersistenceInvariant
		}
		return CancelReservationResult{}, fmt.Errorf("load cancellation result: %w", err)
	}
	return result, nil
}

func validateCreateHoldParams(params CreateHoldParams) error {
	if params.UserID == uuid.Nil || params.TrainRunID == uuid.Nil || params.FromStopIndex < 0 ||
		params.ToStopIndex <= params.FromStopIndex || !validSeatClass(params.SeatClass) ||
		len(params.PassengerIDs) == 0 || params.HoldExpiresAt.IsZero() || params.IdempotencyExpiresAt.IsZero() ||
		len(params.IdempotencyKeyHash) != 32 || len(params.RequestFingerprint) != 32 {
		return ErrInvalidArgument
	}
	for _, id := range params.PassengerIDs {
		if id == uuid.Nil {
			return ErrInvalidArgument
		}
	}
	if params.AdmissionPolicy != nil &&
		(params.AdmissionPolicy.PolicyID == uuid.Nil || params.AdmissionPolicy.Version < 1) {
		return ErrInvalidArgument
	}
	return nil
}

func validateReservationCommandParams(params ReservationCommandParams) error {
	if params.UserID == uuid.Nil || params.ReservationID == uuid.Nil || params.Now.IsZero() ||
		params.IdempotencyExpiresAt.IsZero() || len(params.IdempotencyKeyHash) != 32 ||
		len(params.RequestFingerprint) != 32 {
		return ErrInvalidArgument
	}
	return nil
}
