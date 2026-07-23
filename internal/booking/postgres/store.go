package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidArgument          = errors.New("invalid booking store argument")
	ErrInsufficientInventory    = errors.New("insufficient seat inventory")
	ErrNotFound                 = errors.New("booking resource not found")
	ErrNotBookable              = errors.New("train run is not bookable")
	ErrReservationExpired       = errors.New("reservation expired")
	ErrPassengerConflict        = errors.New("passenger already has an active reservation for this train run")
	ErrReservationQuotaExceeded = errors.New("reservation_quota_exceeded")
	ErrAdmissionRequired        = errors.New("hot-train admission required")
	ErrAdmissionPolicyChanged   = errors.New("hot-train admission policy changed")
	ErrInvalidState             = errors.New("invalid reservation state")
	ErrPersistenceInvariant     = errors.New("booking persistence invariant violated")
)

// Store is the PostgreSQL authority for Booking commands. The exported
// transaction seam exists for the Offering commissioning workflow and focused
// concurrency tests; customer commands should use the Store command methods.
type Store struct {
	pool                   *pgxpool.Pool
	reservationQuotaLimits ReservationQuotaLimits
	shards                 bookingShardRouter
}

func New(pool *pgxpool.Pool) *Store {
	return NewWithReservationQuotaLimits(pool, DefaultReservationQuotaLimits())
}

func NewWithReservationQuotaLimits(pool *pgxpool.Pool, limits ReservationQuotaLimits) *Store {
	return &Store{pool: pool, reservationQuotaLimits: limits}
}

func isRetryableTransactionError(err error) bool {
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) {
		return false
	}
	return pgError.Code == "40001" || pgError.Code == "40P01"
}

type Tx struct {
	tx     pgx.Tx
	route  sharding.ShardRoute
	routed bookingRoutedTx
}

func (s *Store) Begin(ctx context.Context) (*Tx, error) {
	if s == nil || s.pool == nil {
		return nil, ErrInvalidArgument
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin booking transaction: %w", err)
	}
	return &Tx{tx: tx}, nil
}

func (tx *Tx) Commit(ctx context.Context) error {
	if tx == nil || tx.tx == nil {
		return ErrInvalidArgument
	}
	if tx.routed != nil {
		return tx.routed.Commit(ctx)
	}
	if err := tx.tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit booking transaction: %w", err)
	}
	return nil
}

func (tx *Tx) Rollback(ctx context.Context) error {
	if tx == nil || tx.tx == nil {
		return ErrInvalidArgument
	}
	if tx.routed != nil {
		return tx.routed.Rollback(ctx)
	}
	err := tx.tx.Rollback(ctx)
	if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("rollback booking transaction: %w", err)
	}
	return nil
}

func (s *Store) InitializeInventory(ctx context.Context, trainRunID uuid.UUID) (int64, error) {
	var (
		tx  *Tx
		err error
	)
	if s != nil && s.shards != nil {
		tx, err = s.beginTrainRunWrite(ctx, trainRunID)
	} else {
		tx, err = s.Begin(ctx)
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	inserted, err := tx.InitializeInventory(ctx, trainRunID)
	if err != nil {
		return 0, err
	}
	if inserted > 0 {
		if err := tx.recordSuccessfulGenerationWrite(ctx); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return inserted, nil
}

// InitializeInventory locks the train run and creates one zero mask for every
// active seat on its physical train. ON CONFLICT makes commissioning retries
// stable while the surrounding Offering transaction remains authoritative.
func (tx *Tx) InitializeInventory(ctx context.Context, trainRunID uuid.UUID) (int64, error) {
	if trainRunID == uuid.Nil {
		return 0, ErrInvalidArgument
	}
	var trainID uuid.UUID
	var segmentCount int
	if err := tx.tx.QueryRow(ctx, `
SELECT train_id, segment_count
FROM train_runs
WHERE id = $1
FOR UPDATE`, trainRunID).Scan(&trainID, &segmentCount); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("lock train run for inventory initialization: %w", err)
	}

	commandTag, err := tx.tx.Exec(ctx, `
INSERT INTO seat_inventory (train_run_id, segment_count, seat_id, seat_class, occupied_segments)
SELECT $1, $2, s.id, c.seat_class, repeat('0', $2)::bit varying
FROM coaches AS c
JOIN seats AS s ON s.coach_id = c.id
WHERE c.train_id = $3
  AND s.active
ORDER BY c.coach_number, s.seat_number, s.id
ON CONFLICT (train_run_id, seat_id) DO NOTHING`, trainRunID, segmentCount, trainID)
	if err != nil {
		return 0, fmt.Errorf("initialize seat inventory: %w", err)
	}
	return commandTag.RowsAffected(), nil
}

type AllocatedSeat struct {
	SeatID      uuid.UUID
	CoachNumber string
	SeatNumber  string
}

type InventoryMask struct {
	SeatID uuid.UUID
	Mask   domain.SegmentMask
}

func (tx *Tx) AllocateSeat(ctx context.Context, trainRunID uuid.UUID, seatClass string, requested domain.SegmentMask) (AllocatedSeat, error) {
	seats, err := tx.AllocateSeats(ctx, trainRunID, seatClass, requested, 1)
	if err != nil {
		return AllocatedSeat{}, err
	}
	return seats[0], nil
}

// AllocateSeats applies one requested mask to exactly count inventory rows or
// none. The CASE is deliberate: PostgreSQL does not guarantee boolean
// evaluation order, and bitwise operators reject different-length VARBITs.
func (tx *Tx) AllocateSeats(ctx context.Context, trainRunID uuid.UUID, seatClass string, requested domain.SegmentMask, count int) ([]AllocatedSeat, error) {
	if tx == nil || tx.tx == nil || trainRunID == uuid.Nil || count <= 0 || requested.BitLength() <= 0 || !validSeatClass(seatClass) || requested.IsZero() {
		return nil, ErrInvalidArgument
	}

	rows, err := tx.tx.Query(ctx, `
WITH candidates AS MATERIALIZED (
    SELECT si.train_run_id,
           si.seat_id,
           c.coach_number,
           s.seat_number
    FROM seat_inventory AS si
    JOIN seats AS s ON s.id = si.seat_id
    JOIN coaches AS c ON c.id = s.coach_id
    WHERE si.train_run_id = $1
      AND si.seat_class = $2
      AND s.active
      AND CASE
            WHEN bit_length(si.occupied_segments) = bit_length($3::bit varying)
            THEN bit_count($3::bit varying) > 0
             AND (si.occupied_segments & $3::bit varying)
                 = repeat('0', bit_length(si.occupied_segments))::bit varying
            ELSE false
          END
    ORDER BY c.coach_number, s.seat_number, si.seat_id
    FOR UPDATE OF si SKIP LOCKED
    LIMIT $4
), updated AS (
    UPDATE seat_inventory AS si
    SET occupied_segments = si.occupied_segments | $3::bit varying,
        version = si.version + 1
    FROM candidates AS candidate
    WHERE si.train_run_id = candidate.train_run_id
      AND si.seat_id = candidate.seat_id
      AND (SELECT count(*) FROM candidates) = $4
    RETURNING si.seat_id
)
SELECT updated.seat_id, candidate.coach_number, candidate.seat_number
FROM updated
JOIN candidates AS candidate ON candidate.seat_id = updated.seat_id
ORDER BY candidate.coach_number, candidate.seat_number, updated.seat_id`, trainRunID, strings.ToLower(seatClass), EncodeSegmentMask(requested), count)
	if err != nil {
		return nil, fmt.Errorf("allocate seat inventory: %w", err)
	}
	defer rows.Close()

	allocated := make([]AllocatedSeat, 0, count)
	for rows.Next() {
		var seat AllocatedSeat
		if err := rows.Scan(&seat.SeatID, &seat.CoachNumber, &seat.SeatNumber); err != nil {
			return nil, fmt.Errorf("scan allocated seat: %w", err)
		}
		allocated = append(allocated, seat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate allocated seats: %w", err)
	}
	if len(allocated) != count {
		return nil, ErrInsufficientInventory
	}
	return allocated, nil
}

func (tx *Tx) InventoryMasks(ctx context.Context, trainRunID uuid.UUID) ([]InventoryMask, error) {
	if tx == nil || tx.tx == nil || trainRunID == uuid.Nil {
		return nil, ErrInvalidArgument
	}
	rows, err := tx.tx.Query(ctx, `
SELECT seat_id, occupied_segments
FROM seat_inventory
WHERE train_run_id = $1
ORDER BY seat_id`, trainRunID)
	if err != nil {
		return nil, fmt.Errorf("load inventory masks: %w", err)
	}
	defer rows.Close()

	var result []InventoryMask
	for rows.Next() {
		var item InventoryMask
		var bits pgtype.Bits
		if err := rows.Scan(&item.SeatID, &bits); err != nil {
			return nil, fmt.Errorf("scan inventory mask: %w", err)
		}
		item.Mask, err = DecodeSegmentMask(bits)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inventory masks: %w", err)
	}
	return result, nil
}

func validSeatClass(value string) bool {
	switch strings.ToLower(value) {
	case "standard", "business", "first":
		return true
	default:
		return false
	}
}
