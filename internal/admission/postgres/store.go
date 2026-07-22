// Package postgres persists the Admission bounded context's durable policy.
// Redis waiting-room state deliberately has a separate adapter: PostgreSQL is
// the authority for deciding whether a train run is hot.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidConfiguration = errors.New("invalid admission postgres store configuration")
	ErrInvalidInput         = errors.New("invalid admission policy input")
	ErrNotFound             = errors.New("admission policy not found")
	ErrConflict             = errors.New("admission policy conflict")
	ErrVersionConflict      = errors.New("admission policy version conflict")
	ErrPersistence          = errors.New("admission policy persistence failure")
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Store{pool: pool}, nil
}

// New is provided for command wiring that follows the existing Booking
// constructor convention. Application code that needs configuration errors can
// use NewStore instead.
func New(pool *pgxpool.Pool) *Store {
	store, err := NewStore(pool)
	if err != nil {
		return nil
	}
	return store
}

func safeError(err error) error {
	if errors.Is(err, ErrInvalidConfiguration) || errors.Is(err, ErrInvalidInput) ||
		errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) ||
		errors.Is(err, ErrVersionConflict) || errors.Is(err, ErrPersistence) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		switch pgError.Code {
		case "23505":
			return ErrConflict
		case "23503":
			return ErrNotFound
		case "23514", "22001", "22P02", "22003":
			return ErrInvalidInput
		}
	}
	return ErrPersistence
}

func begin(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin admission policy transaction: %w", err)
	}
	return tx, nil
}
