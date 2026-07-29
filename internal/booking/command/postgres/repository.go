// Package postgres persists the control half of the physical booking command
// saga. It never opens or queries a booking-shard connection.
package postgres

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidOptions      = errors.New("invalid booking command repository options")
	ErrIdempotencyConflict = errors.New("booking command idempotency conflict")
	ErrQuotaExceeded       = errors.New("booking command quota exceeded")
	ErrPassengerOwnership  = errors.New("booking command passenger ownership mismatch")
	ErrInvalidPayload      = errors.New("invalid booking command payload")
	ErrRouteUnavailable    = errors.New("booking command route unavailable")
	ErrControlWrite        = errors.New("booking command control write failed")
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Options struct {
	LeaseTTL                   time.Duration
	MaxActiveHoldsPerUser      int
	MaxActiveHoldsPerTrainRun  int
	MaxActivePassengersPerUser int
}

type Repository struct {
	db      DB
	options Options
}

func NewRepository(db DB, options Options) (*Repository, error) {
	if nilInterface(db) || options.LeaseTTL <= 0 || options.LeaseTTL > 24*time.Hour ||
		options.MaxActiveHoldsPerUser < 1 || options.MaxActiveHoldsPerUser > 10_000 ||
		options.MaxActiveHoldsPerTrainRun < 1 || options.MaxActiveHoldsPerTrainRun > options.MaxActiveHoldsPerUser ||
		options.MaxActivePassengersPerUser < options.MaxActiveHoldsPerUser ||
		options.MaxActivePassengersPerUser > 10_000 {
		return nil, ErrInvalidOptions
	}
	return &Repository{db: db, options: options}, nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
