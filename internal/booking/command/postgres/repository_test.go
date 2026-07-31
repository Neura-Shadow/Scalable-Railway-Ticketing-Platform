package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/postgres"
	"github.com/jackc/pgx/v5"
)

func TestNewRepositoryRejectsUnsafeQuotaLeaseLimits(t *testing.T) {
	t.Parallel()

	_, err := postgres.NewRepository(&controlDB{}, postgres.Options{
		LeaseTTL:                   0,
		MaxActiveHoldsPerUser:      10,
		MaxActiveHoldsPerTrainRun:  3,
		MaxActivePassengersPerUser: 24,
	})
	if !errors.Is(err, postgres.ErrInvalidOptions) {
		t.Fatalf("NewRepository() error = %v, want %v", err, postgres.ErrInvalidOptions)
	}

	_, err = postgres.NewRepository(&controlDB{}, postgres.Options{
		LeaseTTL:                   10 * time.Minute,
		MaxActiveHoldsPerUser:      1,
		MaxActiveHoldsPerTrainRun:  2,
		MaxActivePassengersPerUser: 1,
	})
	if !errors.Is(err, postgres.ErrInvalidOptions) {
		t.Fatalf("NewRepository() inconsistent quota error = %v, want %v", err, postgres.ErrInvalidOptions)
	}
}

type controlDB struct{}

func (*controlDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errors.New("unused control database")
}
