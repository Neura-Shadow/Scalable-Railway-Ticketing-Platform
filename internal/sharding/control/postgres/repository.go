// Package postgres persists bounded train-run migration control transactions
// in the Milestone 4 PostgreSQL catalog.
package postgres

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidRepository = errors.New("invalid shard control PostgreSQL repository")
	ErrPersistence       = errors.New("shard control persistence unavailable")
)

// DB is the pgx-compatible transaction seam used by Repository.
type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Repository struct {
	db DB
}

func NewRepository(db DB) (*Repository, error) {
	if db == nil {
		return nil, ErrInvalidRepository
	}
	return &Repository{db: db}, nil
}

func (repository *Repository) WithinTransaction(
	ctx context.Context,
	callback func(context.Context, control.Transaction) error,
) error {
	if repository == nil || repository.db == nil || ctx == nil || callback == nil {
		return ErrInvalidRepository
	}
	pgxTx, err := repository.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ErrPersistence
	}
	tx := &Transaction{tx: pgxTx}
	if err := callback(ctx, tx); err != nil {
		_ = pgxTx.Rollback(context.Background())
		return err
	}
	if err := pgxTx.Commit(ctx); err != nil {
		return ErrPersistence
	}
	return nil
}

// Transaction implements control.Transaction over one pgx transaction.
type Transaction struct {
	tx pgx.Tx
}
