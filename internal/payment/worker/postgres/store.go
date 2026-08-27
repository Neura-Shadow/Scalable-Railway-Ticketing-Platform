// Package postgres implements the payment worker's short control-plane
// transactions. Provider and shard clients are intentionally absent here.
package postgres

import (
	"context"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Store struct {
	db         DB
	deployment *authority.Deployment
}

func New(db DB) (*Store, error) {
	if db == nil {
		return nil, worker.ErrStoreUnavailable
	}
	return &Store{db: db}, nil
}

// NewWithRegionalAuthority is the production constructor. The legacy New
// constructor remains for migration fixtures that predate control schema v11.
func NewWithRegionalAuthority(db DB, deployment authority.Deployment) (*Store, error) {
	store, err := New(db)
	if err != nil {
		return nil, err
	}
	store.deployment = &deployment
	return store, nil
}

func (store *Store) begin(ctx context.Context) (pgx.Tx, error) {
	if store == nil || store.db == nil || ctx == nil {
		return nil, worker.ErrStoreUnavailable
	}
	return store.beginWithOptions(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
}

func (store *Store) beginWithOptions(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if store == nil || store.db == nil || ctx == nil {
		return nil, worker.ErrStoreUnavailable
	}
	tx, err := store.db.BeginTx(ctx, options)
	if err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	if store.deployment != nil {
		if err := authoritypostgres.AuthorizeControlTransaction(ctx, tx, *store.deployment); err != nil {
			rollback(ctx, tx)
			return nil, worker.ErrStoreUnavailable
		}
	}
	return tx, nil
}

func rollback(ctx context.Context, tx pgx.Tx) {
	if tx != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}
}

func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return worker.ErrStoreUnavailable
	}
	return nil
}

func oneRow(tag pgconn.CommandTag, err error) error {
	if err != nil {
		return worker.ErrStoreUnavailable
	}
	if tag.RowsAffected() != 1 {
		return worker.ErrLeaseLost
	}
	return nil
}

var _ worker.Store = (*Store)(nil)
