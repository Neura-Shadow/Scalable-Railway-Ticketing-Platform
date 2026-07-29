// Package postgresx constructs bounded PostgreSQL pools without returning DSN
// parser details that could disclose configured credentials.
package postgresx

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInvalidPoolConfig = errors.New("postgres pool configuration invalid")

const maxOpenConnections = 1_000

func NewBoundedPool(ctx context.Context, dsn string, maxOpen int) (*pgxpool.Pool, error) {
	if ctx == nil || maxOpen < 1 || maxOpen > maxOpenConnections {
		return nil, ErrInvalidPoolConfig
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, ErrInvalidPoolConfig
	}
	config.MaxConns = int32(maxOpen)
	if config.MinConns > config.MaxConns {
		config.MinConns = 0
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, ErrInvalidPoolConfig
	}
	return pool, nil
}
