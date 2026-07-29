package physical

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenPGXPool converts an already allowlisted secret into one bounded pool.
// Parse and construction failures intentionally collapse to ErrInvalidRegistry
// so a DSN can never enter an error returned to callers.
func OpenPGXPool(ctx context.Context, dsn string, limits PoolLimits) (Pool, error) {
	if ctx == nil || limits.MaxOpenConns < 1 || limits.MaxOpenConns > 100 ||
		limits.MaxIdleConns < 0 || limits.MaxIdleConns > limits.MaxOpenConns {
		return nil, ErrInvalidRegistry
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, ErrInvalidRegistry
	}
	config.MaxConns = int32(limits.MaxOpenConns)
	// pgxpool has no MaxIdleConns setting. MaxConns is also the hard idle
	// ceiling; MaxConnIdleTime bounds how long surplus idle connections live.
	config.MinConns = 0
	if limits.MaxLifetime > 0 {
		config.MaxConnLifetime = limits.MaxLifetime
	}
	if limits.MaxIdleTime > 0 {
		config.MaxConnIdleTime = limits.MaxIdleTime
	}
	connectTimeout := limits.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 3 * time.Second
	}
	config.ConnConfig.ConnectTimeout = connectTimeout
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, ErrInvalidRegistry
	}
	return pool, nil
}
