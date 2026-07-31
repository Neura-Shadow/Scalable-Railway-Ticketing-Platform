package physical

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxLocalDatabaseTimeout = 30 * time.Second
const localTimeoutRollbackLimit = 2 * time.Second

type localTimeoutPool struct {
	delegate         Pool
	statementTimeout time.Duration
	lockTimeout      time.Duration
}

func (pool *localTimeoutPool) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := pool.delegate.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
SELECT set_config('statement_timeout', $1, true),
	       set_config('lock_timeout', $2, true)`, timeoutParameter(pool.statementTimeout), timeoutParameter(pool.lockTimeout)); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), localTimeoutRollbackLimit)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
		return nil, ErrInvalidRegistry
	}
	return tx, nil
}

func (pool *localTimeoutPool) Close() { pool.delegate.Close() }

// OpenPGXPool converts an already allowlisted secret into one bounded pool.
// Parse and construction failures intentionally collapse to ErrInvalidRegistry
// so a DSN can never enter an error returned to callers.
func OpenPGXPool(ctx context.Context, dsn string, limits PoolLimits) (Pool, error) {
	if ctx == nil || limits.MaxOpenConns < 1 || limits.MaxOpenConns > 100 ||
		limits.MaxIdleConns < 0 || limits.MaxIdleConns > limits.MaxOpenConns ||
		!validLocalTimeouts(limits.StatementTimeout, limits.LockTimeout) {
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
	if limits.StatementTimeout == 0 {
		return pool, nil
	}
	return &localTimeoutPool{
		delegate: pool, statementTimeout: limits.StatementTimeout, lockTimeout: limits.LockTimeout,
	}, nil
}

func validLocalTimeouts(statementTimeout, lockTimeout time.Duration) bool {
	if statementTimeout == 0 && lockTimeout == 0 {
		return true
	}
	return statementTimeout > 0 && statementTimeout <= maxLocalDatabaseTimeout &&
		lockTimeout > 0 && lockTimeout <= statementTimeout
}

func timeoutParameter(timeout time.Duration) string {
	milliseconds := timeout.Milliseconds()
	if milliseconds < 1 {
		milliseconds = 1
	}
	return fmt.Sprintf("%dms", milliseconds)
}
