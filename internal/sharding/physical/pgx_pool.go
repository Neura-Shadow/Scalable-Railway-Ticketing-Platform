package physical

import (
	"context"
	"fmt"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
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

type pgxPoolAdapter struct{ pool *pgxpool.Pool }

func (pool *pgxPoolAdapter) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	return pool.pool.BeginTx(ctx, options)
}

func (pool *pgxPoolAdapter) Query(ctx context.Context, query string, arguments ...any) (pgx.Rows, error) {
	return pool.pool.Query(ctx, query, arguments...)
}

func (pool *pgxPoolAdapter) QueryRow(ctx context.Context, query string, arguments ...any) pgx.Row {
	return pool.pool.QueryRow(ctx, query, arguments...)
}

func (pool *pgxPoolAdapter) Close() { pool.pool.Close() }

func (pool *pgxPoolAdapter) PoolSnapshot() PoolSnapshot {
	stat := pool.pool.Stat()
	return PoolSnapshot{
		TotalConnections:      stat.TotalConns(),
		AcquiredConnections:   stat.AcquiredConns(),
		IdleConnections:       stat.IdleConns(),
		MaxConnections:        stat.MaxConns(),
		AcquireCount:          stat.AcquireCount(),
		AcquireDuration:       stat.AcquireDuration(),
		EmptyAcquireCount:     stat.EmptyAcquireCount(),
		CancelledAcquireCount: stat.CanceledAcquireCount(),
	}
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

func (pool *localTimeoutPool) PoolSnapshot() PoolSnapshot {
	if source, ok := pool.delegate.(poolSnapshotSource); ok {
		return source.PoolSnapshot()
	}
	return PoolSnapshot{}
}

// OpenPGXPool converts an already allowlisted secret into one bounded pool.
// Parse and construction failures intentionally collapse to ErrInvalidRegistry
// so a DSN can never enter an error returned to callers.
func OpenPGXPool(ctx context.Context, dsn string, limits PoolLimits) (Pool, error) {
	return openPGXPool(ctx, dsn, limits, nil)
}

// RegionalPGXPoolFactory binds every physical connection to the same bounded
// process authority identity used by the control pool.
func RegionalPGXPoolFactory(session postgresx.RegionalSession) PoolFactory {
	return func(ctx context.Context, dsn string, limits PoolLimits) (Pool, error) {
		return openPGXPool(ctx, dsn, limits, &session)
	}
}

func openPGXPool(ctx context.Context, dsn string, limits PoolLimits, session *postgresx.RegionalSession) (Pool, error) {
	if ctx == nil || limits.MaxOpenConns < 1 || limits.MaxOpenConns > 100 ||
		limits.MaxIdleConns < 0 || limits.MaxIdleConns > limits.MaxOpenConns ||
		!validLocalTimeouts(limits.StatementTimeout, limits.LockTimeout) {
		return nil, ErrInvalidRegistry
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, ErrInvalidRegistry
	}
	if session != nil {
		if err := postgresx.ApplyRegionalSession(config.ConnConfig, *session); err != nil {
			return nil, ErrInvalidRegistry
		}
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
	adapter := &pgxPoolAdapter{pool: pool}
	if limits.StatementTimeout == 0 {
		return adapter, nil
	}
	return &localTimeoutPool{
		delegate: adapter, statementTimeout: limits.StatementTimeout, lockTimeout: limits.LockTimeout,
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
