package physical

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type localTimeoutTestPool struct{ tx pgx.Tx }

func (pool *localTimeoutTestPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return pool.tx, nil
}

func (*localTimeoutTestPool) Close() {}

type localTimeoutTestTx struct {
	pgx.Tx
	query            string
	arguments        []any
	execErr          error
	rolledBack       bool
	rollbackDeadline bool
}

func (tx *localTimeoutTestTx) Exec(_ context.Context, query string, arguments ...any) (pgconn.CommandTag, error) {
	tx.query = query
	tx.arguments = append([]any(nil), arguments...)
	return pgconn.NewCommandTag("SELECT 1"), tx.execErr
}

func (tx *localTimeoutTestTx) Rollback(ctx context.Context) error {
	tx.rolledBack = true
	_, tx.rollbackDeadline = ctx.Deadline()
	return nil
}

func TestLocalTimeoutPoolAppliesTransactionLocalStatementAndLockTimeouts(t *testing.T) {
	t.Parallel()

	tx := &localTimeoutTestTx{}
	pool := &localTimeoutPool{
		delegate:         &localTimeoutTestPool{tx: tx},
		statementTimeout: 1250 * time.Millisecond,
		lockTimeout:      250 * time.Millisecond,
	}
	returned, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	if returned != tx {
		t.Fatal("BeginTx() did not return the bounded transaction")
	}
	if !strings.Contains(tx.query, "set_config('statement_timeout'") ||
		!strings.Contains(tx.query, "set_config('lock_timeout'") {
		t.Fatalf("local timeout query = %q", tx.query)
	}
	if len(tx.arguments) != 2 || tx.arguments[0] != "1250ms" || tx.arguments[1] != "250ms" {
		t.Fatalf("local timeout arguments = %#v", tx.arguments)
	}
}

func TestLocalTimeoutPoolRollsBackWhenTimeoutSetupFails(t *testing.T) {
	t.Parallel()

	tx := &localTimeoutTestTx{execErr: errors.New("set local timeout failed")}
	pool := &localTimeoutPool{
		delegate:         &localTimeoutTestPool{tx: tx},
		statementTimeout: time.Second,
		lockTimeout:      250 * time.Millisecond,
	}
	returned, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if !errors.Is(err, ErrInvalidRegistry) {
		t.Fatalf("BeginTx() error = %v, want ErrInvalidRegistry", err)
	}
	if returned != nil {
		t.Fatal("BeginTx() returned a transaction after timeout setup failed")
	}
	if !tx.rolledBack {
		t.Fatal("BeginTx() did not roll back after timeout setup failed")
	}
	if !tx.rollbackDeadline {
		t.Fatal("BeginTx() rollback cleanup was not bounded")
	}
}
