package physical

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type localTimeoutTestPool struct{ tx pgx.Tx }

type migrationQueryPool interface {
	Pool
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (pool *localTimeoutTestPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return pool.tx, nil
}

func (*localTimeoutTestPool) Close() {}

func TestOpenPGXPoolExposesMigrationQueryBoundary(t *testing.T) {
	pool, err := OpenPGXPool(context.Background(),
		"postgres://unused:unused@127.0.0.1:1/unused?sslmode=disable",
		PoolLimits{MaxOpenConns: 1, MaxIdleConns: 0, ConnectTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, ok := pool.(migrationQueryPool); !ok {
		t.Fatal("physical pgx pool does not expose the bounded migration query boundary")
	}
}

func TestRegionalPGXPoolFactoryInstallsAuthorityParameters(t *testing.T) {
	t.Parallel()
	factory := RegionalPGXPoolFactory(postgresx.RegionalSession{
		Region: "region-b", Role: "recovery", Epoch: 8, WritesEnabled: false,
	})
	pool, err := factory(context.Background(),
		"postgres://unused:unused@127.0.0.1:1/unused?sslmode=disable",
		PoolLimits{MaxOpenConns: 1, MaxIdleConns: 0, ConnectTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	adapter, ok := pool.(*pgxPoolAdapter)
	if !ok {
		t.Fatalf("regional pool type = %T", pool)
	}
	params := adapter.pool.Config().ConnConfig.RuntimeParams
	if params["railway.deployment_region"] != "region-b" || params["railway.deployment_role"] != "recovery" ||
		params["railway.region_epoch"] != "8" || params["railway.regional_writes_enabled"] != "false" {
		t.Fatalf("regional runtime parameters = %#v", params)
	}
}

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
