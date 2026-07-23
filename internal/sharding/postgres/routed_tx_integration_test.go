package postgres

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFixedSearchPathIsTransactionLocalIntegration(t *testing.T) {
	pool := openRoutingIntegrationPool(t)

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal("acquire PostgreSQL integration connection")
	}
	defer conn.Release()
	baseline := currentSearchPath(t, conn)

	t.Run("rollback", func(t *testing.T) {
		tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal("begin rollback transaction")
		}
		assertTransactionPath(t, tx, sharding.ShardZero, "pg_catalog, booking_shard_0, public, pg_temp")
		if err := tx.Rollback(context.Background()); err != nil {
			t.Fatal("rollback routed transaction")
		}
		if got := currentSearchPath(t, conn); got != baseline {
			t.Fatalf("search_path after rollback = %q, want baseline %q", got, baseline)
		}
	})

	t.Run("commit", func(t *testing.T) {
		tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal("begin commit transaction")
		}
		assertTransactionPath(t, tx, sharding.ShardOne, "pg_catalog, booking_shard_1, public, pg_temp")
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatal("commit routed transaction")
		}
		if got := currentSearchPath(t, conn); got != baseline {
			t.Fatalf("search_path after commit = %q, want baseline %q", got, baseline)
		}
	})

	t.Run("canceled request cleanup", func(t *testing.T) {
		tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal("begin cancellation transaction")
		}
		assertTransactionPath(t, tx, sharding.ShardZero, "pg_catalog, booking_shard_0, public, pg_temp")
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := tx.Exec(canceled, "SELECT 1"); err == nil {
			t.Fatal("canceled transaction command unexpectedly succeeded")
		}
		if err := tx.Rollback(context.Background()); err != nil {
			t.Fatal("rollback canceled routed transaction")
		}
		if got := currentSearchPath(t, conn); got != baseline {
			t.Fatalf("search_path after canceled request = %q, want baseline %q", got, baseline)
		}
	})
}

func TestFixedSearchPathIsolatedAcrossConcurrentTransactionsIntegration(t *testing.T) {
	pool := openRoutingIntegrationPool(t)
	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal("acquire first PostgreSQL integration connection")
	}
	defer first.Release()
	second, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal("acquire second PostgreSQL integration connection")
	}
	defer second.Release()
	firstBaseline := currentSearchPath(t, first)
	secondBaseline := currentSearchPath(t, second)

	firstTx, err := first.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatal("begin first concurrent transaction")
	}
	defer func() { _ = firstTx.Rollback(context.Background()) }()
	secondTx, err := second.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatal("begin second concurrent transaction")
	}
	defer func() { _ = secondTx.Rollback(context.Background()) }()

	assertTransactionPath(t, firstTx, sharding.ShardZero, "pg_catalog, booking_shard_0, public, pg_temp")
	assertTransactionPath(t, secondTx, sharding.ShardOne, "pg_catalog, booking_shard_1, public, pg_temp")
	if got := currentSearchPath(t, firstTx); got != "pg_catalog, booking_shard_0, public, pg_temp" {
		t.Fatalf("first concurrent transaction search_path = %q", got)
	}
	if got := currentSearchPath(t, secondTx); got != "pg_catalog, booking_shard_1, public, pg_temp" {
		t.Fatalf("second concurrent transaction search_path = %q", got)
	}
	if err := firstTx.Rollback(context.Background()); err != nil {
		t.Fatal("rollback first concurrent transaction")
	}
	if err := secondTx.Rollback(context.Background()); err != nil {
		t.Fatal("rollback second concurrent transaction")
	}
	if got := currentSearchPath(t, first); got != firstBaseline {
		t.Fatalf("first connection search_path after rollback = %q, want %q", got, firstBaseline)
	}
	if got := currentSearchPath(t, second); got != secondBaseline {
		t.Fatalf("second connection search_path after rollback = %q, want %q", got, secondBaseline)
	}
}

func openRoutingIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal("parse PostgreSQL integration configuration")
	}
	if config.MaxConns < 2 {
		config.MaxConns = 2
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal("open PostgreSQL integration pool")
	}
	t.Cleanup(pool.Close)
	return pool
}

func assertTransactionPath(t *testing.T, tx pgx.Tx, shardID sharding.ShardID, want string) {
	t.Helper()
	statement, ok := fixedSearchPath(shardID)
	if !ok {
		t.Fatal("fixed shard did not map to a search path")
	}
	if _, err := tx.Exec(context.Background(), statement); err != nil {
		t.Fatal("set transaction-local search path")
	}
	var got string
	if err := tx.QueryRow(context.Background(), "SHOW search_path").Scan(&got); err != nil {
		t.Fatal("read transaction-local search path")
	}
	if got != want {
		t.Fatalf("transaction search_path = %q, want %q", got, want)
	}
}

type searchPathReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func currentSearchPath(t *testing.T, reader searchPathReader) string {
	t.Helper()
	var path string
	if err := reader.QueryRow(context.Background(), "SHOW search_path").Scan(&path); err != nil {
		t.Fatal("read connection search path")
	}
	return path
}
