package postgres

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/jackc/pgx/v5"
)

const restoreStartupTimeout = 15 * time.Second

// LocalRestoreVerifier boots only the allowlisted restored PGDATA with no TCP
// listener, connects over a private temporary Unix socket, and runs read-only
// application reconciliation. It never accepts a command or SQL fragment from
// the request.
type LocalRestoreVerifier struct{ postgresBinary string }

func NewLocalRestoreVerifier(binary string) (*LocalRestoreVerifier, error) {
	if binary == "" || strings.ContainsAny(binary, "\x00\r\n") ||
		strings.ToLower(filepath.Base(binary)) != "postgres" {
		return nil, ErrInvalidConfiguration
	}
	return &LocalRestoreVerifier{postgresBinary: filepath.Clean(binary)}, nil
}

func (verifier *LocalRestoreVerifier) Observe(
	ctx context.Context,
	request RestoreObservationRequest,
) (RestoreObservation, error) {
	if verifier == nil || ctx == nil || !identityPattern.MatchString(request.Target) ||
		request.PointInTime.IsZero() || request.PointInTime.Location() != time.UTC ||
		!filepath.IsAbs(request.DataPath) {
		return RestoreObservation{}, ErrInvalidConfiguration
	}
	databaseName, expectedSchema, err := restoreDatabaseIdentity(request.Database)
	if err != nil {
		return RestoreObservation{}, err
	}
	socketDir, err := os.MkdirTemp("", "railway-restore-validation-")
	if err != nil {
		return RestoreObservation{}, ErrInvalidOutput
	}
	defer os.RemoveAll(socketDir)

	command := exec.Command(
		verifier.postgresBinary,
		"-D", filepath.Clean(request.DataPath),
		"-c", "listen_addresses=",
		"-c", "unix_socket_directories="+socketDir,
		"-c", "default_transaction_read_only=on",
		"-c", "statement_timeout=15000",
		"-c", "max_connections=10",
	)
	command.Stdout = newBoundedWriter(64 << 10)
	command.Stderr = newBoundedWriter(64 << 10)
	if err := command.Start(); err != nil {
		return RestoreObservation{}, ErrCommandFailed
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	defer stopRestoredPostgres(command, done)

	startupCtx, cancel := context.WithTimeout(ctx, restoreStartupTimeout)
	defer cancel()
	config, err := pgx.ParseConfig("host=" + socketDir + " user=postgres dbname=" + databaseName + " sslmode=disable connect_timeout=1")
	if err != nil {
		return RestoreObservation{}, ErrInvalidConfiguration
	}
	var connection *pgx.Conn
	for connection == nil {
		select {
		case <-startupCtx.Done():
			return RestoreObservation{}, ErrCommandFailed
		case <-done:
			return RestoreObservation{}, ErrCommandFailed
		default:
		}
		connection, _ = pgx.ConnectConfig(startupCtx, config)
		if connection == nil {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-startupCtx.Done():
				timer.Stop()
				return RestoreObservation{}, ErrCommandFailed
			case <-timer.C:
			}
		}
	}
	defer connection.Close(context.Background())

	transaction, err := connection.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return RestoreObservation{}, ErrInvalidOutput
	}
	defer transaction.Rollback(context.Background())
	observation, err := observeRestoreFacts(ctx, transaction, request.Database, expectedSchema)
	if err != nil || transaction.Commit(ctx) != nil {
		return RestoreObservation{}, ErrInvalidOutput
	}
	return observation, nil
}

func stopRestoredPostgres(command *exec.Cmd, done <-chan error) {
	select {
	case <-done:
		return
	default:
	}
	_ = command.Process.Signal(syscall.SIGTERM)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = command.Process.Kill()
		<-done
	}
}

func restoreDatabaseIdentity(database recovery.Database) (string, int, error) {
	switch database {
	case recovery.DatabaseControl:
		return "railway_control", 11, nil
	case recovery.DatabaseShard0, recovery.DatabaseShard1:
		return "railway_booking", 3, nil
	default:
		return "", 0, ErrInvocationRejected
	}
}

type restoreFactQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func observeRestoreFacts(
	ctx context.Context,
	db restoreFactQuerier,
	database recovery.Database,
	expectedSchema int,
) (RestoreObservation, error) {
	var version int
	var dirty, inRecovery bool
	var timelineHex string
	if err := db.QueryRow(ctx, `
SELECT migrations.version,migrations.dirty,
       substring(pg_walfile_name(pg_current_wal_lsn()) from 1 for 8),
       pg_is_in_recovery()
FROM public.schema_migrations AS migrations
LIMIT 1`).Scan(&version, &dirty, &timelineHex, &inRecovery); err != nil || dirty || inRecovery ||
		version != expectedSchema || len(timelineHex) != 8 {
		return RestoreObservation{}, ErrInvalidOutput
	}
	timeline, err := strconv.ParseUint(timelineHex, 16, 32)
	if err != nil || timeline == 0 {
		return RestoreObservation{}, ErrInvalidOutput
	}
	facts := protection.RestoreFacts{SchemaCurrent: true}
	if database == recovery.DatabaseControl {
		facts.Payment = requiredTablesExist(ctx, db, []string{
			"public.payment_intents", "public.payment_sagas", "public.payment_operations",
		}) && scalarFact(ctx, db, `SELECT NOT EXISTS (
SELECT reservation_id FROM public.payment_intents
WHERE state NOT IN ('completed','voided','refunded','cancelled','failed','expired')
GROUP BY reservation_id HAVING count(*) > 1)`)
		facts.Ticket = requiredTablesExist(ctx, db, []string{
			"public.ticket_code_directory", "public.ticket_order_shard_locators", "public.ticket_shard_locators",
		})
		facts.Refund = requiredTablesExist(ctx, db, []string{
			"public.ticket_refund_requests", "public.ticket_refund_request_items", "public.ticket_refund_operations",
		}) && scalarFact(ctx, db, `SELECT NOT EXISTS (
SELECT 1 FROM public.ticket_refund_operations
WHERE refunded_total_minor IS NOT NULL
  AND (captured_total_minor IS NULL OR refunded_total_minor > captured_total_minor))`)
		facts.Ledger = requiredTablesExist(ctx, db, []string{
			"public.financial_ledger_transactions", "public.financial_ledger_postings", "public.financial_ledger_reversals",
		}) && scalarFact(ctx, db, `SELECT NOT EXISTS (
SELECT transaction_id FROM public.financial_ledger_postings
GROUP BY transaction_id
HAVING count(*) < 2
   OR count(DISTINCT currency) <> 1
   OR sum(amount_minor) FILTER (WHERE side='debit') IS DISTINCT FROM
      sum(amount_minor) FILTER (WHERE side='credit'))`)
		facts.Settlement = requiredTablesExist(ctx, db, []string{
			"public.provider_settlement_batches", "public.provider_settlement_lines",
			"public.provider_payouts", "public.settlement_reconciliation_runs",
		})
	} else {
		facts.Payment = requiredTablesExist(ctx, db, []string{
			"public.payment_command_receipts", "public.payment_refund_receipts", "public.payment_compensation_receipts",
		})
		facts.Ticket = requiredTablesExist(ctx, db, []string{
			"public.ticket_orders", "public.tickets", "public.ticket_issuance_receipts",
		})
		facts.Refund = requiredTablesExist(ctx, db, []string{
			"public.ticket_refund_prepare_receipts", "public.ticket_refund_compensation_receipts",
			"public.selected_ticket_refund_receipts",
		})
	}
	facts.Regional = requiredTablesExist(ctx, db, []string{"public.regional_write_authority"}) &&
		scalarFact(ctx, db, `SELECT count(*)=1 AND bool_and(epoch > 0 AND (NOT writes_enabled OR state='active'))
FROM public.regional_write_authority`)
	return RestoreObservation{SchemaVersion: version, Timeline: uint32(timeline), Facts: facts}, nil
}

func requiredTablesExist(ctx context.Context, db restoreFactQuerier, tables []string) bool {
	var count int
	if len(tables) == 0 || db.QueryRow(ctx, `
SELECT count(*)::integer
FROM unnest($1::text[]) AS required(name)
WHERE to_regclass(required.name) IS NOT NULL`, tables).Scan(&count) != nil {
		return false
	}
	return count == len(tables)
}

func scalarFact(ctx context.Context, db restoreFactQuerier, query string) bool {
	var valid bool
	return db.QueryRow(ctx, query).Scan(&valid) == nil && valid
}

var _ RestoreVerifier = (*LocalRestoreVerifier)(nil)
