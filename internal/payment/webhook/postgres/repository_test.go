package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestWebhookConflictQuarantinesEveryClaimableState(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"received", "processing", "failed_retryable"} {
		if !webhookConflictQuarantinable(state) {
			t.Fatalf("state %q must be quarantinable", state)
		}
	}
	for _, state := range []string{"processed", "ignored", "failed_permanent", "security_conflict"} {
		if webhookConflictQuarantinable(state) {
			t.Fatalf("terminal state %q must remain immutable", state)
		}
	}
}

func TestStoreVerifiedLocksRegionalAuthorityBeforeInboxMutation(t *testing.T) {
	t.Parallel()

	tx := &webhookTx{rows: []pgx.Row{
		webhookRow{values: []any{"region-a", int64(7), "active", true}},
		webhookRow{values: []any{true}},
	}}
	db := &webhookDB{tx: tx}
	region, _ := authority.ParseRegion("region-a")
	epoch, _ := authority.NewEpoch(7)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	repository, err := NewRepository(db, WithRegionalAuthority(deployment))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	result, err := repository.StoreVerified(context.Background(), webhook.Record{
		ID: uuid.New(), Provider: "stripe", ProviderEventID: "evt_1",
		ProviderAccountID: "acct_contract", ProviderEnvironment: "test",
		EventType: "payment_intent.succeeded", VerifiedKeyID: "current",
		EventCreatedAt: now, SignatureVerifiedAt: now, ReceivedAt: now,
	})
	if err != nil {
		t.Fatalf("StoreVerified() error = %v", err)
	}
	if result != webhook.StoreAccepted {
		t.Fatalf("StoreVerified() result = %q", result)
	}
	if len(tx.queries) != 2 || !strings.Contains(tx.queries[0], "regional_write_authority") ||
		!strings.Contains(tx.queries[1], "payment_webhook_inbox") ||
		!strings.Contains(tx.queries[1], "provider_account_id") ||
		!strings.Contains(tx.queries[1], "provider_environment") {
		t.Fatalf("query order = %v", tx.queries)
	}
	if tx.commits != 1 || db.options.AccessMode != pgx.ReadWrite {
		t.Fatalf("commits/options = %d/%+v", tx.commits, db.options)
	}
}

func TestChangedPayloadConflictFailureRollsBackWithoutAcknowledgement(t *testing.T) {
	t.Parallel()
	canonical := make([]byte, 32)
	tx := &webhookTx{
		rows: []pgx.Row{
			webhookRow{values: []any{"region-a", int64(7), "active", true}},
			webhookRow{err: pgx.ErrNoRows},
			webhookRow{values: []any{canonical, "received"}},
		},
		execErr: errors.New("conflict insert unavailable"),
	}
	region, _ := authority.ParseRegion("region-a")
	epoch, _ := authority.NewEpoch(7)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	repository, err := NewRepository(&webhookDB{tx: tx}, WithRegionalAuthority(deployment))
	if err != nil {
		t.Fatal(err)
	}
	record := webhook.Record{
		ID: uuid.New(), Provider: "stripe", ProviderEventID: "evt_conflict_rollback",
		ProviderAccountID: "acct_contract", ProviderEnvironment: "test",
		EventType: "payment_intent.succeeded", VerifiedKeyID: "current",
		EventCreatedAt: time.Now().UTC(), SignatureVerifiedAt: time.Now().UTC(),
		ReceivedAt: time.Now().UTC(), BodySizeBytes: 128,
	}
	record.PayloadHash[0] = 1
	result, err := repository.StoreVerified(context.Background(), record)
	if result != "" || !errors.Is(err, webhook.ErrPersistence) {
		t.Fatalf("result=%q error=%v", result, err)
	}
	if tx.commits != 0 || tx.rollbacks != 1 {
		t.Fatalf("commits=%d rollbacks=%d", tx.commits, tx.rollbacks)
	}
}

func TestSynchronizeKeyringCommitsOnlyMetadataUnderRegionalAuthority(t *testing.T) {
	t.Parallel()
	tx := &webhookTx{
		rows:      []pgx.Row{webhookRow{values: []any{"region-a", int64(7), "active", true}}},
		queryRows: &webhookRows{},
	}
	region, _ := authority.ParseRegion("region-a")
	epoch, _ := authority.NewEpoch(7)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	repository, err := NewRepository(&webhookDB{tx: tx}, WithRegionalAuthority(deployment))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	proof := webhook.KeyProof("stripe", "acct_contract", "current", "whsec_current_contract")
	plan, err := repository.SynchronizeKeyring(context.Background(), "stripe", "acct_contract", webhook.DesiredKeyring{
		PrimaryKeyID: "current", AcceptedKeyIDs: []string{"current"},
		SecretProofs: map[string][32]byte{"current": proof}, Grace: time.Hour, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ByID["current"].State != webhook.KeyPrimary || tx.commits != 1 || len(tx.execs) != 6 {
		t.Fatalf("keyring plan/transaction = %+v commits=%d execs=%d", plan, tx.commits, len(tx.execs))
	}
	if !strings.Contains(tx.queries[0], "regional_write_authority") || !strings.Contains(tx.querySQL[0], "payment_webhook_key_versions") {
		t.Fatalf("authority/keyring query order = row:%v rows:%v", tx.queries, tx.querySQL)
	}
	joined := strings.Join(tx.execs, "\n")
	for _, expected := range []string{"pg_advisory_xact_lock", "payment_webhook_key_version_archive", "DELETE FROM public.payment_webhook_key_versions"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("keyring transaction omitted %q: %s", expected, joined)
		}
	}
}

type webhookDB struct {
	tx      pgx.Tx
	options pgx.TxOptions
}

func (db *webhookDB) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	db.options = options
	return db.tx, nil
}

type webhookTx struct {
	pgx.Tx
	rows      []pgx.Row
	queries   []string
	querySQL  []string
	queryRows pgx.Rows
	execs     []string
	execErr   error
	commits   int
	rollbacks int
}

func (tx *webhookTx) Query(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
	tx.querySQL = append(tx.querySQL, query)
	return tx.queryRows, nil
}

func (tx *webhookTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, query)
	if tx.execErr != nil {
		return pgconn.CommandTag{}, tx.execErr
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (tx *webhookTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.queries = append(tx.queries, query)
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}

func (tx *webhookTx) Commit(context.Context) error {
	tx.commits++
	return nil
}

func (tx *webhookTx) Rollback(context.Context) error {
	tx.rollbacks++
	return nil
}

type webhookRow struct {
	values []any
	err    error
}

type webhookRows struct {
	values [][]any
	index  int
	err    error
}

func (*webhookRows) Close()                                       {}
func (*webhookRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*webhookRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (rows *webhookRows) Next() bool                              { return rows.index < len(rows.values) }
func (rows *webhookRows) Scan(destinations ...any) error {
	values := rows.values[rows.index]
	rows.index++
	for index := range destinations {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(values[index]))
	}
	return nil
}
func (rows *webhookRows) Values() ([]any, error) {
	if rows.index == 0 || rows.index > len(rows.values) {
		return nil, rows.err
	}
	return rows.values[rows.index-1], nil
}
func (*webhookRows) RawValues() [][]byte { return nil }
func (rows *webhookRows) Err() error     { return rows.err }
func (*webhookRows) Conn() *pgx.Conn     { return nil }

func (row webhookRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range destinations {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
