package postgres_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	workerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const integrationDatabaseEnv = "M7_PAYMENT_INTEGRATION_DATABASE_URL"

func TestM7PaymentWorkerRunOnceV11Lanes(t *testing.T) {
	dsn := os.Getenv(integrationDatabaseEnv)
	reservationID, err := uuid.Parse(os.Getenv("M7_PAYMENT_INTEGRATION_RESERVATION_ID"))
	if dsn == "" || err != nil {
		t.Skip("focused Milestone 7 PostgreSQL fixture is not enabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := openRegionalPool(t, ctx, dsn)
	defer pool.Close()
	deployment := integrationDeployment(t)
	diagnostics := &boundedPostgresRecorder{}
	store, err := workerpostgres.NewWithRegionalAuthority(diagnosticDB{DB: pool, recorder: diagnostics}, deployment)
	if err != nil {
		t.Fatal("bounded setup failure: operation=new_store lane=claim_operations sqlstate=unknown constraint=unknown")
	}
	fixture := prepareIntegrationFixture(t, ctx, pool, reservationID)
	providerFake := &integrationProvider{paymentID: fixture.providerPaymentID, amount: fixture.amount, currency: fixture.currency}
	shardFake := &integrationShard{}

	t.Run("operation claim", func(t *testing.T) {
		diagnostics.reset()
		result, runErr := newIntegrationWorker(t, operationOnlyStore{Store: store}, providerFake, shardFake, "m7-probe-operation").RunOnce(ctx)
		if runErr != nil || result.OperationsClaimed != 1 || result.OperationsDone != 1 || result.Failures != 0 || len(result.FailureSummaries) != 0 {
			t.Fatalf("operation lane result={claimed:%d done:%d failures:%d %s}", result.OperationsClaimed, result.OperationsDone, result.Failures, boundedLaneDiagnostic(result, diagnostics))
		}
		assertScalar(t, ctx, pool, "operation_state", "claim_operations", "SELECT state FROM public.payment_operations WHERE operation_id=$1", fixture.operationID, "succeeded")
	})

	t.Run("webhook claim", func(t *testing.T) {
		providerFake.setStatus(provider.StatusAuthorized)
		seedVerifiedWebhook(t, ctx, pool, fixture)
		diagnostics.reset()
		result, runErr := newIntegrationWorker(t, webhookOnlyStore{Store: store}, providerFake, shardFake, "m7-probe-webhook").RunOnce(ctx)
		if runErr != nil || result.WebhooksClaimed != 1 || result.WebhooksDone != 1 || result.Failures != 0 || len(result.FailureSummaries) != 0 {
			t.Fatalf("webhook lane result={claimed:%d done:%d failures:%d %s}", result.WebhooksClaimed, result.WebhooksDone, result.Failures, boundedLaneDiagnostic(result, diagnostics))
		}
		assertScalar(t, ctx, pool, "webhook_state", "claim_webhooks", "SELECT state FROM public.payment_webhook_inbox WHERE inbox_id=$1", fixture.webhookID, "processed")
	})

	providerFake.setStatus(provider.StatusCaptured)
	diagnostics.reset()
	prepStore := operationOnlyStore{Store: store}
	prep, prepErr := newIntegrationWorker(t, prepStore, providerFake, shardFake, "m7-probe-capture-prep").RunOnce(ctx)
	if prepErr != nil || prep.OperationsClaimed != 1 || prep.OperationsDone != 1 || prep.Failures != 0 {
		t.Fatalf("capture prerequisite result={claimed:%d done:%d failures:%d %s}", prep.OperationsClaimed, prep.OperationsDone, prep.Failures, boundedLaneDiagnostic(prep, diagnostics))
	}

	t.Run("action claim", func(t *testing.T) {
		diagnostics.reset()
		result, runErr := newIntegrationWorker(t, actionOnlyStore{Store: store}, providerFake, shardFake, "m7-probe-action").RunOnce(ctx)
		if runErr != nil || result.ActionsClaimed != 1 || result.ActionsDone != 1 || result.Failures != 0 || len(result.FailureSummaries) != 0 || shardFake.issueCalls != 1 {
			t.Fatalf("action lane result={claimed:%d done:%d failures:%d shard_calls:%d %s}", result.ActionsClaimed, result.ActionsDone, result.Failures, shardFake.issueCalls, boundedLaneDiagnostic(result, diagnostics))
		}
		assertScalar(t, ctx, pool, "action_state", "claim_actions", `SELECT state FROM public.payment_saga_actions WHERE saga_id=$1 AND action_type='issue_tickets'`, fixture.sagaID, "succeeded")
	})
}

type integrationFixture struct {
	intentID, sagaID, operationID, webhookID uuid.UUID
	providerPaymentID                        string
	amount                                   int64
	currency                                 string
}

func prepareIntegrationFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reservationID uuid.UUID) integrationFixture {
	t.Helper()
	var fixture integrationFixture
	err := pool.QueryRow(ctx, `
SELECT intent.payment_intent_id,saga.saga_id,operation.operation_id,
       COALESCE(intent.provider_payment_id,''),intent.amount_minor,intent.currency
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga USING(payment_intent_id)
JOIN public.payment_operations AS operation USING(payment_intent_id)
WHERE intent.reservation_id=$1 AND operation.operation_type='create_checkout'
  AND operation.state='pending' AND saga.state='reservation_secured'
ORDER BY intent.created_at LIMIT 1`, reservationID).Scan(
		&fixture.intentID, &fixture.sagaID, &fixture.operationID,
		&fixture.providerPaymentID, &fixture.amount, &fixture.currency,
	)
	if err == nil {
		fixture.webhookID = uuid.New()
		return fixture
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(boundedDatabaseDiagnostic("select_fixture", "claim_operations", err))
	}

	var trainRunID, ownerID uuid.UUID
	err = pool.QueryRow(ctx, `
SELECT train_run_id,owner_user_id,amount_minor,currency
FROM public.payment_intents
WHERE reservation_id=$1 AND state IN ('completed','voided','refunded','cancelled','failed','expired')
ORDER BY created_at DESC LIMIT 1`, reservationID).Scan(&trainRunID, &ownerID, &fixture.amount, &fixture.currency)
	if err != nil {
		t.Fatal(boundedDatabaseDiagnostic("select_terminal_fixture", "claim_operations", err))
	}
	fixture.intentID, fixture.sagaID, fixture.operationID, fixture.webhookID = uuid.New(), uuid.New(), uuid.New(), uuid.New()
	fixture.providerPaymentID = ""
	keyHash := sha256.Sum256([]byte("m7-integration-key:" + fixture.intentID.String()))
	fingerprint := sha256.Sum256([]byte("m7-integration-request:" + fixture.intentID.String()))
	operationHash := sha256.Sum256([]byte("m7-integration-operation:" + fixture.intentID.String()))
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatal(boundedDatabaseDiagnostic("begin_fixture", "claim_operations", err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	statements := []struct{ operation, statement string }{
		{"lock_authority", `SELECT public.lock_regional_write_authority()`},
		{"insert_intent", `INSERT INTO public.payment_intents(payment_intent_id,reservation_id,train_run_id,owner_user_id,provider,amount_minor,currency,state,idempotency_key_hash,request_fingerprint) VALUES($1,$2,$3,$4,'sandbox',$5,$6,'reservation_securing',$7,$8)`},
		{"insert_saga", `INSERT INTO public.payment_sagas(saga_id,payment_intent_id,reservation_id,current_step,state) VALUES($1,$2,$3,'secure_reservation','created')`},
		{"advance_intent", `UPDATE public.payment_intents SET state='checkout_pending' WHERE payment_intent_id=$1`},
		{"advance_saga", `UPDATE public.payment_sagas SET state='reservation_secured',current_step='create_checkout' WHERE saga_id=$1`},
		{"insert_operation", `INSERT INTO public.payment_operations(operation_id,payment_intent_id,provider,operation_type,provider_idempotency_key_hash,amount_minor,currency) VALUES($1,$2,'sandbox','create_checkout',$3,$4,$5)`},
	}
	for _, item := range statements {
		operation, statement := item.operation, item.statement
		var execErr error
		switch operation {
		case "lock_authority":
			_, execErr = tx.Exec(ctx, statement)
		case "insert_intent":
			_, execErr = tx.Exec(ctx, statement, fixture.intentID, reservationID, trainRunID, ownerID, fixture.amount, fixture.currency, keyHash[:], fingerprint[:])
		case "insert_saga":
			_, execErr = tx.Exec(ctx, statement, fixture.sagaID, fixture.intentID, reservationID)
		case "advance_intent":
			_, execErr = tx.Exec(ctx, statement, fixture.intentID)
		case "advance_saga":
			_, execErr = tx.Exec(ctx, statement, fixture.sagaID)
		case "insert_operation":
			_, execErr = tx.Exec(ctx, statement, fixture.operationID, fixture.intentID, operationHash[:], fixture.amount, fixture.currency)
		}
		if execErr != nil {
			t.Fatal(boundedDatabaseDiagnostic(operation, "claim_operations", execErr))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(boundedDatabaseDiagnostic("commit_fixture", "claim_operations", err))
	}
	return fixture
}

func seedVerifiedWebhook(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixture integrationFixture) {
	t.Helper()
	var paymentID string
	if err := pool.QueryRow(ctx, "SELECT provider_payment_id FROM public.payment_intents WHERE payment_intent_id=$1", fixture.intentID).Scan(&paymentID); err != nil {
		t.Fatal(boundedDatabaseDiagnostic("read_provider_identity", "claim_webhooks", err))
	}
	fixture.providerPaymentID = paymentID
	payloadHash := sha256.Sum256([]byte("m7-integration-webhook:" + fixture.webhookID.String()))
	_, err := pool.Exec(ctx, `
INSERT INTO public.payment_webhook_inbox(
 inbox_id,provider,provider_event_id,event_type,provider_payment_id,payload_hash,
 verified_key_id,event_created_at,signature_verified_at,body_size_bytes
) VALUES($1,'sandbox',$2,'payment.authorized',$3,$4,'m7-test-key',clock_timestamp(),clock_timestamp(),128)`,
		fixture.webhookID, "evt-"+strings.ReplaceAll(fixture.webhookID.String(), "-", ""), paymentID, payloadHash[:])
	if err != nil {
		t.Fatal(boundedDatabaseDiagnostic("seed_verified_webhook", "claim_webhooks", err))
	}
}

func openRegionalPool(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("bounded setup failure: operation=parse_pool lane=claim_operations sqlstate=unknown constraint=unknown")
	}
	config.ConnConfig.RuntimeParams["railway.deployment_region"] = "region-a"
	config.ConnConfig.RuntimeParams["railway.deployment_role"] = "active"
	config.ConnConfig.RuntimeParams["railway.region_epoch"] = "1"
	config.ConnConfig.RuntimeParams["railway.regional_writes_enabled"] = "true"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(boundedDatabaseDiagnostic("open_pool", "claim_operations", err))
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(boundedDatabaseDiagnostic("ping_pool", "claim_operations", err))
	}
	return pool
}

func integrationDeployment(t *testing.T) authority.Deployment {
	t.Helper()
	region, err := authority.ParseRegion("region-a")
	if err != nil {
		t.Fatal("invalid test region")
	}
	epoch, err := authority.NewEpoch(1)
	if err != nil {
		t.Fatal("invalid test epoch")
	}
	deployment, err := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	if err != nil {
		t.Fatal("invalid test deployment")
	}
	return deployment
}

func newIntegrationWorker(t *testing.T, store worker.Store, client provider.Client, shards worker.ShardGateway, id string) *worker.Worker {
	t.Helper()
	instance, err := worker.New(store, worker.Providers{"sandbox": client}, shards, nil, worker.Config{
		WorkerID: id, BatchSize: 10, MaxAttempts: 5, LeaseTTL: 30 * time.Second,
		RetryBase: time.Second, RetryMax: time.Minute, Interval: time.Second,
		Now: func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		t.Fatal("invalid integration worker")
	}
	return instance
}

type operationOnlyStore struct{ worker.Store }

func (operationOnlyStore) ClaimWebhooks(context.Context, worker.ClaimOptions) ([]worker.WebhookClaim, error) {
	return nil, nil
}

func (operationOnlyStore) ClaimActions(context.Context, worker.ClaimOptions) ([]worker.ActionClaim, error) {
	return nil, nil
}

type webhookOnlyStore struct{ worker.Store }

func (webhookOnlyStore) ClaimOperations(context.Context, worker.ClaimOptions) ([]worker.OperationClaim, error) {
	return nil, nil
}

func (webhookOnlyStore) ClaimActions(context.Context, worker.ClaimOptions) ([]worker.ActionClaim, error) {
	return nil, nil
}

type actionOnlyStore struct{ worker.Store }

func (actionOnlyStore) ClaimOperations(context.Context, worker.ClaimOptions) ([]worker.OperationClaim, error) {
	return nil, nil
}

func (actionOnlyStore) ClaimWebhooks(context.Context, worker.ClaimOptions) ([]worker.WebhookClaim, error) {
	return nil, nil
}

// diagnosticDB records only allowlisted PostgreSQL metadata before the
// production store intentionally collapses database errors. It is test-only
// and never retains SQL text, arguments, connection strings, or raw errors.
type diagnosticDB struct {
	workerpostgres.DB
	recorder *boundedPostgresRecorder
}

func (db diagnosticDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.DB.BeginTx(ctx, options)
	db.recorder.record(err)
	if err != nil {
		return nil, err
	}
	return diagnosticTx{Tx: tx, recorder: db.recorder}, nil
}

type diagnosticTx struct {
	pgx.Tx
	recorder *boundedPostgresRecorder
}

func (tx diagnosticTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	tag, err := tx.Tx.Exec(ctx, sql, arguments...)
	tx.recorder.record(err)
	return tag, err
}

func (tx diagnosticTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := tx.Tx.Query(ctx, sql, args...)
	tx.recorder.record(err)
	if err == nil {
		rows = diagnosticRows{Rows: rows, recorder: tx.recorder}
	}
	return rows, err
}

func (tx diagnosticTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return diagnosticRow{Row: tx.Tx.QueryRow(ctx, sql, args...), recorder: tx.recorder}
}

func (tx diagnosticTx) Commit(ctx context.Context) error {
	err := tx.Tx.Commit(ctx)
	tx.recorder.record(err)
	return err
}

func (tx diagnosticTx) Rollback(ctx context.Context) error {
	err := tx.Tx.Rollback(ctx)
	tx.recorder.record(err)
	return err
}

type diagnosticRow struct {
	pgx.Row
	recorder *boundedPostgresRecorder
}

type diagnosticRows struct {
	pgx.Rows
	recorder *boundedPostgresRecorder
}

func (rows diagnosticRows) Scan(dest ...any) error {
	err := rows.Rows.Scan(dest...)
	rows.recorder.record(err)
	return err
}

func (rows diagnosticRows) Err() error {
	err := rows.Rows.Err()
	rows.recorder.record(err)
	return err
}

func (row diagnosticRow) Scan(dest ...any) error {
	err := row.Row.Scan(dest...)
	row.recorder.record(err)
	return err
}

type boundedPostgresRecorder struct {
	mu         sync.Mutex
	sqlstate   string
	constraint string
}

func (recorder *boundedPostgresRecorder) reset() {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.sqlstate, recorder.constraint = "unknown", "unknown"
}

func (recorder *boundedPostgresRecorder) record(err error) {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return
	}
	code, constraint := boundedPostgresFields(postgresError)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.sqlstate, recorder.constraint = code, constraint
}

func (recorder *boundedPostgresRecorder) snapshot() (string, string) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.sqlstate == "" {
		return "unknown", "unknown"
	}
	return recorder.sqlstate, recorder.constraint
}

func boundedLaneDiagnostic(result worker.Result, recorder *boundedPostgresRecorder) string {
	parts := make([]string, 0, len(result.FailureSummaries))
	for _, summary := range result.FailureSummaries {
		parts = append(parts, fmt.Sprintf("%s:%s", summary.Lane, summary.Reason))
	}
	if len(parts) == 0 {
		parts = append(parts, "none")
	}
	code, constraint := recorder.snapshot()
	return fmt.Sprintf("failure_summaries=%s sqlstate=%s constraint=%s", strings.Join(parts, ","), code, constraint)
}

type integrationProvider struct {
	mu        sync.Mutex
	paymentID string
	status    provider.Status
	amount    int64
	currency  string
}

func (fake *integrationProvider) setStatus(status provider.Status) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.status = status
}

func (fake *integrationProvider) CreateCheckout(_ context.Context, request provider.CreateCheckoutRequest) (provider.Checkout, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.paymentID == "" {
		fake.paymentID = "pay-" + strings.ReplaceAll(request.PaymentIntentID, "-", "")
		fake.amount, fake.currency, fake.status = request.AmountMinor, request.Currency, provider.StatusRequiresCustomerAction
	}
	return provider.Checkout{ProviderPaymentID: fake.paymentID, HostedReference: "hosted-" + fake.paymentID, SyntheticToken: "synthetic-token", Status: fake.status, AmountMinor: fake.amount, Currency: fake.currency}, nil
}

func (fake *integrationProvider) GetPaymentStatus(context.Context, string) (provider.Payment, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	payment := provider.Payment{ProviderPaymentID: fake.paymentID, Status: fake.status, AmountMinor: fake.amount, Currency: fake.currency, ProviderUpdatedAt: time.Now().UTC()}
	if fake.status == provider.StatusCaptured {
		payment.CapturedMinor = fake.amount
	}
	return payment, nil
}

func (*integrationProvider) Authorize(context.Context, provider.AuthorizeRequest) (provider.OperationResult, error) {
	return provider.OperationResult{}, errors.New("unexpected authorize call")
}

func (fake *integrationProvider) Capture(context.Context, provider.CaptureRequest) (provider.OperationResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.status = provider.StatusCaptured
	return provider.OperationResult{ProviderPaymentID: fake.paymentID, ProviderOperationID: "capture-" + fake.paymentID, Status: fake.status, AmountMinor: fake.amount, Currency: fake.currency}, nil
}

func (*integrationProvider) Void(context.Context, provider.VoidRequest) (provider.OperationResult, error) {
	return provider.OperationResult{}, errors.New("unexpected void call")
}

func (*integrationProvider) Refund(context.Context, provider.RefundRequest) (provider.OperationResult, error) {
	return provider.OperationResult{}, errors.New("unexpected refund call")
}

func (*integrationProvider) VerifyWebhook(context.Context, provider.WebhookHeaders, []byte) (provider.WebhookEvent, error) {
	return provider.WebhookEvent{}, errors.New("unexpected webhook verification call")
}

type integrationShard struct{ issueCalls int }

func (fake *integrationShard) IssueTickets(_ context.Context, command shard.IssueTicketsCommand) (shard.IssueTicketsReceipt, error) {
	fake.issueCalls++
	now := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	ticketIDs := []uuid.UUID{
		uuid.NewSHA1(command.ReservationID, []byte("ticket:0")),
		uuid.NewSHA1(command.ReservationID, []byte("ticket:1")),
	}
	ticketCodes := []string{
		"m7_probe_ticket_" + strings.ReplaceAll(ticketIDs[0].String(), "-", ""),
		"m7_probe_ticket_" + strings.ReplaceAll(ticketIDs[1].String(), "-", ""),
	}
	return shard.IssueTicketsReceipt{
		CommandID: command.CommandID, IssuanceID: command.IssuanceID,
		PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID,
		TicketOrderID: uuid.NewSHA1(command.ReservationID, []byte("ticket-order")), TicketIDs: ticketIDs,
		TicketCodes: ticketCodes, AmountMinor: command.AmountMinor,
		Currency: command.Currency, OrderCreatedAt: now, IssuedAt: now,
	}, nil
}

func (*integrationShard) MarkRefundPending(context.Context, shard.MarkRefundPendingCommand) (shard.MarkRefundPendingReceipt, error) {
	return shard.MarkRefundPendingReceipt{}, errors.New("unexpected mark-refund call")
}

func (*integrationShard) CancelVoidedReservation(context.Context, shard.CancelVoidedReservationCommand) (shard.CancelVoidedReservationReceipt, error) {
	return shard.CancelVoidedReservationReceipt{}, errors.New("unexpected cancel call")
}

func (*integrationShard) ApplyRefundCompensation(context.Context, shard.ApplyRefundCompensationCommand) (shard.ApplyRefundCompensationReceipt, error) {
	return shard.ApplyRefundCompensationReceipt{}, errors.New("unexpected compensation call")
}

func assertScalar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operation, lane, query string, argument any, want string) {
	t.Helper()
	var got string
	if err := pool.QueryRow(ctx, query, argument).Scan(&got); err != nil {
		t.Fatal(boundedDatabaseDiagnostic(operation, lane, err))
	}
	if got != want {
		t.Fatalf("bounded invariant failure: operation=%s lane=%s state=%s", operation, lane, got)
	}
}

func boundedDatabaseDiagnostic(operation, lane string, err error) string {
	code, constraint := "unknown", "unknown"
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		code, constraint = boundedPostgresFields(postgresError)
	}
	return fmt.Sprintf("bounded database failure: operation=%s lane=%s sqlstate=%s constraint=%s", operation, lane, code, constraint)
}

func boundedPostgresFields(postgresError *pgconn.PgError) (string, string) {
	code, constraint := "unknown", "unknown"
	if postgresError == nil {
		return code, constraint
	}
	switch postgresError.Code {
	case "42501", "23514", "23503", "40001", "40P01", "55000", "57014":
		code = postgresError.Code
	}
	switch postgresError.ConstraintName {
	case "payment_intents_completion_check", "payment_operations_lease_check", "payment_operations_completion_check",
		"payment_webhook_inbox_lease_check", "payment_webhook_inbox_completion_check", "payment_saga_actions_lease_check",
		"payment_saga_actions_completion_check", "payment_sagas_completion_check":
		constraint = postgresError.ConstraintName
	}
	return code, constraint
}
