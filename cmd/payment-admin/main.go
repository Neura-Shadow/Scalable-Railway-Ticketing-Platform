package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	providerhttp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/httpclient"
	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	reconcilepostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile/postgres"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	paymentshardpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard/postgres"
	paymentticketcodes "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ticketcodes"
	paymentworkerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker/postgres"
	platformconfig "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	defaultLimit   = 100
	maximumLimit   = 1000
	defaultTimeout = 30 * time.Second
	maximumTimeout = 5 * time.Minute
)

var (
	errArguments             = errors.New("invalid arguments")
	errConfirmation          = errors.New("explicit confirmation required")
	errRuntimeWiring         = errors.New("payment administration runtime wiring unavailable")
	errSafeReplayUnavailable = errors.New("safe recorded command replay unavailable")
)

type request struct {
	Command         string
	PaymentIntentID uuid.UUID
	SagaID          uuid.UUID
	WebhookID       uuid.UUID
	OperationID     uuid.UUID
	Limit           int
	Timeout         time.Duration
	DryRun          bool
	Repair          bool
	Confirm         bool
}

// item is deliberately bounded and excludes free-form provider responses,
// customer data, database topology, and secret-bearing metadata.
type item struct {
	ResourceID uuid.UUID `json:"resource_id,omitempty"`
	Kind       string    `json:"kind"`
	State      string    `json:"state,omitempty"`
	Code       string    `json:"code,omitempty"`
}

type outcome struct {
	Items     []item `json:"items,omitempty"`
	Count     int    `json:"count"`
	Truncated bool   `json:"truncated"`
}

type envelope struct {
	Command  string  `json:"command"`
	Status   string  `json:"status"`
	ReadOnly bool    `json:"read_only"`
	Result   outcome `json:"result"`
	Error    string  `json:"error,omitempty"`
}

type backend interface {
	Execute(context.Context, request) (outcome, error)
}

type backendFactory func(context.Context, func(string) (string, bool), request) (backend, func(), error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, openBackend))
}

func run(parent context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer, factory backendFactory) int {
	if parent == nil || lookup == nil || stdout == nil || stderr == nil || factory == nil || len(args) == 0 {
		fmt.Fprintln(stderr, "payment-admin: invalid invocation")
		return 2
	}
	req, err := parse(args)
	if err != nil {
		_ = writeJSON(stdout, envelope{Command: safeName(args[0]), Status: "rejected", ReadOnly: true, Error: errorCode(err)})
		fmt.Fprintln(stderr, "payment-admin: arguments rejected")
		return 2
	}
	ctx, cancel := context.WithTimeout(parent, req.Timeout)
	defer cancel()
	service, closeService, err := factory(ctx, lookup, req)
	if err != nil {
		_ = writeJSON(stdout, envelope{Command: req.Command, Status: "failed", ReadOnly: requestReadOnly(req), Error: "startup_failed"})
		fmt.Fprintln(stderr, "payment-admin: startup failed")
		return 1
	}
	defer closeService()
	result, err := service.Execute(ctx, req)
	result = sanitizeOutcome(result)
	if len(result.Items) > req.Limit {
		result.Items = result.Items[:req.Limit]
		result.Truncated = true
	}
	if result.Count < len(result.Items) {
		result.Count = len(result.Items)
	}
	status := "completed"
	if req.DryRun {
		status = "dry-run"
	}
	if err != nil {
		status = "failed"
	}
	if writeErr := writeJSON(stdout, envelope{Command: req.Command, Status: status, ReadOnly: requestReadOnly(req), Result: result, Error: errorCode(err)}); writeErr != nil {
		fmt.Fprintln(stderr, "payment-admin: output failed")
		return 1
	}
	if err != nil {
		fmt.Fprintln(stderr, "payment-admin: command failed")
		return 1
	}
	return 0
}

func parse(args []string) (request, error) {
	name := safeName(args[0])
	if !knownCommand(name) {
		return request{}, errArguments
	}
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	intentText := flags.String("payment-intent-id", "", "canonical payment intent UUID")
	sagaText := flags.String("saga-id", "", "canonical payment saga UUID")
	webhookText := flags.String("webhook-id", "", "canonical webhook inbox UUID")
	operationText := flags.String("operation-id", "", "canonical recorded operation UUID")
	limit := flags.Int("limit", defaultLimit, "bounded output limit")
	timeout := flags.Duration("timeout", defaultTimeout, "bounded operation timeout")
	dryRun := flags.Bool("dry-run", false, "preview without mutation")
	repair := flags.Bool("repair", false, "replay a safe recorded command")
	confirm := flags.Bool("confirm", false, "explicitly confirm mutation")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || *limit < 1 || *limit > maximumLimit || *timeout <= 0 || *timeout > maximumTimeout {
		return request{}, errArguments
	}
	req := request{Command: name, Limit: *limit, Timeout: *timeout, DryRun: *dryRun, Repair: *repair, Confirm: *confirm}
	var err error
	if requiresIntent(name) {
		req.PaymentIntentID, err = parseUUID(*intentText)
		if err != nil {
			return request{}, errArguments
		}
	}
	if name == "inspect-saga" || name == "retry-saga" {
		req.SagaID, err = parseUUID(*sagaText)
		if err != nil {
			return request{}, errArguments
		}
	}
	if name == "inspect-webhook" {
		req.WebhookID, err = parseUUID(*webhookText)
		if err != nil {
			return request{}, errArguments
		}
	}
	if name == "request-void" || name == "request-refund" {
		req.OperationID, err = parseUUID(*operationText)
		if err != nil {
			return request{}, errArguments
		}
	}
	if name == "reconcile-intent" {
		if req.Repair {
			if req.DryRun == req.Confirm {
				return request{}, errConfirmation
			}
		} else if req.Confirm || req.DryRun {
			return request{}, errArguments
		}
	} else if isMutation(name) {
		if req.Repair || req.DryRun == req.Confirm {
			return request{}, errConfirmation
		}
	} else if req.Repair || req.Confirm || req.DryRun {
		return request{}, errArguments
	}
	return req, nil
}

func knownCommand(name string) bool {
	switch name {
	case "inspect-intent", "inspect-saga", "inspect-provider-status", "inspect-webhook",
		"retry-saga", "reconcile-intent", "mark-manual-review", "resume-ticket-issuance",
		"request-void", "request-refund", "inspect-financial-operations", "inspect-ticket-issuance",
		"backfill-ticket-codes":
		return true
	default:
		return false
	}
}

func requiresIntent(name string) bool {
	switch name {
	case "inspect-intent", "inspect-provider-status", "reconcile-intent", "mark-manual-review",
		"resume-ticket-issuance", "request-void", "request-refund",
		"inspect-financial-operations", "inspect-ticket-issuance":
		return true
	default:
		return false
	}
}

func isMutation(name string) bool {
	switch name {
	case "retry-saga", "mark-manual-review", "resume-ticket-issuance", "request-void", "request-refund", "backfill-ticket-codes":
		return true
	default:
		return false
	}
}

func requestReadOnly(req request) bool {
	return !isMutation(req.Command) && !(req.Command == "reconcile-intent" && req.Repair) || req.DryRun
}

func parseUUID(value string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(value)
	id, err := uuid.Parse(trimmed)
	if err != nil || id == uuid.Nil || id.String() != strings.ToLower(trimmed) {
		return uuid.Nil, errArguments
	}
	return id, nil
}

func safeName(value string) string {
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > 64 {
		return "invalid"
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && character != '-' {
			return "invalid"
		}
	}
	return value
}

func openBackend(ctx context.Context, lookup func(string) (string, bool), req request) (backend, func(), error) {
	cfg, err := platformconfig.LoadFromFor(lookup, platformconfig.ProcessPaymentReconciler)
	if err != nil || cfg.BookingShardMode != platformconfig.BookingShardModePhysical {
		return nil, func() {}, errRuntimeWiring
	}
	control, err := postgresx.NewRegionalBoundedPool(ctx, cfg.DatabaseURL, cfg.ControlDatabaseMaxOpenConns, postgresx.RegionalSession{
		Region: string(cfg.DeploymentRegion), Role: string(cfg.DeploymentRole),
		Epoch: cfg.RegionEpoch, WritesEnabled: cfg.RegionalWritesEnabled,
	})
	if err != nil {
		return nil, func() {}, errRuntimeWiring
	}
	cleanup := []func(){control.Close}
	fail := func() (backend, func(), error) {
		for index := len(cleanup) - 1; index >= 0; index-- {
			cleanup[index]()
		}
		return nil, func() {}, errRuntimeWiring
	}
	if err := requireOperatorRole(ctx, control); err != nil {
		return fail()
	}
	providerClient, err := providerhttp.New(providerhttp.Config{
		BaseURL: cfg.PaymentProviderBaseURL, APIKey: cfg.PaymentProviderAPIKey,
		ConnectTimeout: cfg.PaymentProviderConnectTimeout, RequestTimeout: cfg.PaymentProviderRequestTimeout,
		MaxResponseBytes: int64(cfg.PaymentProviderMaxResponseBytes), Now: time.Now,
	})
	if err != nil {
		return fail()
	}
	cleanup = append(cleanup, providerClient.CloseIdleConnections)
	registry, err := newAdminPhysicalRegistry(ctx, cfg)
	if err != nil {
		return fail()
	}
	cleanup = append(cleanup, registry.Close)
	router, err := shardphysical.NewCatalogRouter(control, registry, cfg.BookingRouteCacheTTL)
	if err != nil {
		return fail()
	}
	ticketBackfill, err := paymentticketcodes.NewBackfiller(control, router)
	if err != nil {
		return fail()
	}
	store, err := reconcilepostgres.New(control, router)
	if err != nil {
		return fail()
	}
	var repairer *reconcilepostgres.Repairer
	if (isMutation(req.Command) || req.Repair) && req.Confirm && !req.DryRun {
		deployment, err := adminRegionalDeployment(cfg)
		if err != nil {
			return fail()
		}
		directory, err := paymentshardpostgres.NewDirectory(control)
		if err != nil {
			return fail()
		}
		shardStore, err := paymentshardpostgres.NewStore(router, paymentshardpostgres.WithRegionalAuthority(deployment))
		if err != nil {
			return fail()
		}
		claimStore, err := paymentworkerpostgres.NewWithRegionalAuthority(control, deployment)
		if err != nil {
			return fail()
		}
		gateway, err := paymentshard.NewGateway(directory, shardStore, paymentshard.WithTicketCodeClaimer(claimStore))
		if err != nil {
			return fail()
		}
		repairer, err = reconcilepostgres.NewRepairer(control, gateway, store)
		if err != nil {
			return fail()
		}
	}
	reconciler, err := paymentreconcile.New(store, adminProviderRegistry{"sandbox": providerClient}, repairer, paymentreconcile.Config{
		BatchSize: defaultLimit, StaleAfter: cfg.PaymentProcessingGrace,
		ReviewDue: cfg.PaymentManualReviewAfter, Now: time.Now,
	})
	if err != nil {
		return fail()
	}
	closeAll := func() {
		for index := len(cleanup) - 1; index >= 0; index-- {
			cleanup[index]()
		}
	}
	var operator adminOperator
	if repairer != nil {
		operator = repairer
	}
	return &runtimeBackend{store: store, reconciler: reconciler, provider: providerClient, operator: operator, ticketBackfill: ticketBackfill}, closeAll, nil
}

func adminRegionalDeployment(cfg platformconfig.Config) (authority.Deployment, error) {
	region, err := authority.ParseRegion(string(cfg.DeploymentRegion))
	if err != nil || cfg.RegionEpoch <= 0 {
		return authority.Deployment{}, authority.ErrInvalidDeployment
	}
	epoch, err := authority.NewEpoch(uint64(cfg.RegionEpoch))
	if err != nil {
		return authority.Deployment{}, err
	}
	return authority.NewDeployment(region, authority.Role(cfg.DeploymentRole), epoch, cfg.RegionalWritesEnabled)
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func requireOperatorRole(ctx context.Context, db queryRower) error {
	if ctx == nil || db == nil {
		return errRuntimeWiring
	}
	var allowed bool
	err := db.QueryRow(ctx, `
SELECT role.rolsuper
    OR role.rolname IN ('operator','admin')
    OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles AS permitted
        WHERE permitted.rolname IN ('operator','admin')
          AND pg_has_role(role.oid,permitted.oid,'member')
    )
FROM pg_catalog.pg_roles AS role
WHERE role.rolname=current_user`).Scan(&allowed)
	if err != nil || !allowed {
		return errRuntimeWiring
	}
	return nil
}

type runtimeBackend struct {
	store          adminStore
	reconciler     adminReconciler
	provider       paymentreconcile.StatusQuerier
	operator       adminOperator
	ticketBackfill ticketCodeBackfiller
}

type adminStore interface {
	LoadControlSnapshot(context.Context, uuid.UUID) (paymentreconcile.ControlSnapshot, error)
	LoadShardSnapshot(context.Context, uuid.UUID) (paymentreconcile.ShardSnapshot, error)
}

type adminReconciler interface {
	InspectPayment(context.Context, uuid.UUID) (paymentreconcile.Report, error)
	ReconcilePayment(context.Context, uuid.UUID) (paymentreconcile.Result, error)
	RepairPayment(context.Context, uuid.UUID) (paymentreconcile.Result, error)
}

type adminOperator interface {
	RetrySaga(context.Context, uuid.UUID) error
	ResumeTicketIssuance(context.Context, uuid.UUID) error
	RetryProviderOperation(context.Context, uuid.UUID, uuid.UUID, string) error
	MarkManualReview(context.Context, uuid.UUID) error
}

type ticketCodeBackfiller interface {
	Inspect(context.Context, int) (paymentticketcodes.Result, error)
	Backfill(context.Context, int) (paymentticketcodes.Result, error)
}

func (b *runtimeBackend) Execute(ctx context.Context, req request) (outcome, error) {
	if b == nil || b.store == nil || b.reconciler == nil || b.provider == nil ||
		(req.Command == "backfill-ticket-codes" && b.ticketBackfill == nil) {
		return outcome{}, errRuntimeWiring
	}
	if (isMutation(req.Command) || req.Repair) && !req.DryRun && !req.Confirm {
		return outcome{}, errConfirmation
	}
	if req.DryRun {
		if req.Command == "backfill-ticket-codes" {
			result, err := b.ticketBackfill.Inspect(ctx, req.Limit)
			return ticketBackfillOutcome(result), err
		}
		report, err := b.reconciler.InspectPayment(ctx, req.PaymentIntentID)
		if req.Command == "retry-saga" {
			return outcome{Items: []item{{ResourceID: req.SagaID, Kind: "payment_saga", State: "recorded_replay_preview"}}, Count: 1}, nil
		}
		if err != nil {
			return outcome{}, err
		}
		return reconciliationOutcome(paymentreconcile.Result{MismatchCount: len(report.Findings), Reports: []paymentreconcile.Report{report}}, req.Limit), nil
	}
	switch req.Command {
	case "inspect-intent":
		return b.inspectIntent(ctx, req.PaymentIntentID, req.Limit)
	case "inspect-provider-status":
		return b.inspectProvider(ctx, req.PaymentIntentID)
	case "reconcile-intent":
		if req.Repair {
			result, err := b.reconciler.RepairPayment(ctx, req.PaymentIntentID)
			return reconciliationOutcome(result, req.Limit), err
		}
		result, err := b.reconciler.ReconcilePayment(ctx, req.PaymentIntentID)
		return reconciliationOutcome(result, req.Limit), err
	case "retry-saga":
		if b.operator == nil {
			return outcome{}, errSafeReplayUnavailable
		}
		err := b.operator.RetrySaga(ctx, req.SagaID)
		return mutationOutcome(req.SagaID, "payment_saga_replay", err)
	case "resume-ticket-issuance":
		if b.operator == nil {
			return outcome{}, errSafeReplayUnavailable
		}
		err := b.operator.ResumeTicketIssuance(ctx, req.PaymentIntentID)
		return mutationOutcome(req.PaymentIntentID, "ticket_issuance_replay", err)
	case "request-void":
		if b.operator == nil {
			return outcome{}, errSafeReplayUnavailable
		}
		err := b.operator.RetryProviderOperation(ctx, req.PaymentIntentID, req.OperationID, "void")
		return mutationOutcome(req.OperationID, "payment_operation_void", err)
	case "request-refund":
		if b.operator == nil {
			return outcome{}, errSafeReplayUnavailable
		}
		err := b.operator.RetryProviderOperation(ctx, req.PaymentIntentID, req.OperationID, "refund")
		return mutationOutcome(req.OperationID, "payment_operation_refund", err)
	case "mark-manual-review":
		if b.operator == nil {
			return outcome{}, errSafeReplayUnavailable
		}
		err := b.operator.MarkManualReview(ctx, req.PaymentIntentID)
		return mutationOutcome(req.PaymentIntentID, "manual_review", err)
	case "inspect-financial-operations":
		return b.inspectFinancial(ctx, req.PaymentIntentID, req.Limit)
	case "inspect-ticket-issuance":
		return b.inspectIssuance(ctx, req.PaymentIntentID)
	case "backfill-ticket-codes":
		result, err := b.ticketBackfill.Backfill(ctx, req.Limit)
		return ticketBackfillOutcome(result), err
	default:
		return outcome{}, errRuntimeWiring
	}
}

func ticketBackfillOutcome(result paymentticketcodes.Result) outcome {
	state := "pending"
	if result.Ready {
		state = "ready"
	}
	count := result.Claimed
	if count == 0 {
		count = result.Missing
	}
	return outcome{Items: []item{{Kind: "ticket_code_directory", State: state}}, Count: count}
}

func mutationOutcome(id uuid.UUID, kind string, err error) (outcome, error) {
	if err != nil {
		return outcome{}, err
	}
	return outcome{Items: []item{{ResourceID: id, Kind: kind, State: "scheduled"}}, Count: 1}, nil
}

func (b *runtimeBackend) inspectIntent(ctx context.Context, id uuid.UUID, limit int) (outcome, error) {
	snapshot, err := b.store.LoadControlSnapshot(ctx, id)
	if err != nil {
		return outcome{}, err
	}
	report, err := b.reconciler.InspectPayment(ctx, id)
	if err != nil {
		return outcome{}, err
	}
	result := outcome{Items: []item{{ResourceID: id, Kind: "payment_intent", State: snapshot.Intent.State}, {ResourceID: snapshot.Saga.ID, Kind: "payment_saga", State: snapshot.Saga.State}}}
	for _, finding := range report.Findings {
		if len(result.Items) >= limit {
			result.Truncated = true
			break
		}
		result.Items = append(result.Items, item{ResourceID: id, Kind: "reconciliation_finding", Code: finding.Code})
	}
	result.Count = len(result.Items)
	return result, nil
}

func (b *runtimeBackend) inspectProvider(ctx context.Context, id uuid.UUID) (outcome, error) {
	snapshot, err := b.store.LoadControlSnapshot(ctx, id)
	if err != nil {
		return outcome{}, err
	}
	if snapshot.Intent.ProviderPaymentID == "" {
		return outcome{Items: []item{{ResourceID: id, Kind: "provider_status", State: "not_created"}}, Count: 1}, nil
	}
	status, err := b.provider.GetPaymentStatus(ctx, snapshot.Intent.ProviderPaymentID)
	if err != nil {
		return outcome{}, err
	}
	return outcome{Items: []item{{ResourceID: id, Kind: "provider_status", State: string(status.Status)}}, Count: 1}, nil
}

func (b *runtimeBackend) inspectFinancial(ctx context.Context, id uuid.UUID, limit int) (outcome, error) {
	snapshot, err := b.store.LoadControlSnapshot(ctx, id)
	if err != nil {
		return outcome{}, err
	}
	result := outcome{Count: len(snapshot.Operations)}
	for _, operation := range snapshot.Operations {
		if len(result.Items) == limit {
			result.Truncated = len(snapshot.Operations) > limit
			break
		}
		result.Items = append(result.Items, item{ResourceID: operation.ID, Kind: "payment_operation_" + operation.Type, State: operation.State})
	}
	return result, nil
}

func (b *runtimeBackend) inspectIssuance(ctx context.Context, id uuid.UUID) (outcome, error) {
	snapshot, err := b.store.LoadShardSnapshot(ctx, id)
	if err != nil {
		return outcome{}, err
	}
	state := "missing"
	if snapshot.TicketOrderFound {
		state = snapshot.TicketOrderState
	}
	code := "issuance_receipt_missing"
	if snapshot.IssuanceReceiptFound {
		code = "issuance_receipt_present"
	}
	return outcome{Items: []item{{ResourceID: snapshot.TicketOrderID, Kind: "ticket_issuance", State: state, Code: code}}, Count: 1}, nil
}

func reconciliationOutcome(result paymentreconcile.Result, limit int) outcome {
	value := outcome{Count: result.MismatchCount, Truncated: result.Truncated}
	for _, report := range result.Reports {
		for _, finding := range report.Findings {
			if len(value.Items) == limit {
				value.Truncated = true
				return value
			}
			value.Items = append(value.Items, item{ResourceID: report.PaymentIntentID, Kind: "reconciliation_finding", Code: finding.Code})
		}
	}
	return value
}

type adminProviderRegistry map[string]paymentreconcile.StatusQuerier

func (registry adminProviderRegistry) Provider(name string) (paymentreconcile.StatusQuerier, bool) {
	client, ok := registry[name]
	return client, ok
}

func newAdminPhysicalRegistry(ctx context.Context, cfg platformconfig.Config) (*shardphysical.Registry, error) {
	connections := make(map[string]shardphysical.ConnectionConfig, len(cfg.PhysicalShardConnections))
	for reference, dsn := range cfg.PhysicalShardConnections {
		shardID, err := sharding.ParseShardID(reference)
		if err != nil {
			return nil, errRuntimeWiring
		}
		connections[reference] = shardphysical.ConnectionConfig{ShardID: shardID, DSN: dsn}
	}
	return shardphysical.NewRegistry(ctx, shardphysical.RegistryConfig{
		Connections: connections, MaxCount: cfg.PhysicalShardMaxCount,
		Limits: shardphysical.PoolLimits{
			MaxOpenConns: cfg.PhysicalShardMaxOpenConns, MaxIdleConns: cfg.PhysicalShardMaxIdleConns,
			MaxLifetime: cfg.PhysicalShardConnMaxLifetime, MaxIdleTime: cfg.PhysicalShardConnMaxIdleTime,
			ConnectTimeout: cfg.PhysicalShardConnectTimeout, StatementTimeout: cfg.PhysicalShardQueryTimeout,
			LockTimeout: cfg.PhysicalShardQueryTimeout,
		},
	}, shardphysical.RegionalPGXPoolFactory(postgresx.RegionalSession{
		Region: string(cfg.DeploymentRegion), Role: string(cfg.DeploymentRole),
		Epoch: cfg.RegionEpoch, WritesEnabled: cfg.RegionalWritesEnabled,
	}))
}

func writeJSON(writer io.Writer, value envelope) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(value)
}

func sanitizeOutcome(value outcome) outcome {
	for index := range value.Items {
		value.Items[index].Kind = boundedToken(value.Items[index].Kind, "resource")
		value.Items[index].State = boundedOptionalToken(value.Items[index].State)
		value.Items[index].Code = boundedOptionalToken(value.Items[index].Code)
	}
	return value
}

func boundedToken(value, fallback string) string {
	if bounded := boundedOptionalToken(value); bounded != "" {
		return bounded
	}
	return fallback
}

func boundedOptionalToken(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) == 0 || len(value) > 64 {
		return ""
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' && character != '-' && character != '.' {
			return ""
		}
	}
	return value
}

func errorCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errArguments):
		return "invalid_arguments"
	case errors.Is(err, errConfirmation):
		return "confirmation_required"
	case errors.Is(err, errSafeReplayUnavailable):
		return "safe_replay_unavailable"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "operation_failed"
	}
}
