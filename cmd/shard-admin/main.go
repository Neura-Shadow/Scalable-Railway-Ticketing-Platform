package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	controlpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control/postgres"
	"github.com/google/uuid"
)

const (
	defaultCommandTimeout = 30 * time.Second
	maxCommandTimeout     = 5 * time.Minute
	defaultListLimit      = 100
	maxAssignmentLimit    = 1000
	maxShardLimit         = 3
	defaultBatchSize      = 100
	maxBatchSize          = 10000
	defaultRowCap         = int64(10000)
	maxRowCap             = int64(100000)
	defaultRollbackWindow = 5 * time.Minute
	maxRollbackWindow     = 24 * time.Hour
)

var (
	errInvalidArguments           = errors.New("invalid arguments")
	errConfirmationRequired       = errors.New("explicit confirmation required")
	errCleanupExecutorUnavailable = errors.New("cleanup executor unavailable")
	errResourceNotFound           = errors.New("resource not found")
	errControlStateInvalid        = errors.New("control state invalid")
	errReconciliationMismatch     = errors.New("reconciliation mismatch")
	errReconciliationIncomplete   = errors.New("reconciliation incomplete")
)

type outputEnvelope struct {
	Command  string `json:"command"`
	Status   string `json:"status"`
	ReadOnly bool   `json:"read_only"`
	Result   any    `json:"result,omitempty"`
	Error    string `json:"error,omitempty"`
}

type assignmentListOptions struct {
	After *uuid.UUID
	Limit int
}

type planOptions struct {
	MigrationID    uuid.UUID
	TrainRunID     uuid.UUID
	TargetShard    sharding.ShardID
	RollbackWindow time.Duration
	Timeout        time.Duration
}

type copyOptions struct {
	MigrationID uuid.UUID
	BatchSize   int
	Timeout     time.Duration
}

type validationOptions struct {
	MigrationID uuid.UUID
	RowCap      int64
	Timeout     time.Duration
}

type cutoverOptions struct {
	MigrationID      uuid.UUID
	ValidationRowCap int64
	LocatorRowCap    int64
	Timeout          time.Duration
}

type rollbackOptions struct {
	MigrationID   uuid.UUID
	LocatorRowCap int64
	Timeout       time.Duration
}

type reconcileOptions struct {
	TrainRunID uuid.UUID
	RowCap     int64
}

type cleanupEligibility struct {
	Eligible   bool       `json:"eligible"`
	EligibleAt *time.Time `json:"eligible_at,omitempty"`
	Reason     string     `json:"reason"`
}

type adminBackend interface {
	Close()
	ListShards(context.Context, int) (any, error)
	ListAssignments(context.Context, assignmentListOptions) (any, error)
	InspectTrainRun(context.Context, uuid.UUID) (any, error)
	PreviewPlan(context.Context, planOptions) (any, error)
	Plan(context.Context, planOptions) (any, error)
	InspectMigration(context.Context, uuid.UUID) (any, error)
	CopyBatch(context.Context, copyOptions) (any, error)
	Validate(context.Context, validationOptions) (any, error)
	Cutover(context.Context, cutoverOptions) (any, error)
	Rollback(context.Context, rollbackOptions) (any, error)
	CleanupEligibility(context.Context, uuid.UUID) (cleanupEligibility, error)
	Reconcile(context.Context, reconcileOptions) (any, error)
	InspectHealth(context.Context) (any, error)
}

type backendFactory func(context.Context, string, postgresx.RegionalSession) (adminBackend, error)

type invocation struct {
	command        string
	readOnly       bool
	dryRun         bool
	confirm        bool
	timeout        time.Duration
	limit          int
	after          *uuid.UUID
	trainRunID     uuid.UUID
	migrationID    uuid.UUID
	targetShard    sharding.ShardID
	batchSize      int
	rowCap         int64
	locatorRowCap  int64
	rollbackWindow time.Duration
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

func run(
	parent context.Context,
	args []string,
	lookup func(string) (string, bool),
	stdout io.Writer,
	stderr io.Writer,
) int {
	return runWithFactory(parent, args, lookup, stdout, stderr, newPostgresBackend)
}

func runWithFactory(
	parent context.Context,
	args []string,
	lookup func(string) (string, bool),
	stdout io.Writer,
	stderr io.Writer,
	factory backendFactory,
) int {
	if parent == nil || lookup == nil || stdout == nil || stderr == nil || factory == nil {
		writeUsage(stderr)
		return 2
	}
	if len(args) == 0 {
		_ = writeEnvelope(stdout, outputEnvelope{
			Command: "shard-admin", Status: "rejected", ReadOnly: true, Error: "invalid_arguments",
		})
		writeUsage(stderr)
		return 2
	}

	request, err := parseInvocation(args)
	if err != nil {
		code := "invalid_arguments"
		if errors.Is(err, errConfirmationRequired) {
			code = "confirmation_required"
		}
		_ = writeEnvelope(stdout, outputEnvelope{
			Command: safeCommandName(args[0]), Status: "rejected", ReadOnly: true, Error: code,
		})
		fmt.Fprintln(stderr, "shard-admin arguments rejected")
		return 2
	}

	databaseURL, _ := lookup("DATABASE_URL")
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		_ = writeEnvelope(stdout, outputEnvelope{
			Command: request.command, Status: "rejected", ReadOnly: request.readOnly || request.dryRun,
			Error: "configuration_invalid",
		})
		fmt.Fprintln(stderr, "shard-admin configuration invalid")
		return 2
	}
	session, err := postgresx.ParseRegionalSession(
		environmentValue(lookup, "DEPLOYMENT_REGION"),
		environmentValue(lookup, "DEPLOYMENT_ROLE"),
		environmentValue(lookup, "REGION_EPOCH"),
		environmentValue(lookup, "REGIONAL_WRITES_ENABLED"),
	)
	if err != nil {
		_ = writeEnvelope(stdout, outputEnvelope{
			Command: request.command, Status: "rejected", ReadOnly: request.readOnly || request.dryRun,
			Error: "configuration_invalid",
		})
		fmt.Fprintln(stderr, "shard-admin configuration invalid")
		return 2
	}

	ctx, cancel := context.WithTimeout(parent, request.timeout)
	defer cancel()
	backend, err := factory(ctx, databaseURL, session)
	if err != nil {
		err = authoritativeContextError(ctx, err)
		_ = writeEnvelope(stdout, outputEnvelope{
			Command: request.command, Status: "failed", ReadOnly: request.readOnly || request.dryRun,
			Error: publicErrorCode(err),
		})
		fmt.Fprintln(stderr, "shard-admin command failed")
		return 1
	}
	defer backend.Close()

	result, executeErr := executeInvocation(ctx, backend, request)
	executeErr = authoritativeContextError(ctx, executeErr)
	status := "completed"
	if request.dryRun {
		status = "dry-run"
	}
	if errors.Is(executeErr, errCleanupExecutorUnavailable) {
		status = "blocked"
	} else if errors.Is(executeErr, errReconciliationIncomplete) {
		status = "partial"
	} else if errors.Is(executeErr, errReconciliationMismatch) {
		status = "violations"
	} else if executeErr != nil {
		status = "failed"
	}
	envelope := outputEnvelope{
		Command: request.command, Status: status, ReadOnly: request.readOnly || request.dryRun,
		Result: result,
	}
	if executeErr != nil {
		envelope.Error = publicErrorCode(executeErr)
	}
	if err := writeEnvelope(stdout, envelope); err != nil {
		fmt.Fprintln(stderr, "shard-admin result encoding failed")
		return 1
	}
	if executeErr != nil {
		fmt.Fprintln(stderr, "shard-admin command failed")
		return 1
	}
	return 0
}

func environmentValue(lookup func(string) (string, bool), name string) string {
	value, _ := lookup(name)
	return strings.TrimSpace(value)
}

func executeInvocation(ctx context.Context, backend adminBackend, request invocation) (any, error) {
	switch request.command {
	case "list-shards":
		return backend.ListShards(ctx, request.limit)
	case "list-assignments":
		return backend.ListAssignments(ctx, assignmentListOptions{After: request.after, Limit: request.limit})
	case "inspect-train-run":
		return backend.InspectTrainRun(ctx, request.trainRunID)
	case "plan-migration":
		options := planOptions{
			MigrationID: request.migrationID, TrainRunID: request.trainRunID,
			TargetShard: request.targetShard, RollbackWindow: request.rollbackWindow, Timeout: request.timeout,
		}
		if request.dryRun {
			return backend.PreviewPlan(ctx, options)
		}
		return backend.Plan(ctx, options)
	case "start-migration", "resume-migration":
		if request.dryRun {
			return backend.InspectMigration(ctx, request.migrationID)
		}
		return backend.CopyBatch(ctx, copyOptions{
			MigrationID: request.migrationID, BatchSize: request.batchSize, Timeout: request.timeout,
		})
	case "validate-migration":
		if request.dryRun {
			return backend.InspectMigration(ctx, request.migrationID)
		}
		return backend.Validate(ctx, validationOptions{
			MigrationID: request.migrationID, RowCap: request.rowCap, Timeout: request.timeout,
		})
	case "cutover":
		if request.dryRun {
			return backend.InspectMigration(ctx, request.migrationID)
		}
		return backend.Cutover(ctx, cutoverOptions{
			MigrationID: request.migrationID, ValidationRowCap: request.rowCap,
			LocatorRowCap: request.locatorRowCap, Timeout: request.timeout,
		})
	case "rollback":
		if request.dryRun {
			return backend.InspectMigration(ctx, request.migrationID)
		}
		return backend.Rollback(ctx, rollbackOptions{
			MigrationID: request.migrationID, LocatorRowCap: request.locatorRowCap, Timeout: request.timeout,
		})
	case "cleanup-source":
		result, err := backend.CleanupEligibility(ctx, request.migrationID)
		if err != nil {
			return result, err
		}
		if request.confirm {
			return result, errCleanupExecutorUnavailable
		}
		return result, nil
	case "reconcile":
		return backend.Reconcile(ctx, reconcileOptions{TrainRunID: request.trainRunID, RowCap: request.rowCap})
	case "inspect-health":
		return backend.InspectHealth(ctx)
	default:
		return nil, errInvalidArguments
	}
}

func parseInvocation(args []string) (invocation, error) {
	request := invocation{command: safeCommandName(args[0]), timeout: defaultCommandTimeout}
	var err error
	switch args[0] {
	case "list-shards":
		request.readOnly = true
		request.limit, request.timeout, err = parseListFlags(args[0], args[1:], maxShardLimit, maxShardLimit)
	case "list-assignments":
		request.readOnly = true
		request.limit, request.after, request.timeout, err = parseAssignmentListFlags(args[1:])
	case "inspect-train-run":
		request.readOnly = true
		request.trainRunID, request.timeout, err = parseIDCommand(args[0], "train-run-id", args[1:])
	case "plan-migration":
		request, err = parsePlanFlags(args[1:])
	case "start-migration", "resume-migration":
		request, err = parseCopyFlags(args[0], args[1:])
	case "validate-migration":
		request, err = parseValidationFlags(args[1:])
	case "cutover":
		request, err = parseCutoverFlags(args[1:])
	case "rollback":
		request, err = parseRollbackFlags(args[1:])
	case "cleanup-source":
		request, err = parseCleanupFlags(args[1:])
	case "reconcile":
		request, err = parseReconcileFlags(args[1:])
	case "inspect-health":
		request.readOnly = true
		request.timeout, err = parseTimeoutOnly(args[0], args[1:])
	default:
		return invocation{}, errInvalidArguments
	}
	if err != nil {
		return invocation{}, err
	}
	request.command = args[0]
	return request, nil
}

func parseListFlags(name string, args []string, defaultLimit, maxLimit int) (int, time.Duration, error) {
	flags := newFlagSet(name)
	limit := flags.Int("limit", defaultLimit, "bounded result limit")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *limit < 1 || *limit > maxLimit || !validTimeout(*timeout) {
		return 0, 0, errInvalidArguments
	}
	return *limit, *timeout, nil
}

func parseAssignmentListFlags(args []string) (int, *uuid.UUID, time.Duration, error) {
	flags := newFlagSet("list-assignments")
	limit := flags.Int("limit", defaultListLimit, "bounded result limit")
	afterText := flags.String("after-train-run-id", "", "opaque bounded resume key")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *limit < 1 || *limit > maxAssignmentLimit || !validTimeout(*timeout) {
		return 0, nil, 0, errInvalidArguments
	}
	var after *uuid.UUID
	if strings.TrimSpace(*afterText) != "" {
		parsed, err := canonicalUUID(*afterText)
		if err != nil {
			return 0, nil, 0, errInvalidArguments
		}
		after = &parsed
	}
	return *limit, after, *timeout, nil
}

func parseIDCommand(command, flagName string, args []string) (uuid.UUID, time.Duration, error) {
	flags := newFlagSet(command)
	idText := flags.String(flagName, "", "canonical UUID")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validTimeout(*timeout) {
		return uuid.Nil, 0, errInvalidArguments
	}
	id, err := canonicalUUID(*idText)
	if err != nil {
		return uuid.Nil, 0, errInvalidArguments
	}
	return id, *timeout, nil
}

func parsePlanFlags(args []string) (invocation, error) {
	flags := newFlagSet("plan-migration")
	trainRunText := flags.String("train-run-id", "", "canonical train-run UUID")
	migrationText := flags.String("migration-id", "", "canonical migration UUID; generated when omitted")
	targetText := flags.String("target-shard", "", "fixed logical target shard")
	rollbackWindow := flags.Duration("rollback-window", defaultRollbackWindow, "bounded retained-source window")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	dryRun := flags.Bool("dry-run", false, "inspect without mutation")
	confirm := flags.Bool("confirm", false, "explicitly confirm mutation")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validTimeout(*timeout) ||
		*rollbackWindow <= 0 || *rollbackWindow > maxRollbackWindow || *rollbackWindow%time.Second != 0 {
		return invocation{}, errInvalidArguments
	}
	trainRunID, err := canonicalUUID(*trainRunText)
	if err != nil {
		return invocation{}, errInvalidArguments
	}
	migrationID := uuid.New()
	if strings.TrimSpace(*migrationText) != "" {
		migrationID, err = canonicalUUID(*migrationText)
		if err != nil {
			return invocation{}, errInvalidArguments
		}
	}
	target, err := sharding.ParseShardID(strings.TrimSpace(*targetText))
	if err != nil {
		return invocation{}, errInvalidArguments
	}
	if err := validateExecutionMode(*dryRun, *confirm); err != nil {
		return invocation{}, err
	}
	return invocation{
		command: "plan-migration", dryRun: *dryRun, confirm: *confirm, timeout: *timeout,
		trainRunID: trainRunID, migrationID: migrationID, targetShard: target,
		rollbackWindow: *rollbackWindow,
	}, nil
}

func parseCopyFlags(command string, args []string) (invocation, error) {
	flags := newFlagSet(command)
	migrationText := flags.String("migration-id", "", "canonical migration UUID")
	batchSize := flags.Int("batch-size", defaultBatchSize, "bounded copy batch")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	dryRun := flags.Bool("dry-run", false, "inspect without mutation")
	confirm := flags.Bool("confirm", false, "explicitly confirm mutation")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *batchSize < 1 || *batchSize > maxBatchSize || !validTimeout(*timeout) {
		return invocation{}, errInvalidArguments
	}
	migrationID, err := canonicalUUID(*migrationText)
	if err != nil {
		return invocation{}, errInvalidArguments
	}
	if err := validateExecutionMode(*dryRun, *confirm); err != nil {
		return invocation{}, err
	}
	return invocation{
		command: command, dryRun: *dryRun, confirm: *confirm, timeout: *timeout,
		migrationID: migrationID, batchSize: *batchSize,
	}, nil
}

func parseValidationFlags(args []string) (invocation, error) {
	flags := newFlagSet("validate-migration")
	migrationText := flags.String("migration-id", "", "canonical migration UUID")
	rowCap := flags.Int64("row-cap", defaultRowCap, "bounded validation row cap")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	dryRun := flags.Bool("dry-run", false, "inspect without mutation")
	confirm := flags.Bool("confirm", false, "explicitly confirm validation update")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validRowCap(*rowCap) || !validTimeout(*timeout) {
		return invocation{}, errInvalidArguments
	}
	migrationID, err := canonicalUUID(*migrationText)
	if err != nil {
		return invocation{}, errInvalidArguments
	}
	if err := validateExecutionMode(*dryRun, *confirm); err != nil {
		return invocation{}, err
	}
	return invocation{
		command: "validate-migration", dryRun: *dryRun, confirm: *confirm, timeout: *timeout,
		migrationID: migrationID, rowCap: *rowCap,
	}, nil
}

func parseCutoverFlags(args []string) (invocation, error) {
	flags := newFlagSet("cutover")
	migrationText := flags.String("migration-id", "", "canonical migration UUID")
	rowCap := flags.Int64("row-cap", defaultRowCap, "bounded immediate validation row cap")
	locatorCap := flags.Int64("locator-row-cap", defaultRowCap, "bounded locator cutover cap")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	dryRun := flags.Bool("dry-run", false, "inspect without mutation")
	confirm := flags.Bool("confirm", false, "explicitly confirm cutover")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validRowCap(*rowCap) ||
		!validRowCap(*locatorCap) || !validTimeout(*timeout) {
		return invocation{}, errInvalidArguments
	}
	migrationID, err := canonicalUUID(*migrationText)
	if err != nil {
		return invocation{}, errInvalidArguments
	}
	if err := validateExecutionMode(*dryRun, *confirm); err != nil {
		return invocation{}, err
	}
	return invocation{
		command: "cutover", dryRun: *dryRun, confirm: *confirm, timeout: *timeout,
		migrationID: migrationID, rowCap: *rowCap, locatorRowCap: *locatorCap,
	}, nil
}

func parseRollbackFlags(args []string) (invocation, error) {
	flags := newFlagSet("rollback")
	migrationText := flags.String("migration-id", "", "canonical migration UUID")
	locatorCap := flags.Int64("locator-row-cap", defaultRowCap, "bounded locator rollback cap")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	dryRun := flags.Bool("dry-run", false, "inspect without mutation")
	confirm := flags.Bool("confirm", false, "explicitly confirm rollback")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validRowCap(*locatorCap) || !validTimeout(*timeout) {
		return invocation{}, errInvalidArguments
	}
	migrationID, err := canonicalUUID(*migrationText)
	if err != nil {
		return invocation{}, errInvalidArguments
	}
	if err := validateExecutionMode(*dryRun, *confirm); err != nil {
		return invocation{}, err
	}
	return invocation{
		command: "rollback", dryRun: *dryRun, confirm: *confirm, timeout: *timeout,
		migrationID: migrationID, locatorRowCap: *locatorCap,
	}, nil
}

func parseCleanupFlags(args []string) (invocation, error) {
	flags := newFlagSet("cleanup-source")
	migrationText := flags.String("migration-id", "", "canonical migration UUID")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	dryRun := flags.Bool("dry-run", false, "inspect cleanup eligibility only")
	confirm := flags.Bool("confirm", false, "confirm cleanup request")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validTimeout(*timeout) {
		return invocation{}, errInvalidArguments
	}
	migrationID, err := canonicalUUID(*migrationText)
	if err != nil {
		return invocation{}, errInvalidArguments
	}
	if err := validateExecutionMode(*dryRun, *confirm); err != nil {
		return invocation{}, err
	}
	return invocation{
		command: "cleanup-source", dryRun: *dryRun, confirm: *confirm,
		timeout: *timeout, migrationID: migrationID,
	}, nil
}

func parseReconcileFlags(args []string) (invocation, error) {
	flags := newFlagSet("reconcile")
	trainRunText := flags.String("train-run-id", "", "canonical train-run UUID")
	rowCap := flags.Int64("row-cap", defaultRowCap, "bounded locator row cap")
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validRowCap(*rowCap) || !validTimeout(*timeout) {
		return invocation{}, errInvalidArguments
	}
	trainRunID, err := canonicalUUID(*trainRunText)
	if err != nil {
		return invocation{}, errInvalidArguments
	}
	return invocation{
		command: "reconcile", readOnly: true, timeout: *timeout,
		trainRunID: trainRunID, rowCap: *rowCap,
	}, nil
}

func parseTimeoutOnly(command string, args []string) (time.Duration, error) {
	flags := newFlagSet(command)
	timeout := flags.Duration("timeout", defaultCommandTimeout, "maximum command duration")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validTimeout(*timeout) {
		return 0, errInvalidArguments
	}
	return *timeout, nil
}

func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func validateExecutionMode(dryRun, confirm bool) error {
	if dryRun && confirm {
		return errInvalidArguments
	}
	if !dryRun && !confirm {
		return errConfirmationRequired
	}
	return nil
}

func validTimeout(timeout time.Duration) bool {
	return timeout > 0 && timeout <= maxCommandTimeout
}

func validRowCap(rowCap int64) bool {
	return rowCap > 0 && rowCap <= maxRowCap
}

func canonicalUUID(raw string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(raw)
	parsed, err := uuid.Parse(trimmed)
	if err != nil || parsed == uuid.Nil || parsed.String() != trimmed {
		return uuid.Nil, errInvalidArguments
	}
	return parsed, nil
}

func safeCommandName(raw string) string {
	switch raw {
	case "list-shards", "list-assignments", "inspect-train-run", "plan-migration",
		"start-migration", "resume-migration", "validate-migration", "cutover",
		"rollback", "cleanup-source", "reconcile", "inspect-health":
		return raw
	default:
		return "shard-admin"
	}
}

func publicErrorCode(err error) string {
	switch {
	case errors.Is(err, errCleanupExecutorUnavailable):
		return "cleanup_executor_unavailable"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, control.ErrMigrationNotFound):
		return "not_found"
	case errors.Is(err, errResourceNotFound):
		return "not_found"
	case errors.Is(err, errReconciliationMismatch):
		return "reconciliation_mismatch"
	case errors.Is(err, errReconciliationIncomplete):
		return "reconciliation_incomplete"
	case errors.Is(err, errControlStateInvalid):
		return "invalid_control_state"
	case errors.Is(err, control.ErrTargetWriteEvidence):
		return "reverse_migration_required"
	case errors.Is(err, control.ErrRollbackWindowOpen), errors.Is(err, control.ErrRollbackWindowExpired):
		return "rollback_not_allowed"
	case errors.Is(err, control.ErrInvalidState), errors.Is(err, control.ErrPlanConflict),
		errors.Is(err, control.ErrActiveRouteMismatch), errors.Is(err, control.ErrWriteFenceMismatch),
		errors.Is(err, control.ErrCutoverValidationFailed):
		return "state_conflict"
	case errors.Is(err, control.ErrShardNotWritable):
		return "shard_not_writable"
	case errors.Is(err, control.ErrValidationRowCapExceeded), errors.Is(err, control.ErrLocatorRowCapExceeded):
		return "bounded_limit_exceeded"
	case errors.Is(err, control.ErrInvalidInput), errors.Is(err, control.ErrInvalidLimits),
		errors.Is(err, control.ErrInvalidRecord), errors.Is(err, control.ErrInvalidCopyResult),
		errors.Is(err, control.ErrInvalidValidation):
		return "invalid_control_state"
	case errors.Is(err, controlpostgres.ErrPersistence), errors.Is(err, controlpostgres.ErrInvalidRepository):
		return "dependency_unavailable"
	default:
		return "operation_failed"
	}
}

func authoritativeContextError(ctx context.Context, err error) error {
	if err == nil || ctx == nil {
		return err
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return err
}

func writeEnvelope(output io.Writer, envelope outputEnvelope) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(envelope)
}

func writeUsage(output io.Writer) {
	fmt.Fprintln(output, "usage: shard-admin {list-shards|list-assignments|inspect-train-run|plan-migration|start-migration|resume-migration|validate-migration|cutover|rollback|cleanup-source|reconcile|inspect-health} [options]")
}
