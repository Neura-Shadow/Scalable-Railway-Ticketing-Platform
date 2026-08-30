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

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	defaultTimeout                        = 30 * time.Second
	maxTimeout                            = 5 * time.Minute
	defaultLimit                          = 100
	maxLimit                              = 1000
	physicalMigrationValidationTableLimit = 64
)

var (
	errArguments                  = errors.New("invalid arguments")
	errConfirmation               = errors.New("explicit confirmation required")
	errRole                       = errors.New("database role is not operator or admin")
	errState                      = errors.New("command is not valid at the durable migration state")
	errUnavailable                = errors.New("operation unavailable")
	errUnsupportedMigrationSource = errors.New("legacy or logical migration source is not supported")
)

type request struct {
	Command            string
	MigrationID        uuid.UUID
	TrainRunID         uuid.UUID
	CommandID          uuid.UUID
	ReverseMigrationID uuid.UUID
	ShardID            string
	TargetShardID      string
	Generation         int64
	Limit              int
	Timeout            time.Duration
	DryRun             bool
	Confirm            bool
}

type envelope struct {
	Command  string `json:"command"`
	Status   string `json:"status"`
	ReadOnly bool   `json:"read_only"`
	Result   any    `json:"result,omitempty"`
	Error    string `json:"error,omitempty"`
}

type backend interface {
	Close()
	Execute(context.Context, request) (any, error)
}

type backendFactory func(context.Context, func(string) (string, bool)) (backend, error)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, openBackend))
}

func run(parent context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer, factory backendFactory) int {
	if parent == nil || lookup == nil || stdout == nil || stderr == nil || factory == nil || len(args) == 0 {
		fmt.Fprintln(stderr, "physical-shard-admin: invalid invocation")
		return 2
	}
	req, err := parse(args)
	if err != nil {
		_ = writeJSON(stdout, envelope{Command: safeName(args[0]), Status: "rejected", ReadOnly: true, Error: errorCode(err)})
		fmt.Fprintln(stderr, "physical-shard-admin: arguments rejected")
		return 2
	}
	ctx, cancel := context.WithTimeout(parent, req.Timeout)
	defer cancel()
	b, err := factory(ctx, lookup)
	if err != nil {
		_ = writeJSON(stdout, envelope{Command: req.Command, Status: "failed", ReadOnly: req.DryRun || isReadOnly(req.Command), Error: errorCode(contextError(ctx, err))})
		fmt.Fprintln(stderr, "physical-shard-admin: startup failed")
		return 1
	}
	defer b.Close()
	result, err := b.Execute(ctx, req)
	err = contextError(ctx, err)
	status := "completed"
	if req.DryRun {
		status = "dry-run"
	}
	if err != nil {
		status = "failed"
	}
	if writeErr := writeJSON(stdout, envelope{Command: req.Command, Status: status, ReadOnly: req.DryRun || isReadOnly(req.Command), Result: result, Error: errorCode(err)}); writeErr != nil {
		fmt.Fprintln(stderr, "physical-shard-admin: output failed")
		return 1
	}
	if err != nil {
		fmt.Fprintln(stderr, "physical-shard-admin: command failed")
		return 1
	}
	return 0
}

func parse(args []string) (request, error) {
	name := safeName(args[0])
	if !knownCommand(name) {
		return request{}, errArguments
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	migrationText := fs.String("migration-id", "", "canonical migration UUID")
	trainRunText := fs.String("train-run-id", "", "canonical train-run UUID")
	commandText := fs.String("command-id", "", "canonical booking command UUID")
	reverseText := fs.String("reverse-migration-id", "", "canonical reverse migration UUID")
	shardID := fs.String("shard-id", "", "fixed physical shard identifier")
	targetShard := fs.String("target-shard", "", "fixed physical target shard identifier")
	generation := fs.Int64("generation", 0, "new positive assignment generation")
	limit := fs.Int("limit", defaultLimit, "bounded output limit")
	timeout := fs.Duration("timeout", defaultTimeout, "bounded operation timeout")
	dryRun := fs.Bool("dry-run", false, "inspect without mutation")
	confirm := fs.Bool("confirm", false, "explicitly confirm mutation")
	if fs.Parse(args[1:]) != nil || fs.NArg() != 0 || *timeout <= 0 || *timeout > maxTimeout || *limit < 1 || *limit > maxLimit {
		return request{}, errArguments
	}
	req := request{Command: name, ShardID: strings.TrimSpace(*shardID), TargetShardID: strings.TrimSpace(*targetShard), Generation: *generation, Limit: *limit, Timeout: *timeout, DryRun: *dryRun, Confirm: *confirm}
	var err error
	if requiresMigration(name) {
		req.MigrationID, err = parseUUID(*migrationText)
		if err != nil {
			return request{}, errArguments
		}
	}
	if requiresTrainRun(name) {
		req.TrainRunID, err = parseUUID(*trainRunText)
		if err != nil {
			return request{}, errArguments
		}
	}
	if name == "repair-command" {
		req.CommandID, err = parseUUID(*commandText)
		if err != nil {
			return request{}, errArguments
		}
	}
	if name == "plan-reverse-migration" {
		req.ReverseMigrationID, err = parseUUID(*reverseText)
		if err != nil || req.Generation <= 0 {
			return request{}, errArguments
		}
	}
	if name == "plan-migration" && !validShard(req.TargetShardID) {
		return request{}, errArguments
	}
	if (name == "inspect-shard" || name == "check-schema" || name == "bootstrap-shard") && !validShard(req.ShardID) {
		return request{}, errArguments
	}
	if isMutation(name) {
		if req.DryRun == req.Confirm {
			return request{}, errConfirmation
		}
	} else if req.Confirm {
		return request{}, errArguments
	}
	return req, nil
}

func knownCommand(name string) bool {
	switch name {
	case "list-shards", "inspect-shard", "check-schema", "bootstrap-shard", "inspect-train-run", "plan-migration", "enable-capture", "start-base-copy", "resume-base-copy", "replay-journal", "validate-online", "begin-quiesce", "final-catchup", "cutover", "inspect-crash-window", "rollback", "plan-reverse-migration", "start-reverse-migration", "cleanup-source", "repair-command", "reconcile":
		return true
	default:
		return false
	}
}

func isReadOnly(name string) bool {
	switch name {
	case "list-shards", "inspect-shard", "check-schema", "inspect-train-run", "inspect-crash-window", "reconcile":
		return true
	default:
		return false
	}
}

func isMutation(name string) bool { return !isReadOnly(name) }

func requiresMigration(name string) bool {
	switch name {
	case "plan-migration", "enable-capture", "start-base-copy", "resume-base-copy", "replay-journal", "validate-online", "begin-quiesce", "final-catchup", "cutover", "inspect-crash-window", "rollback", "plan-reverse-migration", "start-reverse-migration", "cleanup-source", "reconcile":
		return true
	default:
		return false
	}
}

func requiresTrainRun(name string) bool {
	return name == "inspect-train-run" || name == "plan-migration"
}

func validShard(value string) bool {
	return value == "physical-shard-0" || value == "physical-shard-1"
}

func validControlSource(value string) bool {
	return value == "legacy" || value == "shard-0" || value == "shard-1"
}

func parseUUID(value string) (uuid.UUID, error) {
	value = strings.TrimSpace(value)
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil || id.String() != strings.ToLower(value) {
		return uuid.Nil, errArguments
	}
	return id, nil
}

func safeName(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 64 {
		return "invalid"
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && char != '-' {
			return "invalid"
		}
	}
	return value
}

func writeJSON(writer io.Writer, value envelope) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(value)
}

func contextError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func errorCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, errArguments):
		return "invalid_arguments"
	case errors.Is(err, errConfirmation):
		return "confirmation_required"
	case errors.Is(err, errRole):
		return "operator_role_required"
	case errors.Is(err, errState):
		return "invalid_migration_state"
	case errors.Is(err, errUnavailable):
		return "unavailable"
	case errors.Is(err, errUnsupportedMigrationSource):
		return "unsupported_migration_source"
	default:
		if code := boundedPostgresErrorCode(err); code != "" {
			return code
		}
		return "operation_failed"
	}
}

func boundedPostgresErrorCode(err error) string {
	var databaseError *pgconn.PgError
	if !errors.As(err, &databaseError) {
		return ""
	}
	switch databaseError.Code {
	case "23503":
		return "operation_failed_database_foreign_key"
	case "23505":
		return "operation_failed_database_unique"
	case "23514":
		return "operation_failed_database_check"
	case "55000":
		return "operation_failed_database_prerequisite"
	case "22P02":
		return "operation_failed_database_value"
	default:
		return ""
	}
}

func expectedState(command string) []migration.PhysicalState {
	switch command {
	case "bootstrap-shard":
		return []migration.PhysicalState{migration.PhysicalStatePlanned}
	case "enable-capture":
		return []migration.PhysicalState{migration.PhysicalStatePlanned, migration.PhysicalStatePreparingTarget}
	case "start-base-copy":
		return []migration.PhysicalState{migration.PhysicalStateCaptureEnabled}
	case "resume-base-copy":
		return []migration.PhysicalState{migration.PhysicalStateBaseCopying}
	case "replay-journal":
		return []migration.PhysicalState{migration.PhysicalStateCatchingUp}
	case "validate-online":
		return []migration.PhysicalState{migration.PhysicalStateValidatingOnline}
	case "begin-quiesce":
		return []migration.PhysicalState{migration.PhysicalStateDraining}
	case "final-catchup":
		return []migration.PhysicalState{migration.PhysicalStateSourceFenced}
	case "cutover":
		return []migration.PhysicalState{migration.PhysicalStateFinalCatchup, migration.PhysicalStateFinalValidating, migration.PhysicalStateTargetEnabled, migration.PhysicalStateSwitchingAssignment}
	case "start-reverse-migration":
		return []migration.PhysicalState{migration.PhysicalStatePlanned}
	default:
		return nil
	}
}
