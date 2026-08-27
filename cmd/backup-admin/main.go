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
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

const (
	defaultTimeout = 30 * time.Second
	maximumTimeout = 5 * time.Minute
)

var (
	errArguments     = errors.New("invalid arguments")
	errConfirmation  = errors.New("backup safety gate required")
	errRuntimeWiring = errors.New("backup administration runtime unavailable")
	identityPattern  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	backupSetPattern = regexp.MustCompile(`^[0-9]{8}-[0-9]{6}F(?:_[0-9]{8}-[0-9]{6}[DI])?$`)
	statePattern     = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
)

type request struct {
	Command      string
	Database     recovery.Database
	Repository   string
	BackupSet    string
	Target       string
	PITRTarget   time.Time
	ExpirationID uuid.UUID
	OperationID  uuid.UUID
	Timeout      time.Duration
	DryRun       bool
	Confirm      bool
}

type result struct {
	Database    recovery.Database `json:"database,omitempty"`
	BackupSet   string            `json:"backup_set,omitempty"`
	OperationID uuid.UUID         `json:"operation_id,omitempty"`
	State       string            `json:"state,omitempty"`
	Count       int               `json:"count,omitempty"`
}

type envelope struct {
	Command  string `json:"command"`
	Status   string `json:"status"`
	ReadOnly bool   `json:"read_only"`
	Result   result `json:"result"`
	Error    string `json:"error,omitempty"`
}

type backendService interface {
	Execute(context.Context, request) (result, error)
}

type backendFactory func(context.Context, func(string) (string, bool), request) (backendService, func(), error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, openBackend))
}

func run(
	parent context.Context,
	args []string,
	lookup func(string) (string, bool),
	stdout io.Writer,
	stderr io.Writer,
	factory backendFactory,
) int {
	if parent == nil || len(args) == 0 || lookup == nil || stdout == nil || stderr == nil || factory == nil {
		fmt.Fprintln(stderr, "backup-admin: invalid invocation")
		return 2
	}
	req, err := parse(args)
	if err != nil {
		_ = writeJSON(stdout, envelope{Command: safeName(args[0]), Status: "rejected", ReadOnly: true, Error: errorCode(err)})
		fmt.Fprintln(stderr, "backup-admin: arguments rejected")
		return 2
	}
	ctx, cancel := context.WithTimeout(parent, req.Timeout)
	defer cancel()
	backend, closeBackend, err := factory(ctx, lookup, req)
	if err != nil {
		_ = writeJSON(stdout, envelope{Command: req.Command, Status: "failed", ReadOnly: requestReadOnly(req), Error: "startup_failed"})
		fmt.Fprintln(stderr, "backup-admin: startup failed")
		return 1
	}
	defer closeBackend()
	value, err := backend.Execute(ctx, req)
	err = contextError(ctx, err)
	value = sanitizeResult(value)
	status := "completed"
	if req.DryRun {
		status = "dry-run"
	}
	if err != nil {
		status = "failed"
	}
	if writeErr := writeJSON(stdout, envelope{
		Command: req.Command, Status: status, ReadOnly: requestReadOnly(req),
		Result: value, Error: errorCode(err),
	}); writeErr != nil {
		fmt.Fprintln(stderr, "backup-admin: output failed")
		return 1
	}
	if err != nil {
		fmt.Fprintln(stderr, "backup-admin: command failed")
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
	databaseText := flags.String("database", "", "fixed database identity")
	repository := flags.String("repository", "", "allowlisted repository identity")
	backupSet := flags.String("backup-set", "", "bounded pgBackRest backup label")
	target := flags.String("target", "", "allowlisted isolated validation target")
	pitrTarget := flags.String("pitr-target", "", "explicit UTC RFC3339 point-in-time recovery target")
	expirationText := flags.String("expiration-id", "", "prior dry-run operation UUID")
	operationText := flags.String("operation-id", "", "caller-generated durable mutation UUID")
	timeout := flags.Duration("timeout", defaultTimeout, "bounded operation timeout")
	dryRun := flags.Bool("dry-run", false, "preview without destructive mutation")
	confirm := flags.Bool("confirm", false, "explicitly confirm mutation")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || *timeout <= 0 || *timeout > maximumTimeout ||
		!identityPattern.MatchString(*repository) {
		return request{}, errArguments
	}
	req := request{
		Command: name, Repository: *repository, Timeout: *timeout,
		DryRun: *dryRun, Confirm: *confirm,
	}
	var err error
	switch name {
	case "backup-control":
		if *databaseText != "" && *databaseText != recovery.DatabaseControl.String() {
			return request{}, errArguments
		}
		req.Database = recovery.DatabaseControl
	case "backup-shard":
		req.Database, err = parseDatabase(*databaseText)
		if err != nil || req.Database == recovery.DatabaseControl {
			return request{}, errArguments
		}
	default:
		req.Database, err = parseDatabase(*databaseText)
		if err != nil {
			return request{}, errArguments
		}
	}
	if requiresBackupSet(name) {
		if !backupSetPattern.MatchString(*backupSet) {
			return request{}, errArguments
		}
		req.BackupSet = *backupSet
	} else if *backupSet != "" {
		return request{}, errArguments
	}
	if name == "restore-validation" {
		parsedTarget, parseErr := time.Parse(time.RFC3339Nano, *pitrTarget)
		if !identityPattern.MatchString(*target) || parseErr != nil || parsedTarget.Location() != time.UTC ||
			parsedTarget.Format(time.RFC3339Nano) != *pitrTarget {
			return request{}, errArguments
		}
		req.Target = *target
		req.PITRTarget = parsedTarget
	} else if *target != "" || *pitrTarget != "" {
		return request{}, errArguments
	}
	if name == "expire-backup" {
		if req.DryRun == req.Confirm {
			return request{}, errConfirmation
		}
		if req.Confirm {
			req.ExpirationID, err = parseUUID(*expirationText)
			if err != nil {
				return request{}, errConfirmation
			}
		} else if *expirationText != "" {
			return request{}, errArguments
		}
	} else if name == "restore-validation" {
		if req.DryRun == req.Confirm || *expirationText != "" {
			return request{}, errConfirmation
		}
		if req.Confirm {
			req.OperationID, err = parseUUID(*operationText)
			if err != nil {
				return request{}, errConfirmation
			}
		} else if *operationText != "" {
			return request{}, errArguments
		}
	} else if name == "backup-control" || name == "backup-shard" {
		req.OperationID, err = parseUUID(*operationText)
		if err != nil {
			return request{}, errConfirmation
		}
	} else if req.DryRun || req.Confirm || *expirationText != "" || *operationText != "" {
		return request{}, errArguments
	}
	return req, nil
}

func knownCommand(name string) bool {
	switch name {
	case "backup-control", "backup-shard", "verify-backup", "list-backups",
		"restore-validation", "inspect-wal-archive", "inspect-retention", "expire-backup":
		return true
	default:
		return false
	}
}

func requiresBackupSet(name string) bool {
	return name == "verify-backup" || name == "restore-validation" || name == "expire-backup"
}

func requestReadOnly(req request) bool {
	switch req.Command {
	case "verify-backup", "list-backups", "inspect-wal-archive", "inspect-retention":
		return true
	default:
		return req.DryRun
	}
}

func parseDatabase(raw string) (recovery.Database, error) {
	return recovery.ParseDatabase(strings.TrimSpace(raw))
}

func parseUUID(raw string) (uuid.UUID, error) {
	raw = strings.TrimSpace(raw)
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil || id.String() != strings.ToLower(raw) {
		return uuid.Nil, errArguments
	}
	return id, nil
}

func safeName(raw string) string {
	raw = strings.TrimSpace(raw)
	if len(raw) == 0 || len(raw) > 64 {
		return "invalid"
	}
	for _, character := range raw {
		if (character < 'a' || character > 'z') && character != '-' {
			return "invalid"
		}
	}
	return raw
}

func sanitizeResult(value result) result {
	if _, err := recovery.ParseDatabase(value.Database.String()); err != nil {
		value.Database = ""
	}
	if value.BackupSet != "" && !backupSetPattern.MatchString(value.BackupSet) {
		value.BackupSet = ""
	}
	if value.State != "" && !statePattern.MatchString(value.State) {
		value.State = ""
	}
	if value.Count < 0 || value.Count > 1_000_000 {
		value.Count = 0
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
	case errors.Is(err, errArguments):
		return "invalid_arguments"
	case errors.Is(err, errConfirmation):
		return "confirmation_required"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "operation_failed"
	}
}
