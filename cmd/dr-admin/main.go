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

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

const (
	defaultTimeout = 30 * time.Second
	maximumTimeout = 5 * time.Minute
)

var (
	errArguments     = errors.New("invalid arguments")
	errConfirmation  = errors.New("explicit safety gate required")
	errRuntimeWiring = errors.New("dr administration runtime unavailable")
	operatorPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$`)
	reasonPattern    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

type request struct {
	Command      string
	OperationID  uuid.UUID
	IncidentID   uuid.UUID
	From         authority.Region
	To           authority.Region
	SourceEpoch  authority.Epoch
	TargetEpoch  authority.Epoch
	OperatorID   string
	Reason       string
	EvidenceFile string
	Timeout      time.Duration
	DryRun       bool
	Confirm      bool
}

type result struct {
	OperationID uuid.UUID      `json:"operation_id,omitempty"`
	Stage       recovery.Stage `json:"-"`
	StageText   string         `json:"stage,omitempty"`
	Region      string         `json:"region,omitempty"`
	Epoch       uint64         `json:"epoch,omitempty"`
	Version     int64          `json:"version,omitempty"`
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
		fmt.Fprintln(stderr, "dr-admin: invalid invocation")
		return 2
	}
	req, err := parse(args)
	if err != nil {
		_ = writeJSON(stdout, envelope{Command: safeName(args[0]), Status: "rejected", ReadOnly: true, Error: errorCode(err)})
		fmt.Fprintln(stderr, "dr-admin: arguments rejected")
		return 2
	}
	ctx, cancel := context.WithTimeout(parent, req.Timeout)
	defer cancel()
	backend, closeBackend, err := factory(ctx, lookup, req)
	if err != nil {
		_ = writeJSON(stdout, envelope{Command: req.Command, Status: "failed", ReadOnly: requestReadOnly(req), Error: "startup_failed"})
		fmt.Fprintln(stderr, "dr-admin: startup failed")
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
		fmt.Fprintln(stderr, "dr-admin: output failed")
		return 1
	}
	if err != nil {
		fmt.Fprintln(stderr, "dr-admin: command failed")
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
	operationText := flags.String("operation-id", "", "canonical recovery operation UUID")
	incidentText := flags.String("incident-id", "", "canonical incident UUID")
	fromText := flags.String("from", "", "fixed source region")
	toText := flags.String("to", "", "fixed target region")
	sourceEpochValue := flags.Uint64("source-epoch", 0, "positive current regional epoch")
	targetEpochValue := flags.Uint64("target-epoch", 0, "strictly newer regional epoch")
	operatorID := flags.String("operator", "", "bounded operator identity")
	reason := flags.String("reason", "", "bounded reason category")
	evidenceFile := flags.String("evidence-file", "", "strict JSON observation for the next fixed phase")
	timeout := flags.Duration("timeout", defaultTimeout, "bounded operation timeout")
	dryRun := flags.Bool("dry-run", false, "validate without mutation")
	confirm := flags.Bool("confirm", false, "explicitly confirm mutation")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || *timeout <= 0 || *timeout > maximumTimeout {
		return request{}, errArguments
	}
	req := request{Command: name, Timeout: *timeout, DryRun: *dryRun, Confirm: *confirm, EvidenceFile: strings.TrimSpace(*evidenceFile)}
	var err error
	req.OperationID, err = parseUUID(*operationText)
	if err != nil {
		return request{}, errArguments
	}
	if requiresRegions(name) {
		req.From, err = parseRegion(*fromText)
		if err != nil {
			return request{}, errArguments
		}
		req.To, err = parseRegion(*toText)
		if err != nil || req.From == req.To {
			return request{}, errArguments
		}
	} else if *fromText != "" || *toText != "" {
		return request{}, errArguments
	}
	if name == "advance-phase" || name == "refresh-fence" || name == "validate-failback" {
		if req.EvidenceFile == "" || len(req.EvidenceFile) > 4096 || strings.ContainsAny(req.EvidenceFile, "\x00\r\n") || req.DryRun {
			return request{}, errArguments
		}
	} else if req.EvidenceFile != "" {
		return request{}, errArguments
	}
	if requiresIncident(name) {
		req.IncidentID, err = parseUUID(*incidentText)
		if err != nil || !operatorPattern.MatchString(*operatorID) || !reasonPattern.MatchString(*reason) {
			return request{}, errArguments
		}
		req.OperatorID, req.Reason = *operatorID, *reason
		req.SourceEpoch, err = authority.NewEpoch(*sourceEpochValue)
		if err != nil {
			return request{}, errArguments
		}
	} else if *incidentText != "" || *operatorID != "" || *reason != "" || *sourceEpochValue != 0 {
		return request{}, errArguments
	}
	if name == "prepare-failback" || name == "failback" {
		req.TargetEpoch, err = authority.NewEpoch(*targetEpochValue)
		if err != nil || authority.RequireNewerEpoch(req.SourceEpoch, req.TargetEpoch) != nil {
			return request{}, errArguments
		}
	} else if *targetEpochValue != 0 {
		return request{}, errArguments
	}
	if isMutation(name) {
		if req.DryRun == req.Confirm {
			return request{}, errConfirmation
		}
	} else if req.DryRun || req.Confirm {
		return request{}, errArguments
	}
	return req, nil
}

func knownCommand(name string) bool {
	switch name {
	case "failover", "prepare-failback", "reseed-region", "validate-failback", "failback", "advance-phase", "refresh-fence", "verify-fence":
		return true
	default:
		return false
	}
}

func isMutation(name string) bool { return name != "validate-failback" && name != "verify-fence" }

func requiresRegions(name string) bool {
	return name != "validate-failback" && name != "advance-phase" && name != "refresh-fence" && name != "verify-fence"
}

func requiresIncident(name string) bool {
	return name == "failover" || name == "prepare-failback" || name == "failback"
}

func requestReadOnly(req request) bool { return !isMutation(req.Command) || req.DryRun }

func parseRegion(raw string) (authority.Region, error) {
	if raw != "region-a" && raw != "region-b" {
		return "", errArguments
	}
	return authority.ParseRegion(raw)
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
	if value.OperationID == uuid.Nil {
		value.OperationID = uuid.Nil
	}
	if value.Stage.String() != "invalid" {
		value.StageText = value.Stage.String()
	} else {
		value.StageText = ""
	}
	if value.Region != "region-a" && value.Region != "region-b" {
		value.Region = ""
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
