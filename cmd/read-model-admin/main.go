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

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	queryreadmodel "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/readmodel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultAdminTimeout = 2 * time.Minute

type envelope struct {
	Command  string `json:"command"`
	Status   string `json:"status"`
	ReadOnly bool   `json:"read_only"`
	Result   any    `json:"result,omitempty"`
}

type lagResult struct {
	TrainRunsWithoutProjection int64         `json:"train_runs_without_projection"`
	MaximumSourceAhead         time.Duration `json:"maximum_source_ahead"`
	OldestReceiptAge           time.Duration `json:"oldest_receipt_age"`
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

func run(parent context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	if parent == nil || lookup == nil || stdout == nil || stderr == nil || len(args) == 0 {
		writeUsage(stderr)
		return 2
	}
	databaseURL, _ := lookup("DATABASE_URL")
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		fmt.Fprintln(stderr, "configuration invalid: DATABASE_URL is required")
		return 2
	}
	var result envelope
	var err error
	switch args[0] {
	case "rebuild-train-run":
		result, err = rebuildTrainRun(parent, databaseURL, args[1:])
	case "rebuild-all":
		result, err = rebuildAll(parent, databaseURL, args[1:])
	case "reconcile":
		result, err = reconcile(parent, databaseURL, args[1:])
	case "inspect-lag":
		result, err = inspectLag(parent, databaseURL, args[1:])
	default:
		writeUsage(stderr)
		return 2
	}
	if err != nil {
		if result.Command != "" {
			result.Status = "failed"
			_ = json.NewEncoder(stdout).Encode(result)
		}
		fmt.Fprintln(stderr, publicAdminError(err))
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintln(stderr, "failed to encode admin result")
		return 1
	}
	return 0
}

func rebuildTrainRun(parent context.Context, databaseURL string, args []string) (envelope, error) {
	flags := flag.NewFlagSet("rebuild-train-run", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	trainRunText := flags.String("train-run-id", "", "canonical train-run UUID")
	apply := flags.Bool("apply", false, "perform the disposable projection rebuild")
	timeout := flags.Duration("timeout", defaultAdminTimeout, "maximum command duration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *timeout <= 0 {
		return envelope{}, errors.New("usage: read-model-admin rebuild-train-run --train-run-id UUID [--apply] [--timeout 2m]")
	}
	trainRunID, err := canonicalUUID(*trainRunText)
	if err != nil {
		return envelope{}, err
	}
	result := envelope{Command: "rebuild-train-run", Status: "dry-run", ReadOnly: !*apply, Result: map[string]any{"train_run_id": trainRunID}}
	if !*apply {
		return result, nil
	}
	ctx, store, closeStore, err := openStore(parent, databaseURL, *timeout)
	if err != nil {
		return result, err
	}
	defer closeStore()
	rebuild, err := store.RebuildTrainRun(ctx, trainRunID)
	result.Status = "completed"
	result.Result = rebuild
	return result, err
}

func rebuildAll(parent context.Context, databaseURL string, args []string) (envelope, error) {
	flags := flag.NewFlagSet("rebuild-all", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	after := flags.String("after", "", "opaque resume cursor")
	batchSize := flags.Int("batch-size", 50, "bounded train-run batch size")
	apply := flags.Bool("apply", false, "perform disposable projection rebuilds")
	timeout := flags.Duration("timeout", defaultAdminTimeout, "maximum command duration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *timeout <= 0 ||
		*batchSize < 1 || *batchSize > queryreadmodel.MaxRebuildAllBatchSize {
		return envelope{}, errors.New("usage: read-model-admin rebuild-all [--after CURSOR] [--batch-size 50] [--apply] [--timeout 2m]")
	}
	ctx, store, closeStore, err := openStore(parent, databaseURL, *timeout)
	if err != nil {
		return envelope{}, err
	}
	defer closeStore()
	options := queryreadmodel.RebuildAllOptions{After: *after, Limit: *batchSize}
	result := envelope{Command: "rebuild-all", Status: "dry-run", ReadOnly: !*apply}
	if !*apply {
		result.Result, err = store.PreviewRebuildAll(ctx, options)
		return result, err
	}
	result.Status = "completed"
	result.Result, err = store.RebuildAll(ctx, options)
	return result, err
}

func reconcile(parent context.Context, databaseURL string, args []string) (envelope, error) {
	flags := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	trainRunText := flags.String("train-run-id", "", "canonical train-run UUID")
	timeout := flags.Duration("timeout", defaultAdminTimeout, "maximum command duration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *timeout <= 0 {
		return envelope{}, errors.New("usage: read-model-admin reconcile --train-run-id UUID [--timeout 2m]")
	}
	trainRunID, err := canonicalUUID(*trainRunText)
	if err != nil {
		return envelope{}, err
	}
	ctx, store, closeStore, err := openStore(parent, databaseURL, *timeout)
	if err != nil {
		return envelope{}, err
	}
	defer closeStore()
	comparison, err := store.ReconcileTrainRun(ctx, trainRunID)
	result := envelope{Command: "reconcile", Status: "healthy", ReadOnly: true, Result: comparison}
	if err != nil {
		return result, err
	}
	if !comparison.Consistent {
		return result, errors.New("projection mismatches detected")
	}
	return result, nil
}

func inspectLag(parent context.Context, databaseURL string, args []string) (envelope, error) {
	flags := flag.NewFlagSet("inspect-lag", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	timeout := flags.Duration("timeout", defaultAdminTimeout, "maximum command duration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *timeout <= 0 {
		return envelope{}, errors.New("usage: read-model-admin inspect-lag [--timeout 2m]")
	}
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return envelope{}, errors.New("postgres configuration invalid")
	}
	defer pool.Close()
	var missing int64
	var sourceAheadSeconds, receiptAgeSeconds float64
	err = pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM train_runs tr WHERE NOT EXISTS (
				SELECT 1 FROM train_run_journey_read_model rm WHERE rm.train_run_id = tr.id
			)),
			COALESCE((SELECT max(EXTRACT(EPOCH FROM (tr.updated_at - rm.projected_at)))
			 FROM train_runs tr
			 JOIN (SELECT train_run_id, max(source_updated_at) AS projected_at
			       FROM train_run_journey_read_model GROUP BY train_run_id) rm ON rm.train_run_id = tr.id
			 WHERE tr.updated_at > rm.projected_at), 0),
			COALESCE((SELECT EXTRACT(EPOCH FROM (clock_timestamp() - min(processed_at)))
			 FROM read_model_event_receipts), 0)
	`).Scan(&missing, &sourceAheadSeconds, &receiptAgeSeconds)
	if err != nil {
		return envelope{}, errors.New("read-model lag inspection failed")
	}
	result := lagResult{
		TrainRunsWithoutProjection: missing,
		MaximumSourceAhead:         time.Duration(sourceAheadSeconds * float64(time.Second)),
		OldestReceiptAge:           time.Duration(receiptAgeSeconds * float64(time.Second)),
	}
	return envelope{Command: "inspect-lag", Status: "completed", ReadOnly: true, Result: result}, nil
}

func openStore(parent context.Context, databaseURL string, timeout time.Duration) (context.Context, *queryreadmodel.Store, func(), error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		cancel()
		return nil, nil, func() {}, errors.New("postgres configuration invalid")
	}
	store, err := queryreadmodel.NewStore(pool, clock.RealClock{})
	if err != nil {
		pool.Close()
		cancel()
		return nil, nil, func() {}, errors.New("read-model store initialization failed")
	}
	return ctx, store, func() { pool.Close(); cancel() }, nil
}

func canonicalUUID(raw string) (string, error) {
	value, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || value == uuid.Nil {
		return "", errors.New("train-run-id must be a canonical UUID")
	}
	return value.String(), nil
}

func publicAdminError(err error) string {
	if errors.Is(err, context.Canceled) {
		return "admin command canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "admin command deadline exceeded"
	}
	return "admin command failed; inspect structured result and service logs"
}

func writeUsage(output io.Writer) {
	fmt.Fprintln(output, "usage: read-model-admin {rebuild-train-run|rebuild-all|reconcile|inspect-lag} [options]")
}
