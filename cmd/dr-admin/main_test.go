package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

const (
	testOperation = "11111111-1111-4111-8111-111111111111"
	testIncident  = "22222222-2222-4222-8222-222222222222"
)

func TestFailoverRequiresBoundedIdentityAndExactlyOneSafetyGate(t *testing.T) {
	t.Parallel()

	base := []string{
		"failover", "--operation-id", testOperation, "--incident-id", testIncident,
		"--from", "region-a", "--to", "region-b", "--source-epoch", "7",
		"--operator", "operator:alice", "--reason", "region_failure",
	}
	for _, args := range [][]string{
		base,
		append(append([]string{}, base...), "--confirm", "--dry-run"),
		append(append([]string{}, base...), "--host", "db.example", "--confirm"),
	} {
		if _, err := parse(args); err == nil {
			t.Fatalf("parse(%v) accepted unsafe invocation", args)
		}
	}
	confirmed := append(append([]string{}, base...), "--confirm")
	req, err := parse(confirmed)
	if err != nil {
		t.Fatalf("parse(confirmed) error = %v", err)
	}
	if !req.Confirm || req.DryRun || req.SourceEpoch.Uint64() != 7 || req.From.String() != "region-a" {
		t.Fatalf("request = %+v", req)
	}
}

func TestDRAdminOutputRedactsBackendErrorsAndTopology(t *testing.T) {
	t.Parallel()

	backend := &fakeBackend{err: errors.New("postgres://secret@hidden-host/control")}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"validate-failback", "--operation-id", testOperation, "--evidence-file", `C:\evidence\failback.json`,
	}, noEnv, &stdout, &stderr, func(context.Context, func(string) (string, bool), request) (backendService, func(), error) {
		return backend, func() {}, nil
	})
	if code != 1 || strings.Contains(stdout.String(), "secret") || strings.Contains(stderr.String(), "hidden-host") ||
		!strings.Contains(stdout.String(), `"error":"operation_failed"`) {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDRAdminConfirmedResultContainsOnlyBoundedRecoveryState(t *testing.T) {
	t.Parallel()

	backend := &fakeBackend{result: result{
		OperationID: uuid.MustParse(testOperation), Stage: recovery.StagePlanned,
		Region: "region-b", Epoch: 8,
	}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"failover", "--operation-id", testOperation, "--incident-id", testIncident,
		"--from", "region-a", "--to", "region-b", "--source-epoch", "7",
		"--operator", "operator:alice", "--reason", "region_failure", "--confirm",
	}, noEnv, &stdout, &stderr, func(context.Context, func(string) (string, bool), request) (backendService, func(), error) {
		return backend, func() {}, nil
	})
	if code != 0 || !strings.Contains(stdout.String(), `"stage":"planned"`) ||
		!strings.Contains(stdout.String(), `"read_only":false`) || stderr.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestAdvancePhaseRequiresStrictEvidenceFileAndConfirmation(t *testing.T) {
	t.Parallel()
	valid := []string{"advance-phase", "--operation-id", testOperation, "--evidence-file", `C:\evidence\phase.json`, "--confirm"}
	req, err := parse(valid)
	if err != nil || req.EvidenceFile == "" || !req.Confirm {
		t.Fatalf("parse(valid) request=%+v error=%v", req, err)
	}
	for _, args := range [][]string{
		{"advance-phase", "--operation-id", testOperation, "--confirm"},
		{"advance-phase", "--operation-id", testOperation, "--evidence-file", `C:\evidence\phase.json`, "--dry-run"},
		{"advance-phase", "--operation-id", testOperation, "--evidence-file", "phase.json\nother", "--confirm"},
	} {
		if _, err := parse(args); err == nil {
			t.Fatalf("parse(%v) accepted unsafe invocation", args)
		}
	}
}

type fakeBackend struct {
	request request
	result  result
	err     error
}

func (backend *fakeBackend) Execute(_ context.Context, req request) (result, error) {
	backend.request = req
	return backend.result, backend.err
}

func noEnv(string) (string, bool) { return "", false }
