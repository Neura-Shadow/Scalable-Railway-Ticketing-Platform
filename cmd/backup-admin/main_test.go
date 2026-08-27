package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

const (
	testBackupSet    = "20260811-130000F"
	testExpirationID = "33333333-3333-4333-8333-333333333333"
	testOperationID  = "44444444-4444-4444-8444-444444444444"
)

func TestMutatingBackupRequiresCallerSuppliedDurableOperationIdentity(t *testing.T) {
	t.Parallel()

	base := []string{"backup-control", "--repository", "repo-dr"}
	if _, err := parse(base); err == nil {
		t.Fatal("parse() accepted backup without durable operation identity")
	}
	got, err := parse(append(append([]string{}, base...), "--operation-id", testOperationID))
	if err != nil || got.OperationID.String() != testOperationID {
		t.Fatalf("parse() operation = %s, error=%v", got.OperationID, err)
	}
}

func TestExpireBackupRequiresDryRunThenASeparateConfirmedPlanIdentity(t *testing.T) {
	t.Parallel()

	base := []string{
		"expire-backup", "--database", "control", "--repository", "repo-dr",
		"--backup-set", testBackupSet,
	}
	for _, args := range [][]string{
		base,
		append(append([]string{}, base...), "--dry-run", "--confirm"),
		append(append([]string{}, base...), "--confirm"),
	} {
		if _, err := parse(args); err == nil {
			t.Fatalf("parse(%v) accepted unsafe expiration", args)
		}
	}
	dryRun, err := parse(append(append([]string{}, base...), "--dry-run"))
	if err != nil || !dryRun.DryRun || dryRun.Confirm {
		t.Fatalf("dry-run request=%+v error=%v", dryRun, err)
	}
	confirmed, err := parse(append(append([]string{}, base...), "--confirm", "--expiration-id", testExpirationID))
	if err != nil || !confirmed.Confirm || confirmed.ExpirationID == uuid.Nil {
		t.Fatalf("confirmed request=%+v error=%v", confirmed, err)
	}
}

func TestRestoreValidationRejectsRequestControlledPathHostAndCommand(t *testing.T) {
	t.Parallel()

	base := []string{
		"restore-validation", "--database", "control", "--repository", "repo-dr",
		"--backup-set", testBackupSet, "--target", "validation-control",
		"--pitr-target", "2026-08-11T13:05:00Z", "--dry-run",
	}
	for _, forbidden := range []string{"--path", "--host", "--command"} {
		args := append(append([]string{}, base...), forbidden, "unsafe")
		if _, err := parse(args); err == nil {
			t.Fatalf("parse() accepted %s", forbidden)
		}
	}
}

func TestRestoreValidationRequiresCanonicalUTCPITRTarget(t *testing.T) {
	t.Parallel()

	base := []string{
		"restore-validation", "--database", "control", "--repository", "repo-dr",
		"--backup-set", testBackupSet, "--target", "validation-control", "--dry-run",
	}
	for _, value := range []string{"", "2026-08-11T21:05:00+08:00", "next tuesday", "2026-08-11T13:05:00Z\n"} {
		args := append(append([]string{}, base...), "--pitr-target", value)
		if _, err := parse(args); err == nil {
			t.Fatalf("parse() accepted unsafe PITR target %q", value)
		}
	}
	got, err := parse(append(append([]string{}, base...), "--pitr-target", "2026-08-11T13:05:00Z"))
	if err != nil || got.PITRTarget.Format(time.RFC3339) != "2026-08-11T13:05:00Z" {
		t.Fatalf("parse() PITR target = %v, error=%v", got.PITRTarget, err)
	}
}

func TestBackupAdminSanitizesResultAndBackendFailure(t *testing.T) {
	t.Parallel()

	backend := &fakeBackupBackend{result: result{
		Database:  recovery.DatabaseControl,
		BackupSet: testBackupSet,
		State:     "verified",
	}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"verify-backup", "--database", "control", "--repository", "repo-dr", "--backup-set", testBackupSet,
	}, noBackupEnv, &stdout, &stderr, func(context.Context, func(string) (string, bool), request) (backendService, func(), error) {
		return backend, func() {}, nil
	})
	if code != 0 || !strings.Contains(stdout.String(), `"database":"control"`) ||
		!strings.Contains(stdout.String(), `"state":"verified"`) {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	backend.err = errors.New("D:\\secret-backups postgres://credential@host")
	stdout.Reset()
	stderr.Reset()
	code = run(context.Background(), []string{
		"verify-backup", "--database", "control", "--repository", "repo-dr", "--backup-set", testBackupSet,
	}, noBackupEnv, &stdout, &stderr, func(context.Context, func(string) (string, bool), request) (backendService, func(), error) {
		return backend, func() {}, nil
	})
	if code != 1 || strings.Contains(stdout.String(), "secret-backups") || strings.Contains(stderr.String(), "credential") ||
		!strings.Contains(stdout.String(), `"error":"operation_failed"`) {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

type fakeBackupBackend struct {
	request request
	result  result
	err     error
}

func (backend *fakeBackupBackend) Execute(_ context.Context, req request) (result, error) {
	backend.request = req
	return backend.result, backend.err
}

func noBackupEnv(string) (string, bool) { return "", false }
