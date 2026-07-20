package policylock

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

type executorSpy struct {
	query string
	args  []any
	err   error
}

func (s *executorSpy) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	s.query = query
	s.args = append([]any(nil), args...)
	return pgconn.CommandTag{}, s.err
}

func TestLockModesUseCanonicalVersionedDatabaseKeyTuple(t *testing.T) {
	t.Parallel()
	runID := uuid.New()
	shared := &executorSpy{}
	if err := AcquireBookingRead(context.Background(), shared, runID, " STANDARD "); err != nil {
		t.Fatalf("AcquireBookingRead() error = %v", err)
	}
	if !strings.Contains(shared.query, "pg_advisory_xact_lock_shared") ||
		!strings.Contains(shared.query, "'admission:hot-train-policy:v1|'") ||
		!strings.Contains(shared.query, "hashtextextended") {
		t.Fatalf("shared advisory-lock SQL lacks mode or namespace: %q", shared.query)
	}
	if len(shared.args) != 2 || shared.args[0] != runID || shared.args[1] != "standard" {
		t.Fatalf("shared lock tuple = %#v, want [%s standard]", shared.args, runID)
	}
	exclusive := &executorSpy{}
	if err := AcquirePolicyMutation(context.Background(), exclusive, runID, "standard"); err != nil {
		t.Fatalf("AcquirePolicyMutation() error = %v", err)
	}
	if strings.Contains(exclusive.query, "_shared") ||
		!strings.Contains(exclusive.query, "pg_advisory_xact_lock(") {
		t.Fatalf("exclusive advisory-lock SQL has wrong mode: %q", exclusive.query)
	}
	if len(exclusive.args) != 2 || exclusive.args[0] != runID || exclusive.args[1] != "standard" {
		t.Fatalf("exclusive lock tuple = %#v, want [%s standard]", exclusive.args, runID)
	}
}

func TestLockModesRejectInvalidScopeAndWrapBackendFailure(t *testing.T) {
	t.Parallel()
	spy := &executorSpy{}
	if err := AcquireBookingRead(context.Background(), spy, uuid.Nil, "standard"); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("nil run error = %v, want %v", err, ErrInvalidScope)
	}
	if spy.query != "" {
		t.Fatal("invalid scope reached PostgreSQL")
	}
	backendErr := errors.New("database unavailable")
	spy.err = backendErr
	if err := AcquirePolicyMutation(context.Background(), spy, uuid.New(), "standard"); !errors.Is(err, backendErr) {
		t.Fatalf("backend error = %v, want wrapped %v", err, backendErr)
	}
}
