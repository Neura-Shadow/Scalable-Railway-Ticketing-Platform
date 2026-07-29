package main

import (
	"context"
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/jackc/pgx/v5"
)

func TestRequireOperatorRoleAcceptsOnlyAnAllowedDatabasePrincipal(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name    string
		allowed bool
		wantErr bool
	}{
		{name: "operator or admin membership", allowed: true},
		{name: "ordinary application role", allowed: false, wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := requireOperatorRole(context.Background(), roleDB{allowed: testCase.allowed})
			if testCase.wantErr != errors.Is(err, errRole) {
				t.Fatalf("requireOperatorRole() error = %v", err)
			}
		})
	}
}

func TestPhaseRankRejectsEarlierStateButRecognizesCompletedResume(t *testing.T) {
	t.Parallel()
	allowed := expectedState("final-catchup")
	if phaseRank(migration.PhysicalStateDraining) >= maxStateRank(allowed) {
		t.Fatal("an earlier state would be treated as an idempotent resume")
	}
	if phaseRank(migration.PhysicalStateCompleted) <= maxStateRank(allowed) {
		t.Fatal("completed state was not recognized as later than final catchup")
	}
}

func TestBackendErrorsCollapseToBoundedPublicCategories(t *testing.T) {
	t.Parallel()
	secretBearing := errors.New("postgres://user:password@private-host/database")
	if got := errorCode(secretBearing); got != "operation_failed" {
		t.Fatalf("errorCode() = %q", got)
	}
}

type roleDB struct{ allowed bool }

func (db roleDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return roleRow{allowed: db.allowed}
}

type roleRow struct{ allowed bool }

func (row roleRow) Scan(destinations ...any) error {
	if len(destinations) != 1 {
		return errors.New("unexpected destination count")
	}
	value, ok := destinations[0].(*bool)
	if !ok {
		return errors.New("unexpected destination type")
	}
	*value = row.allowed
	return nil
}
