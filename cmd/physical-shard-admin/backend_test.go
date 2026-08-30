package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/controlsource"
	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

func TestBackendErrorsExposeOnlyBoundedPostgresFailureClasses(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		code string
		want string
	}{
		{"23503", "operation_failed_database_foreign_key"},
		{"23505", "operation_failed_database_unique"},
		{"23514", "operation_failed_database_check"},
		{"55000", "operation_failed_database_prerequisite"},
		{"22P02", "operation_failed_database_value"},
		{"XX000", "operation_failed"},
	} {
		err := fmt.Errorf("outer operation: %w", &pgconn.PgError{
			Code: test.code, Message: "sentinel secret-bearing database detail",
		})
		if got := errorCode(err); got != test.want {
			t.Fatalf("errorCode(%s) = %q, want %q", test.code, got, test.want)
		}
	}
}

func TestReverseBaseRowPostgresCauseReachesOnlyTheBoundedEnvelope(t *testing.T) {
	t.Parallel()
	rawMessage := "sentinel secret-bearing database detail"
	databaseError := &pgconn.PgError{Code: "55000", Message: rawMessage}
	tx := &reverseDiagnosticTx{failure: databaseError}
	control := &reverseDiagnosticDB{tx: tx}
	adapter, err := controlsource.NewReverse(control, &reverseDiagnosticDB{}, "legacy")
	if err != nil {
		t.Fatal(err)
	}
	record := physicalmigration.Record{
		MigrationID: uuid.New(), TrainRunID: uuid.New(), SourceShardID: "shard-0",
		TargetShardID: "legacy", SourceGeneration: 2, TargetGeneration: 3,
		ReverseMigration: true,
	}
	row := physicalpostgres.JSONRow{
		Table: "seat_inventory", ID: uuid.New(),
		Data: []byte(fmt.Sprintf(`{"train_run_id":%q,"assignment_generation":2}`, record.TrainRunID.String())),
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(row.Table))
	_, _ = hash.Write(row.ID[:])
	_, _ = hash.Write(row.Data)
	var fingerprint [32]byte
	copy(fingerprint[:], hash.Sum(nil))
	err = adapter.ApplyBaseBatch(context.Background(), record, physicalmigration.BaseBatch{
		Rows: 1, Fingerprint: fingerprint,
		Payload: physicalpostgres.BasePayload{Rows: []physicalpostgres.JSONRow{row}},
	})
	if !errors.Is(err, physicalpostgres.ErrShardOperation) {
		t.Fatalf("ApplyBaseBatch() error = %v, want ErrShardOperation", err)
	}
	var observedDatabaseError *pgconn.PgError
	if !errors.As(err, &observedDatabaseError) || observedDatabaseError.Code != "55000" {
		t.Fatalf("ApplyBaseBatch() did not preserve PgError: %v", err)
	}
	bounded := envelope{Command: "resume-base-copy", Status: "failed", Error: errorCode(err)}
	encoded, marshalErr := json.Marshal(bounded)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if bounded.Error != "operation_failed_database_prerequisite" || strings.Contains(string(encoded), rawMessage) {
		t.Fatalf("bounded envelope = %s", encoded)
	}
}

type roleDB struct{ allowed bool }

type reverseDiagnosticDB struct {
	physicalpostgres.DB
	tx pgx.Tx
}

func (db *reverseDiagnosticDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return db.tx, nil
}

type reverseDiagnosticTx struct {
	pgx.Tx
	executions int
	failure    error
}

func (tx *reverseDiagnosticTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	tx.executions++
	if tx.executions == 1 {
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}
	return pgconn.CommandTag{}, tx.failure
}

func (*reverseDiagnosticTx) Rollback(context.Context) error { return nil }

func (db roleDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return roleRow(db)
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
