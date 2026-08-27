package postgres_test

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	recoverypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestCheckpointStoreCreatesLoadsAndCASUpdatesTypedFailover(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	operationID := uuid.New()
	operation := mustFailover(t, operationID, now)
	db := &checkpointDB{tags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("UPDATE 1")}}
	store, err := recoverypostgres.New(db)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := store.Create(context.Background(), operation, recoverypostgres.Metadata{
		Kind: recoverypostgres.OperationFailover, ReasonCategory: "region_failure",
	}, now); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !containsSQL(db.execSQL[0], "regional_failover_operations") || !containsSQL(db.execSQL[0], "checkpoint") ||
		!containsSQL(db.execSQL[0], "jsonb_build_object($10::text,$13::timestamptz)") {
		t.Fatalf("create SQL = %s", db.execSQL[0])
	}
	payload, ok := db.execArgs[0][10].([]byte)
	if !ok || len(payload) == 0 {
		t.Fatalf("checkpoint payload argument = %T", db.execArgs[0][10])
	}
	db.row = scanRow{values: []any{payload, int64(1)}}
	loaded, version, err := store.Load(context.Background(), operationID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if version != 1 || loaded.Stage() != recovery.StagePlanned || loaded.Binding().OperationID() != operationID {
		t.Fatalf("loaded version/stage/id = %d/%s/%s", version, loaded.Stage(), loaded.Binding().OperationID())
	}

	attestation := mustStoreAttestation(t, loaded.Binding(), now.Add(time.Minute))
	advanced, err := recovery.Advance(loaded, recovery.ExternalFencingVerified{Attestation: attestation})
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	nextVersion, err := store.Save(context.Background(), version, advanced, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if nextVersion != 2 || !containsSQL(db.execSQL[1], "checkpoint_version=$2") ||
		!containsSQL(db.execSQL[1], "phase_timestamps") ||
		!containsSQL(db.execSQL[1], "jsonb_build_object($3::text,$6::timestamptz)") {
		t.Fatalf("save version/SQL = %d/%s", nextVersion, db.execSQL[1])
	}
}

func TestCheckpointStoreRejectsLostCASOwnership(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 11, 0, 0, 0, time.UTC)
	db := &checkpointDB{tags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}
	store, err := recoverypostgres.New(db)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = store.Save(context.Background(), 4, mustFailover(t, uuid.New(), now), now)
	if !errors.Is(err, recoverypostgres.ErrCheckpointConflict) {
		t.Fatalf("Save() error = %v, want ErrCheckpointConflict", err)
	}
}

func TestCheckpointStorePlansOnceAndReturnsAnIdempotentExistingPlan(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	operationID := uuid.New()
	operation := mustFailover(t, operationID, now)
	metadata := recoverypostgres.Metadata{Kind: recoverypostgres.OperationFailover, ReasonCategory: "region_failure"}
	db := &checkpointDB{tags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("INSERT 0 0")}}
	store, err := recoverypostgres.New(db)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	planned, created, err := store.Plan(context.Background(), operation, metadata, now)
	if err != nil || !created || planned.Version != 1 || planned.Operation.Stage() != recovery.StagePlanned {
		t.Fatalf("Plan(first) = %+v/%v/%v", planned, created, err)
	}
	if !containsSQL(db.execSQL[0], "jsonb_build_object($10::text,$13::timestamptz)") {
		t.Fatalf("plan SQL leaves polymorphic JSON parameters untyped: %s", db.execSQL[0])
	}
	payload := db.execArgs[0][10].([]byte)
	db.row = scanRow{values: []any{payload, int64(1), string(recoverypostgres.OperationFailover), "region_failure", (*int64)(nil)}}

	planned, created, err = store.Plan(context.Background(), operation, metadata, now.Add(time.Minute))
	if err != nil || created || planned.Operation.Binding().OperationID() != operationID || planned.Version != 1 {
		t.Fatalf("Plan(retry) = %+v/%v/%v", planned, created, err)
	}
}

func mustFailover(t *testing.T, operationID uuid.UUID, declaredAt time.Time) recovery.Failover {
	t.Helper()
	source, _ := authority.ParseRegion("region-a")
	target, _ := authority.ParseRegion("region-b")
	epoch, _ := authority.NewEpoch(1)
	operation, err := recovery.NewFailover(
		operationID,
		source,
		target,
		epoch,
		uuid.New(),
		"operator:test",
		declaredAt,
	)
	if err != nil {
		t.Fatalf("NewFailover() error = %v", err)
	}
	return operation
}

func mustStoreAttestation(t *testing.T, binding recovery.FenceBinding, at time.Time) recovery.FencingAttestation {
	t.Helper()
	hashes := recovery.ObservationHashes{
		Ingress:         recovery.HashObservation([]byte("ingress")),
		Processes:       recovery.HashObservation([]byte("processes")),
		Credentials:     recovery.HashObservation([]byte("credentials")),
		DatabaseNetwork: recovery.HashObservation([]byte("database-network")),
	}
	seed := sha256.Sum256([]byte("regional-store-test-fencing-key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	attestation, err := recovery.NewFencingAttestation(binding, recovery.FencingPurposeInitial, at, at.Add(5*time.Minute), "test-fence-authority", "test-key-1", "test-nonce-00000002", hashes, make([]byte, ed25519.SignatureSize))
	if err == nil {
		attestation, err = recovery.NewFencingAttestation(binding, recovery.FencingPurposeInitial, at, at.Add(5*time.Minute), "test-fence-authority", "test-key-1", "test-nonce-00000002", hashes, ed25519.Sign(privateKey, attestation.CanonicalPayload()))
	}
	if err != nil {
		t.Fatalf("NewFencingAttestation() error = %v", err)
	}
	return attestation
}

func containsSQL(sql, fragment string) bool {
	return strings.Contains(sql, fragment)
}

type checkpointDB struct {
	tags     []pgconn.CommandTag
	row      pgx.Row
	execSQL  []string
	execArgs [][]any
}

func (db *checkpointDB) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	db.execSQL = append(db.execSQL, sql)
	db.execArgs = append(db.execArgs, arguments)
	tag := db.tags[0]
	db.tags = db.tags[1:]
	return tag, nil
}

func (db *checkpointDB) QueryRow(context.Context, string, ...any) pgx.Row { return db.row }

type scanRow struct {
	values []any
	err    error
}

func (row scanRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range destinations {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
