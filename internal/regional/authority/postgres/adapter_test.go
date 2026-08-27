package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestControlAdapterLocksRegionalAuthorityAndCommitsTheLocalProgram(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{rows: []pgx.Row{scanRow{values: []any{"region-a", int64(7), "active", true}}}}
	adapter, err := authoritypostgres.NewControl(&fakeBeginner{tx: tx})
	if err != nil {
		t.Fatalf("NewControl() error = %v", err)
	}

	var snapshot authority.Snapshot
	err = adapter.WithinTransaction(context.Background(), func(control authoritypostgres.ControlTx) error {
		var loadErr error
		snapshot, loadErr = control.RegionalAuthority(context.Background())
		return loadErr
	})
	if err != nil {
		t.Fatalf("WithinTransaction() error = %v", err)
	}
	if snapshot.Region().String() != "region-a" || snapshot.Epoch().Uint64() != 7 ||
		snapshot.State() != authority.StateActive || !snapshot.WritesEnabled() {
		t.Fatalf("snapshot = region %s epoch %d state %s writes %v", snapshot.Region(), snapshot.Epoch().Uint64(), snapshot.State(), snapshot.WritesEnabled())
	}
	if tx.commits != 1 || tx.rollbacks != 0 || len(tx.queries) != 1 ||
		!strings.Contains(tx.queries[0], "FOR UPDATE") || !strings.Contains(tx.queries[0], "regional_write_authority") {
		t.Fatalf("tx commits/rollbacks/queries = %d/%d/%v", tx.commits, tx.rollbacks, tx.queries)
	}
}

func TestCheckActiveReadinessBindsDeploymentAndPrimaryIdentity(t *testing.T) {
	t.Parallel()

	region, _ := authority.ParseRegion("region-b")
	epoch, _ := authority.NewEpoch(9)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	tests := []struct {
		name   string
		values []any
		wantOK bool
	}{
		{name: "matching primary", values: []any{"region-b", int64(9), "active", true, false}, wantOK: true},
		{name: "standby recovery", values: []any{"region-b", int64(9), "active", true, true}},
		{name: "wrong region", values: []any{"region-a", int64(9), "active", true, false}},
		{name: "stale epoch", values: []any{"region-b", int64(8), "active", true, false}},
		{name: "recovery authority", values: []any{"region-b", int64(9), "recovery", false, false}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := &fakeTx{rows: []pgx.Row{scanRow{values: test.values}}}
			err := authoritypostgres.CheckActiveReadiness(context.Background(), db, deployment)
			if (err == nil) != test.wantOK {
				t.Fatalf("CheckActiveReadiness() error = %v, wantOK %v", err, test.wantOK)
			}
			if len(db.queries) != 1 || !strings.Contains(db.queries[0], "pg_is_in_recovery") {
				t.Fatalf("queries = %v", db.queries)
			}
		})
	}
}

func TestControlWriterChecksAuthorityBeforeExposingTheSameSerializableTransaction(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{rows: []pgx.Row{scanRow{values: []any{"region-a", int64(7), "active", true}}}}
	beginner := &fakeBeginner{tx: tx}
	region, _ := authority.ParseRegion("region-a")
	epoch, _ := authority.NewEpoch(7)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	writer, err := authoritypostgres.NewControlWriter(
		beginner,
		deployment,
		pgx.TxOptions{IsoLevel: pgx.Serializable},
	)
	if err != nil {
		t.Fatalf("NewControlWriter() error = %v", err)
	}

	called := false
	err = writer.Write(context.Background(), func(got pgx.Tx) error {
		called = true
		if got != tx {
			t.Fatal("mutation received a different transaction")
		}
		if len(tx.queries) != 1 || !strings.Contains(tx.queries[0], "regional_write_authority") {
			t.Fatalf("queries before mutation = %v", tx.queries)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if !called || beginner.options.IsoLevel != pgx.Serializable || beginner.options.AccessMode != pgx.ReadWrite {
		t.Fatalf("called/options = %v/%+v", called, beginner.options)
	}
}

func TestShardAdapterLoadsRegionalAndGenerationFencesFromOneTransaction(t *testing.T) {
	t.Parallel()

	trainRunID := uuid.New()
	tx := &fakeTx{rows: []pgx.Row{
		scanRow{values: []any{"region-b", int64(9), "active", true}},
		scanRow{values: []any{trainRunID, int64(13), "active", true}},
	}}
	adapter, err := authoritypostgres.NewShard(&fakeBeginner{tx: tx}, sharding.ShardPhysicalZero)
	if err != nil {
		t.Fatalf("NewShard() error = %v", err)
	}

	err = adapter.WithinTransaction(context.Background(), func(shard authoritypostgres.ShardTx) error {
		if _, err := shard.RegionalAuthority(context.Background()); err != nil {
			return err
		}
		fence, err := shard.TrainRunFence(context.Background(), trainRunID)
		if err != nil {
			return err
		}
		if fence.TrainRunID() != trainRunID || fence.ShardID() != sharding.ShardPhysicalZero ||
			fence.Generation().Int64() != 13 || fence.State() != authority.ShardFenceActive {
			t.Fatalf("fence = train %s shard %s generation %d state %s", fence.TrainRunID(), fence.ShardID(), fence.Generation().Int64(), fence.State())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithinTransaction() error = %v", err)
	}
	if tx.commits != 1 || len(tx.queries) != 2 || !strings.Contains(tx.queries[1], "train_run_write_fences") ||
		!strings.Contains(tx.queries[1], "FOR UPDATE") {
		t.Fatalf("tx commits/queries = %d/%v", tx.commits, tx.queries)
	}
}

func TestExistingShardTransactionIsAuthorizedBeforeMutation(t *testing.T) {
	t.Parallel()

	trainRunID := uuid.New()
	generation, _ := sharding.NewAssignmentGeneration(13)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	region, _ := authority.ParseRegion("region-b")
	epoch, _ := authority.NewEpoch(9)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	tx := &fakeTx{rows: []pgx.Row{
		scanRow{values: []any{"region-b", int64(9), "active", true}},
		scanRow{values: []any{trainRunID, int64(13), "active", true}},
	}}

	if err := authoritypostgres.AuthorizeShardTransaction(context.Background(), tx, deployment, route); err != nil {
		t.Fatalf("AuthorizeShardTransaction() error = %v", err)
	}
	if len(tx.queries) != 2 || !strings.Contains(tx.queries[0], "regional_write_authority") ||
		!strings.Contains(tx.queries[1], "train_run_write_fences") {
		t.Fatalf("authorization query order = %v", tx.queries)
	}
}

func TestExistingControlTransactionRejectsStaleEpoch(t *testing.T) {
	t.Parallel()

	region, _ := authority.ParseRegion("region-a")
	epoch, _ := authority.NewEpoch(8)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	tx := &fakeTx{rows: []pgx.Row{scanRow{values: []any{"region-a", int64(7), "active", true}}}}

	err := authoritypostgres.AuthorizeControlTransaction(context.Background(), tx, deployment)
	if !errors.Is(err, authority.ErrEpochMismatch) {
		t.Fatalf("AuthorizeControlTransaction() error = %v, want ErrEpochMismatch", err)
	}
}

type fakeBeginner struct {
	tx      pgx.Tx
	options pgx.TxOptions
}

func (beginner *fakeBeginner) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	beginner.options = options
	return beginner.tx, nil
}

type fakeTx struct {
	pgx.Tx
	rows      []pgx.Row
	queries   []string
	commits   int
	rollbacks int
}

func (tx *fakeTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.queries = append(tx.queries, query)
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}

func (tx *fakeTx) Commit(context.Context) error {
	tx.commits++
	return nil
}

func (tx *fakeTx) Rollback(context.Context) error {
	tx.rollbacks++
	return nil
}

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
