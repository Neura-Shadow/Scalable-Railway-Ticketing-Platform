package postgres_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	recoverypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery/postgres"
	"github.com/jackc/pgx/v5"
)

func TestTopologyObserverReturnsBoundedIndependentDatabaseEvidence(t *testing.T) {
	t.Parallel()

	observer, err := recoverypostgres.NewTopologyObserver(recovery.NewDatabaseSet[recoverypostgres.ObservationDB](
		observationDB{row: observationRow(false, "3/00000064", int64(3), "region-a", int64(7), "active", true, 11, false)},
		observationDB{row: observationRow(false, "4/000000C8", int64(4), "region-a", int64(7), "active", true, 3, false)},
		observationDB{row: observationRow(true, "5/0000012C", int64(5), "region-a", int64(7), "active", true, 3, false)},
	))
	if err != nil {
		t.Fatalf("NewTopologyObserver() error = %v", err)
	}

	evidence, err := observer.Observe(context.Background())
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if evidence.Control().Role != recovery.DatabaseRolePrimary || evidence.Control().Position.WAL() != 3<<32|100 ||
		evidence.Shard0().SchemaVersion != 3 || evidence.Shard1().Role != recovery.DatabaseRoleStandby {
		t.Fatalf("evidence = %+v", evidence)
	}
	if evidence.Control().Authority.Region().String() != "region-a" ||
		evidence.Control().Authority.Epoch().Uint64() != 7 || !evidence.Control().Authority.WritesEnabled() {
		t.Fatalf("control authority = %+v", evidence.Control().Authority)
	}
}

func TestTopologyObserverRejectsMalformedLSNAndAuthority(t *testing.T) {
	t.Parallel()

	observer, err := recoverypostgres.NewTopologyObserver(recovery.NewDatabaseSet[recoverypostgres.ObservationDB](
		observationDB{row: observationRow(false, "not-an-lsn", int64(3), "region-a", int64(7), "active", true, 11, false)},
		observationDB{row: observationRow(false, "4/1", int64(4), "region-a", int64(7), "active", true, 3, false)},
		observationDB{row: observationRow(false, "5/1", int64(5), "region-a", int64(7), "active", true, 3, false)},
	))
	if err != nil {
		t.Fatalf("NewTopologyObserver() error = %v", err)
	}
	if _, err := observer.Observe(context.Background()); err == nil {
		t.Fatal("Observe() accepted malformed database evidence")
	}
}

func observationRow(inRecovery bool, lsn string, timeline int64, region string, epoch int64, state string, writes bool, version int, dirty bool) pgx.Row {
	return observationScanRow{values: []any{inRecovery, lsn, timeline, region, epoch, state, writes, version, dirty}}
}

type observationDB struct{ row pgx.Row }

func (db observationDB) QueryRow(context.Context, string, ...any) pgx.Row { return db.row }

type observationScanRow struct {
	values []any
	err    error
}

func (row observationScanRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range destinations {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
