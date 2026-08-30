package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	recoverypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestTargetActivationCASIsAtomicInPostgres(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin PostgreSQL transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var sourceRegionText string
	var sourceEpochRaw int64
	if err := tx.QueryRow(ctx, `SELECT region,epoch FROM public.regional_write_authority WHERE singleton FOR UPDATE`).Scan(&sourceRegionText, &sourceEpochRaw); err != nil {
		t.Fatalf("load regional authority: %v", err)
	}
	sourceRegion, _ := authority.ParseRegion(sourceRegionText)
	targetRegionText := "region-b"
	if sourceRegionText == targetRegionText {
		targetRegionText = "region-a"
	}
	targetRegion, _ := authority.ParseRegion(targetRegionText)
	sourceEpoch, _ := authority.NewEpoch(uint64(sourceEpochRaw))
	targetEpoch, _ := authority.NewEpoch(uint64(sourceEpochRaw + 1))
	setStoreRecoveryContext(t, ctx, tx, targetRegionText, int64(targetEpoch.Uint64()))
	if _, err := tx.Exec(ctx, `
UPDATE public.regional_write_authority
SET region=$1,epoch=$2,state='recovery',writes_enabled=false
WHERE singleton`, targetRegionText, int64(targetEpoch.Uint64())); err != nil {
		t.Fatalf("install recovery authority: %v", err)
	}

	store, err := recoverypostgres.New(tx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	operation := mustStoreOperationAtCustomerWrites(t, uuid.New(), sourceRegion, targetRegion, sourceEpoch, targetEpoch, now)
	if err := store.Create(ctx, operation, recoverypostgres.Metadata{
		Kind: recoverypostgres.OperationFailover, ReasonCategory: "integration_test",
	}, now); err != nil {
		t.Fatalf("Create(customer_writes_configured) error = %v", err)
	}
	active := mustStoreSnapshot(t, targetRegion, targetEpoch, authority.StateActive, true)
	activated, err := recovery.Advance(operation, recovery.TargetActivated{
		Authorities: recovery.NewAuthoritySet(active, active, active), ObservedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	version, err := store.Save(ctx, 1, activated, now.Add(2*time.Second))
	if err != nil || version != 2 {
		t.Fatalf("Save(target_active) version/error = %d/%v", version, err)
	}
	assertStoreState(t, ctx, tx, operation.Binding().OperationID(), "target_active", 2, targetRegionText, int64(targetEpoch.Uint64()), "active", true)

	if _, err := tx.Exec(ctx, `UPDATE public.regional_write_authority SET state='recovery',writes_enabled=false WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Save(ctx, 1, activated, now.Add(3*time.Second)); !errors.Is(err, recoverypostgres.ErrCheckpointConflict) {
		t.Fatalf("stale Save(target_active) error = %v, want ErrCheckpointConflict", err)
	}
	assertStoreState(t, ctx, tx, operation.Binding().OperationID(), "target_active", 2, targetRegionText, int64(targetEpoch.Uint64()), "recovery", false)

	authorityEpoch := int64(targetEpoch.Uint64() + 1)
	setStoreRecoveryContext(t, ctx, tx, targetRegionText, authorityEpoch)
	if _, err := tx.Exec(ctx, `
UPDATE public.regional_write_authority
SET epoch=$1,state='recovery',writes_enabled=false
WHERE singleton`, authorityEpoch); err != nil {
		t.Fatal(err)
	}
	mismatchSourceEpoch := mustStoreEpoch(t, uint64(authorityEpoch))
	mismatchTargetEpoch := mustStoreEpoch(t, uint64(authorityEpoch+1))
	mismatch := mustStoreOperationAtCustomerWrites(t, uuid.New(), targetRegion, sourceRegion, mismatchSourceEpoch, mismatchTargetEpoch, now.Add(4*time.Second))
	if err := store.Create(ctx, mismatch, recoverypostgres.Metadata{
		Kind: recoverypostgres.OperationFailover, ReasonCategory: "integration_mismatch",
	}, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	mismatchActive := mustStoreSnapshot(t, sourceRegion, mismatchTargetEpoch, authority.StateActive, true)
	mismatchActivated, err := recovery.Advance(mismatch, recovery.TargetActivated{
		Authorities: recovery.NewAuthoritySet(mismatchActive, mismatchActive, mismatchActive), ObservedAt: now.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Save(ctx, 1, mismatchActivated, now.Add(6*time.Second)); !errors.Is(err, recoverypostgres.ErrCheckpointConflict) {
		t.Fatalf("mismatched-authority Save(target_active) error = %v, want ErrCheckpointConflict", err)
	}
	assertStoreState(t, ctx, tx, mismatch.Binding().OperationID(), "customer_writes_configured", 1, targetRegionText, authorityEpoch, "recovery", false)

	nonTarget, err := recovery.NewFailover(uuid.New(), targetRegion, sourceRegion, mustStoreEpoch(t, uint64(authorityEpoch)), uuid.New(), "operator:integration", now.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, nonTarget, recoverypostgres.Metadata{
		Kind: recoverypostgres.OperationFailover, ReasonCategory: "integration_non_target",
	}, now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	nonTarget, err = recovery.Advance(nonTarget, recovery.ExternalFencingVerified{
		Attestation: mustStoreAttestation(t, nonTarget.Binding(), now.Add(8*time.Second)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Save(ctx, 1, nonTarget, now.Add(9*time.Second)); err != nil {
		t.Fatalf("Save(non-target stage) error = %v", err)
	}
	assertStoreState(t, ctx, tx, nonTarget.Binding().OperationID(), "external_fencing_verified", 2, targetRegionText, authorityEpoch, "recovery", false)
}

func assertStoreState(t *testing.T, ctx context.Context, tx pgx.Tx, operationID uuid.UUID, wantStage string, wantVersion int64, wantRegion string, wantEpoch int64, wantAuthorityState string, wantWrites bool) {
	t.Helper()
	var stage, region, authorityState string
	var version, epoch int64
	var writes bool
	if err := tx.QueryRow(ctx, `
SELECT operation.stage,operation.checkpoint_version,
       authority.region,authority.epoch,authority.state,authority.writes_enabled
FROM public.regional_failover_operations AS operation
CROSS JOIN public.regional_write_authority AS authority
WHERE operation.operation_id=$1 AND authority.singleton`, operationID).Scan(
		&stage, &version, &region, &epoch, &authorityState, &writes,
	); err != nil {
		t.Fatalf("load operation/authority state: %v", err)
	}
	if stage != wantStage || version != wantVersion || region != wantRegion || epoch != wantEpoch || authorityState != wantAuthorityState || writes != wantWrites {
		t.Fatalf("operation/authority = %s/%d %s/%d/%s/%t, want %s/%d %s/%d/%s/%t",
			stage, version, region, epoch, authorityState, writes,
			wantStage, wantVersion, wantRegion, wantEpoch, wantAuthorityState, wantWrites)
	}
}

func setStoreRecoveryContext(t *testing.T, ctx context.Context, tx pgx.Tx, region string, epoch int64) {
	t.Helper()
	if _, err := tx.Exec(ctx, `
SELECT set_config('railway.deployment_region',$1,true),
       set_config('railway.deployment_role','recovery',true),
       set_config('railway.region_epoch',$2,true),
       set_config('railway.regional_writes_enabled','false',true)`, region, fmt.Sprint(epoch)); err != nil {
		t.Fatalf("set recovery transaction context: %v", err)
	}
}

func mustStoreOperationAtCustomerWrites(t *testing.T, operationID uuid.UUID, source, target authority.Region, sourceEpoch, targetEpoch authority.Epoch, declaredAt time.Time) recovery.Failover {
	t.Helper()
	operation, err := recovery.NewFailover(operationID, source, target, sourceEpoch, uuid.New(), "operator:integration", declaredAt)
	if err != nil {
		t.Fatal(err)
	}
	positions := recovery.NewDatabaseSet(mustStorePosition(t, 1, 100), mustStorePosition(t, 2, 200), mustStorePosition(t, 3, 300))
	promotions := recovery.NewDatabaseSet(mustStorePosition(t, 2, 100), mustStorePosition(t, 3, 200), mustStorePosition(t, 4, 300))
	recoverySnapshot := mustStoreSnapshot(t, target, targetEpoch, authority.StateRecovery, false)
	steps := []recovery.Evidence{
		recovery.ExternalFencingVerified{Attestation: mustStoreAttestation(t, operation.Binding(), declaredAt.Add(time.Second))},
		recovery.PositionsRecorded{Positions: positions},
		recovery.PassiveReadinessRemoved{Observation: recovery.HashObservation([]byte("passive-readiness"))},
		recovery.DatabasePromoted{Database: recovery.DatabaseControl, Position: promotions.Control()},
		recovery.DatabasePromoted{Database: recovery.DatabaseShard0, Position: promotions.Shard0()},
		recovery.DatabasePromoted{Database: recovery.DatabaseShard1, Position: promotions.Shard1()},
		recovery.RolesAndTimelinesVerified{Databases: recovery.NewDatabaseSet(
			recovery.DatabaseVerification{Role: recovery.DatabaseRolePrimary, Timeline: 2},
			recovery.DatabaseVerification{Role: recovery.DatabaseRolePrimary, Timeline: 3},
			recovery.DatabaseVerification{Role: recovery.DatabaseRolePrimary, Timeline: 4},
		)},
		recovery.EpochAllocated{Epoch: targetEpoch},
		recovery.ControlRecoveryInstalled{Authority: recoverySnapshot},
		recovery.ShardAuthoritiesInstalled{Authorities: recovery.NewShardAuthoritySet(recoverySnapshot, recoverySnapshot)},
		recovery.RecoveryAPIsStarted{Observation: recovery.HashObservation([]byte("recovery-apis"))},
		recovery.ReconciliationPassed{Control: true, Shards: true, Payments: true, Tickets: true, Refunds: true, Ledger: true, Routing: true, Observation: recovery.HashObservation([]byte("reconciliation"))},
		recovery.PaymentWorkersEnabled{Observation: recovery.HashObservation([]byte("payment-workers"))},
		recovery.SettlementWorkersEnabled{Observation: recovery.HashObservation([]byte("settlement-workers"))},
		recovery.IngressSwitched{Webhook: true, Global: true, Observation: recovery.HashObservation([]byte("ingress"))},
		recovery.CustomerWritesConfigured{Enabled: true, ReadinessGated: true, Observation: recovery.HashObservation([]byte("customer-writes"))},
	}
	for _, step := range steps {
		operation, err = recovery.Advance(operation, step)
		if err != nil {
			t.Fatal(err)
		}
	}
	return operation
}

func mustStorePosition(t *testing.T, timeline uint32, wal uint64) recovery.ReplicationPosition {
	t.Helper()
	position, err := recovery.NewReplicationPosition(timeline, wal)
	if err != nil {
		t.Fatal(err)
	}
	return position
}

func mustStoreSnapshot(t *testing.T, region authority.Region, epoch authority.Epoch, state authority.State, writes bool) authority.Snapshot {
	t.Helper()
	snapshot, err := authority.NewSnapshot(region, epoch, state, writes)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func mustStoreEpoch(t *testing.T, value uint64) authority.Epoch {
	t.Helper()
	epoch, err := authority.NewEpoch(value)
	if err != nil {
		t.Fatal(err)
	}
	return epoch
}
