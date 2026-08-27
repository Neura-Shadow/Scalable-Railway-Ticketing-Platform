package main

import (
	"context"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	recoverypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery/postgres"
	"github.com/google/uuid"
)

func TestRuntimeBackendPlansOnlyAfterIndependentTopologyValidation(t *testing.T) {
	t.Parallel()

	source := mustBackendRegion(t, "region-a")
	target := mustBackendRegion(t, "region-b")
	epoch, _ := authority.NewEpoch(7)
	store := &runtimePlanStore{}
	backend := &runtimeBackend{
		source: topologySource{evidence: backendTopology(t, recovery.DatabaseRolePrimary, source, epoch, true)},
		target: topologySource{evidence: backendTopology(t, recovery.DatabaseRoleStandby, source, epoch, true)},
		store:  store,
		now:    func() time.Time { return time.Date(2026, 8, 11, 15, 0, 0, 0, time.UTC) },
	}
	req := request{
		Command: "failover", OperationID: uuid.New(), IncidentID: uuid.New(), From: source, To: target,
		SourceEpoch: epoch, OperatorID: "operator:test", Reason: "region_failure", DryRun: true,
	}

	got, err := backend.Execute(context.Background(), req)
	if err != nil || store.planCalls != 0 || got.Stage != recovery.StagePlanned || got.Region != "region-b" || got.Epoch != 7 {
		t.Fatalf("Execute(dry-run) = %+v, calls=%d, error=%v", got, store.planCalls, err)
	}
	req.DryRun, req.Confirm = false, true
	got, err = backend.Execute(context.Background(), req)
	if err != nil || store.planCalls != 1 || got.OperationID != req.OperationID || got.Stage != recovery.StagePlanned {
		t.Fatalf("Execute(confirm) = %+v, calls=%d, error=%v", got, store.planCalls, err)
	}
}

func TestRuntimeBackendRejectsTargetThatIsAlreadyWritable(t *testing.T) {
	t.Parallel()

	source := mustBackendRegion(t, "region-a")
	target := mustBackendRegion(t, "region-b")
	epoch, _ := authority.NewEpoch(7)
	store := &runtimePlanStore{}
	backend := &runtimeBackend{
		source: topologySource{evidence: backendTopology(t, recovery.DatabaseRolePrimary, source, epoch, true)},
		target: topologySource{evidence: backendTopology(t, recovery.DatabaseRolePrimary, source, epoch, true)},
		store:  store,
		now:    time.Now,
	}
	_, err := backend.Execute(context.Background(), request{
		Command: "failover", OperationID: uuid.New(), IncidentID: uuid.New(), From: source, To: target,
		SourceEpoch: epoch, OperatorID: "operator:test", Reason: "region_failure", DryRun: true,
	})
	if err == nil || store.planCalls != 0 {
		t.Fatalf("Execute(writable target) error/calls = %v/%d", err, store.planCalls)
	}
}

func TestOpenBackendBuildsFixedRegionPoolsWithoutConnecting(t *testing.T) {
	t.Parallel()

	source := mustBackendRegion(t, "region-a")
	target := mustBackendRegion(t, "region-b")
	epoch, _ := authority.NewEpoch(7)
	operator := "operator:test"
	values := map[string]string{
		"DR_ADMIN_ENABLED": "true", "DR_ALLOWED_OPERATOR_ID": operator,
		"DR_RECOVERY_EPOCH": "7",
	}
	for _, region := range []string{"REGION_A", "REGION_B"} {
		for _, database := range []string{"CONTROL", "SHARD_0", "SHARD_1"} {
			values["DR_"+region+"_"+database+"_DATABASE_URL"] =
				"postgres://runtime:secret@127.0.0.1:1/" + region + "_" + database + "?sslmode=disable"
		}
	}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	service, closeBackend, err := openBackend(context.Background(), lookup, request{
		Command: "failover", From: source, To: target, SourceEpoch: epoch, OperatorID: operator,
	})
	if err != nil || service == nil || closeBackend == nil {
		t.Fatalf("openBackend() service/close/error = %T/%v/%v", service, closeBackend != nil, err)
	}
	closeBackend()
}

func TestOpenAdminPoolInstallsRecoveryAuthorityRuntimeParameters(t *testing.T) {
	t.Parallel()

	region := mustBackendRegion(t, "region-a")
	epoch, _ := authority.NewEpoch(7)
	pool, err := openAdminPool(
		context.Background(),
		"postgres://runtime:secret@127.0.0.1:1/control?sslmode=disable",
		databaseRuntimeAuthority{region: region, epoch: epoch, role: "recovery", writes: false},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	parameters := pool.Config().ConnConfig.RuntimeParams
	for key, want := range map[string]string{
		"railway.deployment_region":       "region-a",
		"railway.deployment_role":         "recovery",
		"railway.region_epoch":            "7",
		"railway.regional_writes_enabled": "false",
	} {
		if got := parameters[key]; got != want {
			t.Fatalf("runtime parameter %s = %q, want %q", key, got, want)
		}
	}
}

func TestNonemptyEnvRejectsTrimmedControlCharactersAndOversizedValues(t *testing.T) {
	t.Parallel()

	for name, raw := range map[string]string{
		"newline": "postgres://runtime/db\n",
		"nul":     "postgres://runtime/db\x00suffix",
		"large":   string(make([]byte, 4097)),
	} {
		t.Run(name, func(t *testing.T) {
			lookup := func(string) (string, bool) { return raw, true }
			if _, ok := nonemptyEnv(lookup, "DATABASE_URL"); ok {
				t.Fatalf("nonemptyEnv(%s) accepted unsafe value", name)
			}
		})
	}
}

func TestCrashAfterLoadHookRequiresExplicitTestGatesAndExactStage(t *testing.T) {
	values := map[string]string{
		"APP_ENV":                                "production",
		"DR_ADMIN_TEST_CRASH_AFTER_LOAD_ENABLED": "true",
		"DR_ADMIN_TEST_CRASH_AFTER_LOAD_STAGE":   "control_promoted",
	}
	lookup := func(name string) (string, bool) { value, ok := values[name]; return value, ok }
	exitCode := 0
	exit := func(code int) { exitCode = code }
	if hook := newTestCrashAfterLoadHook(lookup, exit); hook != nil {
		t.Fatal("production environment enabled the crash-after-load hook")
	}
	values["APP_ENV"] = "test"
	hook := newTestCrashAfterLoadHook(lookup, exit)
	if hook == nil {
		t.Fatal("explicit test gates did not enable the crash-after-load hook")
	}
	hook(recovery.StagePositionsRecorded)
	if exitCode != 0 {
		t.Fatalf("wrong stage exit code = %d", exitCode)
	}
	hook(recovery.StageControlPromoted)
	if exitCode != 86 {
		t.Fatalf("matching stage exit code = %d, want 86", exitCode)
	}
	values["DR_ADMIN_TEST_CRASH_AFTER_LOAD_STAGE"] = "not-a-stage"
	if hook := newTestCrashAfterLoadHook(lookup, exit); hook != nil {
		t.Fatal("unknown stage enabled the crash-after-load hook")
	}
}

type topologySource struct {
	evidence recovery.DatabaseSet[recoverypostgres.DatabaseObservation]
	err      error
}

func (source topologySource) Observe(context.Context) (recovery.DatabaseSet[recoverypostgres.DatabaseObservation], error) {
	return source.evidence, source.err
}

type runtimePlanStore struct {
	planCalls int
	planned   recoverypostgres.PlannedOperation
}

func (store *runtimePlanStore) Plan(_ context.Context, operation recovery.Failover, metadata recoverypostgres.Metadata, _ time.Time) (recoverypostgres.PlannedOperation, bool, error) {
	store.planCalls++
	store.planned = recoverypostgres.PlannedOperation{Operation: operation, Version: 1, Metadata: metadata, PlannedTargetEpoch: metadata.PlannedTargetEpoch}
	return store.planned, true, nil
}

func (store *runtimePlanStore) LoadPlan(context.Context, uuid.UUID) (recoverypostgres.PlannedOperation, error) {
	return store.planned, nil
}

func backendTopology(t *testing.T, role recovery.DatabaseRole, region authority.Region, epoch authority.Epoch, writes bool) recovery.DatabaseSet[recoverypostgres.DatabaseObservation] {
	t.Helper()
	snapshot, err := authority.NewSnapshot(region, epoch, authority.StateActive, writes)
	if err != nil {
		t.Fatal(err)
	}
	position := func(timeline uint32, wal uint64) recovery.ReplicationPosition {
		value, positionErr := recovery.NewReplicationPosition(timeline, wal)
		if positionErr != nil {
			t.Fatal(positionErr)
		}
		return value
	}
	return recovery.NewDatabaseSet(
		recoverypostgres.DatabaseObservation{Database: recovery.DatabaseControl, Role: role, Position: position(3, 100), Authority: snapshot, SchemaVersion: 11},
		recoverypostgres.DatabaseObservation{Database: recovery.DatabaseShard0, Role: role, Position: position(4, 200), Authority: snapshot, SchemaVersion: 3},
		recoverypostgres.DatabaseObservation{Database: recovery.DatabaseShard1, Role: role, Position: position(5, 300), Authority: snapshot, SchemaVersion: 3},
	)
}

func mustBackendRegion(t *testing.T, raw string) authority.Region {
	t.Helper()
	region, err := authority.ParseRegion(raw)
	if err != nil {
		t.Fatal(err)
	}
	return region
}
