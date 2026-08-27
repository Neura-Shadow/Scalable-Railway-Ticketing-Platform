package main

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	recoverypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errTopologyPrecondition = errors.New("regional topology precondition failed")

type topologyObserver interface {
	Observe(context.Context) (recovery.DatabaseSet[recoverypostgres.DatabaseObservation], error)
}

type recoveryPlanStore interface {
	Plan(context.Context, recovery.Failover, recoverypostgres.Metadata, time.Time) (recoverypostgres.PlannedOperation, bool, error)
	LoadPlan(context.Context, uuid.UUID) (recoverypostgres.PlannedOperation, error)
}

type databaseRuntimeAuthority struct {
	region authority.Region
	epoch  authority.Epoch
	role   authority.Role
	writes bool
}

// runtimeBackend deliberately stops at durable planning and independent
// observation. External fencing, PostgreSQL promotion/reseed, process control,
// and ingress changes remain separately authorized operator actions.
type runtimeBackend struct {
	source    topologyObserver
	target    topologyObserver
	store     recoveryPlanStore
	now       func() time.Time
	afterLoad func(recovery.Stage)
	verifier  recovery.FencingVerifier
}

func (backend *runtimeBackend) Execute(ctx context.Context, req request) (result, error) {
	if backend == nil || backend.store == nil || backend.now == nil || ctx == nil {
		return result{}, errRuntimeWiring
	}
	switch req.Command {
	case "failover", "prepare-failback", "failback":
		if err := backend.validateTopology(ctx, req.From, req.SourceEpoch); err != nil {
			return result{}, err
		}
		operation, metadata, err := operationForRequest(req, backend.now().UTC())
		if err != nil {
			return result{}, err
		}
		planned := recoverypostgres.PlannedOperation{
			Operation: operation, Version: 1, Metadata: metadata, PlannedTargetEpoch: metadata.PlannedTargetEpoch,
		}
		if !req.DryRun {
			planned, _, err = backend.store.Plan(ctx, operation, metadata, backend.now().UTC())
			if err != nil {
				return result{}, err
			}
		}
		return plannedResult(planned), nil
	case "reseed-region":
		planned, err := backend.store.LoadPlan(ctx, req.OperationID)
		if err != nil || planned.Metadata.Kind != recoverypostgres.OperationFailback ||
			planned.Operation.Binding().Source() != req.From || planned.Operation.Target() != req.To {
			return result{}, errTopologyPrecondition
		}
		if err := backend.validateTopology(ctx, req.From, planned.Operation.Binding().SourceEpoch()); err != nil {
			return result{}, err
		}
		return plannedResult(planned), nil
	case "validate-failback":
		planned, err := backend.store.LoadPlan(ctx, req.OperationID)
		if err != nil || planned.Metadata.Kind != recoverypostgres.OperationFailback ||
			planned.PlannedTargetEpoch.Uint64() == 0 {
			return result{}, errTopologyPrecondition
		}
		document, err := readEvidenceFile(req.EvidenceFile)
		if err != nil {
			return result{}, errTopologyPrecondition
		}
		if _, err := recovery.DecodeFailbackValidationDocument(planned.Operation, planned.PlannedTargetEpoch, document, backend.verifier); err != nil {
			return result{}, errTopologyPrecondition
		}
		return plannedResult(planned), nil
	case "advance-phase":
		checkpointStore, ok := backend.store.(recovery.CheckpointStore)
		if !ok {
			return result{}, errRuntimeWiring
		}
		document, err := readEvidenceFile(req.EvidenceFile)
		if err != nil {
			return result{}, errTopologyPrecondition
		}
		orchestrator, err := recovery.NewOrchestrator(checkpointStore, backend.now, backend.verifier)
		if err != nil {
			return result{}, errRuntimeWiring
		}
		advanced, version, err := orchestrator.AdvanceNext(ctx, req.OperationID, recovery.PhaseActionFunc(
			func(_ context.Context, operation recovery.Failover) (recovery.Evidence, error) {
				evidence, decodeErr := recovery.DecodeEvidenceDocument(operation, document, backend.verifier)
				if decodeErr == nil && backend.afterLoad != nil {
					backend.afterLoad(recovery.Stage(operation.Stage() + 1))
				}
				return evidence, decodeErr
			},
		))
		if err != nil {
			return result{}, err
		}
		return result{OperationID: req.OperationID, Stage: advanced.Stage(), Region: advanced.Target().String(),
			Epoch: advanced.TargetEpoch().Uint64(), Version: version}, nil
	case "refresh-fence":
		checkpointStore, ok := backend.store.(recovery.CheckpointStore)
		if !ok {
			return result{}, errRuntimeWiring
		}
		document, err := readEvidenceFile(req.EvidenceFile)
		if err != nil {
			return result{}, errTopologyPrecondition
		}
		loaded, _, err := checkpointStore.Load(ctx, req.OperationID)
		if err != nil {
			return result{}, errTopologyPrecondition
		}
		attestation, err := recovery.DecodeFenceRefreshDocument(loaded, document, backend.verifier)
		if err != nil {
			return result{}, errTopologyPrecondition
		}
		orchestrator, err := recovery.NewOrchestrator(checkpointStore, backend.now, backend.verifier)
		if err != nil {
			return result{}, errRuntimeWiring
		}
		refreshed, version, err := orchestrator.RefreshFence(ctx, req.OperationID, attestation)
		if err != nil {
			return result{}, err
		}
		return result{OperationID: req.OperationID, Stage: refreshed.Stage(), Region: refreshed.Target().String(), Epoch: refreshed.TargetEpoch().Uint64(), Version: version}, nil
	case "verify-fence":
		checkpointStore, ok := backend.store.(recovery.CheckpointStore)
		if !ok {
			return result{}, errRuntimeWiring
		}
		loaded, version, err := checkpointStore.Load(ctx, req.OperationID)
		if err != nil || loaded.ValidateFreshFence(backend.verifier) != nil {
			return result{}, errTopologyPrecondition
		}
		return result{OperationID: req.OperationID, Stage: loaded.Stage(), Region: loaded.Target().String(), Epoch: loaded.TargetEpoch().Uint64(), Version: version}, nil
	default:
		return result{}, errArguments
	}
}

func operationForRequest(req request, now time.Time) (recovery.Failover, recoverypostgres.Metadata, error) {
	operation, err := recovery.NewFailover(
		req.OperationID, req.From, req.To, req.SourceEpoch, req.IncidentID, req.OperatorID, now,
	)
	if err != nil {
		return recovery.Failover{}, recoverypostgres.Metadata{}, errTopologyPrecondition
	}
	metadata := recoverypostgres.Metadata{
		Kind: recoverypostgres.OperationFailover, ReasonCategory: req.Reason,
	}
	if req.Command == "prepare-failback" || req.Command == "failback" {
		metadata.Kind = recoverypostgres.OperationFailback
		metadata.PlannedTargetEpoch = req.TargetEpoch
	}
	return operation, metadata, nil
}

func (backend *runtimeBackend) validateTopology(ctx context.Context, sourceRegion authority.Region, sourceEpoch authority.Epoch) error {
	if backend.source == nil || backend.target == nil || sourceEpoch.Uint64() == 0 {
		return errRuntimeWiring
	}
	source, err := backend.source.Observe(ctx)
	if err != nil {
		return errTopologyPrecondition
	}
	target, err := backend.target.Observe(ctx)
	if err != nil {
		return errTopologyPrecondition
	}
	return source.Visit(func(database recovery.Database, current recoverypostgres.DatabaseObservation) error {
		standby, valueErr := target.Value(database)
		if valueErr != nil || current.Database != database || standby.Database != database ||
			current.Role != recovery.DatabaseRolePrimary || standby.Role != recovery.DatabaseRoleStandby ||
			current.SchemaDirty || standby.SchemaDirty || current.SchemaVersion != expectedSchemaVersion(database) ||
			standby.SchemaVersion != current.SchemaVersion ||
			!matchesSourceAuthority(current.Authority, sourceRegion, sourceEpoch) ||
			!matchesSourceAuthority(standby.Authority, sourceRegion, sourceEpoch) ||
			standby.Position.Timeline() != current.Position.Timeline() ||
			standby.Position.WAL() > current.Position.WAL() {
			return errTopologyPrecondition
		}
		return nil
	})
}

func expectedSchemaVersion(database recovery.Database) int {
	if database == recovery.DatabaseControl {
		return 11
	}
	return 3
}

func matchesSourceAuthority(snapshot authority.Snapshot, region authority.Region, epoch authority.Epoch) bool {
	return snapshot.Region() == region && snapshot.Epoch() == epoch &&
		snapshot.State() == authority.StateActive && snapshot.WritesEnabled()
}

func plannedResult(planned recoverypostgres.PlannedOperation) result {
	epoch := planned.Operation.Binding().SourceEpoch()
	if planned.PlannedTargetEpoch.Uint64() > 0 {
		epoch = planned.PlannedTargetEpoch
	}
	return result{
		OperationID: planned.Operation.Binding().OperationID(), Stage: planned.Operation.Stage(),
		Region: planned.Operation.Target().String(), Epoch: epoch.Uint64(),
	}
}

func openBackend(ctx context.Context, lookup func(string) (string, bool), req request) (backendService, func(), error) {
	if ctx == nil || lookup == nil {
		return nil, func() {}, errRuntimeWiring
	}
	enabled, _ := lookup("DR_ADMIN_ENABLED")
	if enabled != "true" {
		return nil, func() {}, errRuntimeWiring
	}
	if req.OperatorID != "" {
		allowed, _ := lookup("DR_ALLOWED_OPERATOR_ID")
		if allowed == "" || allowed != req.OperatorID {
			return nil, func() {}, errRuntimeWiring
		}
	}
	runtimeEpoch, ok := configuredRecoveryEpoch(lookup, req.SourceEpoch)
	if !ok {
		return nil, func() {}, errRuntimeWiring
	}
	if req.Command == "validate-failback" || req.Command == "advance-phase" || req.Command == "refresh-fence" || req.Command == "verify-fence" {
		dsn, ok := nonemptyEnv(lookup, "DR_JOURNAL_DATABASE_URL")
		if !ok {
			return nil, func() {}, errRuntimeWiring
		}
		journalRegionRaw, ok := nonemptyEnv(lookup, "DR_JOURNAL_REGION")
		if !ok {
			return nil, func() {}, errRuntimeWiring
		}
		journalRegion, err := authority.ParseRegion(journalRegionRaw)
		if err != nil {
			return nil, func() {}, errRuntimeWiring
		}
		pool, err := openAdminPool(ctx, dsn, recoveryRuntimeAuthority(journalRegion, runtimeEpoch))
		if err != nil {
			return nil, func() {}, errRuntimeWiring
		}
		store, err := recoverypostgres.New(pool)
		if err != nil {
			pool.Close()
			return nil, func() {}, errRuntimeWiring
		}
		var verifier recovery.FencingVerifier
		if req.Command == "advance-phase" || req.Command == "refresh-fence" || req.Command == "validate-failback" || req.Command == "verify-fence" {
			var verifierErr error
			verifier, verifierErr = configuredFencingVerifier(lookup, time.Now)
			if verifierErr != nil {
				pool.Close()
				return nil, func() {}, errRuntimeWiring
			}
		}
		return &runtimeBackend{store: store, now: time.Now, afterLoad: newTestCrashAfterLoadHook(lookup, os.Exit), verifier: verifier}, pool.Close, nil
	}
	sourcePools, sourceClose, err := openRegion(ctx, lookup, req.From, runtimeEpoch)
	if err != nil {
		return nil, func() {}, errRuntimeWiring
	}
	targetPools, targetClose, err := openRegion(ctx, lookup, req.To, runtimeEpoch)
	if err != nil {
		sourceClose()
		return nil, func() {}, errRuntimeWiring
	}
	closeAll := func() { targetClose(); sourceClose() }
	sourceObserver, err := observerForPools(sourcePools)
	if err != nil {
		closeAll()
		return nil, func() {}, errRuntimeWiring
	}
	targetObserver, err := observerForPools(targetPools)
	if err != nil {
		closeAll()
		return nil, func() {}, errRuntimeWiring
	}
	store, err := recoverypostgres.New(sourcePools.Control())
	if err != nil {
		closeAll()
		return nil, func() {}, errRuntimeWiring
	}
	return &runtimeBackend{source: sourceObserver, target: targetObserver, store: store, now: time.Now}, closeAll, nil
}

func configuredFencingVerifier(lookup func(string) (string, bool), now func() time.Time) (recovery.FencingVerifier, error) {
	issuer, issuerOK := nonemptyEnv(lookup, "DR_FENCE_ATTESTATION_ISSUER")
	keyID, keyOK := nonemptyEnv(lookup, "DR_FENCE_ATTESTATION_KEY_ID")
	encoded, publicOK := nonemptyEnv(lookup, "DR_FENCE_ATTESTATION_PUBLIC_KEY_B64")
	if !issuerOK || !keyOK || !publicOK {
		return recovery.FencingVerifier{}, errRuntimeWiring
	}
	publicKey, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return recovery.FencingVerifier{}, errRuntimeWiring
	}
	verifier, err := recovery.NewFencingVerifier(issuer, keyID, publicKey, now)
	if err != nil {
		return recovery.FencingVerifier{}, errRuntimeWiring
	}
	return verifier, nil
}

func newTestCrashAfterLoadHook(lookup func(string) (string, bool), exit func(int)) func(recovery.Stage) {
	if lookup == nil || exit == nil {
		return nil
	}
	appEnv, _ := lookup("APP_ENV")
	enabled, _ := lookup("DR_ADMIN_TEST_CRASH_AFTER_LOAD_ENABLED")
	if !strings.EqualFold(strings.TrimSpace(appEnv), "test") || !strings.EqualFold(strings.TrimSpace(enabled), "true") {
		return nil
	}
	want, _ := lookup("DR_ADMIN_TEST_CRASH_AFTER_LOAD_STAGE")
	want = strings.TrimSpace(want)
	valid := false
	for stage := recovery.StageExternalFencingVerified; stage <= recovery.StageSourceRetainedFenced; stage++ {
		if stage.String() == want {
			valid = true
			break
		}
	}
	if !valid {
		return nil
	}
	return func(stage recovery.Stage) {
		if stage.String() == want {
			exit(86)
		}
	}
}

func readEvidenceFile(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 64<<10 {
		return nil, errTopologyPrecondition
	}
	document, err := os.ReadFile(path)
	if err != nil || int64(len(document)) != info.Size() {
		return nil, errTopologyPrecondition
	}
	return document, nil
}

func openRegion(
	ctx context.Context,
	lookup func(string) (string, bool),
	region authority.Region,
	epoch authority.Epoch,
) (recovery.DatabaseSet[*pgxpool.Pool], func(), error) {
	prefix := "DR_" + strings.ToUpper(strings.ReplaceAll(region.String(), "-", "_")) + "_"
	keys := recovery.NewDatabaseSet(prefix+"CONTROL_DATABASE_URL", prefix+"SHARD_0_DATABASE_URL", prefix+"SHARD_1_DATABASE_URL")
	var opened []*pgxpool.Pool
	closeOpened := func() {
		for index := len(opened) - 1; index >= 0; index-- {
			opened[index].Close()
		}
	}
	values := make(map[recovery.Database]*pgxpool.Pool, 3)
	err := keys.Visit(func(database recovery.Database, key string) error {
		dsn, ok := nonemptyEnv(lookup, key)
		if !ok {
			return errRuntimeWiring
		}
		pool, err := openAdminPool(ctx, dsn, recoveryRuntimeAuthority(region, epoch))
		if err != nil {
			return errRuntimeWiring
		}
		opened = append(opened, pool)
		values[database] = pool
		return nil
	})
	if err != nil {
		closeOpened()
		return recovery.DatabaseSet[*pgxpool.Pool]{}, func() {}, err
	}
	return recovery.NewDatabaseSet(
		values[recovery.DatabaseControl], values[recovery.DatabaseShard0], values[recovery.DatabaseShard1],
	), closeOpened, nil
}

func observerForPools(pools recovery.DatabaseSet[*pgxpool.Pool]) (*recoverypostgres.TopologyObserver, error) {
	return recoverypostgres.NewTopologyObserver(recovery.NewDatabaseSet[recoverypostgres.ObservationDB](
		pools.Control(), pools.Shard0(), pools.Shard1(),
	))
}

func openAdminPool(ctx context.Context, dsn string, runtime databaseRuntimeAuthority) (*pgxpool.Pool, error) {
	if runtime.region.String() == "" || runtime.epoch.Uint64() == 0 ||
		runtime.role != authority.RoleRecovery || runtime.writes {
		return nil, errRuntimeWiring
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errRuntimeWiring
	}
	config.MaxConns = 2
	config.MinConns = 0
	config.MaxConnLifetime = 5 * time.Minute
	config.ConnConfig.ConnectTimeout = 3 * time.Second
	config.ConnConfig.RuntimeParams["application_name"] = "railway-dr-admin"
	session, err := postgresx.ParseRegionalSession(
		runtime.region.String(), string(runtime.role), strconv.FormatUint(runtime.epoch.Uint64(), 10), strconv.FormatBool(runtime.writes),
	)
	if err != nil || postgresx.ApplyRegionalSession(config.ConnConfig, session) != nil {
		return nil, errRuntimeWiring
	}
	return pgxpool.NewWithConfig(ctx, config)
}

func recoveryRuntimeAuthority(region authority.Region, epoch authority.Epoch) databaseRuntimeAuthority {
	return databaseRuntimeAuthority{region: region, epoch: epoch, role: authority.RoleRecovery, writes: false}
}

func configuredRecoveryEpoch(lookup func(string) (string, bool), requestEpoch authority.Epoch) (authority.Epoch, bool) {
	raw, ok := nonemptyEnv(lookup, "DR_RECOVERY_EPOCH")
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	epoch, err := authority.NewEpoch(value)
	if err != nil || requestEpoch.Uint64() != 0 && epoch != requestEpoch {
		return 0, false
	}
	return epoch, true
}

func nonemptyEnv(lookup func(string) (string, bool), key string) (string, bool) {
	raw, ok := lookup(key)
	if !ok || len(raw) > 4096 || strings.ContainsAny(raw, "\x00\r\n") {
		return "", false
	}
	value := strings.TrimSpace(raw)
	return value, value != ""
}
