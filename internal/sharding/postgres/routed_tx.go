package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RoutedTx is a PostgreSQL transaction whose fixed local schema context and
// train-run authority have already been validated. The assignment and fence
// locks remain held until Commit or Rollback.
type RoutedTx struct {
	tx    pgx.Tx
	route sharding.ShardRoute
}

// PGXTx exposes the established transaction to a storage adapter. Callers
// must not change search_path or issue transaction control statements.
func (tx *RoutedTx) PGXTx() pgx.Tx {
	if tx == nil {
		return nil
	}
	return tx.tx
}

func (tx *RoutedTx) Route() sharding.ShardRoute {
	if tx == nil {
		return sharding.ShardRoute{}
	}
	return tx.route
}

func (tx *RoutedTx) Commit(ctx context.Context) error {
	if tx == nil || tx.tx == nil {
		return sharding.ErrShardUnavailable
	}
	if err := tx.tx.Commit(ctx); err != nil {
		return sharding.ErrShardUnavailable
	}
	return nil
}

func (tx *RoutedTx) Rollback(ctx context.Context) error {
	if tx == nil || tx.tx == nil {
		return sharding.ErrShardUnavailable
	}
	if err := tx.tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return sharding.ErrShardUnavailable
	}
	return nil
}

// BeginTrainRunWrite starts a write transaction, installs a fixed
// transaction-local schema context, and locks both public assignment authority
// and the selected storage's local fence before returning it to a repository.
func (router *Router) BeginTrainRunWrite(ctx context.Context, expected sharding.ShardRoute) (routed *RoutedTx, resultErr error) {
	started := time.Now()
	metricShardID := boundedShardID(expected.ShardID())
	defer func() { router.observeRoute(ctx, "write", metricShardID, started, resultErr) }()
	if router == nil || router.db == nil || !validRoute(expected) || !router.shardAllowed(expected.ShardID()) {
		return nil, sharding.ErrShardUnavailable
	}
	ctx, cancel := router.boundedQueryContext(ctx)
	defer cancel()
	searchPath, ok := fixedSearchPath(expected.ShardID())
	if !ok {
		return nil, sharding.ErrShardUnavailable
	}

	tx, err := router.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, sharding.ErrShardUnavailable
	}
	fail := func(category error) (*RoutedTx, error) {
		_ = tx.Rollback(context.Background())
		return nil, category
	}
	if _, err := tx.Exec(ctx, searchPath); err != nil {
		return fail(sharding.ErrShardUnavailable)
	}

	var rawShardID string
	var rawGeneration int64
	var catalogEnabled bool
	var catalogWriteEnabled bool
	var catalogState string
	var minimumFencingProtocolVersion int32
	var assignmentState string
	var hasActiveMigration bool
	err = tx.QueryRow(ctx, `
SELECT assignment.shard_id,
       assignment.assignment_generation,
       shard.enabled,
       shard.write_enabled,
       shard.state,
       shard.minimum_fencing_protocol_version,
       assignment.assignment_state,
       assignment.active_migration_id IS NOT NULL
FROM public.train_run_shard_assignments AS assignment
JOIN public.booking_shards AS shard ON shard.shard_id = assignment.shard_id
WHERE assignment.train_run_id = $1
FOR UPDATE OF assignment
FOR SHARE OF shard`, expected.TrainRunID()).Scan(
		&rawShardID,
		&rawGeneration,
		&catalogEnabled,
		&catalogWriteEnabled,
		&catalogState,
		&minimumFencingProtocolVersion,
		&assignmentState,
		&hasActiveMigration,
	)
	if err != nil {
		return fail(sharding.ErrShardUnavailable)
	}
	observedShardID, parseErr := sharding.ParseShardID(rawShardID)
	if parseErr != nil || rawGeneration <= 0 {
		return fail(sharding.ErrShardUnavailable)
	}
	if observedShardID != expected.ShardID() || rawGeneration != expected.Generation().Int64() {
		return fail(sharding.ErrAssignmentStale)
	}
	if !catalogEnabled || !catalogStateAllowsRoute(catalogState) ||
		minimumFencingProtocolVersion <= 0 ||
		minimumFencingProtocolVersion > sharding.SupportedFencingProtocolVersion {
		return fail(sharding.ErrShardUnavailable)
	}
	switch assignmentState {
	case "stable":
		if hasActiveMigration {
			return fail(sharding.ErrTrainRunMigrating)
		}
	case "rollback_window":
		// The cut-over target is authoritative and intentionally retains an
		// active migration row during the bounded rollback window.
	case "draining", "migrating":
		return fail(sharding.ErrTrainRunMigrating)
	default:
		return fail(sharding.ErrShardUnavailable)
	}
	if !catalogWriteEnabled {
		return fail(sharding.ErrWriteFenced)
	}

	var fenceGeneration int64
	var fenceWriteEnabled bool
	if err := tx.QueryRow(ctx, `
SELECT assignment_generation, write_enabled
FROM train_run_write_fences
WHERE train_run_id = $1
FOR UPDATE`, expected.TrainRunID()).Scan(&fenceGeneration, &fenceWriteEnabled); err != nil {
		return fail(sharding.ErrWriteFenced)
	}
	if fenceGeneration != expected.Generation().Int64() || !fenceWriteEnabled {
		return fail(sharding.ErrWriteFenced)
	}

	return &RoutedTx{tx: tx, route: expected}, nil
}

// BeginTrainRunWriteWithRefresh retries only an assignment-stale rejection,
// only after one authoritative PostgreSQL refresh, and at most once. It never
// probes another shard or retries fenced, migrating, or unavailable failures.
func (router *Router) BeginTrainRunWriteWithRefresh(ctx context.Context, expected sharding.ShardRoute) (*RoutedTx, error) {
	tx, err := router.BeginTrainRunWrite(ctx, expected)
	if !errors.Is(err, sharding.ErrAssignmentStale) {
		return tx, err
	}
	fresh, err := router.RefreshTrainRun(ctx, expected.TrainRunID())
	if err != nil {
		return nil, err
	}
	return router.BeginTrainRunWrite(ctx, fresh)
}

// BeginTrainRunRead starts a bounded read-only snapshot on one resolved
// storage. It validates current assignment authority but deliberately does not
// require catalog or local-fence write enablement.
func (router *Router) BeginTrainRunRead(ctx context.Context, expected sharding.ShardRoute) (routed *RoutedTx, resultErr error) {
	started := time.Now()
	metricShardID := boundedShardID(expected.ShardID())
	defer func() { router.observeRoute(ctx, "read", metricShardID, started, resultErr) }()
	if router == nil || router.db == nil || !validRoute(expected) || !router.shardAllowed(expected.ShardID()) {
		return nil, sharding.ErrShardUnavailable
	}
	ctx, cancel := router.boundedQueryContext(ctx)
	defer cancel()
	searchPath, ok := fixedSearchPath(expected.ShardID())
	if !ok {
		return nil, sharding.ErrShardUnavailable
	}

	tx, err := router.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, sharding.ErrShardUnavailable
	}
	fail := func(category error) (*RoutedTx, error) {
		_ = tx.Rollback(context.Background())
		return nil, category
	}
	if _, err := tx.Exec(ctx, searchPath); err != nil {
		return fail(sharding.ErrShardUnavailable)
	}

	var rawShardID string
	var rawGeneration int64
	var catalogEnabled bool
	var catalogState string
	var minimumFencingProtocolVersion int32
	if err := tx.QueryRow(ctx, `
SELECT assignment.shard_id,
       assignment.assignment_generation,
       shard.enabled,
       shard.state,
       shard.minimum_fencing_protocol_version
FROM public.train_run_shard_assignments AS assignment
JOIN public.booking_shards AS shard ON shard.shard_id = assignment.shard_id
WHERE assignment.train_run_id = $1`, expected.TrainRunID()).Scan(
		&rawShardID,
		&rawGeneration,
		&catalogEnabled,
		&catalogState,
		&minimumFencingProtocolVersion,
	); err != nil {
		return fail(sharding.ErrShardUnavailable)
	}
	observedShardID, parseErr := sharding.ParseShardID(rawShardID)
	if parseErr != nil || rawGeneration <= 0 {
		return fail(sharding.ErrShardUnavailable)
	}
	if observedShardID != expected.ShardID() || rawGeneration != expected.Generation().Int64() {
		return fail(sharding.ErrAssignmentStale)
	}
	if !catalogEnabled || !catalogStateAllowsRoute(catalogState) ||
		minimumFencingProtocolVersion <= 0 ||
		minimumFencingProtocolVersion > sharding.SupportedFencingProtocolVersion {
		return fail(sharding.ErrShardUnavailable)
	}
	return &RoutedTx{tx: tx, route: expected}, nil
}

func validRoute(route sharding.ShardRoute) bool {
	if route.TrainRunID() == uuid.Nil || route.Generation().Int64() <= 0 {
		return false
	}
	_, err := sharding.ParseShardID(route.ShardID().String())
	return err == nil
}

func catalogStateAllowsRoute(state string) bool {
	return state == "active" || state == "draining"
}

// fixedSearchPath is deliberately a switch over compile-time literals. No
// database, environment, cache, HTTP, or JWT value can become SQL syntax.
func fixedSearchPath(shardID sharding.ShardID) (string, bool) {
	switch shardID {
	case sharding.ShardLegacy:
		return "SET LOCAL search_path TO pg_catalog, public, pg_temp", true
	case sharding.ShardZero:
		return "SET LOCAL search_path TO pg_catalog, booking_shard_0, public, pg_temp", true
	case sharding.ShardOne:
		return "SET LOCAL search_path TO pg_catalog, booking_shard_1, public, pg_temp", true
	default:
		return "", false
	}
}
