// Package postgres adapts pgx transactions to the regional authority module.
// Every authority and generation query is executed inside the same local
// transaction that receives the caller's mutation program.
package postgres

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidConfiguration = errors.New("regional postgres adapter configuration invalid")
	ErrInvalidAuthorityRow  = errors.New("regional postgres authority row invalid")
	ErrAuthorityNotReady    = errors.New("regional postgres authority unavailable")
)

// QueryRower is the common read surface implemented by pgx pools and
// transactions. It keeps the readiness check usable for both the control
// database and physical booking shards.
type QueryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// CheckActiveReadiness proves that this database is a writable primary whose
// durable authority row matches the process deployment. Callers must check
// every database they can mutate before advertising readiness or running a
// background pass.
func CheckActiveReadiness(ctx context.Context, db QueryRower, deployment authority.Deployment) error {
	if ctx == nil || db == nil {
		return ErrInvalidConfiguration
	}
	var region, state string
	var epoch int64
	var writesEnabled, inRecovery bool
	if err := db.QueryRow(ctx, `
SELECT authority.region,authority.epoch,authority.state,authority.writes_enabled,
       pg_is_in_recovery()
FROM public.regional_write_authority AS authority
WHERE authority.singleton`).Scan(&region, &epoch, &state, &writesEnabled, &inRecovery); err != nil || inRecovery {
		return ErrAuthorityNotReady
	}
	parsedRegion, err := authority.ParseRegion(region)
	if err != nil {
		return ErrAuthorityNotReady
	}
	if epoch < 1 {
		return ErrAuthorityNotReady
	}
	parsedEpoch, err := authority.NewEpoch(uint64(epoch))
	if err != nil {
		return ErrAuthorityNotReady
	}
	snapshot, err := authority.NewSnapshot(parsedRegion, parsedEpoch, authority.State(state), writesEnabled)
	if err != nil || deployment.Authorize(snapshot) != nil {
		return ErrAuthorityNotReady
	}
	return nil
}

type Beginner interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Control struct {
	db      Beginner
	options pgx.TxOptions
}

func NewControl(db Beginner) (*Control, error) {
	return NewControlWithOptions(db, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
}

func NewControlWithOptions(db Beginner, options pgx.TxOptions) (*Control, error) {
	if db == nil {
		return nil, ErrInvalidConfiguration
	}
	options, err := writeOptions(options)
	if err != nil {
		return nil, err
	}
	return &Control{db: db, options: options}, nil
}

// ControlTx embeds the local pgx transaction so a caller may execute its
// control mutation after RegionalAuthority succeeds. The authority contract
// forbids using this callback for external or cross-database I/O.
type ControlTx struct{ pgx.Tx }

func (tx ControlTx) RegionalAuthority(ctx context.Context) (authority.Snapshot, error) {
	if ctx == nil || tx.Tx == nil {
		return authority.Snapshot{}, ErrInvalidConfiguration
	}
	var (
		regionText    string
		epochValue    int64
		stateText     string
		writesEnabled bool
	)
	if err := tx.QueryRow(ctx, regionalAuthorityForShareSQL).Scan(
		&regionText,
		&epochValue,
		&stateText,
		&writesEnabled,
	); err != nil {
		return authority.Snapshot{}, err
	}
	return snapshot(regionText, epochValue, stateText, writesEnabled)
}

func (adapter *Control) WithinTransaction(ctx context.Context, program func(ControlTx) error) error {
	if adapter == nil || adapter.db == nil || ctx == nil || program == nil {
		return ErrInvalidConfiguration
	}
	tx, err := adapter.db.BeginTx(ctx, adapter.options)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err := program(ControlTx{Tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

type Shard struct {
	db      Beginner
	shardID sharding.ShardID
	options pgx.TxOptions
}

func NewShard(db Beginner, shardID sharding.ShardID) (*Shard, error) {
	return NewShardWithOptions(db, shardID, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
}

func NewShardWithOptions(db Beginner, shardID sharding.ShardID, options pgx.TxOptions) (*Shard, error) {
	if db == nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
		return nil, ErrInvalidConfiguration
	}
	options, err := writeOptions(options)
	if err != nil {
		return nil, err
	}
	return &Shard{db: db, shardID: shardID, options: options}, nil
}

type ShardTx struct {
	ControlTx
	shardID sharding.ShardID
}

func (tx ShardTx) TrainRunFence(ctx context.Context, trainRunID uuid.UUID) (authority.ShardFence, error) {
	if ctx == nil || tx.Tx == nil || trainRunID == uuid.Nil {
		return authority.ShardFence{}, ErrInvalidConfiguration
	}
	var (
		storedTrainRunID uuid.UUID
		generationValue  int64
		stateText        string
		writesEnabled    bool
	)
	if err := tx.QueryRow(ctx, trainRunFenceForUpdateSQL, trainRunID).Scan(
		&storedTrainRunID,
		&generationValue,
		&stateText,
		&writesEnabled,
	); err != nil {
		return authority.ShardFence{}, err
	}
	generation, err := sharding.NewAssignmentGeneration(generationValue)
	if err != nil {
		return authority.ShardFence{}, ErrInvalidAuthorityRow
	}
	state, err := shardFenceState(stateText)
	if err != nil {
		return authority.ShardFence{}, err
	}
	fence, err := authority.NewShardFence(storedTrainRunID, tx.shardID, generation, state, writesEnabled)
	if err != nil {
		return authority.ShardFence{}, ErrInvalidAuthorityRow
	}
	return fence, nil
}

func (adapter *Shard) WithinTransaction(ctx context.Context, program func(ShardTx) error) error {
	if adapter == nil || adapter.db == nil || ctx == nil || program == nil {
		return ErrInvalidConfiguration
	}
	tx, err := adapter.db.BeginTx(ctx, adapter.options)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	wrapped := ShardTx{ControlTx: ControlTx{Tx: tx}, shardID: adapter.shardID}
	if err := program(wrapped); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

func snapshot(regionText string, epochValue int64, stateText string, writesEnabled bool) (authority.Snapshot, error) {
	region, err := authority.ParseRegion(regionText)
	if err != nil || epochValue <= 0 {
		return authority.Snapshot{}, ErrInvalidAuthorityRow
	}
	epoch, err := authority.NewEpoch(uint64(epochValue))
	if err != nil {
		return authority.Snapshot{}, ErrInvalidAuthorityRow
	}
	state, err := authorityState(stateText)
	if err != nil {
		return authority.Snapshot{}, err
	}
	value, err := authority.NewSnapshot(region, epoch, state, writesEnabled)
	if err != nil {
		return authority.Snapshot{}, ErrInvalidAuthorityRow
	}
	return value, nil
}

func authorityState(raw string) (authority.State, error) {
	state := authority.State(raw)
	switch state {
	case authority.StateActive, authority.StateDraining, authority.StateFenced,
		authority.StatePromoting, authority.StateRecovery, authority.StateFailed:
		return state, nil
	default:
		return "", ErrInvalidAuthorityRow
	}
}

func shardFenceState(raw string) (authority.ShardFenceState, error) {
	switch raw {
	case "active":
		return authority.ShardFenceActive, nil
	case "quiescing":
		return authority.ShardFenceDraining, nil
	case "standby":
		return authority.ShardFenceRecovery, nil
	case "disabled", "retained":
		return authority.ShardFenceFenced, nil
	default:
		return "", ErrInvalidAuthorityRow
	}
}

func writeOptions(options pgx.TxOptions) (pgx.TxOptions, error) {
	if options.AccessMode == pgx.ReadOnly {
		return pgx.TxOptions{}, ErrInvalidConfiguration
	}
	if options.IsoLevel == "" {
		options.IsoLevel = pgx.ReadCommitted
	}
	options.AccessMode = pgx.ReadWrite
	return options, nil
}

// ControlWriter is the production-facing pgx seam. It exposes only the local
// transaction after the configured deployment matches the locked authority
// row. The callback contract permits PostgreSQL I/O on tx only; external,
// filesystem, provider, and cross-database I/O are forbidden.
type ControlWriter struct {
	writer authority.ControlWriter[ControlTx]
}

func NewControlWriter(
	db Beginner,
	deployment authority.Deployment,
	options pgx.TxOptions,
) (*ControlWriter, error) {
	adapter, err := NewControlWithOptions(db, options)
	if err != nil {
		return nil, err
	}
	writer, err := authority.NewControlWriter(deployment, adapter)
	if err != nil {
		return nil, err
	}
	return &ControlWriter{writer: writer}, nil
}

func (writer *ControlWriter) Write(ctx context.Context, mutation func(pgx.Tx) error) error {
	if writer == nil || mutation == nil {
		return ErrInvalidConfiguration
	}
	return writer.writer.Write(ctx, func(tx ControlTx) error {
		return mutation(tx.Tx)
	})
}

// ShardWriter additionally checks the fixed physical route and generation
// fence, in that order, before exposing the same local transaction.
type ShardWriter struct {
	writer authority.ShardWriter[ShardTx]
}

func NewShardWriter(
	db Beginner,
	deployment authority.Deployment,
	shardID sharding.ShardID,
	options pgx.TxOptions,
) (*ShardWriter, error) {
	adapter, err := NewShardWithOptions(db, shardID, options)
	if err != nil {
		return nil, err
	}
	writer, err := authority.NewShardWriter(deployment, shardID, adapter)
	if err != nil {
		return nil, err
	}
	return &ShardWriter{writer: writer}, nil
}

func (writer *ShardWriter) Write(
	ctx context.Context,
	route sharding.ShardRoute,
	mutation func(pgx.Tx) error,
) error {
	if writer == nil || mutation == nil {
		return ErrInvalidConfiguration
	}
	return writer.writer.Write(ctx, route, func(tx ShardTx) error {
		return mutation(tx.Tx)
	})
}

// AuthorizeControlTransaction locks and validates the database-local regional
// authority row inside an existing caller-owned transaction.
func AuthorizeControlTransaction(ctx context.Context, tx pgx.Tx, deployment authority.Deployment) error {
	if ctx == nil || tx == nil {
		return ErrInvalidConfiguration
	}
	snapshot, err := (ControlTx{Tx: tx}).RegionalAuthority(ctx)
	if err != nil {
		return err
	}
	return deployment.Authorize(snapshot)
}

// AuthorizeShardTransaction validates regional authority first and then the
// existing train-run generation fence in the same caller-owned transaction.
func AuthorizeShardTransaction(
	ctx context.Context,
	tx pgx.Tx,
	deployment authority.Deployment,
	route sharding.ShardRoute,
) error {
	if ctx == nil || tx == nil || (route.ShardID() != sharding.ShardPhysicalZero && route.ShardID() != sharding.ShardPhysicalOne) {
		return ErrInvalidConfiguration
	}
	if err := AuthorizeControlTransaction(ctx, tx, deployment); err != nil {
		return err
	}
	wrapper := ShardTx{ControlTx: ControlTx{Tx: tx}, shardID: route.ShardID()}
	fence, err := wrapper.TrainRunFence(ctx, route.TrainRunID())
	if err != nil {
		return err
	}
	return deployment.AuthorizeShard(route, fence)
}

const regionalAuthorityForShareSQL = `
SELECT region,epoch,state,writes_enabled
FROM public.regional_write_authority
WHERE singleton=true
FOR SHARE`

const trainRunFenceForUpdateSQL = `
SELECT train_run_id,assignment_generation,state,write_enabled
FROM public.train_run_write_fences
WHERE train_run_id=$1
FOR UPDATE`

var _ authority.TransactionRunner[ControlTx] = (*Control)(nil)
var _ authority.TransactionRunner[ShardTx] = (*Shard)(nil)
