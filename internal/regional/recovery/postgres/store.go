// Package postgres persists typed regional recovery checkpoints in the control
// database with optimistic compare-and-swap ownership.
package postgres

import (
	"context"
	"errors"
	"math"
	"regexp"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalidStore       = errors.New("regional recovery postgres store invalid")
	ErrInvalidCheckpoint  = errors.New("regional recovery checkpoint invalid")
	ErrCheckpointConflict = errors.New("regional recovery checkpoint ownership lost")
)

type OperationKind string

const (
	OperationFailover OperationKind = "failover"
	OperationFailback OperationKind = "failback"
)

var reasonPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

type Metadata struct {
	Kind               OperationKind
	ReasonCategory     string
	PlannedTargetEpoch authority.Epoch
}

type PlannedOperation struct {
	Operation          recovery.Failover
	Version            int64
	Metadata           Metadata
	PlannedTargetEpoch authority.Epoch
}

type DB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store struct{ db DB }

func New(db DB) (*Store, error) {
	if db == nil {
		return nil, ErrInvalidStore
	}
	return &Store{db: db}, nil
}

func (store *Store) Create(
	ctx context.Context,
	operation recovery.Failover,
	metadata Metadata,
	now time.Time,
) error {
	if store == nil || store.db == nil || ctx == nil || now.IsZero() ||
		(metadata.Kind != OperationFailover && metadata.Kind != OperationFailback) ||
		!reasonPattern.MatchString(metadata.ReasonCategory) {
		return ErrInvalidStore
	}
	checkpoint := operation.Checkpoint()
	payload, err := marshalCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	sourceEpoch, ok := positiveInt64(checkpoint.Binding.SourceEpoch().Uint64())
	if !ok {
		return ErrInvalidCheckpoint
	}
	var targetEpoch any
	if checkpoint.TargetEpoch.Uint64() > 0 {
		value, ok := positiveInt64(checkpoint.TargetEpoch.Uint64())
		if !ok {
			return ErrInvalidCheckpoint
		}
		targetEpoch = value
	}
	tag, err := store.db.Exec(ctx, createCheckpointSQL,
		checkpoint.Binding.OperationID(),
		string(metadata.Kind),
		checkpoint.Binding.Source().String(),
		checkpoint.Target.String(),
		sourceEpoch,
		targetEpoch,
		checkpoint.Binding.IncidentID(),
		checkpoint.Binding.OperatorID(),
		metadata.ReasonCategory,
		checkpoint.Stage.String(),
		payload,
		checkpoint.Binding.DeclaredAt(),
		now.UTC(),
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrCheckpointConflict
	}
	return nil
}

// Plan inserts one immutable planned operation or returns the operation with
// the same bounded identity. Conflicts never overwrite an existing incident or
// operation. Later phase changes continue to use Save's compare-and-swap.
func (store *Store) Plan(
	ctx context.Context,
	operation recovery.Failover,
	metadata Metadata,
	now time.Time,
) (PlannedOperation, bool, error) {
	if store == nil || store.db == nil || ctx == nil || now.IsZero() ||
		!validMetadata(metadata, operation.Binding().SourceEpoch()) || operation.Stage() != recovery.StagePlanned {
		return PlannedOperation{}, false, ErrInvalidStore
	}
	checkpoint := operation.Checkpoint()
	payload, err := marshalCheckpoint(checkpoint)
	if err != nil {
		return PlannedOperation{}, false, err
	}
	sourceEpoch, ok := positiveInt64(checkpoint.Binding.SourceEpoch().Uint64())
	if !ok {
		return PlannedOperation{}, false, ErrInvalidCheckpoint
	}
	var targetEpoch any
	if metadata.PlannedTargetEpoch.Uint64() > 0 {
		value, valid := positiveInt64(metadata.PlannedTargetEpoch.Uint64())
		if !valid {
			return PlannedOperation{}, false, ErrInvalidCheckpoint
		}
		targetEpoch = value
	}
	tag, err := store.db.Exec(ctx, planCheckpointSQL,
		checkpoint.Binding.OperationID(), string(metadata.Kind), checkpoint.Binding.Source().String(),
		checkpoint.Target.String(), sourceEpoch, targetEpoch, checkpoint.Binding.IncidentID(),
		checkpoint.Binding.OperatorID(), metadata.ReasonCategory, checkpoint.Stage.String(), payload,
		checkpoint.Binding.DeclaredAt(), now.UTC(),
	)
	if err != nil {
		return PlannedOperation{}, false, err
	}
	if tag.RowsAffected() == 1 {
		return PlannedOperation{
			Operation: operation, Version: 1, Metadata: metadata,
			PlannedTargetEpoch: metadata.PlannedTargetEpoch,
		}, true, nil
	}
	if tag.RowsAffected() != 0 {
		return PlannedOperation{}, false, ErrCheckpointConflict
	}
	existing, err := store.LoadPlan(ctx, checkpoint.Binding.OperationID())
	if err != nil || !samePlanIdentity(existing, operation, metadata) {
		return PlannedOperation{}, false, ErrCheckpointConflict
	}
	return existing, false, nil
}

func (store *Store) LoadPlan(ctx context.Context, operationID uuid.UUID) (PlannedOperation, error) {
	if store == nil || store.db == nil || ctx == nil || operationID == uuid.Nil {
		return PlannedOperation{}, ErrInvalidStore
	}
	var (
		payload        []byte
		version        int64
		rawKind        string
		reason         string
		targetEpochRaw *int64
	)
	if err := store.db.QueryRow(ctx, loadPlanSQL, operationID).Scan(
		&payload, &version, &rawKind, &reason, &targetEpochRaw,
	); err != nil || version <= 0 {
		return PlannedOperation{}, ErrInvalidCheckpoint
	}
	operation, err := unmarshalCheckpoint(payload)
	if err != nil || operation.Binding().OperationID() != operationID {
		return PlannedOperation{}, ErrInvalidCheckpoint
	}
	metadata := Metadata{Kind: OperationKind(rawKind), ReasonCategory: reason}
	var targetEpoch authority.Epoch
	if targetEpochRaw != nil {
		if *targetEpochRaw <= 0 {
			return PlannedOperation{}, ErrInvalidCheckpoint
		}
		targetEpoch, err = authority.NewEpoch(uint64(*targetEpochRaw))
		if err != nil {
			return PlannedOperation{}, ErrInvalidCheckpoint
		}
		metadata.PlannedTargetEpoch = targetEpoch
	}
	if !validMetadata(metadata, operation.Binding().SourceEpoch()) {
		return PlannedOperation{}, ErrInvalidCheckpoint
	}
	return PlannedOperation{
		Operation: operation, Version: version, Metadata: metadata, PlannedTargetEpoch: targetEpoch,
	}, nil
}

func validMetadata(metadata Metadata, sourceEpoch authority.Epoch) bool {
	if (metadata.Kind != OperationFailover && metadata.Kind != OperationFailback) ||
		!reasonPattern.MatchString(metadata.ReasonCategory) {
		return false
	}
	if metadata.PlannedTargetEpoch.Uint64() == 0 {
		return metadata.Kind == OperationFailover
	}
	return metadata.Kind == OperationFailback &&
		authority.RequireNewerEpoch(sourceEpoch, metadata.PlannedTargetEpoch) == nil
}

func samePlanIdentity(existing PlannedOperation, requested recovery.Failover, metadata Metadata) bool {
	existingBinding := existing.Operation.Binding()
	requestedBinding := requested.Binding()
	return existing.Operation.Target() == requested.Target() &&
		existingBinding.OperationID() == requestedBinding.OperationID() &&
		existingBinding.Source() == requestedBinding.Source() &&
		existingBinding.SourceEpoch() == requestedBinding.SourceEpoch() &&
		existingBinding.IncidentID() == requestedBinding.IncidentID() &&
		existingBinding.OperatorID() == requestedBinding.OperatorID() &&
		existing.Metadata.Kind == metadata.Kind &&
		existing.Metadata.ReasonCategory == metadata.ReasonCategory &&
		existing.PlannedTargetEpoch == metadata.PlannedTargetEpoch
}

func (store *Store) Load(ctx context.Context, operationID uuid.UUID) (recovery.Failover, int64, error) {
	if store == nil || store.db == nil || ctx == nil || operationID == uuid.Nil {
		return recovery.Failover{}, 0, ErrInvalidStore
	}
	var payload []byte
	var version int64
	if err := store.db.QueryRow(ctx, loadCheckpointSQL, operationID).Scan(&payload, &version); err != nil {
		return recovery.Failover{}, 0, err
	}
	if version <= 0 {
		return recovery.Failover{}, 0, ErrInvalidCheckpoint
	}
	operation, err := unmarshalCheckpoint(payload)
	if err != nil || operation.Binding().OperationID() != operationID {
		return recovery.Failover{}, 0, ErrInvalidCheckpoint
	}
	return operation, version, nil
}

func (store *Store) Save(
	ctx context.Context,
	expectedVersion int64,
	operation recovery.Failover,
	now time.Time,
) (int64, error) {
	if store == nil || store.db == nil || ctx == nil || expectedVersion <= 0 ||
		expectedVersion == math.MaxInt64 || now.IsZero() {
		return 0, ErrInvalidStore
	}
	checkpoint := operation.Checkpoint()
	payload, err := marshalCheckpoint(checkpoint)
	if err != nil {
		return 0, err
	}
	var targetEpoch any
	if checkpoint.TargetEpoch.Uint64() > 0 {
		value, ok := positiveInt64(checkpoint.TargetEpoch.Uint64())
		if !ok {
			return 0, ErrInvalidCheckpoint
		}
		targetEpoch = value
	}
	completed := checkpoint.Stage == recovery.StageSourceRetainedFenced
	tag, err := store.db.Exec(ctx, saveCheckpointSQL,
		checkpoint.Binding.OperationID(),
		expectedVersion,
		checkpoint.Stage.String(),
		targetEpoch,
		payload,
		now.UTC(),
		completed,
		checkpoint.Target.String(),
	)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() != 1 {
		return 0, ErrCheckpointConflict
	}
	return expectedVersion + 1, nil
}

// Refresh replaces only the checkpoint payload/version for a same-stage fence
// refresh, preserving the original phase completion timestamp.
func (store *Store) Refresh(ctx context.Context, expectedVersion int64, operation recovery.Failover, now time.Time) (int64, error) {
	if store == nil || store.db == nil || ctx == nil || expectedVersion <= 0 || expectedVersion == math.MaxInt64 || now.IsZero() {
		return 0, ErrInvalidStore
	}
	payload, err := marshalCheckpoint(operation.Checkpoint())
	if err != nil {
		return 0, err
	}
	tag, err := store.db.Exec(ctx, refreshFenceSQL, operation.Binding().OperationID(), expectedVersion, operation.Stage().String(), payload, now.UTC())
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() != 1 {
		return 0, ErrCheckpointConflict
	}
	return expectedVersion + 1, nil
}

func positiveInt64(value uint64) (int64, bool) {
	if value == 0 || value > math.MaxInt64 {
		return 0, false
	}
	return int64(value), true
}

const createCheckpointSQL = `
INSERT INTO public.regional_failover_operations(
 operation_id,operation_kind,source_region,target_region,source_epoch,target_epoch,
 incident_id,operator_id,reason_category,stage,checkpoint,phase_timestamps,
 declared_at,updated_at
) VALUES(
 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
 jsonb_build_object($10::text,$13::timestamptz),$12,$13
)`

const planCheckpointSQL = `
INSERT INTO public.regional_failover_operations(
 operation_id,operation_kind,source_region,target_region,source_epoch,target_epoch,
 incident_id,operator_id,reason_category,stage,checkpoint,phase_timestamps,
 declared_at,updated_at
) VALUES(
 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
 jsonb_build_object($10::text,$13::timestamptz),$12,$13
)
ON CONFLICT DO NOTHING`

const loadCheckpointSQL = `
SELECT checkpoint,checkpoint_version
FROM public.regional_failover_operations
WHERE operation_id=$1`

const loadPlanSQL = `
SELECT checkpoint,checkpoint_version,operation_kind,reason_category,target_epoch
FROM public.regional_failover_operations
WHERE operation_id=$1`

const saveCheckpointSQL = `
WITH eligible_operation AS MATERIALIZED (
    SELECT operation_id
    FROM public.regional_failover_operations
    WHERE operation_id=$1
      AND checkpoint_version=$2
    FOR UPDATE
), activated_authority AS (
    UPDATE public.regional_write_authority AS authority
    SET region=$8,
        epoch=$4,
        state='active',
        writes_enabled=true,
        updated_at=$6
    FROM eligible_operation
    WHERE $3='target_active'
      AND authority.singleton
      AND authority.region=$8
      AND authority.epoch=$4
      AND (
          (authority.state='recovery' AND NOT authority.writes_enabled)
          OR (authority.state='active' AND authority.writes_enabled)
      )
    RETURNING authority.singleton
)
UPDATE public.regional_failover_operations AS operation
SET stage=$3,
    target_epoch=$4,
    checkpoint=$5,
    checkpoint_version=checkpoint_version+1,
    phase_timestamps=phase_timestamps || jsonb_build_object($3::text,$6::timestamptz),
    updated_at=$6,
    completed_at=CASE WHEN $7 THEN $6 ELSE completed_at END
FROM eligible_operation
WHERE operation.operation_id=eligible_operation.operation_id
  AND (
      $3<>'target_active'
      OR EXISTS (SELECT 1 FROM activated_authority)
  )`

const refreshFenceSQL = `
UPDATE public.regional_failover_operations
SET checkpoint=$4,
    checkpoint_version=checkpoint_version+1,
    updated_at=$5
WHERE operation_id=$1
  AND checkpoint_version=$2
  AND stage=$3`
