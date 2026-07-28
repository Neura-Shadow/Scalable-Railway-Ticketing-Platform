package control

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/google/uuid"
)

const (
	cleanupReasonWindowOpen  = "rollback_window_open"
	cleanupReasonNotTerminal = "migration_not_completed_or_rolled_back"
	cleanupReasonNoWindow    = "no_post_cutover_rollback_window"
	cleanupReasonEligible    = "eligible_for_separate_cleanup_executor"
	maxValidationTables      = 32
	maxTableNameBytes        = 64
	maxChecksumBytes         = 128
)

type Service struct {
	repository Repository
	clock      Clock
	limits     Limits
}

func NewService(repository Repository, clock Clock, limits Limits) (*Service, error) {
	if repository == nil || clock == nil {
		return nil, ErrInvalidInput
	}
	if limits.MaxBatchSize <= 0 || limits.MaxCheckpointBytes <= 0 || limits.MaxOperationTimeout <= 0 ||
		limits.MaxValidationRows <= 0 || limits.MaxLocatorRows <= 0 || limits.MaxRollbackWindow <= 0 {
		return nil, ErrInvalidLimits
	}
	return &Service{repository: repository, clock: clock, limits: limits}, nil
}

func (service *Service) Plan(ctx context.Context, input PlanInput) (Record, error) {
	if err := service.validatePlanInput(input); err != nil {
		return Record{}, err
	}
	ctx, cancel, err := service.boundedContext(ctx, input.OperationTimeout)
	if err != nil {
		return Record{}, err
	}
	defer cancel()

	var result Record
	err = service.repository.WithinTransaction(ctx, func(ctx context.Context, tx Transaction) error {
		existing, found, err := tx.FindMigrationForUpdate(ctx, input.MigrationID)
		if err != nil {
			return err
		}
		if found {
			if err := validateRecord(existing); err != nil {
				return err
			}
			if !samePlan(existing, input) {
				return ErrPlanConflict
			}
			result = existing
			return nil
		}

		source, err := sharding.NewShardRoute(input.TrainRunID, input.SourceShard, input.SourceGeneration)
		if err != nil {
			return fmt.Errorf("%w: source route: %v", ErrInvalidInput, err)
		}
		target, err := sharding.NewShardRoute(input.TrainRunID, input.TargetShard, input.TargetGeneration)
		if err != nil {
			return fmt.Errorf("%w: target route: %v", ErrInvalidInput, err)
		}
		active, err := tx.ActiveRouteForUpdate(ctx, input.TrainRunID)
		if err != nil {
			return err
		}
		if !routesEqual(active, source) {
			return ErrActiveRouteMismatch
		}
		if err := tx.RequireShardWritableForUpdate(ctx, target.ShardID()); err != nil {
			return err
		}
		if err := requireFenceState(ctx, tx, source, true); err != nil {
			return err
		}
		// Planning establishes the target's fail-closed posture. It never
		// enables a second writer.
		if err := tx.SetWriteFence(ctx, target, false); err != nil {
			return err
		}
		now := service.clock.Now()
		result = Record{
			MigrationID:      input.MigrationID,
			TrainRunID:       input.TrainRunID,
			SourceShard:      input.SourceShard,
			TargetShard:      input.TargetShard,
			SourceGeneration: input.SourceGeneration,
			TargetGeneration: input.TargetGeneration,
			RollbackWindow:   input.RollbackWindow,
			State:            migration.StatePlanned,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		return tx.InsertMigration(ctx, result)
	})
	if err != nil {
		return Record{}, err
	}
	return result, nil
}

func (service *Service) CopyBatch(ctx context.Context, input CopyBatchInput) (Record, error) {
	if input.MigrationID == uuid.Nil || input.BatchSize <= 0 || input.BatchSize > service.limits.MaxBatchSize {
		return Record{}, ErrInvalidInput
	}
	ctx, cancel, err := service.boundedContext(ctx, input.Timeout)
	if err != nil {
		return Record{}, err
	}
	defer cancel()

	var result Record
	err = service.repository.WithinTransaction(ctx, func(ctx context.Context, tx Transaction) error {
		record, err := loadRecord(ctx, tx, input.MigrationID)
		if err != nil {
			return err
		}
		if record.CopyComplete && stateAtOrAfterValidation(record.State) {
			result = record
			return nil
		}
		source, target, err := recordRoutes(record)
		if err != nil {
			return err
		}
		switch record.State {
		case migration.StatePlanned:
			if err := transitionRecord(&record, migration.StateDraining); err != nil {
				return err
			}
			if err := service.quiesceSourceForCopy(ctx, tx, record, source); err != nil {
				return err
			}
			if err := transitionRecord(&record, migration.StateCopying); err != nil {
				return err
			}
		case migration.StateDraining:
			if err := service.quiesceSourceForCopy(ctx, tx, record, source); err != nil {
				return err
			}
			if err := transitionRecord(&record, migration.StateCopying); err != nil {
				return err
			}
		case migration.StateCopying:
		default:
			return ErrInvalidState
		}

		batch, err := tx.CopyBatch(ctx, CopyBatchRequest{
			MigrationID: record.MigrationID,
			TrainRunID:  record.TrainRunID,
			Source:      source,
			Target:      target,
			Checkpoint:  record.Checkpoint,
			Limit:       input.BatchSize,
		})
		if err != nil {
			return err
		}
		if err := validateCopyResult(record, input.BatchSize, service.limits.MaxCheckpointBytes, batch); err != nil {
			return err
		}
		if int64(batch.RowsCopied) > math.MaxInt64-record.CopiedRows {
			return ErrInvalidCopyResult
		}
		record.Checkpoint = batch.NextCheckpoint
		record.CopiedRows += int64(batch.RowsCopied)
		if batch.Done {
			record.CopyComplete = true
			if err := transitionRecord(&record, migration.StateValidating); err != nil {
				return err
			}
		}
		record.UpdatedAt = service.clock.Now()
		if err := tx.SaveMigration(ctx, record); err != nil {
			return err
		}
		result = record
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	return result, nil
}

func (service *Service) Validate(ctx context.Context, input ValidateInput) (ValidateResult, error) {
	if input.MigrationID == uuid.Nil || input.RowCap <= 0 || input.RowCap > service.limits.MaxValidationRows {
		return ValidateResult{}, ErrInvalidInput
	}
	ctx, cancel, err := service.boundedContext(ctx, input.Timeout)
	if err != nil {
		return ValidateResult{}, err
	}
	defer cancel()

	var result ValidateResult
	err = service.repository.WithinTransaction(ctx, func(ctx context.Context, tx Transaction) error {
		record, err := loadRecord(ctx, tx, input.MigrationID)
		if err != nil {
			return err
		}
		if stateAtOrAfterCutoverReady(record.State) {
			if record.LastValidation == nil || !record.LastValidation.Passed {
				return ErrInvalidRecord
			}
			result = ValidateResult{Record: record, Passed: true}
			return nil
		}
		if record.State != migration.StateValidating || !record.CopyComplete {
			return ErrInvalidState
		}
		source, target, err := recordRoutes(record)
		if err != nil {
			return err
		}
		snapshot, err := tx.Validate(ctx, ValidationRequest{
			MigrationID: record.MigrationID,
			TrainRunID:  record.TrainRunID,
			Source:      source,
			Target:      target,
			RowCap:      input.RowCap,
		})
		if err != nil {
			return err
		}
		if snapshot.Truncated || snapshot.RowsExamined > input.RowCap {
			return ErrValidationRowCapExceeded
		}
		if err := validateSnapshot(snapshot); err != nil {
			return err
		}
		passed := validationPassed(snapshot)
		record.LastValidation = &ValidationOutcome{
			Snapshot:  snapshot,
			Passed:    passed,
			CheckedAt: service.clock.Now(),
		}
		if passed {
			if err := transitionRecord(&record, migration.StateCutoverReady); err != nil {
				return err
			}
		}
		record.UpdatedAt = service.clock.Now()
		if err := tx.SaveMigration(ctx, record); err != nil {
			return err
		}
		result = ValidateResult{Record: record, Passed: passed}
		return nil
	})
	if err != nil {
		return ValidateResult{}, err
	}
	return result, nil
}

func (service *Service) Cutover(ctx context.Context, input CutoverInput) (Record, error) {
	if input.MigrationID == uuid.Nil || input.ValidationRowCap <= 0 ||
		input.ValidationRowCap > service.limits.MaxValidationRows || input.LocatorRowCap <= 0 ||
		input.LocatorRowCap > service.limits.MaxLocatorRows {
		return Record{}, ErrInvalidInput
	}
	ctx, cancel, err := service.boundedContext(ctx, input.Timeout)
	if err != nil {
		return Record{}, err
	}
	defer cancel()

	var result Record
	err = service.repository.WithinTransaction(ctx, func(ctx context.Context, tx Transaction) error {
		record, err := loadRecord(ctx, tx, input.MigrationID)
		if err != nil {
			return err
		}
		if record.State == migration.StateRollbackWindow || record.State == migration.StateCompleted {
			result = record
			return nil
		}
		if record.State != migration.StateCutoverReady && record.State != migration.StateCuttingOver {
			return ErrInvalidState
		}
		if record.LastValidation == nil || !record.LastValidation.Passed {
			return ErrInvalidRecord
		}
		if record.State == migration.StateCutoverReady {
			if err := transitionRecord(&record, migration.StateCuttingOver); err != nil {
				return err
			}
		}
		source, target, err := recordRoutes(record)
		if err != nil {
			return err
		}
		active, err := tx.ActiveRouteForUpdate(ctx, record.TrainRunID)
		if err != nil {
			return err
		}
		if !routesEqual(active, source) {
			return ErrActiveRouteMismatch
		}
		if err := tx.RequireShardWritableForUpdate(ctx, target.ShardID()); err != nil {
			return err
		}
		snapshot, err := tx.Validate(ctx, ValidationRequest{
			MigrationID: record.MigrationID,
			TrainRunID:  record.TrainRunID,
			Source:      source,
			Target:      target,
			RowCap:      input.ValidationRowCap,
		})
		if err != nil {
			return err
		}
		if snapshot.Truncated || snapshot.RowsExamined > input.ValidationRowCap {
			return ErrValidationRowCapExceeded
		}
		if err := validateSnapshot(snapshot); err != nil {
			return err
		}
		if !validationPassed(snapshot) {
			return ErrCutoverValidationFailed
		}
		checkedAt := service.clock.Now()
		record.LastValidation = &ValidationOutcome{
			Snapshot:  snapshot,
			Passed:    true,
			CheckedAt: checkedAt,
		}
		locatorRows, err := tx.LockLocatorsForUpdate(ctx, record.TrainRunID, input.LocatorRowCap)
		if err != nil {
			return err
		}
		if locatorRows < 0 || locatorRows > input.LocatorRowCap {
			return ErrLocatorRowCapExceeded
		}
		if err := requireFenceState(ctx, tx, source, false); err != nil {
			return err
		}
		if err := requireFenceState(ctx, tx, target, false); err != nil {
			return err
		}
		// This order is the single-writer invariant: drain the old owner,
		// disable it, enable the new generation, then publish the route.
		if err := tx.QuiesceWrites(ctx, source); err != nil {
			return err
		}
		if err := tx.SetWriteFence(ctx, source, false); err != nil {
			return err
		}
		if err := tx.SetWriteFence(ctx, target, true); err != nil {
			return err
		}
		if err := tx.ActivateRoute(ctx, source, target); err != nil {
			return err
		}
		if err := transitionRecord(&record, migration.StateRollbackWindow); err != nil {
			return err
		}
		now := service.clock.Now()
		deadline := now.Add(record.RollbackWindow)
		record.CutoverAt = &now
		record.RollbackDeadline = &deadline
		record.UpdatedAt = now
		if err := tx.SaveMigration(ctx, record); err != nil {
			return err
		}
		result = record
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	return result, nil
}

func (service *Service) DirectRollback(ctx context.Context, input DirectRollbackInput) (Record, error) {
	if input.MigrationID == uuid.Nil {
		return Record{}, ErrInvalidInput
	}
	ctx, cancel, err := service.boundedContext(ctx, input.Timeout)
	if err != nil {
		return Record{}, err
	}
	defer cancel()

	var result Record
	err = service.repository.WithinTransaction(ctx, func(ctx context.Context, tx Transaction) error {
		record, err := loadRecord(ctx, tx, input.MigrationID)
		if err != nil {
			return err
		}
		if record.State == migration.StateRolledBack {
			if record.RollbackGeneration != nil && *record.RollbackGeneration != input.RollbackGeneration {
				return ErrPlanConflict
			}
			result = record
			return nil
		}
		if isPreCutoverState(record.State) {
			source, target, err := recordRoutes(record)
			if err != nil {
				return err
			}
			active, err := tx.ActiveRouteForUpdate(ctx, record.TrainRunID)
			if err != nil {
				return err
			}
			if !routesEqual(active, source) {
				return ErrActiveRouteMismatch
			}
			if err := tx.RequireShardWritableForUpdate(ctx, source.ShardID()); err != nil {
				return err
			}
			if err := tx.SetWriteFence(ctx, target, false); err != nil {
				return err
			}
			if err := tx.SetWriteFence(ctx, source, true); err != nil {
				return err
			}
			if err := transitionRecord(&record, migration.StateRolledBack); err != nil {
				return err
			}
			record.UpdatedAt = service.clock.Now()
			if err := tx.SaveMigration(ctx, record); err != nil {
				return err
			}
			result = record
			return nil
		}
		if record.State != migration.StateRollbackWindow {
			return ErrInvalidState
		}
		if record.RollbackDeadline == nil || record.CutoverAt == nil {
			return ErrInvalidRecord
		}
		if !service.clock.Now().Before(*record.RollbackDeadline) {
			return ErrRollbackWindowExpired
		}
		if input.RollbackGeneration.Int64() <= record.TargetGeneration.Int64() {
			return ErrInvalidInput
		}
		if input.LocatorRowCap <= 0 || input.LocatorRowCap > service.limits.MaxLocatorRows {
			return ErrInvalidInput
		}
		target, err := record.TargetRoute()
		if err != nil {
			return ErrInvalidRecord
		}
		rollbackRoute, err := sharding.NewShardRoute(record.TrainRunID, record.SourceShard, input.RollbackGeneration)
		if err != nil {
			return fmt.Errorf("%w: rollback route: %v", ErrInvalidInput, err)
		}
		active, err := tx.ActiveRouteForUpdate(ctx, record.TrainRunID)
		if err != nil {
			return err
		}
		if !routesEqual(active, target) {
			return ErrActiveRouteMismatch
		}
		if err := tx.RequireShardWritableForUpdate(ctx, rollbackRoute.ShardID()); err != nil {
			return err
		}
		source, err := record.SourceRoute()
		if err != nil {
			return ErrInvalidRecord
		}
		locatorRows, err := tx.LockLocatorsForUpdate(ctx, record.TrainRunID, input.LocatorRowCap)
		if err != nil {
			return err
		}
		if locatorRows < 0 || locatorRows > input.LocatorRowCap {
			return ErrLocatorRowCapExceeded
		}
		if err := requireFenceState(ctx, tx, target, true); err != nil {
			return err
		}
		if err := requireFenceState(ctx, tx, source, false); err != nil {
			return err
		}
		if err := tx.QuiesceWrites(ctx, target); err != nil {
			return err
		}
		hasWrites, err := tx.HasDurableTargetWrites(ctx, target)
		if err != nil {
			return err
		}
		if hasWrites {
			return ErrTargetWriteEvidence
		}
		// As at cutover, the current owner is disabled before another owner
		// can be enabled. Rollback uses a fresh monotonic generation.
		if err := tx.SetWriteFence(ctx, target, false); err != nil {
			return err
		}
		if err := tx.SetWriteFence(ctx, rollbackRoute, true); err != nil {
			return err
		}
		if err := tx.ActivateRoute(ctx, target, rollbackRoute); err != nil {
			return err
		}
		if err := transitionRecord(&record, migration.StateRolledBack); err != nil {
			return err
		}
		record.RollbackGeneration = &input.RollbackGeneration
		record.UpdatedAt = service.clock.Now()
		if err := tx.SaveMigration(ctx, record); err != nil {
			return err
		}
		result = record
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	return result, nil
}

func (service *Service) Complete(ctx context.Context, input CompleteInput) (Record, error) {
	if input.MigrationID == uuid.Nil {
		return Record{}, ErrInvalidInput
	}
	ctx, cancel, err := service.boundedContext(ctx, input.Timeout)
	if err != nil {
		return Record{}, err
	}
	defer cancel()

	var result Record
	err = service.repository.WithinTransaction(ctx, func(ctx context.Context, tx Transaction) error {
		record, err := loadRecord(ctx, tx, input.MigrationID)
		if err != nil {
			return err
		}
		if record.State == migration.StateCompleted {
			result = record
			return nil
		}
		if record.State != migration.StateRollbackWindow || record.RollbackDeadline == nil {
			return ErrInvalidState
		}
		if service.clock.Now().Before(*record.RollbackDeadline) {
			return ErrRollbackWindowOpen
		}
		target, err := record.TargetRoute()
		if err != nil {
			return ErrInvalidRecord
		}
		active, err := tx.ActiveRouteForUpdate(ctx, record.TrainRunID)
		if err != nil {
			return err
		}
		if !routesEqual(active, target) {
			return ErrActiveRouteMismatch
		}
		if err := requireFenceState(ctx, tx, target, true); err != nil {
			return err
		}
		source, err := record.SourceRoute()
		if err != nil {
			return ErrInvalidRecord
		}
		if err := requireFenceState(ctx, tx, source, false); err != nil {
			return err
		}
		if err := transitionRecord(&record, migration.StateCompleted); err != nil {
			return err
		}
		record.UpdatedAt = service.clock.Now()
		if err := tx.SaveMigration(ctx, record); err != nil {
			return err
		}
		result = record
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	return result, nil
}

func (service *Service) CleanupEligibility(ctx context.Context, input CleanupEligibilityInput) (CleanupEligibility, error) {
	if input.MigrationID == uuid.Nil {
		return CleanupEligibility{}, ErrInvalidInput
	}
	ctx, cancel, err := service.boundedContext(ctx, input.Timeout)
	if err != nil {
		return CleanupEligibility{}, err
	}
	defer cancel()

	var result CleanupEligibility
	err = service.repository.WithinTransaction(ctx, func(ctx context.Context, tx Transaction) error {
		record, err := loadRecord(ctx, tx, input.MigrationID)
		if err != nil {
			return err
		}
		if record.State != migration.StateCompleted && record.State != migration.StateRolledBack {
			result.Reason = cleanupReasonNotTerminal
			result.EligibleAt = cloneTime(record.RollbackDeadline)
			return nil
		}
		if record.RollbackDeadline == nil {
			result.Reason = cleanupReasonNoWindow
			return nil
		}
		result.EligibleAt = cloneTime(record.RollbackDeadline)
		if service.clock.Now().Before(*record.RollbackDeadline) {
			result.Reason = cleanupReasonWindowOpen
			return nil
		}
		var expected, inactive sharding.ShardRoute
		if record.State == migration.StateCompleted {
			expected, err = record.TargetRoute()
			if err == nil {
				inactive, err = record.SourceRoute()
			}
		} else if record.RollbackGeneration != nil {
			expected, err = sharding.NewShardRoute(record.TrainRunID, record.SourceShard, *record.RollbackGeneration)
			if err == nil {
				inactive, err = record.TargetRoute()
			}
		} else {
			expected, err = record.SourceRoute()
			if err == nil {
				inactive, err = record.TargetRoute()
			}
		}
		if err != nil {
			return ErrInvalidRecord
		}
		active, err := tx.ActiveRouteForUpdate(ctx, record.TrainRunID)
		if err != nil {
			return err
		}
		if !routesEqual(active, expected) {
			return ErrActiveRouteMismatch
		}
		if err := requireFenceState(ctx, tx, expected, true); err != nil {
			return err
		}
		if err := requireFenceState(ctx, tx, inactive, false); err != nil {
			return err
		}
		result.Eligible = true
		result.Reason = cleanupReasonEligible
		return nil
	})
	if err != nil {
		return CleanupEligibility{}, err
	}
	return result, nil
}

func (service *Service) validatePlanInput(input PlanInput) error {
	if input.MigrationID == uuid.Nil || input.TrainRunID == uuid.Nil || input.SourceShard == input.TargetShard ||
		input.SourceGeneration.Int64() <= 0 || input.TargetGeneration.Int64() <= input.SourceGeneration.Int64() ||
		input.RollbackWindow <= 0 || input.RollbackWindow > service.limits.MaxRollbackWindow {
		return ErrInvalidInput
	}
	if _, err := sharding.ParseShardID(input.SourceShard.String()); err != nil {
		return ErrInvalidInput
	}
	if _, err := sharding.ParseShardID(input.TargetShard.String()); err != nil {
		return ErrInvalidInput
	}
	if input.OperationTimeout <= 0 || input.OperationTimeout > service.limits.MaxOperationTimeout {
		return ErrInvalidInput
	}
	return nil
}

func (service *Service) quiesceSourceForCopy(ctx context.Context, tx Transaction, record Record, source sharding.ShardRoute) error {
	active, err := tx.ActiveRouteForUpdate(ctx, record.TrainRunID)
	if err != nil {
		return err
	}
	if !routesEqual(active, source) {
		return ErrActiveRouteMismatch
	}
	if err := requireFenceState(ctx, tx, source, true); err != nil {
		return err
	}
	target, err := record.TargetRoute()
	if err != nil {
		return ErrInvalidRecord
	}
	if err := requireFenceState(ctx, tx, target, false); err != nil {
		return err
	}
	if err := tx.QuiesceWrites(ctx, source); err != nil {
		return err
	}
	return tx.SetWriteFence(ctx, source, false)
}

func (service *Service) boundedContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx == nil || timeout <= 0 || timeout > service.limits.MaxOperationTimeout {
		return nil, nil, ErrInvalidInput
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	return bounded, cancel, nil
}

func loadRecord(ctx context.Context, tx Transaction, id uuid.UUID) (Record, error) {
	record, found, err := tx.FindMigrationForUpdate(ctx, id)
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, ErrMigrationNotFound
	}
	if err := validateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func validateRecord(record Record) error {
	if record.MigrationID == uuid.Nil || record.TrainRunID == uuid.Nil || record.SourceShard == record.TargetShard ||
		record.SourceGeneration.Int64() <= 0 || record.TargetGeneration.Int64() <= record.SourceGeneration.Int64() ||
		record.RollbackWindow <= 0 {
		return ErrInvalidRecord
	}
	if _, err := record.SourceRoute(); err != nil {
		return ErrInvalidRecord
	}
	if _, err := record.TargetRoute(); err != nil {
		return ErrInvalidRecord
	}
	if _, err := machineAt(record.State, record.TargetGeneration.Int64()); err != nil {
		return ErrInvalidRecord
	}
	if record.CopiedRows < 0 || (record.CopyComplete && record.State == migration.StateCopying) {
		return ErrInvalidRecord
	}
	return nil
}

func recordRoutes(record Record) (sharding.ShardRoute, sharding.ShardRoute, error) {
	source, err := record.SourceRoute()
	if err != nil {
		return sharding.ShardRoute{}, sharding.ShardRoute{}, ErrInvalidRecord
	}
	target, err := record.TargetRoute()
	if err != nil {
		return sharding.ShardRoute{}, sharding.ShardRoute{}, ErrInvalidRecord
	}
	return source, target, nil
}

func samePlan(record Record, input PlanInput) bool {
	return record.MigrationID == input.MigrationID && record.TrainRunID == input.TrainRunID &&
		record.SourceShard == input.SourceShard && record.TargetShard == input.TargetShard &&
		record.SourceGeneration == input.SourceGeneration && record.TargetGeneration == input.TargetGeneration &&
		record.RollbackWindow == input.RollbackWindow
}

func routesEqual(left, right sharding.ShardRoute) bool {
	return left.TrainRunID() == right.TrainRunID() && left.ShardID() == right.ShardID() && left.Generation() == right.Generation()
}

func validateCopyResult(record Record, limit, maxCheckpointBytes int, result CopyBatchResult) error {
	if result.RowsCopied < 0 || result.RowsCopied > limit {
		return ErrInvalidCopyResult
	}
	if result.NextCheckpoint == "" || len(result.NextCheckpoint) > maxCheckpointBytes {
		return ErrInvalidCopyResult
	}
	if !result.Done && (result.RowsCopied == 0 || result.NextCheckpoint == record.Checkpoint) {
		return ErrInvalidCopyResult
	}
	return nil
}

func requireFenceState(ctx context.Context, tx Transaction, route sharding.ShardRoute, expected bool) error {
	enabled, err := tx.WriteFenceEnabledForUpdate(ctx, route)
	if err != nil {
		return err
	}
	if enabled != expected {
		return ErrWriteFenceMismatch
	}
	return nil
}

func validateSnapshot(snapshot ValidationSnapshot) error {
	if snapshot.RowsExamined < 0 || snapshot.InvariantViolations < 0 ||
		snapshot.MissingReservationLocators < 0 || snapshot.MissingTicketOrderLocators < 0 ||
		snapshot.MissingTicketLocators < 0 {
		return ErrInvalidValidation
	}
	if err := validateDigest(snapshot.Source); err != nil {
		return err
	}
	return validateDigest(snapshot.Target)
}

func validateDigest(digest DatasetDigest) error {
	if len(digest.Tables) > maxValidationTables {
		return ErrInvalidValidation
	}
	seen := make(map[string]struct{}, len(digest.Tables))
	for _, table := range digest.Tables {
		if table.Name == "" || len(table.Name) > maxTableNameBytes || table.Rows < 0 ||
			table.Checksum == "" || len(table.Checksum) > maxChecksumBytes {
			return ErrInvalidValidation
		}
		if _, duplicate := seen[table.Name]; duplicate {
			return ErrInvalidValidation
		}
		seen[table.Name] = struct{}{}
	}
	return nil
}

func validationPassed(snapshot ValidationSnapshot) bool {
	return snapshot.InvariantViolations == 0 &&
		snapshot.MissingReservationLocators == 0 &&
		snapshot.MissingTicketOrderLocators == 0 &&
		snapshot.MissingTicketLocators == 0 &&
		digestsEqual(snapshot.Source, snapshot.Target)
}

func digestsEqual(source, target DatasetDigest) bool {
	if len(source.Tables) != len(target.Tables) {
		return false
	}
	sourceTables := append([]TableDigest(nil), source.Tables...)
	targetTables := append([]TableDigest(nil), target.Tables...)
	sort.Slice(sourceTables, func(i, j int) bool { return sourceTables[i].Name < sourceTables[j].Name })
	sort.Slice(targetTables, func(i, j int) bool { return targetTables[i].Name < targetTables[j].Name })
	for index := range sourceTables {
		if sourceTables[index] != targetTables[index] {
			return false
		}
	}
	return true
}

func transitionRecord(record *Record, next migration.State) error {
	machine, err := machineAt(record.State, record.TargetGeneration.Int64())
	if err != nil {
		return ErrInvalidRecord
	}
	if err := machine.Transition(next); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	record.State = machine.State()
	return nil
}

func machineAt(state migration.State, generation int64) (*migration.Machine, error) {
	machine, err := migration.New(generation)
	if err != nil {
		return nil, err
	}
	var path []migration.State
	switch state {
	case migration.StatePlanned:
	case migration.StateDraining:
		path = []migration.State{migration.StateDraining}
	case migration.StateCopying:
		path = []migration.State{migration.StateDraining, migration.StateCopying}
	case migration.StateValidating:
		path = []migration.State{migration.StateDraining, migration.StateCopying, migration.StateValidating}
	case migration.StateCutoverReady:
		path = []migration.State{migration.StateDraining, migration.StateCopying, migration.StateValidating, migration.StateCutoverReady}
	case migration.StateCuttingOver:
		path = []migration.State{migration.StateDraining, migration.StateCopying, migration.StateValidating, migration.StateCutoverReady, migration.StateCuttingOver}
	case migration.StateRollbackWindow:
		path = []migration.State{migration.StateDraining, migration.StateCopying, migration.StateValidating, migration.StateCutoverReady, migration.StateCuttingOver, migration.StateRollbackWindow}
	case migration.StateCompleted:
		path = []migration.State{migration.StateDraining, migration.StateCopying, migration.StateValidating, migration.StateCutoverReady, migration.StateCuttingOver, migration.StateRollbackWindow, migration.StateCompleted}
	case migration.StateFailed:
		path = []migration.State{migration.StateFailed}
	case migration.StateRolledBack:
		path = []migration.State{migration.StateRolledBack}
	default:
		return nil, ErrInvalidRecord
	}
	for _, next := range path {
		if err := machine.Transition(next); err != nil {
			return nil, err
		}
	}
	return machine, nil
}

func isPreCutoverState(state migration.State) bool {
	switch state {
	case migration.StatePlanned, migration.StateDraining, migration.StateCopying,
		migration.StateValidating, migration.StateCutoverReady:
		return true
	default:
		return false
	}
}

func stateAtOrAfterValidation(state migration.State) bool {
	switch state {
	case migration.StateValidating, migration.StateCutoverReady, migration.StateCuttingOver,
		migration.StateRollbackWindow, migration.StateCompleted, migration.StateRolledBack:
		return true
	default:
		return false
	}
}

func stateAtOrAfterCutoverReady(state migration.State) bool {
	switch state {
	case migration.StateCutoverReady, migration.StateCuttingOver, migration.StateRollbackWindow, migration.StateCompleted:
		return true
	default:
		return false
	}
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
