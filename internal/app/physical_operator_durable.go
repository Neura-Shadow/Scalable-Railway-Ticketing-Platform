package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	operatorpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type operatorCommandCoordinator interface {
	Execute(context.Context, operatorcommand.Request) (operatorcommand.Result, error)
}

// DurablePhysicalOperatorSnapshotMutations converts the bounded operator HTTP
// contract into a durable control command before any physical-shard write.
type DurablePhysicalOperatorSnapshotMutations struct{ coordinator operatorCommandCoordinator }

func NewDurablePhysicalOperatorSnapshotMutations(coordinator operatorCommandCoordinator) (*DurablePhysicalOperatorSnapshotMutations, error) {
	if coordinator == nil {
		return nil, operatorcommand.ErrInvalidRequest
	}
	return &DurablePhysicalOperatorSnapshotMutations{coordinator: coordinator}, nil
}

func (mutations *DurablePhysicalOperatorSnapshotMutations) InstallFare(ctx context.Context, input OperatorFareMutation) (httpapi.ResourceView, error) {
	input.ActorID = strings.TrimSpace(input.ActorID)
	input.SeatClass = strings.ToLower(strings.TrimSpace(input.SeatClass))
	input.Currency = strings.ToUpper(strings.TrimSpace(input.Currency))
	actorID, err := uuid.Parse(input.ActorID)
	if mutations == nil || mutations.coordinator == nil || ctx == nil || err != nil ||
		!validOperatorMutationIdentity(input.ActorID, input.IdempotencyKey) || input.TrainRunID == uuid.Nil ||
		input.FareID == uuid.Nil || input.ExpectedSourceVersion < 1 || input.FromStopIndex < 0 ||
		input.FromStopIndex >= input.ToStopIndex || input.AmountMinor < 0 {
		return httpapi.ResourceView{}, httpapi.ErrInvalidInput
	}
	payload := operatorcommand.BoundedFinalizePayload{FromStopIndex: input.FromStopIndex,
		ToStopIndex: input.ToStopIndex, SeatClass: input.SeatClass, AmountMinor: input.AmountMinor,
		Currency: input.Currency}
	result, err := mutations.execute(ctx, actorID, input.IdempotencyKey, input.TrainRunID, input.FareID,
		operatorcommand.OperationFareInstall, input.ExpectedSourceVersion, 0, payload)
	return operatorResourceView(result, err)
}

func (mutations *DurablePhysicalOperatorSnapshotMutations) SetSeatActive(ctx context.Context, input OperatorSeatMutation) (httpapi.ResourceView, error) {
	input.ActorID = strings.TrimSpace(input.ActorID)
	actorID, err := uuid.Parse(input.ActorID)
	if mutations == nil || mutations.coordinator == nil || ctx == nil || err != nil ||
		!validOperatorMutationIdentity(input.ActorID, input.IdempotencyKey) || input.TrainRunID == uuid.Nil ||
		input.SeatID == uuid.Nil || input.ExpectedSourceVersion < 1 {
		return httpapi.ResourceView{}, httpapi.ErrInvalidInput
	}
	operation := operatorcommand.OperationSeatDisable
	if input.Active {
		operation = operatorcommand.OperationSeatEnable
	}
	result, err := mutations.execute(ctx, actorID, input.IdempotencyKey, input.TrainRunID, input.SeatID,
		operation, input.ExpectedSourceVersion, 0,
		operatorcommand.BoundedFinalizePayload{SeatActive: input.Active})
	return operatorResourceView(result, err)
}

func (mutations *DurablePhysicalOperatorSnapshotMutations) BumpBookingPolicy(ctx context.Context, input OperatorPolicyMutation) (httpapi.ResourceView, error) {
	input.ActorID = strings.TrimSpace(input.ActorID)
	actorID, err := uuid.Parse(input.ActorID)
	if mutations == nil || mutations.coordinator == nil || ctx == nil || err != nil ||
		!validOperatorMutationIdentity(input.ActorID, input.IdempotencyKey) || input.TrainRunID == uuid.Nil ||
		input.ExpectedSourceVersion < 1 || input.ExpectedBookingPolicyVersion < 1 {
		return httpapi.ResourceView{}, httpapi.ErrInvalidInput
	}
	result, err := mutations.execute(ctx, actorID, input.IdempotencyKey, input.TrainRunID, input.TrainRunID,
		operatorcommand.OperationBookingPolicyBump, input.ExpectedSourceVersion,
		input.ExpectedBookingPolicyVersion, operatorcommand.BoundedFinalizePayload{})
	return operatorResourceView(result, err)
}

func (mutations *DurablePhysicalOperatorSnapshotMutations) execute(
	ctx context.Context,
	actorID uuid.UUID,
	idempotencyKey string,
	trainRunID uuid.UUID,
	resourceID uuid.UUID,
	operation operatorcommand.Operation,
	expectedSourceVersion int64,
	expectedBookingPolicyVersion int64,
	payload operatorcommand.BoundedFinalizePayload,
) (operatorcommand.Result, error) {
	keyHash := sha256.Sum256([]byte(idempotencyKey))
	fingerprintPayload, err := json.Marshal(struct {
		TrainRunID                   uuid.UUID                              `json:"train_run_id"`
		ResourceID                   uuid.UUID                              `json:"resource_id"`
		Operation                    operatorcommand.Operation              `json:"operation"`
		ExpectedSourceVersion        int64                                  `json:"expected_source_version"`
		ExpectedBookingPolicyVersion int64                                  `json:"expected_booking_policy_version"`
		Payload                      operatorcommand.BoundedFinalizePayload `json:"payload"`
	}{trainRunID, resourceID, operation, expectedSourceVersion, expectedBookingPolicyVersion, payload})
	if err != nil {
		return operatorcommand.Result{}, operatorcommand.ErrInvalidRequest
	}
	return mutations.coordinator.Execute(ctx, operatorcommand.Request{
		ActorID: actorID, TrainRunID: trainRunID, ResourceID: resourceID, Operation: operation,
		IdempotencyKeyHash: keyHash, RequestFingerprint: sha256.Sum256(fingerprintPayload),
		ExpectedSourceVersion:        expectedSourceVersion,
		ExpectedBookingPolicyVersion: expectedBookingPolicyVersion, Mutation: payload,
	})
}

func operatorResourceView(result operatorcommand.Result, err error) (httpapi.ResourceView, error) {
	if err == nil {
		return httpapi.ResourceView{ID: result.ResourceID.String()}, nil
	}
	switch {
	case errors.Is(err, operatorcommand.ErrInvalidRequest):
		return httpapi.ResourceView{}, httpapi.ErrInvalidInput
	case errors.Is(err, operatorpostgres.ErrIdempotencyConflict):
		return httpapi.ResourceView{}, httpapi.ErrConflict
	case errors.Is(err, operatorcommand.ErrReceiptMismatch):
		return httpapi.ResourceView{}, httpapi.ErrConflict
	default:
		return httpapi.ResourceView{}, httpapi.ErrUnavailable
	}
}

// PostgresDurableOperatorCommandFinalizer atomically advances the durable
// command and its bounded control projection after validating the shard receipt.
type PostgresDurableOperatorCommandFinalizer struct{ db operatorControlDB }

func NewPostgresDurableOperatorCommandFinalizer(db operatorControlDB) (*PostgresDurableOperatorCommandFinalizer, error) {
	if db == nil {
		return nil, operatorcommand.ErrControlUnavailable
	}
	return &PostgresDurableOperatorCommandFinalizer{db: db}, nil
}

func (finalizer *PostgresDurableOperatorCommandFinalizer) Finalize(ctx context.Context, command operatorcommand.Command, receipt operatorcommand.Receipt) error {
	if finalizer == nil || ctx == nil || !operatorcommand.ValidReceipt(command, receipt) {
		return operatorcommand.ErrReceiptMismatch
	}
	tx, err := finalizer.db.Begin(ctx)
	if err != nil {
		return operatorcommand.ErrControlUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var state string
	var resultSource, resultPolicy pgtype.Int8
	lockArgs := []any{command.ID, command.ActorID, command.TrainRunID, command.ResourceID, command.Operation,
		command.RequestFingerprint[:], command.Route.ShardID().String(), command.Route.Generation().Int64(),
		command.ExpectedSourceVersion, nullableOperatorInt(command.ExpectedBookingPolicyVersion)}
	lockArgs = append(lockArgs, operatorFinalizeValues(command.Operation, command.FinalizePayload)...)
	err = tx.QueryRow(ctx, `SELECT state,result_source_version,result_booking_policy_version
FROM public.operator_booking_commands
WHERE command_id=$1 AND actor_id=$2 AND train_run_id=$3 AND resource_id=$4
  AND operation=$5 AND request_fingerprint=$6 AND target_shard_id=$7
  AND assignment_generation=$8 AND expected_source_version=$9
  AND expected_booking_policy_version IS NOT DISTINCT FROM $10
  AND finalize_from_stop_index IS NOT DISTINCT FROM $11
  AND finalize_to_stop_index IS NOT DISTINCT FROM $12
  AND finalize_seat_class IS NOT DISTINCT FROM $13
  AND finalize_amount_minor IS NOT DISTINCT FROM $14
  AND finalize_currency IS NOT DISTINCT FROM $15
  AND finalize_seat_active IS NOT DISTINCT FROM $16
FOR UPDATE`, lockArgs...).Scan(&state, &resultSource, &resultPolicy)
	if err != nil {
		return operatorcommand.ErrControlUnavailable
	}
	if state == string(operatorcommand.StateFinalized) {
		if !resultSource.Valid || resultSource.Int64 != receipt.ResultSourceVersion ||
			resultPolicy.Valid != (receipt.ResultBookingPolicyVersion > 0) ||
			(resultPolicy.Valid && resultPolicy.Int64 != receipt.ResultBookingPolicyVersion) {
			return operatorcommand.ErrReceiptMismatch
		}
		return tx.Commit(ctx)
	}
	if state != string(operatorcommand.StateReserved) && state != string(operatorcommand.StateCommittedOnShard) &&
		state != string(operatorcommand.StateNeedsRepair) {
		return operatorcommand.ErrReceiptMismatch
	}
	if err := applyDurableOperatorProjection(ctx, tx, command, receipt); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE public.operator_booking_commands
SET result_source_version=$2,result_booking_policy_version=$3,state='finalized',
    lease_owner=NULL,lease_until=NULL,bounded_error_category=NULL,updated_at=clock_timestamp(),
    completed_at=COALESCE(completed_at,clock_timestamp())
WHERE command_id=$1 AND state IN ('reserved','committed_on_shard','needs_repair')`,
		command.ID, receipt.ResultSourceVersion, nullableOperatorInt(receipt.ResultBookingPolicyVersion))
	if err != nil || tag.RowsAffected() != 1 {
		return operatorcommand.ErrControlUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return operatorcommand.ErrControlUnavailable
	}
	return nil
}

func applyDurableOperatorProjection(ctx context.Context, tx pgx.Tx, command operatorcommand.Command, receipt operatorcommand.Receipt) error {
	var eventType, aggregateType string
	switch command.Operation {
	case operatorcommand.OperationFareInstall:
		tag, err := tx.Exec(ctx, `UPDATE public.fares
SET amount_minor=$3,currency=$4,active=true,source_version=$5,last_booking_command_id=$6,updated_at=clock_timestamp()
WHERE id=$1 AND train_run_id=$2 AND source_version=$7
  AND from_stop_index=$8 AND to_stop_index=$9 AND seat_class=$10`, command.ResourceID, command.TrainRunID,
			command.FinalizePayload.AmountMinor, command.FinalizePayload.Currency, receipt.ResultSourceVersion,
			command.ID, command.ExpectedSourceVersion, command.FinalizePayload.FromStopIndex,
			command.FinalizePayload.ToStopIndex, command.FinalizePayload.SeatClass)
		if err != nil || tag.RowsAffected() != 1 {
			return operatorcommand.ErrControlUnavailable
		}
		eventType, aggregateType = "fare.updated", "fare"
	case operatorcommand.OperationSeatDisable, operatorcommand.OperationSeatEnable:
		tag, err := tx.Exec(ctx, `INSERT INTO public.train_run_seat_booking_overrides(
 train_run_id,seat_id,active,source_version,command_id,updated_at
) VALUES($1,$2,$3,$4,$5,clock_timestamp())
ON CONFLICT(train_run_id,seat_id) DO UPDATE SET active=EXCLUDED.active,
 source_version=EXCLUDED.source_version,command_id=EXCLUDED.command_id,updated_at=EXCLUDED.updated_at
WHERE train_run_seat_booking_overrides.source_version=$6`, command.TrainRunID, command.ResourceID,
			command.FinalizePayload.SeatActive, receipt.ResultSourceVersion, command.ID, command.ExpectedSourceVersion)
		if err != nil || tag.RowsAffected() != 1 {
			return operatorcommand.ErrControlUnavailable
		}
		eventType, aggregateType = "seat.updated", "seat"
	case operatorcommand.OperationBookingPolicyBump:
		tag, err := tx.Exec(ctx, `INSERT INTO public.train_run_booking_policy_versions(
 train_run_id,booking_policy_version,source_version,command_id,updated_at
) VALUES($1,$2,$3,$4,clock_timestamp())
ON CONFLICT(train_run_id) DO UPDATE SET booking_policy_version=EXCLUDED.booking_policy_version,
 source_version=EXCLUDED.source_version,command_id=EXCLUDED.command_id,updated_at=EXCLUDED.updated_at
WHERE train_run_booking_policy_versions.booking_policy_version=$5
  AND train_run_booking_policy_versions.source_version=$6`, command.TrainRunID,
			receipt.ResultBookingPolicyVersion, receipt.ResultSourceVersion, command.ID,
			command.ExpectedBookingPolicyVersion, command.ExpectedSourceVersion)
		if err != nil || tag.RowsAffected() != 1 {
			return operatorcommand.ErrControlUnavailable
		}
		eventType, aggregateType = "trainrun.updated", "train_run"
	default:
		return operatorcommand.ErrInvalidRequest
	}
	return appendControlProjectionEvent(ctx, tx, command.TrainRunID, command.Route.Generation().Int64(),
		aggregateType, command.ResourceID, eventType, receipt.ResultSourceVersion)
}

func nullableOperatorInt(value int64) any {
	if value < 1 {
		return nil
	}
	return value
}

func operatorFinalizeValues(operation operatorcommand.Operation, payload operatorcommand.BoundedFinalizePayload) []any {
	switch operation {
	case operatorcommand.OperationFareInstall:
		return []any{payload.FromStopIndex, payload.ToStopIndex, payload.SeatClass, payload.AmountMinor, payload.Currency, nil}
	case operatorcommand.OperationSeatDisable, operatorcommand.OperationSeatEnable:
		return []any{nil, nil, nil, nil, nil, payload.SeatActive}
	default:
		return []any{nil, nil, nil, nil, nil, nil}
	}
}

var (
	_ operatorcommand.Finalizer = (*PostgresDurableOperatorCommandFinalizer)(nil)
	_ interface {
		InstallFare(context.Context, OperatorFareMutation) (httpapi.ResourceView, error)
		SetSeatActive(context.Context, OperatorSeatMutation) (httpapi.ResourceView, error)
		BumpBookingPolicy(context.Context, OperatorPolicyMutation) (httpapi.ResourceView, error)
	} = (*DurablePhysicalOperatorSnapshotMutations)(nil)
)
