package app

import (
	"context"
	"errors"

	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
)

type operatorCommandRouteResolver interface {
	Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error)
}

type operatorCommandFareResolver interface {
	ResolveFareSnapshotID(context.Context, uuid.UUID, uuid.UUID) (uuid.UUID, error)
}

type PhysicalOperatorCommandShardExecutor struct {
	routes operatorCommandRouteResolver
	fares  operatorCommandFareResolver
	shard  physicalOperatorSnapshotExecutor
}

func NewPhysicalOperatorCommandShardExecutor(routes operatorCommandRouteResolver, fares operatorCommandFareResolver, shard physicalOperatorSnapshotExecutor) (*PhysicalOperatorCommandShardExecutor, error) {
	if routes == nil || fares == nil || shard == nil {
		return nil, operatorcommand.ErrInvalidRequest
	}
	return &PhysicalOperatorCommandShardExecutor{routes: routes, fares: fares, shard: shard}, nil
}

func (executor *PhysicalOperatorCommandShardExecutor) Execute(ctx context.Context, command operatorcommand.Command, mutation operatorcommand.Mutation) (operatorcommand.Receipt, error) {
	if executor == nil || ctx == nil || command.ID == uuid.Nil || command.TrainRunID == uuid.Nil ||
		command.ResourceID == uuid.Nil || mutation != command.FinalizePayload {
		return operatorcommand.Receipt{}, operatorcommand.ErrInvalidRequest
	}
	resolved, err := executor.routes.Resolve(ctx, command.TrainRunID, false)
	if err != nil {
		return operatorcommand.Receipt{}, err
	}
	if resolved.Handle.Pool() == nil {
		return operatorcommand.Receipt{}, sharding.ErrShardUnavailable
	}
	if resolved.Route.TrainRunID() != command.TrainRunID ||
		resolved.Route.ShardID() != command.Route.ShardID() ||
		resolved.Route.Generation() != command.Route.Generation() ||
		resolved.Handle.ShardID() != command.Route.ShardID() {
		return operatorcommand.Receipt{}, sharding.ErrAssignmentStale
	}
	if !resolved.Handle.WriteEnabled() {
		return operatorcommand.Receipt{}, sharding.ErrWriteFenced
	}

	var result commandphysical.OperatorMutationResult
	expectedShardResourceID := command.ResourceID
	switch command.Operation {
	case operatorcommand.OperationFareInstall:
		snapshotID, resolveErr := executor.fares.ResolveFareSnapshotID(ctx, command.TrainRunID, command.ResourceID)
		if resolveErr != nil || snapshotID == uuid.Nil {
			return operatorcommand.Receipt{}, operatorcommand.ErrShardExecution
		}
		expectedShardResourceID = snapshotID
		result, err = executor.shard.InstallFare(ctx, commandphysical.FareInstallCommand{
			CommandID: command.ID, TrainRunID: command.TrainRunID, SourceFareID: command.ResourceID,
			SnapshotFareID: snapshotID, ExpectedSourceVersion: command.ExpectedSourceVersion,
			FromStopIndex: mutation.FromStopIndex, ToStopIndex: mutation.ToStopIndex,
			SeatClass: mutation.SeatClass, AmountMinor: mutation.AmountMinor, Currency: mutation.Currency,
			RequestFingerprint: command.RequestFingerprint,
		})
	case operatorcommand.OperationSeatDisable, operatorcommand.OperationSeatEnable:
		if mutation.SeatActive != (command.Operation == operatorcommand.OperationSeatEnable) {
			return operatorcommand.Receipt{}, operatorcommand.ErrInvalidRequest
		}
		result, err = executor.shard.SetSeatActive(ctx, commandphysical.SeatActiveCommand{
			CommandID: command.ID, TrainRunID: command.TrainRunID, SeatID: command.ResourceID,
			ExpectedSourceVersion: command.ExpectedSourceVersion, Active: mutation.SeatActive,
			RequestFingerprint: command.RequestFingerprint,
		})
	case operatorcommand.OperationBookingPolicyBump:
		if command.ResourceID != command.TrainRunID || command.ExpectedBookingPolicyVersion <= 0 {
			return operatorcommand.Receipt{}, operatorcommand.ErrInvalidRequest
		}
		result, err = executor.shard.BumpBookingPolicy(ctx, commandphysical.BookingPolicyBumpCommand{
			CommandID: command.ID, TrainRunID: command.TrainRunID,
			ExpectedSourceVersion:        command.ExpectedSourceVersion,
			ExpectedBookingPolicyVersion: command.ExpectedBookingPolicyVersion,
			RequestFingerprint:           command.RequestFingerprint,
		})
	default:
		return operatorcommand.Receipt{}, operatorcommand.ErrInvalidRequest
	}
	if err != nil {
		return operatorcommand.Receipt{}, err
	}
	if result.ControlResourceID != command.ResourceID || result.ShardResourceID != expectedShardResourceID ||
		result.AssignmentGeneration != command.Route.Generation().Int64() ||
		result.SourceVersion != command.ExpectedSourceVersion+1 {
		return operatorcommand.Receipt{}, operatorcommand.ErrReceiptMismatch
	}
	if (command.Operation == operatorcommand.OperationBookingPolicyBump &&
		result.BookingPolicyVersion != command.ExpectedBookingPolicyVersion+1) ||
		(command.Operation != operatorcommand.OperationBookingPolicyBump && result.BookingPolicyVersion != 0) {
		return operatorcommand.Receipt{}, operatorcommand.ErrReceiptMismatch
	}
	receipt := operatorcommand.Receipt{
		CommandID: command.ID, TrainRunID: command.TrainRunID, ResourceID: command.ResourceID,
		Operation: command.Operation, RequestFingerprint: command.RequestFingerprint,
		HistoricalShardID:    command.Route.ShardID(),
		HistoricalGeneration: command.Route.Generation().Int64(),
		ResultSourceVersion:  result.SourceVersion,
	}
	if command.Operation == operatorcommand.OperationBookingPolicyBump {
		receipt.ResultBookingPolicyVersion = result.BookingPolicyVersion
	}
	if !operatorcommand.ValidReceipt(command, receipt) {
		return operatorcommand.Receipt{}, operatorcommand.ErrReceiptMismatch
	}
	return receipt, nil
}

// PostgresOperatorCommandFareResolver resolves the deterministic copied fare
// identity without exposing route or connection details to the caller.
type PostgresOperatorCommandFareResolver struct{ control controlRouteReader }

func NewPostgresOperatorCommandFareResolver(control controlRouteReader) (*PostgresOperatorCommandFareResolver, error) {
	if control == nil {
		return nil, operatorcommand.ErrInvalidRequest
	}
	return &PostgresOperatorCommandFareResolver{control: control}, nil
}

func (resolver *PostgresOperatorCommandFareResolver) ResolveFareSnapshotID(ctx context.Context, trainRunID, sourceFareID uuid.UUID) (uuid.UUID, error) {
	if resolver == nil || ctx == nil || trainRunID == uuid.Nil || sourceFareID == uuid.Nil {
		return uuid.Nil, operatorcommand.ErrInvalidRequest
	}
	var snapshotID uuid.UUID
	err := resolver.control.QueryRow(ctx, `SELECT public.physical_source_entity_id($1,'fare',$2)`,
		trainRunID, sourceFareID).Scan(&snapshotID)
	if err != nil || snapshotID == uuid.Nil {
		return uuid.Nil, errors.New("operator fare snapshot unavailable")
	}
	return snapshotID, nil
}

var _ operatorcommand.ShardExecutor = (*PhysicalOperatorCommandShardExecutor)(nil)
