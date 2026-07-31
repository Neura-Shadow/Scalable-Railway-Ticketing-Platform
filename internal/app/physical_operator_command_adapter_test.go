package app

import (
	"context"
	"errors"
	"testing"

	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestPhysicalOperatorCommandAdapterMapsFixedOperationsAndVersions(t *testing.T) {
	for _, operation := range []operatorcommand.Operation{
		operatorcommand.OperationFareInstall, operatorcommand.OperationSeatDisable,
		operatorcommand.OperationSeatEnable, operatorcommand.OperationBookingPolicyBump,
	} {
		t.Run(string(operation), func(t *testing.T) {
			command := adapterCommand(t, operation, sharding.ShardPhysicalZero, 4)
			shard := &operatorCommandShardFake{}
			fareID := uuid.New()
			resolver := &operatorCommandFareFake{snapshotID: fareID}
			adapter, err := NewPhysicalOperatorCommandShardExecutor(
				adapterRouteResolver(t, command.TrainRunID, sharding.ShardPhysicalZero, 4), resolver, shard,
			)
			if err != nil {
				t.Fatal(err)
			}
			shard.result = commandphysical.OperatorMutationResult{ControlResourceID: command.ResourceID,
				ShardResourceID: command.ResourceID, AssignmentGeneration: 4,
				SourceVersion: command.ExpectedSourceVersion + 1, Replayed: true}
			if operation == operatorcommand.OperationFareInstall {
				shard.result.ShardResourceID = fareID
			}
			if operation == operatorcommand.OperationBookingPolicyBump {
				shard.result.BookingPolicyVersion = command.ExpectedBookingPolicyVersion + 1
			}
			receipt, err := adapter.Execute(context.Background(), command, command.FinalizePayload)
			if err != nil || !operatorcommand.ValidReceipt(command, receipt) || shard.calls != 1 {
				t.Fatalf("Execute = (%+v,%v), calls=%d", receipt, err, shard.calls)
			}
			if operation == operatorcommand.OperationFareInstall &&
				(shard.fare.SourceFareID != command.ResourceID || shard.fare.SnapshotFareID != fareID) {
				t.Fatalf("fare mapping = %+v", shard.fare)
			}
			if operation == operatorcommand.OperationBookingPolicyBump &&
				receipt.ResultBookingPolicyVersion != command.ExpectedBookingPolicyVersion+1 {
				t.Fatalf("policy receipt = %+v", receipt)
			}
		})
	}
}

func TestPhysicalOperatorCommandAdapterRouteMismatchFailsBeforeShardExecution(t *testing.T) {
	command := adapterCommand(t, operatorcommand.OperationFareInstall, sharding.ShardPhysicalZero, 4)
	for name, resolver := range map[string]*operatorCommandRouteFake{
		"shard":      adapterRouteResolver(t, command.TrainRunID, sharding.ShardPhysicalOne, 4),
		"generation": adapterRouteResolver(t, command.TrainRunID, sharding.ShardPhysicalZero, 5),
	} {
		t.Run(name, func(t *testing.T) {
			shard := &operatorCommandShardFake{}
			fares := &operatorCommandFareFake{snapshotID: uuid.New()}
			adapter, _ := NewPhysicalOperatorCommandShardExecutor(resolver, fares, shard)
			if _, err := adapter.Execute(context.Background(), command, command.FinalizePayload); !errors.Is(err, sharding.ErrAssignmentStale) {
				t.Fatalf("route mismatch error = %v", err)
			}
			if shard.calls != 0 || fares.calls != 0 {
				t.Fatalf("mismatch reached fare/shard: fare=%d shard=%d", fares.calls, shard.calls)
			}
		})
	}
}

func TestPhysicalOperatorCommandAdapterPreservesAmbiguousRouteErrors(t *testing.T) {
	command := adapterCommand(t, operatorcommand.OperationSeatDisable, sharding.ShardPhysicalZero, 4)
	for name, routeError := range map[string]error{
		"timeout":     context.DeadlineExceeded,
		"unavailable": sharding.ErrShardUnavailable,
	} {
		t.Run(name, func(t *testing.T) {
			shard := &operatorCommandShardFake{}
			adapter, _ := NewPhysicalOperatorCommandShardExecutor(
				&operatorCommandRouteFake{err: routeError},
				&operatorCommandFareFake{snapshotID: uuid.New()},
				shard,
			)

			_, err := adapter.Execute(context.Background(), command, command.FinalizePayload)
			if !errors.Is(err, routeError) || errors.Is(err, sharding.ErrAssignmentStale) {
				t.Fatalf("ambiguous route error = %v", err)
			}
			if shard.calls != 0 {
				t.Fatalf("ambiguous route error reached shard %d times", shard.calls)
			}
		})
	}
}

func TestPhysicalOperatorCommandAdapterRejectsShardResultRouteMismatch(t *testing.T) {
	command := adapterCommand(t, operatorcommand.OperationSeatDisable, sharding.ShardPhysicalZero, 4)
	shard := &operatorCommandShardFake{result: commandphysical.OperatorMutationResult{
		ControlResourceID: command.ResourceID, ShardResourceID: command.ResourceID,
		AssignmentGeneration: 5, SourceVersion: command.ExpectedSourceVersion + 1,
	}}
	adapter, _ := NewPhysicalOperatorCommandShardExecutor(
		adapterRouteResolver(t, command.TrainRunID, sharding.ShardPhysicalZero, 4),
		&operatorCommandFareFake{snapshotID: uuid.New()}, shard,
	)
	if _, err := adapter.Execute(context.Background(), command, command.FinalizePayload); !errors.Is(err, operatorcommand.ErrReceiptMismatch) {
		t.Fatalf("result route mismatch error = %v", err)
	}
}

func adapterCommand(t *testing.T, operation operatorcommand.Operation, shardID sharding.ShardID, generationValue int64) operatorcommand.Command {
	t.Helper()
	trainRunID := uuid.New()
	resourceID := uuid.New()
	payload := operatorcommand.BoundedFinalizePayload{}
	policy := int64(0)
	switch operation {
	case operatorcommand.OperationFareInstall:
		payload = operatorcommand.BoundedFinalizePayload{FromStopIndex: 0, ToStopIndex: 2,
			SeatClass: "standard", AmountMinor: 100, Currency: "TWD"}
	case operatorcommand.OperationSeatEnable:
		payload.SeatActive = true
	case operatorcommand.OperationBookingPolicyBump:
		resourceID = trainRunID
		policy = 7
	}
	generation, _ := sharding.NewAssignmentGeneration(generationValue)
	route, _ := sharding.NewShardRoute(trainRunID, shardID, generation)
	return operatorcommand.Command{ID: uuid.New(), ActorID: uuid.New(), TrainRunID: trainRunID,
		ResourceID: resourceID, Operation: operation, IdempotencyKeyHash: [32]byte{1},
		RequestFingerprint: [32]byte{2}, Route: route, ExpectedSourceVersion: 11,
		ExpectedBookingPolicyVersion: policy, FinalizePayload: payload, State: operatorcommand.StateReserved}
}

type operatorCommandRouteFake struct {
	resolution shardphysical.Resolution
	err        error
}

func (resolver *operatorCommandRouteFake) Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error) {
	return resolver.resolution, resolver.err
}

func adapterRouteResolver(t *testing.T, trainRunID uuid.UUID, shardID sharding.ShardID, generationValue int64) *operatorCommandRouteFake {
	t.Helper()
	pool := &operatorCommandPool{}
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{shardID.String(): {ShardID: shardID, DSN: "synthetic"}},
		MaxCount:    1, Limits: shardphysical.PoolLimits{MaxOpenConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) { return pool, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	handle, err := registry.Resolve(shardphysical.CatalogEntry{ShardID: shardID,
		StorageKind: shardphysical.StoragePostgres, ConnectionRef: shardID.String(), ProtocolVersion: 1,
		SchemaVersion: 1, Enabled: true, WriteEnabled: true, HealthState: shardphysical.HealthHealthy,
		State: shardphysical.StateActive})
	if err != nil {
		t.Fatal(err)
	}
	generation, _ := sharding.NewAssignmentGeneration(generationValue)
	route, _ := sharding.NewShardRoute(trainRunID, shardID, generation)
	return &operatorCommandRouteFake{resolution: shardphysical.Resolution{Route: route, Handle: handle}}
}

type operatorCommandPool struct{}

func (*operatorCommandPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (*operatorCommandPool) Close() {}

type operatorCommandFareFake struct {
	snapshotID uuid.UUID
	calls      int
}

func (resolver *operatorCommandFareFake) ResolveFareSnapshotID(context.Context, uuid.UUID, uuid.UUID) (uuid.UUID, error) {
	resolver.calls++
	return resolver.snapshotID, nil
}

type operatorCommandShardFake struct {
	result commandphysical.OperatorMutationResult
	calls  int
	fare   commandphysical.FareInstallCommand
}

func (shard *operatorCommandShardFake) InstallFare(_ context.Context, command commandphysical.FareInstallCommand) (commandphysical.OperatorMutationResult, error) {
	shard.calls++
	shard.fare = command
	return shard.result, nil
}
func (shard *operatorCommandShardFake) SetSeatActive(context.Context, commandphysical.SeatActiveCommand) (commandphysical.OperatorMutationResult, error) {
	shard.calls++
	return shard.result, nil
}
func (shard *operatorCommandShardFake) BumpBookingPolicy(context.Context, commandphysical.BookingPolicyBumpCommand) (commandphysical.OperatorMutationResult, error) {
	shard.calls++
	return shard.result, nil
}
