// Package physical reads immutable operator-command receipts from the train
// run's current authoritative physical shard.
package physical

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type CurrentRouteResolver interface {
	Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error)
}

type Inspector struct{ resolver CurrentRouteResolver }

func NewInspector(resolver CurrentRouteResolver) (*Inspector, error) {
	if resolver == nil {
		return nil, operatorcommand.ErrInvalidOptions
	}
	return &Inspector{resolver: resolver}, nil
}

func (inspector *Inspector) Inspect(ctx context.Context, candidate operatorcommand.Candidate) (operatorcommand.Receipt, bool, error) {
	if inspector == nil || ctx == nil || candidate.Command.ID == uuid.Nil || candidate.Command.TrainRunID == uuid.Nil {
		return operatorcommand.Receipt{}, false, operatorcommand.ErrReceiptMismatch
	}
	resolution, err := inspector.resolver.Resolve(ctx, candidate.Command.TrainRunID, false)
	if err != nil || resolution.Route.TrainRunID() != candidate.Command.TrainRunID ||
		(resolution.Route.ShardID() != sharding.ShardPhysicalZero &&
			resolution.Route.ShardID() != sharding.ShardPhysicalOne) || resolution.Handle.Pool() == nil {
		return operatorcommand.Receipt{}, false, operatorcommand.ErrShardUnreachable
	}
	tx, err := resolution.Handle.Pool().BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return operatorcommand.Receipt{}, false, operatorcommand.ErrShardUnreachable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var receipt operatorcommand.Receipt
	var rawFingerprint []byte
	var status string
	var resultSource pgtype.Int8
	var resultPolicy pgtype.Int8
	err = tx.QueryRow(ctx, `SELECT command_id,train_run_id,assignment_generation,command_type,
 request_fingerprint,status,result_id,result_source_version,result_booking_policy_version
FROM booking_command_receipts WHERE command_id=$1`, candidate.Command.ID).Scan(
		&receipt.CommandID, &receipt.TrainRunID, &receipt.HistoricalGeneration, &receipt.Operation,
		&rawFingerprint, &status, &receipt.ResourceID, &resultSource, &resultPolicy,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return operatorcommand.Receipt{}, false, operatorcommand.ErrShardUnreachable
		}
		return operatorcommand.Receipt{}, false, nil
	}
	if err != nil {
		return operatorcommand.Receipt{}, false, operatorcommand.ErrShardUnreachable
	}
	if len(rawFingerprint) != 32 || status != "succeeded" || !resultSource.Valid {
		return operatorcommand.Receipt{}, false, operatorcommand.ErrReceiptMismatch
	}
	copy(receipt.RequestFingerprint[:], rawFingerprint)
	receipt.HistoricalShardID = candidate.Command.Route.ShardID()
	receipt.ResultSourceVersion = resultSource.Int64
	if resultPolicy.Valid {
		receipt.ResultBookingPolicyVersion = resultPolicy.Int64
	}
	if !operatorcommand.ValidReceipt(candidate.Command, receipt) {
		return operatorcommand.Receipt{}, false, operatorcommand.ErrReceiptMismatch
	}
	if err := tx.Commit(ctx); err != nil {
		return operatorcommand.Receipt{}, false, operatorcommand.ErrShardUnreachable
	}
	return receipt, true, nil
}

var _ operatorcommand.ReceiptInspector = (*Inspector)(nil)
