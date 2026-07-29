package physical

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CancelTrainRun commits the booking-critical local snapshot transition before
// the caller updates the control read model. Its deterministic server command
// identity makes a retry after control failure return the same shard receipt.
func (executor *Executor) CancelTrainRun(ctx context.Context, trainRunID uuid.UUID) error {
	if executor == nil || ctx == nil || trainRunID == uuid.Nil {
		return ErrInvalidPayload
	}
	resolved, err := executor.router.Resolve(ctx, trainRunID, false)
	if err != nil || resolved.Handle.Pool() == nil {
		return sharding.ErrShardUnavailable
	}
	generation := resolved.Route.Generation().Int64()
	commandID := uuid.NewSHA1(trainRunID, []byte("train-run.cancel:"+strconv.FormatInt(generation, 10)))
	fingerprint := sha256.Sum256([]byte("train_run.cancel:" + trainRunID.String() + ":" + strconv.FormatInt(generation, 10)))
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return sharding.ErrShardUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var storedFingerprint []byte
	var resultID uuid.UUID
	var receiptStatus string
	err = tx.QueryRow(ctx, `SELECT request_fingerprint,result_id,status FROM booking_command_receipts WHERE command_id=$1`, commandID).Scan(&storedFingerprint, &resultID, &receiptStatus)
	if err == nil {
		if receiptStatus != "succeeded" || resultID != trainRunID || !bytes.Equal(storedFingerprint, fingerprint[:]) {
			return ErrShardPersistence
		}
		if err := tx.Commit(ctx); err != nil {
			return ErrShardPersistence
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ErrShardPersistence
	}
	if !resolved.Handle.WriteEnabled() {
		return sharding.ErrWriteFenced
	}
	var localGeneration int64
	var writeEnabled bool
	var fenceState, snapshotStatus string
	err = tx.QueryRow(ctx, `
SELECT fence.assignment_generation,fence.write_enabled,fence.state,snapshot.status
FROM train_run_write_fences AS fence
JOIN train_run_booking_snapshots AS snapshot
  ON snapshot.train_run_id=fence.train_run_id
 AND snapshot.assignment_generation=fence.assignment_generation
WHERE fence.train_run_id=$1
FOR UPDATE OF fence,snapshot`, trainRunID).Scan(&localGeneration, &writeEnabled, &fenceState, &snapshotStatus)
	if err != nil {
		return sharding.ErrShardUnavailable
	}
	if localGeneration != generation {
		return sharding.ErrAssignmentStale
	}
	if !writeEnabled || fenceState != "active" {
		return sharding.ErrWriteFenced
	}
	if snapshotStatus == "departed" || snapshotStatus == "completed" {
		return ErrInvalidPayload
	}
	if err := execOne(ctx, tx, `
INSERT INTO booking_command_receipts(id,command_id,train_run_id,assignment_generation,command_type,request_fingerprint,status)
VALUES($1,$2,$3,$4,'train_run.cancel',$5,'started')`, uuid.NewSHA1(commandID, []byte("command-receipt")), commandID, trainRunID, generation, fingerprint[:]); err != nil {
		return err
	}
	if err := execOne(ctx, tx, `UPDATE train_run_booking_snapshots
SET status='cancelled',bookable=false,source_version=source_version+1,source_updated_at=clock_timestamp()
WHERE train_run_id=$1 AND assignment_generation=$2 AND active`, trainRunID, generation); err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{"command_id": commandID, "train_run_id": trainRunID, "assignment_generation": generation, "status": "cancelled"})
	if err != nil {
		return ErrShardPersistence
	}
	if err := execOne(ctx, tx, `INSERT INTO outbox_events(id,train_run_id,assignment_generation,aggregate_type,aggregate_id,event_type,payload)
VALUES($1,$2,$3,'train_run',$2,'trainrun.cancelled',$4::jsonb)`, uuid.NewSHA1(commandID, []byte("trainrun.cancelled")), trainRunID, generation, string(payload)); err != nil {
		return err
	}
	if err := execOne(ctx, tx, `INSERT INTO train_run_target_write_evidence(
 id,train_run_id,assignment_generation,successful_write_count,first_successful_write_at,last_successful_write_at,last_command_id
) VALUES($1,$2,$3,1,clock_timestamp(),clock_timestamp(),$4)
ON CONFLICT(train_run_id,assignment_generation) DO UPDATE
SET successful_write_count=train_run_target_write_evidence.successful_write_count+1,
 first_successful_write_at=COALESCE(train_run_target_write_evidence.first_successful_write_at,EXCLUDED.first_successful_write_at),
 last_successful_write_at=EXCLUDED.last_successful_write_at,last_command_id=EXCLUDED.last_command_id`, uuid.NewSHA1(trainRunID, []byte("target-write-evidence:"+resolved.Route.ShardID().String()+":"+strconv.FormatInt(generation, 10))), trainRunID, generation, commandID); err != nil {
		return err
	}
	if err := execOne(ctx, tx, `UPDATE booking_command_receipts
SET status='succeeded',result_type='train_run',result_id=$2,completed_at=clock_timestamp()
WHERE command_id=$1 AND status='started'`, commandID, trainRunID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrShardPersistence
	}
	return nil
}
