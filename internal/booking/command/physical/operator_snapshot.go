package physical

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type OperatorMutationResult struct {
	ControlResourceID    uuid.UUID
	ShardResourceID      uuid.UUID
	AssignmentGeneration int64
	SourceVersion        int64
	BookingPolicyVersion int64
	Replayed             bool
}

type FareInstallCommand struct {
	CommandID             uuid.UUID
	TrainRunID            uuid.UUID
	SourceFareID          uuid.UUID
	SnapshotFareID        uuid.UUID
	ExpectedSourceVersion int64
	FromStopIndex         int
	ToStopIndex           int
	SeatClass             string
	AmountMinor           int64
	Currency              string
	RequestFingerprint    [32]byte
}

type SeatActiveCommand struct {
	CommandID             uuid.UUID
	TrainRunID            uuid.UUID
	SeatID                uuid.UUID
	ExpectedSourceVersion int64
	Active                bool
	RequestFingerprint    [32]byte
}

type BookingPolicyBumpCommand struct {
	CommandID                    uuid.UUID
	TrainRunID                   uuid.UUID
	ExpectedSourceVersion        int64
	ExpectedBookingPolicyVersion int64
	RequestFingerprint           [32]byte
}

type operatorAuthority struct {
	resolution           shardphysical.Resolution
	generation           int64
	snapshotSource       int64
	bookingPolicyVersion int64
}

func (executor *Executor) InstallFare(ctx context.Context, command FareInstallCommand) (OperatorMutationResult, error) {
	command.SeatClass = strings.ToLower(strings.TrimSpace(command.SeatClass))
	command.Currency = strings.ToUpper(strings.TrimSpace(command.Currency))
	if executor == nil || ctx == nil || command.CommandID == uuid.Nil || command.TrainRunID == uuid.Nil ||
		command.SourceFareID == uuid.Nil || command.SnapshotFareID == uuid.Nil || command.ExpectedSourceVersion < 1 || command.FromStopIndex < 0 ||
		command.FromStopIndex >= command.ToStopIndex || command.AmountMinor < 0 || !validOperatorSeatClass(command.SeatClass) ||
		!validOperatorCurrency(command.Currency) || command.RequestFingerprint == [32]byte{} {
		return OperatorMutationResult{}, ErrInvalidPayload
	}
	tx, authority, err := executor.beginOperatorMutation(ctx, command.TrainRunID)
	if err != nil {
		return OperatorMutationResult{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if receipt, found, err := loadOperatorReceipt(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
		"fare.install", command.RequestFingerprint, command.SourceFareID); err != nil {
		return OperatorMutationResult{}, err
	} else if found {
		return commitOperatorReplay(ctx, tx, command.SourceFareID, command.SnapshotFareID,
			receipt.generation, receipt.sourceVersion, receipt.bookingPolicyVersion,
			command.ExpectedSourceVersion, 0)
	}
	if authority, err = lockOperatorAuthority(ctx, tx, authority); err != nil {
		return OperatorMutationResult{}, err
	}
	if receipt, found, err := loadOperatorReceipt(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
		"fare.install", command.RequestFingerprint, command.SourceFareID); err != nil {
		return OperatorMutationResult{}, err
	} else if found {
		return commitOperatorReplay(ctx, tx, command.SourceFareID, command.SnapshotFareID,
			receipt.generation, receipt.sourceVersion, receipt.bookingPolicyVersion,
			command.ExpectedSourceVersion, 0)
	}
	currentVersion, same, err := currentFareVersion(ctx, tx, command, authority.generation, false)
	if err != nil {
		return OperatorMutationResult{}, err
	}
	changed := currentVersion == command.ExpectedSourceVersion
	if !changed && !(currentVersion == command.ExpectedSourceVersion+1 && same) {
		return OperatorMutationResult{}, sharding.ErrAssignmentStale
	}
	newVersion := command.ExpectedSourceVersion + 1
	if err := insertOperatorReceipt(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
		"fare.install", command.RequestFingerprint); err != nil {
		return OperatorMutationResult{}, err
	}
	if changed {
		if err := execOne(ctx, tx, `UPDATE booking_fare_snapshots
SET amount_minor=$4,currency=$5,source_version=$6,active=true,source_updated_at=clock_timestamp()
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3 AND source_version=$7
  AND from_stop_index=$8 AND to_stop_index=$9 AND seat_class=$10`,
			command.SnapshotFareID, command.TrainRunID, authority.generation, command.AmountMinor,
			command.Currency, newVersion, command.ExpectedSourceVersion,
			command.FromStopIndex, command.ToStopIndex, command.SeatClass); err != nil {
			return OperatorMutationResult{}, err
		}
		if err := bumpOperatorSnapshotSource(ctx, tx, command.TrainRunID, authority); err != nil {
			return OperatorMutationResult{}, err
		}
		if err := appendOperatorSnapshotEvent(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
			command.SourceFareID, newVersion, "booking_snapshot.fare_installed"); err != nil {
			return OperatorMutationResult{}, err
		}
	}
	if err := finishOperatorMutation(ctx, tx, command.CommandID, command.TrainRunID, command.SourceFareID,
		newVersion, 0, authority, "fare", changed); err != nil {
		return OperatorMutationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperatorMutationResult{}, ErrShardPersistence
	}
	return OperatorMutationResult{ControlResourceID: command.SourceFareID, ShardResourceID: command.SnapshotFareID,
		AssignmentGeneration: authority.generation, SourceVersion: newVersion, Replayed: !changed}, nil
}

func (executor *Executor) SetSeatActive(ctx context.Context, command SeatActiveCommand) (OperatorMutationResult, error) {
	commandType := "seat.disable"
	if command.Active {
		commandType = "seat.enable"
	}
	if executor == nil || ctx == nil || command.CommandID == uuid.Nil || command.TrainRunID == uuid.Nil ||
		command.SeatID == uuid.Nil || command.ExpectedSourceVersion < 1 || command.RequestFingerprint == [32]byte{} {
		return OperatorMutationResult{}, ErrInvalidPayload
	}
	tx, authority, err := executor.beginOperatorMutation(ctx, command.TrainRunID)
	if err != nil {
		return OperatorMutationResult{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if receipt, found, err := loadOperatorReceipt(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
		commandType, command.RequestFingerprint, command.SeatID); err != nil {
		return OperatorMutationResult{}, err
	} else if found {
		return commitOperatorReplay(ctx, tx, command.SeatID, command.SeatID,
			receipt.generation, receipt.sourceVersion, receipt.bookingPolicyVersion,
			command.ExpectedSourceVersion, 0)
	}
	if authority, err = lockOperatorAuthority(ctx, tx, authority); err != nil {
		return OperatorMutationResult{}, err
	}
	if receipt, found, err := loadOperatorReceipt(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
		commandType, command.RequestFingerprint, command.SeatID); err != nil {
		return OperatorMutationResult{}, err
	} else if found {
		return commitOperatorReplay(ctx, tx, command.SeatID, command.SeatID,
			receipt.generation, receipt.sourceVersion, receipt.bookingPolicyVersion,
			command.ExpectedSourceVersion, 0)
	}
	currentVersion, same, err := currentSeatVersion(ctx, tx, command, authority.generation, false)
	if err != nil {
		return OperatorMutationResult{}, err
	}
	changed := currentVersion == command.ExpectedSourceVersion
	if !changed && !(currentVersion == command.ExpectedSourceVersion+1 && same) {
		return OperatorMutationResult{}, sharding.ErrAssignmentStale
	}
	newVersion := command.ExpectedSourceVersion + 1
	if err := insertOperatorReceipt(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
		commandType, command.RequestFingerprint); err != nil {
		return OperatorMutationResult{}, err
	}
	if changed {
		if err := execOne(ctx, tx, `UPDATE booking_seat_catalog
SET active=$4,source_version=$5,source_updated_at=clock_timestamp()
WHERE train_run_id=$1 AND assignment_generation=$2 AND seat_id=$3 AND source_version=$6`,
			command.TrainRunID, authority.generation, command.SeatID, command.Active,
			newVersion, command.ExpectedSourceVersion); err != nil {
			return OperatorMutationResult{}, err
		}
		if err := bumpOperatorSnapshotSource(ctx, tx, command.TrainRunID, authority); err != nil {
			return OperatorMutationResult{}, err
		}
		eventType := "booking_snapshot.seat_disabled"
		if command.Active {
			eventType = "booking_snapshot.seat_enabled"
		}
		if err := appendOperatorSnapshotEvent(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
			command.SeatID, newVersion, eventType); err != nil {
			return OperatorMutationResult{}, err
		}
	}
	if err := finishOperatorMutation(ctx, tx, command.CommandID, command.TrainRunID, command.SeatID,
		newVersion, 0, authority, "seat", changed); err != nil {
		return OperatorMutationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperatorMutationResult{}, ErrShardPersistence
	}
	return OperatorMutationResult{ControlResourceID: command.SeatID, ShardResourceID: command.SeatID,
		AssignmentGeneration: authority.generation, SourceVersion: newVersion, Replayed: !changed}, nil
}

func (executor *Executor) BumpBookingPolicy(ctx context.Context, command BookingPolicyBumpCommand) (OperatorMutationResult, error) {
	if executor == nil || ctx == nil || command.CommandID == uuid.Nil || command.TrainRunID == uuid.Nil ||
		command.ExpectedSourceVersion < 1 || command.ExpectedBookingPolicyVersion < 1 ||
		command.RequestFingerprint == [32]byte{} {
		return OperatorMutationResult{}, ErrInvalidPayload
	}
	tx, authority, err := executor.beginOperatorMutation(ctx, command.TrainRunID)
	if err != nil {
		return OperatorMutationResult{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if receipt, found, err := loadOperatorReceipt(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
		"booking_policy.bump", command.RequestFingerprint, command.TrainRunID); err != nil {
		return OperatorMutationResult{}, err
	} else if found {
		return commitOperatorReplay(ctx, tx, command.TrainRunID, command.TrainRunID,
			receipt.generation, receipt.sourceVersion, receipt.bookingPolicyVersion,
			command.ExpectedSourceVersion, command.ExpectedBookingPolicyVersion)
	}
	if authority, err = lockOperatorAuthority(ctx, tx, authority); err != nil {
		return OperatorMutationResult{}, err
	}
	if receipt, found, err := loadOperatorReceipt(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
		"booking_policy.bump", command.RequestFingerprint, command.TrainRunID); err != nil {
		return OperatorMutationResult{}, err
	} else if found {
		return commitOperatorReplay(ctx, tx, command.TrainRunID, command.TrainRunID,
			receipt.generation, receipt.sourceVersion, receipt.bookingPolicyVersion,
			command.ExpectedSourceVersion, command.ExpectedBookingPolicyVersion)
	}
	changed := authority.snapshotSource == command.ExpectedSourceVersion &&
		authority.bookingPolicyVersion == command.ExpectedBookingPolicyVersion
	semanticReplay := authority.snapshotSource == command.ExpectedSourceVersion+1 &&
		authority.bookingPolicyVersion == command.ExpectedBookingPolicyVersion+1
	if !changed && !semanticReplay {
		return OperatorMutationResult{}, sharding.ErrAssignmentStale
	}
	if err := insertOperatorReceipt(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
		"booking_policy.bump", command.RequestFingerprint); err != nil {
		return OperatorMutationResult{}, err
	}
	if changed {
		if err := execOne(ctx, tx, `UPDATE train_run_booking_snapshots
SET source_version=$3,booking_policy_version=$4,source_updated_at=clock_timestamp()
WHERE train_run_id=$1 AND assignment_generation=$2 AND source_version=$5 AND booking_policy_version=$6`,
			command.TrainRunID, authority.generation, command.ExpectedSourceVersion+1,
			command.ExpectedBookingPolicyVersion+1, command.ExpectedSourceVersion,
			command.ExpectedBookingPolicyVersion); err != nil {
			return OperatorMutationResult{}, err
		}
		if err := appendOperatorSnapshotEvent(ctx, tx, command.CommandID, command.TrainRunID, authority.generation,
			command.TrainRunID, command.ExpectedSourceVersion+1, "booking_snapshot.policy_bumped"); err != nil {
			return OperatorMutationResult{}, err
		}
	}
	if err := finishOperatorMutation(ctx, tx, command.CommandID, command.TrainRunID, command.TrainRunID,
		command.ExpectedSourceVersion+1, command.ExpectedBookingPolicyVersion+1,
		authority, "train_run", changed); err != nil {
		return OperatorMutationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperatorMutationResult{}, ErrShardPersistence
	}
	return OperatorMutationResult{ControlResourceID: command.TrainRunID, ShardResourceID: command.TrainRunID,
		AssignmentGeneration: authority.generation, SourceVersion: command.ExpectedSourceVersion + 1,
		BookingPolicyVersion: command.ExpectedBookingPolicyVersion + 1, Replayed: !changed}, nil
}

func (executor *Executor) beginOperatorMutation(ctx context.Context, trainRunID uuid.UUID) (pgx.Tx, operatorAuthority, error) {
	resolved, err := executor.router.Resolve(ctx, trainRunID, false)
	if err != nil || resolved.Handle.Pool() == nil || resolved.Route.TrainRunID() != trainRunID ||
		resolved.Handle.ShardID() != resolved.Route.ShardID() {
		return nil, operatorAuthority{}, sharding.ErrShardUnavailable
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, operatorAuthority{}, sharding.ErrShardUnavailable
	}
	return tx, operatorAuthority{resolution: resolved, generation: resolved.Route.Generation().Int64()}, nil
}

func lockOperatorAuthority(ctx context.Context, tx pgx.Tx, authority operatorAuthority) (operatorAuthority, error) {
	var writeEnabled bool
	var fenceState, snapshotStatus string
	var segmentCount int
	err := tx.QueryRow(ctx, `SELECT fence.assignment_generation,fence.write_enabled,fence.state,
 snapshot.status,snapshot.segment_count,snapshot.source_version,snapshot.booking_policy_version
FROM train_run_write_fences AS fence
JOIN train_run_booking_snapshots AS snapshot ON snapshot.train_run_id=fence.train_run_id
 AND snapshot.assignment_generation=fence.assignment_generation
WHERE fence.train_run_id=$1 AND snapshot.active FOR UPDATE OF fence,snapshot`, authority.resolution.Route.TrainRunID()).Scan(
		&authority.generation, &writeEnabled, &fenceState, &snapshotStatus, &segmentCount,
		&authority.snapshotSource, &authority.bookingPolicyVersion)
	if err != nil || segmentCount < 1 || authority.snapshotSource < 1 || authority.bookingPolicyVersion < 1 {
		return operatorAuthority{}, sharding.ErrShardUnavailable
	}
	if authority.generation != authority.resolution.Route.Generation().Int64() {
		return operatorAuthority{}, sharding.ErrAssignmentStale
	}
	if !authority.resolution.Handle.WriteEnabled() || !writeEnabled || fenceState != "active" {
		return operatorAuthority{}, sharding.ErrWriteFenced
	}
	if snapshotStatus != "scheduled" && snapshotStatus != "boarding" {
		return operatorAuthority{}, ErrInvalidPayload
	}
	return authority, nil
}

type operatorReceipt struct {
	generation           int64
	sourceVersion        int64
	bookingPolicyVersion int64
}

func loadOperatorReceipt(ctx context.Context, tx pgx.Tx, commandID, trainRunID uuid.UUID, generation int64, commandType string, fingerprint [32]byte, resourceID uuid.UUID) (operatorReceipt, bool, error) {
	var storedType, status string
	var storedFingerprint []byte
	var storedTrainRunID, storedResult uuid.UUID
	var storedGeneration, storedSourceVersion int64
	var storedPolicyVersion pgtype.Int8
	err := tx.QueryRow(ctx, `SELECT command_type,request_fingerprint,result_id,status,
 train_run_id,assignment_generation,result_source_version,result_booking_policy_version
FROM booking_command_receipts WHERE command_id=$1`, commandID).Scan(
		&storedType, &storedFingerprint, &storedResult, &status,
		&storedTrainRunID, &storedGeneration, &storedSourceVersion, &storedPolicyVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return operatorReceipt{}, false, nil
	}
	if err != nil {
		return operatorReceipt{}, false, ErrShardPersistence
	}
	if storedType != commandType || status != "succeeded" || storedTrainRunID != trainRunID ||
		storedGeneration != generation || storedResult != resourceID || storedSourceVersion < 1 ||
		!bytes.Equal(storedFingerprint, fingerprint[:]) {
		return operatorReceipt{}, false, ErrShardPersistence
	}
	if (commandType == "booking_policy.bump" && (!storedPolicyVersion.Valid || storedPolicyVersion.Int64 < 1)) ||
		(commandType != "booking_policy.bump" && storedPolicyVersion.Valid) {
		return operatorReceipt{}, false, ErrShardPersistence
	}
	return operatorReceipt{generation: storedGeneration, sourceVersion: storedSourceVersion,
		bookingPolicyVersion: storedPolicyVersion.Int64}, true, nil
}

func insertOperatorReceipt(ctx context.Context, tx pgx.Tx, commandID, trainRunID uuid.UUID, generation int64, commandType string, fingerprint [32]byte) error {
	return execOne(ctx, tx, `INSERT INTO booking_command_receipts(
 id,command_id,train_run_id,assignment_generation,command_type,request_fingerprint,status
) VALUES($1,$2,$3,$4,$5,$6,'started')`, uuid.NewSHA1(commandID, []byte("command-receipt")),
		commandID, trainRunID, generation, commandType, fingerprint[:])
}

func currentFareVersion(ctx context.Context, tx pgx.Tx, command FareInstallCommand, generation int64, replayOnly bool) (int64, bool, error) {
	var version, amount int64
	var currency, seatClass string
	var active bool
	var from, to int
	err := tx.QueryRow(ctx, `SELECT source_version,amount_minor,currency,active,from_stop_index,to_stop_index,seat_class
FROM booking_fare_snapshots WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3 FOR UPDATE`, command.SnapshotFareID, command.TrainRunID, generation).Scan(
		&version, &amount, &currency, &active, &from, &to, &seatClass)
	if err != nil || version < 1 {
		return 0, false, ErrShardPersistence
	}
	if from != command.FromStopIndex || to != command.ToStopIndex || seatClass != command.SeatClass {
		return 0, false, ErrInvalidPayload
	}
	same := amount == command.AmountMinor && currency == command.Currency && active
	if replayOnly && !same {
		return 0, false, ErrShardPersistence
	}
	return version, same, nil
}

func currentSeatVersion(ctx context.Context, tx pgx.Tx, command SeatActiveCommand, generation int64, replayOnly bool) (int64, bool, error) {
	var version int64
	var active bool
	err := tx.QueryRow(ctx, `SELECT source_version,active FROM booking_seat_catalog
WHERE train_run_id=$1 AND assignment_generation=$2 AND seat_id=$3 FOR UPDATE`, command.TrainRunID, generation, command.SeatID).Scan(&version, &active)
	if err != nil || version < 1 {
		return 0, false, ErrShardPersistence
	}
	same := active == command.Active
	if replayOnly && !same {
		return 0, false, ErrShardPersistence
	}
	return version, same, nil
}

func bumpOperatorSnapshotSource(ctx context.Context, tx pgx.Tx, trainRunID uuid.UUID, authority operatorAuthority) error {
	return execOne(ctx, tx, `UPDATE train_run_booking_snapshots SET source_version=source_version+1,
 source_updated_at=clock_timestamp() WHERE train_run_id=$1 AND assignment_generation=$2 AND source_version=$3`,
		trainRunID, authority.generation, authority.snapshotSource)
}

func appendOperatorSnapshotEvent(ctx context.Context, tx pgx.Tx, commandID, trainRunID uuid.UUID, generation int64, resourceID uuid.UUID, sourceVersion int64, eventType string) error {
	payload, err := json.Marshal(map[string]any{"command_id": commandID, "train_run_id": trainRunID,
		"assignment_generation": generation, "resource_id": resourceID, "source_version": sourceVersion})
	if err != nil {
		return ErrShardPersistence
	}
	eventID := OperatorSnapshotEventID(trainRunID, generation, resourceID, eventType, sourceVersion)
	return execOne(ctx, tx, `INSERT INTO outbox_events(id,train_run_id,assignment_generation,
 aggregate_type,aggregate_id,event_type,payload) VALUES($1,$2,$3,'booking_command',$4,$5,$6::jsonb)`,
		eventID, trainRunID, generation, commandID, eventType, string(payload))
}

// OperatorSnapshotEventID scopes an operator snapshot event to one train-run
// assignment. Global seats and route fares can appear in multiple train runs,
// so resource identity and version alone are not collision-safe.
func OperatorSnapshotEventID(trainRunID uuid.UUID, generation int64, resourceID uuid.UUID, eventType string, sourceVersion int64) uuid.UUID {
	name := strconv.FormatInt(generation, 10) + ":" + resourceID.String() + ":" + eventType + ":" + strconv.FormatInt(sourceVersion, 10)
	return uuid.NewSHA1(trainRunID, []byte(name))
}

func finishOperatorMutation(ctx context.Context, tx pgx.Tx, commandID, trainRunID, resourceID uuid.UUID, sourceVersion, bookingPolicyVersion int64, authority operatorAuthority, resultType string, changed bool) error {
	if changed {
		if err := execOne(ctx, tx, `INSERT INTO train_run_target_write_evidence(
 id,train_run_id,assignment_generation,successful_write_count,first_successful_write_at,last_successful_write_at,last_command_id
) VALUES($1,$2,$3,1,clock_timestamp(),clock_timestamp(),$4)
ON CONFLICT(train_run_id,assignment_generation) DO UPDATE SET
 successful_write_count=train_run_target_write_evidence.successful_write_count+1,
 first_successful_write_at=COALESCE(train_run_target_write_evidence.first_successful_write_at,EXCLUDED.first_successful_write_at),
 last_successful_write_at=EXCLUDED.last_successful_write_at,last_command_id=EXCLUDED.last_command_id`,
			uuid.NewSHA1(trainRunID, []byte("target-write-evidence:"+authority.resolution.Route.ShardID().String()+":"+strconv.FormatInt(authority.generation, 10))),
			trainRunID, authority.generation, commandID); err != nil {
			return err
		}
	}
	return execOne(ctx, tx, `UPDATE booking_command_receipts SET status='succeeded',result_type=$2,
 result_id=$3,result_source_version=$4,result_booking_policy_version=NULLIF($5,0),
 completed_at=clock_timestamp()
WHERE command_id=$1 AND status='started'`, commandID, resultType, resourceID,
		sourceVersion, bookingPolicyVersion)
}

func commitOperatorReplay(ctx context.Context, tx pgx.Tx, controlResourceID, shardResourceID uuid.UUID,
	generation, version, bookingPolicyVersion, expectedSourceVersion, expectedBookingPolicyVersion int64,
) (OperatorMutationResult, error) {
	if expectedSourceVersion < 1 || version != expectedSourceVersion+1 ||
		(expectedBookingPolicyVersion > 0 && bookingPolicyVersion != expectedBookingPolicyVersion+1) ||
		(expectedBookingPolicyVersion == 0 && bookingPolicyVersion != 0) {
		return OperatorMutationResult{}, ErrShardPersistence
	}
	if err := tx.Commit(ctx); err != nil {
		return OperatorMutationResult{}, ErrShardPersistence
	}
	return OperatorMutationResult{ControlResourceID: controlResourceID, ShardResourceID: shardResourceID,
		AssignmentGeneration: generation, SourceVersion: version,
		BookingPolicyVersion: bookingPolicyVersion, Replayed: true}, nil
}

func validOperatorSeatClass(value string) bool {
	return value == "standard" || value == "business" || value == "first"
}

func validOperatorCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}
