package postgres

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

const (
	checkpointSchemaVersion = 4
	maxCheckpointBytes      = 64 << 10
)

type checkpointDTO struct {
	SchemaVersion     int                  `json:"schema_version"`
	Binding           bindingDTO           `json:"binding"`
	Target            string               `json:"target"`
	Stage             string               `json:"stage"`
	Fencing           *attestationDTO      `json:"fencing,omitempty"`
	Positions         *positionSetDTO      `json:"positions,omitempty"`
	Promotions        *positionSetDTO      `json:"promotions,omitempty"`
	TargetEpoch       uint64               `json:"target_epoch,omitempty"`
	ControlAuthority  *authorityDTO        `json:"control_authority,omitempty"`
	ShardAuthorities  *shardAuthoritiesDTO `json:"shard_authorities,omitempty"`
	RTOMilliseconds   int64                `json:"rto_ms,omitempty"`
	RPO               *lossSetDTO          `json:"rpo,omitempty"`
	TargetAuthorities *authoritySetDTO     `json:"target_authorities,omitempty"`
	ActivatedAt       time.Time            `json:"activated_at,omitempty"`
	RetainedFence     *attestationDTO      `json:"retained_fence,omitempty"`
	Actions           *actionEvidenceDTO   `json:"action_evidence,omitempty"`
}

type bindingDTO struct {
	OperationID string    `json:"operation_id"`
	Source      string    `json:"source"`
	SourceEpoch uint64    `json:"source_epoch"`
	IncidentID  string    `json:"incident_id"`
	OperatorID  string    `json:"operator_id"`
	DeclaredAt  time.Time `json:"declared_at"`
}

type attestationDTO struct {
	ObservedAt      time.Time `json:"observed_at"`
	ExpiresAt       time.Time `json:"expires_at"`
	Issuer          string    `json:"issuer"`
	KeyID           string    `json:"key_id"`
	Nonce           string    `json:"nonce"`
	Purpose         string    `json:"purpose"`
	SignatureBase64 string    `json:"signature_b64"`
	Ingress         string    `json:"ingress_hash"`
	Processes       string    `json:"process_hash"`
	Credentials     string    `json:"credential_hash"`
	DatabaseNetwork string    `json:"database_network_hash"`
}

type positionDTO struct {
	Timeline uint32 `json:"timeline"`
	WAL      uint64 `json:"wal"`
}

type positionSetDTO struct {
	Control *positionDTO `json:"control,omitempty"`
	Shard0  *positionDTO `json:"shard_0,omitempty"`
	Shard1  *positionDTO `json:"shard_1,omitempty"`
}

type authorityDTO struct {
	Region        string `json:"region"`
	Epoch         uint64 `json:"epoch"`
	State         string `json:"state"`
	WritesEnabled bool   `json:"writes_enabled"`
}

type shardAuthoritiesDTO struct {
	Shard0 authorityDTO `json:"shard_0"`
	Shard1 authorityDTO `json:"shard_1"`
}

type authoritySetDTO struct {
	Control authorityDTO `json:"control"`
	Shard0  authorityDTO `json:"shard_0"`
	Shard1  authorityDTO `json:"shard_1"`
}

type lossDTO struct {
	MissingRecords uint64 `json:"missing_records"`
	WindowMS       int64  `json:"window_ms"`
}

type lossSetDTO struct {
	Control lossDTO `json:"control"`
	Shard0  lossDTO `json:"shard_0"`
	Shard1  lossDTO `json:"shard_1"`
}

type actionEvidenceDTO struct {
	PassiveReadiness  string `json:"passive_readiness,omitempty"`
	RecoveryAPIs      string `json:"recovery_apis,omitempty"`
	Reconciliation    string `json:"reconciliation,omitempty"`
	PaymentWorkers    string `json:"payment_workers,omitempty"`
	SettlementWorkers string `json:"settlement_workers,omitempty"`
	Ingress           string `json:"ingress,omitempty"`
	CustomerWrites    string `json:"customer_writes,omitempty"`
}

func marshalCheckpoint(checkpoint recovery.Checkpoint) ([]byte, error) {
	if _, err := recovery.RestoreFailover(checkpoint); err != nil {
		return nil, ErrInvalidCheckpoint
	}
	dto := checkpointDTO{
		SchemaVersion: checkpointSchemaVersion,
		Binding:       encodeBinding(checkpoint.Binding),
		Target:        checkpoint.Target.String(),
		Stage:         checkpoint.Stage.String(),
	}
	if checkpoint.Stage >= recovery.StageExternalFencingVerified {
		value := encodeAttestation(checkpoint.Fencing)
		dto.Fencing = &value
	}
	if checkpoint.Stage >= recovery.StagePositionsRecorded {
		value := encodePositionSet(checkpoint.Positions, recovery.StagePositionsRecorded, checkpoint.Stage)
		dto.Positions = &value
	}
	if checkpoint.Stage >= recovery.StagePassiveReadinessRemoved {
		actions := actionEvidenceDTO{PassiveReadiness: encodeObservationHash(checkpoint.Actions.PassiveReadinessHash())}
		if checkpoint.Stage >= recovery.StageRecoveryAPIsStarted {
			actions.RecoveryAPIs = encodeObservationHash(checkpoint.Actions.RecoveryAPIsHash())
		}
		if checkpoint.Stage >= recovery.StageReconciled {
			actions.Reconciliation = encodeObservationHash(checkpoint.Actions.ReconciliationHash())
		}
		if checkpoint.Stage >= recovery.StagePaymentWorkersEnabled {
			actions.PaymentWorkers = encodeObservationHash(checkpoint.Actions.PaymentWorkersHash())
		}
		if checkpoint.Stage >= recovery.StageSettlementWorkersEnabled {
			actions.SettlementWorkers = encodeObservationHash(checkpoint.Actions.SettlementWorkersHash())
		}
		if checkpoint.Stage >= recovery.StageIngressSwitched {
			actions.Ingress = encodeObservationHash(checkpoint.Actions.IngressHash())
		}
		if checkpoint.Stage >= recovery.StageCustomerWritesConfigured {
			actions.CustomerWrites = encodeObservationHash(checkpoint.Actions.CustomerWritesHash())
		}
		dto.Actions = &actions
	}
	if checkpoint.Stage >= recovery.StageControlPromoted {
		value := encodePositionSet(checkpoint.Promotions, recovery.StageControlPromoted, checkpoint.Stage)
		dto.Promotions = &value
	}
	if checkpoint.Stage >= recovery.StageEpochAllocated {
		dto.TargetEpoch = checkpoint.TargetEpoch.Uint64()
	}
	if checkpoint.Stage >= recovery.StageControlRecoveryInstalled {
		value := encodeAuthority(checkpoint.ControlAuthority)
		dto.ControlAuthority = &value
	}
	if checkpoint.Stage >= recovery.StageShardAuthoritiesInstalled {
		dto.ShardAuthorities = &shardAuthoritiesDTO{
			Shard0: encodeAuthority(checkpoint.ShardAuthorities.Shard0()),
			Shard1: encodeAuthority(checkpoint.ShardAuthorities.Shard1()),
		}
	}
	if checkpoint.Stage >= recovery.StageRTORecorded {
		dto.RTOMilliseconds = checkpoint.RTO.Milliseconds()
	}
	if checkpoint.Stage >= recovery.StageRPORecorded {
		dto.RPO = &lossSetDTO{
			Control: encodeLoss(checkpoint.RPO.Control()),
			Shard0:  encodeLoss(checkpoint.RPO.Shard0()),
			Shard1:  encodeLoss(checkpoint.RPO.Shard1()),
		}
	}
	if checkpoint.Stage >= recovery.StageTargetActive {
		dto.TargetAuthorities = &authoritySetDTO{
			Control: encodeAuthority(checkpoint.TargetAuthorities.Control()),
			Shard0:  encodeAuthority(checkpoint.TargetAuthorities.Shard0()),
			Shard1:  encodeAuthority(checkpoint.TargetAuthorities.Shard1()),
		}
		dto.ActivatedAt = checkpoint.ActivatedAt
	}
	if checkpoint.Stage >= recovery.StageSourceRetainedFenced {
		value := encodeAttestation(checkpoint.RetainedFence)
		dto.RetainedFence = &value
	}
	payload, err := json.Marshal(dto)
	if err != nil || len(payload) == 0 || len(payload) > maxCheckpointBytes {
		return nil, ErrInvalidCheckpoint
	}
	return payload, nil
}

func unmarshalCheckpoint(payload []byte) (recovery.Failover, error) {
	if len(payload) == 0 || len(payload) > maxCheckpointBytes {
		return recovery.Failover{}, ErrInvalidCheckpoint
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var dto checkpointDTO
	if err := decoder.Decode(&dto); err != nil {
		return recovery.Failover{}, ErrInvalidCheckpoint
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || dto.SchemaVersion != checkpointSchemaVersion {
		return recovery.Failover{}, ErrInvalidCheckpoint
	}
	binding, err := decodeBinding(dto.Binding)
	if err != nil {
		return recovery.Failover{}, err
	}
	target, err := authority.ParseRegion(dto.Target)
	if err != nil {
		return recovery.Failover{}, ErrInvalidCheckpoint
	}
	stage, err := parseStage(dto.Stage)
	if err != nil {
		return recovery.Failover{}, err
	}
	checkpoint := recovery.Checkpoint{Binding: binding, Target: target, Stage: stage}
	if stage >= recovery.StageExternalFencingVerified {
		checkpoint.Fencing, err = decodeAttestation(binding, dto.Fencing)
	}
	if err == nil && stage >= recovery.StagePositionsRecorded {
		checkpoint.Positions, err = decodePositionSet(dto.Positions, recovery.StagePositionsRecorded, stage)
	}
	if err == nil && stage >= recovery.StagePassiveReadinessRemoved {
		checkpoint.Actions, err = decodeActionEvidence(dto.Actions, stage)
	}
	if err == nil && stage >= recovery.StageControlPromoted {
		checkpoint.Promotions, err = decodePositionSet(dto.Promotions, recovery.StageControlPromoted, stage)
	}
	if err == nil && stage >= recovery.StageEpochAllocated {
		checkpoint.TargetEpoch, err = authority.NewEpoch(dto.TargetEpoch)
	}
	if err == nil && stage >= recovery.StageControlRecoveryInstalled {
		checkpoint.ControlAuthority, err = decodeAuthority(dto.ControlAuthority)
	}
	if err == nil && stage >= recovery.StageShardAuthoritiesInstalled {
		if dto.ShardAuthorities == nil {
			err = ErrInvalidCheckpoint
		} else {
			var shard0, shard1 authority.Snapshot
			shard0, err = decodeAuthority(&dto.ShardAuthorities.Shard0)
			if err == nil {
				shard1, err = decodeAuthority(&dto.ShardAuthorities.Shard1)
			}
			checkpoint.ShardAuthorities = recovery.NewShardAuthoritySet(shard0, shard1)
		}
	}
	if err == nil && stage >= recovery.StageRTORecorded {
		if dto.RTOMilliseconds <= 0 {
			err = ErrInvalidCheckpoint
		} else {
			checkpoint.RTO = time.Duration(dto.RTOMilliseconds) * time.Millisecond
		}
	}
	if err == nil && stage >= recovery.StageRPORecorded {
		checkpoint.RPO, err = decodeLossSet(dto.RPO)
	}
	if err == nil && stage >= recovery.StageTargetActive {
		if dto.TargetAuthorities == nil {
			err = ErrInvalidCheckpoint
		} else {
			var control, shard0, shard1 authority.Snapshot
			control, err = decodeAuthority(&dto.TargetAuthorities.Control)
			if err == nil {
				shard0, err = decodeAuthority(&dto.TargetAuthorities.Shard0)
			}
			if err == nil {
				shard1, err = decodeAuthority(&dto.TargetAuthorities.Shard1)
			}
			checkpoint.TargetAuthorities = recovery.NewAuthoritySet(control, shard0, shard1)
		}
		checkpoint.ActivatedAt = dto.ActivatedAt
	}
	if err == nil && stage >= recovery.StageSourceRetainedFenced {
		checkpoint.RetainedFence, err = decodeAttestation(binding, dto.RetainedFence)
	}
	if err != nil {
		return recovery.Failover{}, ErrInvalidCheckpoint
	}
	operation, err := recovery.RestoreFailover(checkpoint)
	if err != nil {
		return recovery.Failover{}, ErrInvalidCheckpoint
	}
	return operation, nil
}

func encodeBinding(binding recovery.FenceBinding) bindingDTO {
	return bindingDTO{
		OperationID: binding.OperationID().String(), Source: binding.Source().String(),
		SourceEpoch: binding.SourceEpoch().Uint64(), IncidentID: binding.IncidentID().String(),
		OperatorID: binding.OperatorID(), DeclaredAt: binding.DeclaredAt(),
	}
}

func decodeBinding(value bindingDTO) (recovery.FenceBinding, error) {
	operationID, err := uuid.Parse(value.OperationID)
	if err != nil || operationID == uuid.Nil {
		return recovery.FenceBinding{}, ErrInvalidCheckpoint
	}
	incidentID, err := uuid.Parse(value.IncidentID)
	if err != nil || incidentID == uuid.Nil {
		return recovery.FenceBinding{}, ErrInvalidCheckpoint
	}
	source, err := authority.ParseRegion(value.Source)
	if err != nil {
		return recovery.FenceBinding{}, ErrInvalidCheckpoint
	}
	epoch, err := authority.NewEpoch(value.SourceEpoch)
	if err != nil {
		return recovery.FenceBinding{}, ErrInvalidCheckpoint
	}
	binding, err := recovery.NewFenceBinding(operationID, source, epoch, incidentID, value.OperatorID, value.DeclaredAt)
	if err != nil {
		return recovery.FenceBinding{}, ErrInvalidCheckpoint
	}
	return binding, nil
}

func encodeAttestation(value recovery.FencingAttestation) attestationDTO {
	hashes := value.Hashes()
	return attestationDTO{
		ObservedAt: value.ObservedAt(), ExpiresAt: value.ExpiresAt(), Issuer: value.Issuer(), KeyID: value.KeyID(), Nonce: value.Nonce(), Purpose: string(value.Purpose()), SignatureBase64: base64.StdEncoding.EncodeToString(value.Signature()), Ingress: hex.EncodeToString(hashes.Ingress[:]),
		Processes: hex.EncodeToString(hashes.Processes[:]), Credentials: hex.EncodeToString(hashes.Credentials[:]),
		DatabaseNetwork: hex.EncodeToString(hashes.DatabaseNetwork[:]),
	}
}

func decodeAttestation(binding recovery.FenceBinding, value *attestationDTO) (recovery.FencingAttestation, error) {
	if value == nil {
		return recovery.FencingAttestation{}, ErrInvalidCheckpoint
	}
	hashes := recovery.ObservationHashes{}
	for encoded, destination := range map[string]*recovery.ObservationHash{
		value.Ingress: &hashes.Ingress, value.Processes: &hashes.Processes,
		value.Credentials: &hashes.Credentials, value.DatabaseNetwork: &hashes.DatabaseNetwork,
	} {
		decoded, err := hex.DecodeString(encoded)
		if err != nil || len(decoded) != len(*destination) {
			return recovery.FencingAttestation{}, ErrInvalidCheckpoint
		}
		copy(destination[:], decoded)
	}
	signature, signatureErr := base64.StdEncoding.Strict().DecodeString(value.SignatureBase64)
	attestation, err := recovery.NewFencingAttestation(binding, recovery.FencingPurpose(value.Purpose), value.ObservedAt, value.ExpiresAt, value.Issuer, value.KeyID, value.Nonce, hashes, signature)
	if err != nil || signatureErr != nil {
		return recovery.FencingAttestation{}, ErrInvalidCheckpoint
	}
	return attestation, nil
}

func encodeObservationHash(value recovery.ObservationHash) string {
	return hex.EncodeToString(value[:])
}

func decodeObservationHash(value string) (recovery.ObservationHash, error) {
	var result recovery.ObservationHash
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(result) {
		return result, ErrInvalidCheckpoint
	}
	copy(result[:], decoded)
	if result == (recovery.ObservationHash{}) {
		return recovery.ObservationHash{}, ErrInvalidCheckpoint
	}
	return result, nil
}

func decodeActionEvidence(value *actionEvidenceDTO, stage recovery.Stage) (recovery.ActionEvidenceSet, error) {
	if value == nil {
		return recovery.ActionEvidenceSet{}, ErrInvalidCheckpoint
	}
	var result recovery.ActionEvidenceSet
	var err error
	result.PassiveReadiness, err = decodeObservationHash(value.PassiveReadiness)
	if err == nil && stage >= recovery.StageRecoveryAPIsStarted {
		result.RecoveryAPIs, err = decodeObservationHash(value.RecoveryAPIs)
	}
	if err == nil && stage >= recovery.StageReconciled {
		result.Reconciliation, err = decodeObservationHash(value.Reconciliation)
	}
	if err == nil && stage >= recovery.StagePaymentWorkersEnabled {
		result.PaymentWorkers, err = decodeObservationHash(value.PaymentWorkers)
	}
	if err == nil && stage >= recovery.StageSettlementWorkersEnabled {
		result.SettlementWorkers, err = decodeObservationHash(value.SettlementWorkers)
	}
	if err == nil && stage >= recovery.StageIngressSwitched {
		result.Ingress, err = decodeObservationHash(value.Ingress)
	}
	if err == nil && stage >= recovery.StageCustomerWritesConfigured {
		result.CustomerWrites, err = decodeObservationHash(value.CustomerWrites)
	}
	if err != nil {
		return recovery.ActionEvidenceSet{}, ErrInvalidCheckpoint
	}
	return result, nil
}

func encodePositionSet(
	set recovery.DatabaseSet[recovery.ReplicationPosition],
	firstStage recovery.Stage,
	current recovery.Stage,
) positionSetDTO {
	value := positionSetDTO{}
	control := encodePosition(set.Control())
	value.Control = &control
	if firstStage == recovery.StagePositionsRecorded || current >= recovery.StageShard0Promoted {
		shard0 := encodePosition(set.Shard0())
		value.Shard0 = &shard0
	}
	if firstStage == recovery.StagePositionsRecorded || current >= recovery.StageShard1Promoted {
		shard1 := encodePosition(set.Shard1())
		value.Shard1 = &shard1
	}
	return value
}

func decodePositionSet(value *positionSetDTO, firstStage, current recovery.Stage) (recovery.DatabaseSet[recovery.ReplicationPosition], error) {
	if value == nil || value.Control == nil {
		return recovery.DatabaseSet[recovery.ReplicationPosition]{}, ErrInvalidCheckpoint
	}
	control, err := decodePosition(value.Control)
	if err != nil {
		return recovery.DatabaseSet[recovery.ReplicationPosition]{}, err
	}
	var shard0, shard1 recovery.ReplicationPosition
	if firstStage == recovery.StagePositionsRecorded || current >= recovery.StageShard0Promoted {
		shard0, err = decodePosition(value.Shard0)
	}
	if err == nil && (firstStage == recovery.StagePositionsRecorded || current >= recovery.StageShard1Promoted) {
		shard1, err = decodePosition(value.Shard1)
	}
	if err != nil {
		return recovery.DatabaseSet[recovery.ReplicationPosition]{}, err
	}
	return recovery.NewDatabaseSet(control, shard0, shard1), nil
}

func encodePosition(value recovery.ReplicationPosition) positionDTO {
	return positionDTO{Timeline: value.Timeline(), WAL: value.WAL()}
}

func decodePosition(value *positionDTO) (recovery.ReplicationPosition, error) {
	if value == nil {
		return recovery.ReplicationPosition{}, ErrInvalidCheckpoint
	}
	position, err := recovery.NewReplicationPosition(value.Timeline, value.WAL)
	if err != nil {
		return recovery.ReplicationPosition{}, ErrInvalidCheckpoint
	}
	return position, nil
}

func encodeAuthority(value authority.Snapshot) authorityDTO {
	return authorityDTO{
		Region: value.Region().String(), Epoch: value.Epoch().Uint64(),
		State: string(value.State()), WritesEnabled: value.WritesEnabled(),
	}
}

func decodeAuthority(value *authorityDTO) (authority.Snapshot, error) {
	if value == nil {
		return authority.Snapshot{}, ErrInvalidCheckpoint
	}
	region, err := authority.ParseRegion(value.Region)
	if err != nil {
		return authority.Snapshot{}, ErrInvalidCheckpoint
	}
	epoch, err := authority.NewEpoch(value.Epoch)
	if err != nil {
		return authority.Snapshot{}, ErrInvalidCheckpoint
	}
	snapshot, err := authority.NewSnapshot(region, epoch, authority.State(value.State), value.WritesEnabled)
	if err != nil {
		return authority.Snapshot{}, ErrInvalidCheckpoint
	}
	return snapshot, nil
}

func encodeLoss(value recovery.Loss) lossDTO {
	return lossDTO{MissingRecords: value.MissingRecords, WindowMS: value.Window.Milliseconds()}
}

func decodeLossSet(value *lossSetDTO) (recovery.DatabaseSet[recovery.Loss], error) {
	if value == nil || value.Control.WindowMS < 0 || value.Shard0.WindowMS < 0 || value.Shard1.WindowMS < 0 {
		return recovery.DatabaseSet[recovery.Loss]{}, ErrInvalidCheckpoint
	}
	return recovery.NewDatabaseSet(
		recovery.Loss{MissingRecords: value.Control.MissingRecords, Window: time.Duration(value.Control.WindowMS) * time.Millisecond},
		recovery.Loss{MissingRecords: value.Shard0.MissingRecords, Window: time.Duration(value.Shard0.WindowMS) * time.Millisecond},
		recovery.Loss{MissingRecords: value.Shard1.MissingRecords, Window: time.Duration(value.Shard1.WindowMS) * time.Millisecond},
	), nil
}

func parseStage(raw string) (recovery.Stage, error) {
	for _, stage := range []recovery.Stage{
		recovery.StagePlanned,
		recovery.StageExternalFencingVerified,
		recovery.StagePositionsRecorded,
		recovery.StagePassiveReadinessRemoved,
		recovery.StageControlPromoted,
		recovery.StageShard0Promoted,
		recovery.StageShard1Promoted,
		recovery.StageRolesAndTimelinesVerified,
		recovery.StageEpochAllocated,
		recovery.StageControlRecoveryInstalled,
		recovery.StageShardAuthoritiesInstalled,
		recovery.StageRecoveryAPIsStarted,
		recovery.StageReconciled,
		recovery.StagePaymentWorkersEnabled,
		recovery.StageSettlementWorkersEnabled,
		recovery.StageIngressSwitched,
		recovery.StageCustomerWritesConfigured,
		recovery.StageTargetActive,
		recovery.StageRTORecorded,
		recovery.StageRPORecorded,
		recovery.StageSourceRetainedFenced,
	} {
		if stage.String() == raw {
			return stage, nil
		}
	}
	return recovery.StagePlanned, ErrInvalidCheckpoint
}
