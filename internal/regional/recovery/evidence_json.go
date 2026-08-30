package recovery

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
)

var ErrInvalidEvidenceDocument = errors.New("regional recovery evidence document invalid")

const maximumEvidenceDocumentBytes = 64 << 10

type evidenceHeader struct {
	Stage string `json:"stage"`
}

type hashEvidenceDocument struct {
	Stage          string `json:"stage"`
	ArtifactSHA256 string `json:"artifact_sha256"`
}

type fenceEvidenceDocument struct {
	Stage                 string    `json:"stage"`
	ObservedAt            time.Time `json:"observed_at"`
	IngressSHA256         string    `json:"ingress_sha256"`
	ProcessesSHA256       string    `json:"processes_sha256"`
	CredentialsSHA256     string    `json:"credentials_sha256"`
	DatabaseNetworkSHA256 string    `json:"database_network_sha256"`
	ExpiresAt             time.Time `json:"expires_at"`
	Issuer                string    `json:"issuer"`
	KeyID                 string    `json:"key_id"`
	Nonce                 string    `json:"nonce"`
	Purpose               string    `json:"purpose"`
	SignatureBase64       string    `json:"signature_b64"`
}

type positionDocument struct {
	Timeline uint32 `json:"timeline"`
	WAL      uint64 `json:"wal"`
}

type positionsEvidenceDocument struct {
	Stage   string           `json:"stage"`
	Control positionDocument `json:"control"`
	Shard0  positionDocument `json:"shard_0"`
	Shard1  positionDocument `json:"shard_1"`
}

type promotedEvidenceDocument struct {
	Stage    string           `json:"stage"`
	Database string           `json:"database"`
	Position positionDocument `json:"position"`
}

type verificationDocument struct {
	Role     string `json:"role"`
	Timeline uint32 `json:"timeline"`
}

type rolesEvidenceDocument struct {
	Stage   string               `json:"stage"`
	Control verificationDocument `json:"control"`
	Shard0  verificationDocument `json:"shard_0"`
	Shard1  verificationDocument `json:"shard_1"`
}

type recoveryAuthorityDocument struct {
	Stage string `json:"stage"`
}

type authorityEvidenceDocument struct {
	Region        string `json:"region"`
	Epoch         uint64 `json:"epoch"`
	State         string `json:"state"`
	WritesEnabled bool   `json:"writes_enabled"`
}

type activeAuthoritySetDocument struct {
	Stage      string                    `json:"stage"`
	ObservedAt time.Time                 `json:"observed_at"`
	Control    authorityEvidenceDocument `json:"control"`
	Shard0     authorityEvidenceDocument `json:"shard_0"`
	Shard1     authorityEvidenceDocument `json:"shard_1"`
}

type reconciliationEvidenceDocument struct {
	Stage          string `json:"stage"`
	Control        bool   `json:"control"`
	Shards         bool   `json:"shards"`
	Payments       bool   `json:"payments"`
	Tickets        bool   `json:"tickets"`
	Refunds        bool   `json:"refunds"`
	Ledger         bool   `json:"ledger"`
	Routing        bool   `json:"routing"`
	ArtifactSHA256 string `json:"artifact_sha256"`
}

type ingressEvidenceDocument struct {
	Stage          string `json:"stage"`
	Webhook        bool   `json:"webhook"`
	Global         bool   `json:"global"`
	ArtifactSHA256 string `json:"artifact_sha256"`
}

type writesEvidenceDocument struct {
	Stage          string `json:"stage"`
	Enabled        bool   `json:"enabled"`
	ReadinessGated bool   `json:"readiness_gated"`
	ArtifactSHA256 string `json:"artifact_sha256"`
}

type rtoEvidenceDocument struct {
	Stage      string `json:"stage"`
	DurationMS int64  `json:"duration_ms"`
}

type lossDocument struct {
	MissingRecords uint64 `json:"missing_records"`
	WindowMS       int64  `json:"window_ms"`
}

type rpoEvidenceDocument struct {
	Stage   string       `json:"stage"`
	Control lossDocument `json:"control"`
	Shard0  lossDocument `json:"shard_0"`
	Shard1  lossDocument `json:"shard_1"`
}

type targetEpochEvidenceDocument struct {
	Stage       string `json:"stage"`
	TargetEpoch uint64 `json:"target_epoch"`
}

// DecodeEvidenceDocument converts one strict, bounded, secret-free JSON
// observation into the sealed evidence type for exactly the next phase.
func DecodeEvidenceDocument(operation Failover, document []byte, verifier FencingVerifier) (Evidence, error) {
	if len(document) == 0 || len(document) > maximumEvidenceDocumentBytes {
		return nil, ErrInvalidEvidenceDocument
	}
	var header evidenceHeader
	if err := json.Unmarshal(document, &header); err != nil || header.Stage == "" {
		return nil, ErrInvalidEvidenceDocument
	}
	documentStage := Stage(operation.Stage() + 1)
	if operation.Stage() != StagePlanned && header.Stage == operation.Stage().String() {
		documentStage = operation.Stage()
	} else if operation.Stage() == StageSourceRetainedFenced || header.Stage != documentStage.String() {
		return nil, ErrFailoverOutOfOrder
	}
	switch documentStage {
	case StageExternalFencingVerified, StageSourceRetainedFenced:
		var value fenceEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		signature, signatureErr := base64.StdEncoding.Strict().DecodeString(value.SignatureBase64)
		expectedPurpose := FencingPurposeInitial
		if documentStage == StageSourceRetainedFenced {
			expectedPurpose = FencingPurposeRetainedSource
		}
		if value.Purpose != string(expectedPurpose) {
			return nil, ErrInvalidEvidenceDocument
		}
		attestation, err := NewFencingAttestation(operation.Binding(), expectedPurpose, value.ObservedAt, value.ExpiresAt, value.Issuer, value.KeyID, value.Nonce, ObservationHashes{
			Ingress: mustObservationHash(value.IngressSHA256), Processes: mustObservationHash(value.ProcessesSHA256),
			Credentials: mustObservationHash(value.CredentialsSHA256), DatabaseNetwork: mustObservationHash(value.DatabaseNetworkSHA256),
		}, signature)
		if err != nil || signatureErr != nil || verifier.Verify(attestation) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		if documentStage == StageExternalFencingVerified {
			return ExternalFencingVerified{Attestation: attestation}, nil
		}
		return SourceRetainedFenced{Attestation: attestation}, nil
	case StagePositionsRecorded:
		var value positionsEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		positions, err := positionSet(value.Control, value.Shard0, value.Shard1)
		return PositionsRecorded{Positions: positions}, evidenceError(err)
	case StagePassiveReadinessRemoved, StageRecoveryAPIsStarted, StagePaymentWorkersEnabled, StageSettlementWorkersEnabled:
		var value hashEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		hash := mustObservationHash(value.ArtifactSHA256)
		if hash == (ObservationHash{}) {
			return nil, ErrInvalidEvidenceDocument
		}
		switch documentStage {
		case StagePassiveReadinessRemoved:
			return PassiveReadinessRemoved{Observation: hash}, nil
		case StageRecoveryAPIsStarted:
			return RecoveryAPIsStarted{Observation: hash}, nil
		case StagePaymentWorkersEnabled:
			return PaymentWorkersEnabled{Observation: hash}, nil
		default:
			return SettlementWorkersEnabled{Observation: hash}, nil
		}
	case StageControlPromoted, StageShard0Promoted, StageShard1Promoted:
		var value promotedEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		database, err := ParseDatabase(value.Database)
		position, positionErr := NewReplicationPosition(value.Position.Timeline, value.Position.WAL)
		if err != nil || positionErr != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		return DatabasePromoted{Database: database, Position: position}, nil
	case StageRolesAndTimelinesVerified:
		var value rolesEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		return RolesAndTimelinesVerified{Databases: NewDatabaseSet(
			DatabaseVerification{Role: DatabaseRole(value.Control.Role), Timeline: value.Control.Timeline},
			DatabaseVerification{Role: DatabaseRole(value.Shard0.Role), Timeline: value.Shard0.Timeline},
			DatabaseVerification{Role: DatabaseRole(value.Shard1.Role), Timeline: value.Shard1.Timeline},
		)}, nil
	case StageEpochAllocated:
		var value targetEpochEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		epoch, err := authority.NewEpoch(value.TargetEpoch)
		if err != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		return EpochAllocated{Epoch: epoch}, nil
	case StageControlRecoveryInstalled, StageShardAuthoritiesInstalled:
		var value recoveryAuthorityDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		snapshot, err := authority.NewSnapshot(operation.Target(), operation.TargetEpoch(), authority.StateRecovery, false)
		if err != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		if documentStage == StageControlRecoveryInstalled {
			return ControlRecoveryInstalled{Authority: snapshot}, nil
		}
		if documentStage == StageShardAuthoritiesInstalled {
			return ShardAuthoritiesInstalled{Authorities: NewShardAuthoritySet(snapshot, snapshot)}, nil
		}
		return nil, ErrInvalidEvidenceDocument
	case StageTargetActive:
		var value activeAuthoritySetDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		control, err := decodeActiveAuthority(value.Control)
		if err != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		shard0, err := decodeActiveAuthority(value.Shard0)
		if err != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		shard1, err := decodeActiveAuthority(value.Shard1)
		if err != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		if value.ObservedAt.IsZero() {
			return nil, ErrInvalidEvidenceDocument
		}
		return TargetActivated{Authorities: NewAuthoritySet(control, shard0, shard1), ObservedAt: value.ObservedAt}, nil
	case StageReconciled:
		var value reconciliationEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		return ReconciliationPassed{Control: value.Control, Shards: value.Shards, Payments: value.Payments,
			Tickets: value.Tickets, Refunds: value.Refunds, Ledger: value.Ledger, Routing: value.Routing,
			Observation: mustObservationHash(value.ArtifactSHA256)}, nil
	case StageIngressSwitched:
		var value ingressEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		return IngressSwitched{Webhook: value.Webhook, Global: value.Global, Observation: mustObservationHash(value.ArtifactSHA256)}, nil
	case StageCustomerWritesConfigured:
		var value writesEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		return CustomerWritesConfigured{Enabled: value.Enabled, ReadinessGated: value.ReadinessGated,
			Observation: mustObservationHash(value.ArtifactSHA256)}, nil
	case StageRTORecorded:
		var value rtoEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil || value.DurationMS <= 0 {
			return nil, ErrInvalidEvidenceDocument
		}
		return RTORecorded{Duration: time.Duration(value.DurationMS) * time.Millisecond}, nil
	case StageRPORecorded:
		var value rpoEvidenceDocument
		if strictEvidenceJSON(document, &value) != nil {
			return nil, ErrInvalidEvidenceDocument
		}
		losses, err := lossSet(value.Control, value.Shard0, value.Shard1)
		return RPORecorded{Loss: losses}, evidenceError(err)
	default:
		return nil, ErrInvalidEvidenceDocument
	}
}

// DecodeFenceRefreshDocument verifies a same-operation ongoing fence without
// advancing the fixed phase marker.
func DecodeFenceRefreshDocument(operation Failover, document []byte, verifier FencingVerifier) (FencingAttestation, error) {
	if len(document) == 0 || len(document) > maximumEvidenceDocumentBytes {
		return FencingAttestation{}, ErrInvalidEvidenceDocument
	}
	var value fenceEvidenceDocument
	if strictEvidenceJSON(document, &value) != nil || value.Stage != "fence_refreshed" || value.Purpose != string(FencingPurposeOngoing) {
		return FencingAttestation{}, ErrInvalidEvidenceDocument
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(value.SignatureBase64)
	if err != nil {
		return FencingAttestation{}, ErrInvalidEvidenceDocument
	}
	attestation, err := NewFencingAttestation(operation.Binding(), FencingPurposeOngoing, value.ObservedAt, value.ExpiresAt, value.Issuer, value.KeyID, value.Nonce, ObservationHashes{
		Ingress: mustObservationHash(value.IngressSHA256), Processes: mustObservationHash(value.ProcessesSHA256),
		Credentials: mustObservationHash(value.CredentialsSHA256), DatabaseNetwork: mustObservationHash(value.DatabaseNetworkSHA256),
	}, signature)
	if err != nil || verifier.Verify(attestation) != nil {
		return FencingAttestation{}, ErrInvalidEvidenceDocument
	}
	return attestation, nil
}

func decodeActiveAuthority(value authorityEvidenceDocument) (authority.Snapshot, error) {
	region, err := authority.ParseRegion(value.Region)
	if err != nil {
		return authority.Snapshot{}, err
	}
	epoch, err := authority.NewEpoch(value.Epoch)
	if err != nil || value.State != string(authority.StateActive) || !value.WritesEnabled {
		return authority.Snapshot{}, ErrInvalidEvidenceDocument
	}
	return authority.NewSnapshot(region, epoch, authority.StateActive, true)
}

func strictEvidenceJSON(document []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing evidence: %w", err)
	}
	return nil
}

func mustObservationHash(raw string) ObservationHash {
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != len(ObservationHash{}) {
		return ObservationHash{}
	}
	var result ObservationHash
	copy(result[:], decoded)
	return result
}

func positionSet(control, shard0, shard1 positionDocument) (DatabaseSet[ReplicationPosition], error) {
	c, err := NewReplicationPosition(control.Timeline, control.WAL)
	if err != nil {
		return DatabaseSet[ReplicationPosition]{}, err
	}
	s0, err := NewReplicationPosition(shard0.Timeline, shard0.WAL)
	if err != nil {
		return DatabaseSet[ReplicationPosition]{}, err
	}
	s1, err := NewReplicationPosition(shard1.Timeline, shard1.WAL)
	if err != nil {
		return DatabaseSet[ReplicationPosition]{}, err
	}
	return NewDatabaseSet(c, s0, s1), nil
}

func lossSet(control, shard0, shard1 lossDocument) (DatabaseSet[Loss], error) {
	if control.WindowMS < 0 || shard0.WindowMS < 0 || shard1.WindowMS < 0 {
		return DatabaseSet[Loss]{}, ErrInvalidEvidenceDocument
	}
	return NewDatabaseSet(
		Loss{MissingRecords: control.MissingRecords, Window: time.Duration(control.WindowMS) * time.Millisecond},
		Loss{MissingRecords: shard0.MissingRecords, Window: time.Duration(shard0.WindowMS) * time.Millisecond},
		Loss{MissingRecords: shard1.MissingRecords, Window: time.Duration(shard1.WindowMS) * time.Millisecond},
	), nil
}

func evidenceError(err error) error {
	if err != nil {
		return ErrInvalidEvidenceDocument
	}
	return nil
}
