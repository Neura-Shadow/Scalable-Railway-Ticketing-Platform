package recovery

import (
	"encoding/base64"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
)

var ErrInvalidFailbackValidationDocument = errors.New("failback validation document invalid")

type reseedProvenanceDocument struct {
	SourceRegion     string           `json:"source_region"`
	SourceEpoch      uint64           `json:"source_epoch"`
	StartedAt        time.Time        `json:"started_at"`
	CompletedAt      time.Time        `json:"completed_at"`
	SourcePosition   positionDocument `json:"source_position"`
	ReplayedPosition positionDocument `json:"replayed_position"`
	Reconciled       bool             `json:"reconciled"`
}

type failbackValidationDocument struct {
	ReseedAfter  time.Time                `json:"reseed_after"`
	Control      reseedProvenanceDocument `json:"control"`
	Shard0       reseedProvenanceDocument `json:"shard_0"`
	Shard1       reseedProvenanceDocument `json:"shard_1"`
	CurrentFence fenceEvidenceDocument    `json:"current_fence"`
}

// DecodeFailbackValidationDocument binds strict reseed provenance and a fresh
// independently verified source fence to the already durable failback plan.
func DecodeFailbackValidationDocument(
	operation Failover,
	targetEpoch authority.Epoch,
	document []byte,
	verifier FencingVerifier,
) (FailbackPlan, error) {
	if len(document) == 0 || len(document) > maximumEvidenceDocumentBytes {
		return FailbackPlan{}, ErrInvalidFailbackValidationDocument
	}
	var value failbackValidationDocument
	if strictEvidenceJSON(document, &value) != nil {
		return FailbackPlan{}, ErrInvalidFailbackValidationDocument
	}
	control, err := decodeReseedProvenance(value.Control)
	if err != nil {
		return FailbackPlan{}, ErrInvalidFailbackValidationDocument
	}
	shard0, err := decodeReseedProvenance(value.Shard0)
	if err != nil {
		return FailbackPlan{}, ErrInvalidFailbackValidationDocument
	}
	shard1, err := decodeReseedProvenance(value.Shard1)
	if err != nil {
		return FailbackPlan{}, ErrInvalidFailbackValidationDocument
	}
	fence, err := decodeFailbackFence(operation.Binding(), value.CurrentFence, verifier)
	if err != nil {
		return FailbackPlan{}, ErrInvalidFailbackValidationDocument
	}
	plan, err := PrepareFailback(operation.Binding(), operation.Target(), targetEpoch, value.ReseedAfter,
		NewDatabaseSet(control, shard0, shard1), fence)
	if err != nil {
		return FailbackPlan{}, ErrInvalidFailbackValidationDocument
	}
	return plan, nil
}

func decodeReseedProvenance(value reseedProvenanceDocument) (ReseedProvenance, error) {
	region, err := authority.ParseRegion(value.SourceRegion)
	if err != nil {
		return ReseedProvenance{}, err
	}
	epoch, err := authority.NewEpoch(value.SourceEpoch)
	if err != nil {
		return ReseedProvenance{}, err
	}
	source, err := NewReplicationPosition(value.SourcePosition.Timeline, value.SourcePosition.WAL)
	if err != nil {
		return ReseedProvenance{}, err
	}
	replayed, err := NewReplicationPosition(value.ReplayedPosition.Timeline, value.ReplayedPosition.WAL)
	if err != nil {
		return ReseedProvenance{}, err
	}
	return NewReseedProvenance(region, epoch, value.StartedAt, value.CompletedAt, source, replayed, value.Reconciled)
}

func decodeFailbackFence(binding FenceBinding, value fenceEvidenceDocument, verifier FencingVerifier) (FencingAttestation, error) {
	if value.Purpose != string(FencingPurposeFailbackValidation) {
		return FencingAttestation{}, ErrInvalidFailbackValidationDocument
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(value.SignatureBase64)
	if err != nil {
		return FencingAttestation{}, err
	}
	attestation, err := NewFencingAttestation(binding, FencingPurposeFailbackValidation, value.ObservedAt, value.ExpiresAt, value.Issuer, value.KeyID, value.Nonce, ObservationHashes{
		Ingress: mustObservationHash(value.IngressSHA256), Processes: mustObservationHash(value.ProcessesSHA256),
		Credentials: mustObservationHash(value.CredentialsSHA256), DatabaseNetwork: mustObservationHash(value.DatabaseNetworkSHA256),
	}, signature)
	if err != nil {
		return FencingAttestation{}, err
	}
	if err := verifier.Verify(attestation); err != nil {
		return FencingAttestation{}, err
	}
	return attestation, nil
}
