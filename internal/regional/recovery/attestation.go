package recovery

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
)

var (
	ErrInvalidFenceBinding = errors.New("invalid external fencing binding")
	ErrInvalidAttestation  = errors.New("invalid external fencing attestation")
	ErrFencingBinding      = errors.New("external fencing attestation binding mismatch")
	ErrFencingSignature    = errors.New("external fencing attestation signature invalid")
)

const maximumFencingAttestationTTL = 10 * time.Minute

type FencingPurpose string

const (
	FencingPurposeInitial            FencingPurpose = "initial_fence"
	FencingPurposeOngoing            FencingPurpose = "ongoing_source_fence"
	FencingPurposeRetainedSource     FencingPurpose = "retained_source_fence"
	FencingPurposeFailbackValidation FencingPurpose = "failback_validation"
)

func (purpose FencingPurpose) valid() bool {
	return purpose == FencingPurposeInitial || purpose == FencingPurposeOngoing || purpose == FencingPurposeRetainedSource || purpose == FencingPurposeFailbackValidation
}

var (
	operatorPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$`)
	attestorIDPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$`)
	attestationNonceRE = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)
)

type FenceBinding struct {
	operationID uuid.UUID
	source      authority.Region
	sourceEpoch authority.Epoch
	incidentID  uuid.UUID
	operatorID  string
	declaredAt  time.Time
}

func NewFenceBinding(operationID uuid.UUID, source authority.Region, sourceEpoch authority.Epoch, incidentID uuid.UUID, operatorID string, declaredAt time.Time) (FenceBinding, error) {
	_, regionErr := authority.ParseRegion(source.String())
	if operationID == uuid.Nil || incidentID == uuid.Nil || regionErr != nil || sourceEpoch.Uint64() == 0 || !operatorPattern.MatchString(operatorID) || declaredAt.IsZero() {
		return FenceBinding{}, ErrInvalidFenceBinding
	}
	return FenceBinding{operationID: operationID, source: source, sourceEpoch: sourceEpoch, incidentID: incidentID, operatorID: operatorID, declaredAt: declaredAt.UTC()}, nil
}

func (binding FenceBinding) OperationID() uuid.UUID       { return binding.operationID }
func (binding FenceBinding) Source() authority.Region     { return binding.source }
func (binding FenceBinding) SourceEpoch() authority.Epoch { return binding.sourceEpoch }
func (binding FenceBinding) IncidentID() uuid.UUID        { return binding.incidentID }
func (binding FenceBinding) OperatorID() string           { return binding.operatorID }
func (binding FenceBinding) DeclaredAt() time.Time        { return binding.declaredAt }
func (binding FenceBinding) equal(other FenceBinding) bool {
	return binding.operationID == other.operationID && binding.source == other.source && binding.sourceEpoch == other.sourceEpoch && binding.incidentID == other.incidentID && binding.operatorID == other.operatorID && binding.declaredAt.Equal(other.declaredAt)
}

type ObservationHash [sha256.Size]byte

func HashObservation(observation []byte) ObservationHash { return sha256.Sum256(observation) }
func (hash ObservationHash) valid() bool                 { return hash != ObservationHash{} }

type ObservationHashes struct {
	Ingress         ObservationHash
	Processes       ObservationHash
	Credentials     ObservationHash
	DatabaseNetwork ObservationHash
}

func (hashes ObservationHashes) valid() bool {
	return hashes.Ingress.valid() && hashes.Processes.valid() && hashes.Credentials.valid() && hashes.DatabaseNetwork.valid()
}

// FencingAttestation is an independently signed, short-lived, single-operation
// statement. Raw observations remain outside the recovery journal.
type FencingAttestation struct {
	binding    FenceBinding
	observedAt time.Time
	expiresAt  time.Time
	issuer     string
	keyID      string
	nonce      string
	purpose    FencingPurpose
	hashes     ObservationHashes
	signature  [ed25519.SignatureSize]byte
}

func NewFencingAttestation(binding FenceBinding, purpose FencingPurpose, observedAt, expiresAt time.Time, issuer, keyID, nonce string, hashes ObservationHashes, signature []byte) (FencingAttestation, error) {
	if binding.operationID == uuid.Nil || !purpose.valid() || observedAt.IsZero() || expiresAt.IsZero() || observedAt.Before(binding.declaredAt) || !expiresAt.After(observedAt) || expiresAt.Sub(observedAt) > maximumFencingAttestationTTL || !attestorIDPattern.MatchString(issuer) || !attestorIDPattern.MatchString(keyID) || !attestationNonceRE.MatchString(nonce) || !hashes.valid() || len(signature) != ed25519.SignatureSize {
		return FencingAttestation{}, ErrInvalidAttestation
	}
	value := FencingAttestation{binding: binding, purpose: purpose, observedAt: observedAt.UTC(), expiresAt: expiresAt.UTC(), issuer: issuer, keyID: keyID, nonce: nonce, hashes: hashes}
	copy(value.signature[:], signature)
	return value, nil
}

func (attestation FencingAttestation) Binding() FenceBinding     { return attestation.binding }
func (attestation FencingAttestation) ObservedAt() time.Time     { return attestation.observedAt }
func (attestation FencingAttestation) ExpiresAt() time.Time      { return attestation.expiresAt }
func (attestation FencingAttestation) Issuer() string            { return attestation.issuer }
func (attestation FencingAttestation) KeyID() string             { return attestation.keyID }
func (attestation FencingAttestation) Nonce() string             { return attestation.nonce }
func (attestation FencingAttestation) Purpose() FencingPurpose   { return attestation.purpose }
func (attestation FencingAttestation) Hashes() ObservationHashes { return attestation.hashes }
func (attestation FencingAttestation) Signature() []byte {
	return append([]byte(nil), attestation.signature[:]...)
}

func (attestation FencingAttestation) ValidateFor(binding FenceBinding) error {
	if !attestation.binding.equal(binding) {
		return ErrFencingBinding
	}
	if !attestation.purpose.valid() || attestation.observedAt.IsZero() || attestation.expiresAt.IsZero() || !attestation.expiresAt.After(attestation.observedAt) || attestation.expiresAt.Sub(attestation.observedAt) > maximumFencingAttestationTTL || !attestorIDPattern.MatchString(attestation.issuer) || !attestorIDPattern.MatchString(attestation.keyID) || !attestationNonceRE.MatchString(attestation.nonce) || !attestation.hashes.valid() || attestation.signature == [ed25519.SignatureSize]byte{} {
		return ErrInvalidAttestation
	}
	return nil
}

func (attestation FencingAttestation) ValidateForPurpose(binding FenceBinding, purpose FencingPurpose) error {
	if err := attestation.ValidateFor(binding); err != nil {
		return err
	}
	if !purpose.valid() || attestation.purpose != purpose {
		return ErrFencingBinding
	}
	return nil
}

func (attestation FencingAttestation) equal(other FencingAttestation) bool {
	return attestation.binding.equal(other.binding) && attestation.purpose == other.purpose && attestation.observedAt.Equal(other.observedAt) && attestation.expiresAt.Equal(other.expiresAt) && attestation.issuer == other.issuer && attestation.keyID == other.keyID && attestation.nonce == other.nonce && attestation.hashes == other.hashes && attestation.signature == other.signature
}

type FencingVerifier struct {
	issuer    string
	keyID     string
	publicKey ed25519.PublicKey
	now       func() time.Time
}

func (verifier FencingVerifier) valid() bool {
	return verifier.now != nil && len(verifier.publicKey) == ed25519.PublicKeySize && attestorIDPattern.MatchString(verifier.issuer) && attestorIDPattern.MatchString(verifier.keyID)
}

func NewFencingVerifier(issuer, keyID string, publicKey []byte, now func() time.Time) (FencingVerifier, error) {
	if !attestorIDPattern.MatchString(issuer) || !attestorIDPattern.MatchString(keyID) || len(publicKey) != ed25519.PublicKeySize || now == nil {
		return FencingVerifier{}, ErrInvalidAttestation
	}
	return FencingVerifier{issuer: issuer, keyID: keyID, publicKey: append(ed25519.PublicKey(nil), publicKey...), now: now}, nil
}

func (verifier FencingVerifier) Verify(attestation FencingAttestation) error {
	if verifier.now == nil || len(verifier.publicKey) != ed25519.PublicKeySize || attestation.issuer != verifier.issuer || attestation.keyID != verifier.keyID {
		return ErrFencingSignature
	}
	now := verifier.now().UTC()
	if now.Before(attestation.observedAt) || !now.Before(attestation.expiresAt) || !ed25519.Verify(verifier.publicKey, attestation.CanonicalPayload(), attestation.signature[:]) {
		return ErrFencingSignature
	}
	return nil
}

// CanonicalPayload is the cross-language signing contract used by the external
// fencing authority. It ends with one newline.
func (attestation FencingAttestation) CanonicalPayload() []byte {
	h := attestation.hashes
	fields := []string{
		"railway-fence-v1", string(attestation.purpose), attestation.binding.operationID.String(), attestation.binding.source.String(), strconv.FormatUint(attestation.binding.sourceEpoch.Uint64(), 10),
		attestation.binding.incidentID.String(), attestation.binding.operatorID, attestation.observedAt.Format(time.RFC3339Nano), attestation.expiresAt.Format(time.RFC3339Nano),
		attestation.issuer, attestation.keyID, attestation.nonce, hex.EncodeToString(h.Ingress[:]), hex.EncodeToString(h.Processes[:]), hex.EncodeToString(h.Credentials[:]), hex.EncodeToString(h.DatabaseNetwork[:]),
	}
	return []byte(strings.Join(fields, "\n") + "\n")
}
