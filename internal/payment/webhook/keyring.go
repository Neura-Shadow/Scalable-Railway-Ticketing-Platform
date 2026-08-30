package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"regexp"
	"sort"
	"time"
)

var (
	ErrInvalidKeyring     = errors.New("webhook keyring metadata invalid")
	ErrKeyringConflict    = errors.New("webhook keyring metadata conflict")
	ErrKeyRetirementGrace = errors.New("webhook key retirement grace not elapsed")
	keyIDPattern          = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

type KeyState string

const (
	KeyAccepted KeyState = "accepted"
	KeyPrimary  KeyState = "primary"
	KeyRetired  KeyState = "retired"
)

type KeyVersion struct {
	KeyID               string
	State               KeyState
	ActivatedAt         time.Time
	RetirementNotBefore *time.Time
	RetiredAt           *time.Time
	SecretProof         [sha256.Size]byte
}

type DesiredKeyring struct {
	PrimaryKeyID   string
	AcceptedKeyIDs []string
	SecretProofs   map[string][sha256.Size]byte
	Grace          time.Duration
	Now            time.Time
}

// KeyProof produces a domain-separated, one-way equality proof for independently
// provisioned webhook secret material. The secret itself never enters durable
// metadata, logs, metrics, or evidence.
func KeyProof(provider, accountID, keyID, secret string) [sha256.Size]byte {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("railway-webhook-key-proof-v1\x00"))
	_, _ = mac.Write([]byte(provider))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(accountID))
	// The key ID is the durable row identity rather than part of the proof.
	// This makes accidental reuse of one secret under two IDs detectable by a
	// unique database constraint while still detecting swapped ID/secret
	// mappings during readiness validation.
	_ = keyID
	var proof [sha256.Size]byte
	copy(proof[:], mac.Sum(nil))
	return proof
}

type KeyringPlan struct {
	Versions []KeyVersion
	ByID     map[string]KeyVersion
}

// PlanKeyring is the provider-neutral lifecycle decision. Secret bytes never
// enter this model; callers persist only bounded key identities and times.
func PlanKeyring(current []KeyVersion, desired DesiredKeyring) (KeyringPlan, error) {
	accepted, err := validateDesiredKeyring(desired)
	if err != nil {
		return KeyringPlan{}, err
	}
	versions := make(map[string]KeyVersion, len(current)+len(accepted))
	primaryCount := 0
	for _, version := range current {
		if !validKeyVersion(version) {
			return KeyringPlan{}, ErrKeyringConflict
		}
		if _, duplicate := versions[version.KeyID]; duplicate {
			return KeyringPlan{}, ErrKeyringConflict
		}
		if version.State == KeyPrimary {
			primaryCount++
		}
		versions[version.KeyID] = cloneKeyVersion(version)
	}
	if primaryCount > 1 {
		return KeyringPlan{}, ErrKeyringConflict
	}

	for keyID := range accepted {
		version, found := versions[keyID]
		if found && version.State == KeyRetired {
			return KeyringPlan{}, ErrKeyringConflict
		}
		desiredProof := desired.SecretProofs[keyID]
		if found && !hmac.Equal(version.SecretProof[:], desiredProof[:]) {
			return KeyringPlan{}, ErrKeyringConflict
		}
		if !found {
			version = KeyVersion{KeyID: keyID, State: KeyAccepted, ActivatedAt: desired.Now, SecretProof: desiredProof}
		}
		versions[keyID] = version
	}

	for keyID, version := range versions {
		if version.State == KeyPrimary && keyID != desired.PrimaryKeyID {
			version.State = KeyAccepted
			deadline := desired.Now.Add(desired.Grace)
			version.RetirementNotBefore = &deadline
			versions[keyID] = version
		}
	}
	primary := versions[desired.PrimaryKeyID]
	primary.State = KeyPrimary
	primary.RetirementNotBefore = nil
	primary.RetiredAt = nil
	versions[desired.PrimaryKeyID] = primary

	for keyID, version := range versions {
		if _, wanted := accepted[keyID]; wanted || version.State == KeyRetired {
			continue
		}
		// A replacement may be staged as accepted without ever becoming primary.
		// Removing that staged key must start (and persist) a retirement grace;
		// returning an error here would roll the metadata transaction back and make
		// the key impossible to retire on every later synchronization attempt.
		if version.RetirementNotBefore == nil {
			deadline := desired.Now.Add(desired.Grace)
			version.RetirementNotBefore = &deadline
			versions[keyID] = version
			continue
		}
		if desired.Now.Before(*version.RetirementNotBefore) {
			return KeyringPlan{}, ErrKeyRetirementGrace
		}
		retiredAt := desired.Now
		version.State = KeyRetired
		version.RetiredAt = &retiredAt
		versions[keyID] = version
	}

	keys := make([]string, 0, len(versions))
	for keyID := range versions {
		keys = append(keys, keyID)
	}
	sort.Strings(keys)
	plan := KeyringPlan{Versions: make([]KeyVersion, 0, len(keys)), ByID: make(map[string]KeyVersion, len(keys))}
	for _, keyID := range keys {
		version := cloneKeyVersion(versions[keyID])
		plan.Versions = append(plan.Versions, version)
		plan.ByID[keyID] = version
	}
	return plan, nil
}

func validateDesiredKeyring(desired DesiredKeyring) (map[string]struct{}, error) {
	if !keyIDPattern.MatchString(desired.PrimaryKeyID) || len(desired.AcceptedKeyIDs) < 1 ||
		len(desired.AcceptedKeyIDs) > 2 || desired.Grace <= 0 || desired.Grace > 30*24*time.Hour || desired.Now.IsZero() {
		return nil, ErrInvalidKeyring
	}
	accepted := make(map[string]struct{}, len(desired.AcceptedKeyIDs))
	proofs := make(map[[sha256.Size]byte]struct{}, len(desired.AcceptedKeyIDs))
	for _, keyID := range desired.AcceptedKeyIDs {
		proof, hasProof := desired.SecretProofs[keyID]
		if !keyIDPattern.MatchString(keyID) || !hasProof || proof == ([sha256.Size]byte{}) {
			return nil, ErrInvalidKeyring
		}
		if _, duplicate := accepted[keyID]; duplicate {
			return nil, ErrInvalidKeyring
		}
		if _, reused := proofs[proof]; reused {
			return nil, ErrInvalidKeyring
		}
		accepted[keyID] = struct{}{}
		proofs[proof] = struct{}{}
	}
	if _, found := accepted[desired.PrimaryKeyID]; !found {
		return nil, ErrInvalidKeyring
	}
	return accepted, nil
}

func validKeyVersion(version KeyVersion) bool {
	if !keyIDPattern.MatchString(version.KeyID) || version.ActivatedAt.IsZero() || version.SecretProof == ([sha256.Size]byte{}) {
		return false
	}
	switch version.State {
	case KeyPrimary:
		return version.RetiredAt == nil
	case KeyAccepted:
		return version.RetiredAt == nil && (version.RetirementNotBefore == nil ||
			!version.RetirementNotBefore.Before(version.ActivatedAt))
	case KeyRetired:
		return version.RetiredAt != nil && !version.RetiredAt.Before(version.ActivatedAt)
	default:
		return false
	}
}

func cloneKeyVersion(version KeyVersion) KeyVersion {
	if version.RetirementNotBefore != nil {
		value := *version.RetirementNotBefore
		version.RetirementNotBefore = &value
	}
	if version.RetiredAt != nil {
		value := *version.RetiredAt
		version.RetiredAt = &value
	}
	return version
}
