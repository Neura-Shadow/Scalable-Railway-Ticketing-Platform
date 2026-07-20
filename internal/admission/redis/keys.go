package admissionredis

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

var ErrInvalidKeyScope = errors.New("invalid admission redis key scope")

const MaxPolicyVersion = int64(1_000_000_000)

var namespacePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

type KeyBuilder struct {
	namespace string
}

type PolicyKeys struct {
	PolicyVersion string
	Continuity    string
	Queue         string
	Sequence      string
	Entries       string
	Users         string
	Tokens        string
	Inflight      string
	Rate          string
	Leases        string
}

func (b KeyBuilder) EntryLocator(entryID string) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(entryID))
	if err != nil || !namespacePattern.MatchString(b.namespace) {
		return "", ErrInvalidKeyScope
	}
	return fmt.Sprintf("%s:wr:entry-locator:%s", b.namespace, parsed.String()), nil
}

// TokenLocator derives an exact, fixed-width lookup key from a domain-separated
// digest of SHA-256(raw token). The token hash itself therefore never appears
// in a Redis key name, diagnostic key listing, or arbitrary caller-controlled
// suffix.
func (b KeyBuilder) TokenLocator(tokenHash [sha256.Size]byte) (string, error) {
	if !namespacePattern.MatchString(b.namespace) {
		return "", ErrInvalidKeyScope
	}
	locatorInput := append([]byte("railway-admission-token-locator/v1\x00"), tokenHash[:]...)
	locatorDigest := sha256.Sum256(locatorInput)
	return fmt.Sprintf("%s:wr:token-locator:%s", b.namespace, hex.EncodeToString(locatorDigest[:])), nil
}

func NewKeyBuilder(namespace string) (KeyBuilder, error) {
	namespace = strings.ToLower(strings.TrimSpace(namespace))
	if !namespacePattern.MatchString(namespace) {
		return KeyBuilder{}, ErrInvalidKeyScope
	}
	return KeyBuilder{namespace: namespace}, nil
}

func (b KeyBuilder) ForPolicy(trainRunID, seatClass string, version int64) (PolicyKeys, error) {
	runID, err := uuid.Parse(strings.TrimSpace(trainRunID))
	seatClass = strings.ToLower(strings.TrimSpace(seatClass))
	if err != nil || !validSeatClass(seatClass) || version < 1 || version > MaxPolicyVersion ||
		!namespacePattern.MatchString(b.namespace) {
		return PolicyKeys{}, ErrInvalidKeyScope
	}
	tag := fmt.Sprintf("{%s|%s}", runID.String(), seatClass)
	scope := fmt.Sprintf("%s:wr:%s", b.namespace, tag)
	generation := fmt.Sprintf("%s:v%d", scope, version)
	return PolicyKeys{
		PolicyVersion: scope + ":policy-version",
		Continuity:    generation + ":continuity",
		Queue:         generation + ":queue",
		Sequence:      generation + ":sequence",
		Entries:       generation + ":entries",
		Users:         generation + ":users",
		Tokens:        generation + ":tokens",
		Inflight:      generation + ":inflight",
		Rate:          generation + ":rate",
		Leases:        generation + ":leases",
	}, nil
}

func validSeatClass(value string) bool {
	switch value {
	case "standard", "business", "first":
		return true
	default:
		return false
	}
}
