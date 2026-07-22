package admissionredis

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// PutTokenLocator writes an exact, bounded-TTL mapping from a token hash to its
// immutable policy generation. The locator contains no raw bearer material and
// enables durable PostgreSQL replay to repair an interrupted Redis finalize
// without scanning keys or trusting the current policy version.
func (s *Store) PutTokenLocator(
	ctx context.Context,
	tokenHash [sha256.Size]byte,
	scope PolicyScope,
	ttl time.Duration,
) error {
	if s == nil || s.client == nil {
		return ErrInvalidInput
	}
	key, err := s.keys.TokenLocator(tokenHash)
	value, valueErr := encodeLocatorScope(scope)
	if err != nil || valueErr != nil ||
		ttl < domain.MinQueueEntryTTL || ttl > domain.MaxQueueEntryTTL+cleanupMargin {
		return ErrInvalidInput
	}
	result, scriptErr := s.locate.Run(ctx, s.client, []string{key}, value, ttl.Milliseconds()).Text()
	if scriptErr != nil {
		return backendError(scriptErr)
	}
	return resultError(result)
}

// ResolveTokenLocator performs one exact bounded lookup. It never uses SCAN or
// KEYS and it strictly revalidates every stored scope component.
func (s *Store) ResolveTokenLocator(ctx context.Context, tokenHash [sha256.Size]byte) (PolicyScope, error) {
	if s == nil || s.client == nil {
		return PolicyScope{}, ErrInvalidInput
	}
	key, err := s.keys.TokenLocator(tokenHash)
	if err != nil {
		return PolicyScope{}, ErrInvalidInput
	}
	value, err := s.client.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return PolicyScope{}, ErrNotFound
	}
	if err != nil {
		return PolicyScope{}, backendError(err)
	}
	return decodeLocatorScope(s, value)
}

func (s *Store) DeleteTokenLocator(ctx context.Context, tokenHash [sha256.Size]byte) error {
	if s == nil || s.client == nil {
		return ErrInvalidInput
	}
	key, err := s.keys.TokenLocator(tokenHash)
	if err != nil {
		return ErrInvalidInput
	}
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return backendError(err)
	}
	return nil
}

func encodeLocatorScope(scope PolicyScope) (string, error) {
	policyID, policyErr := uuid.Parse(strings.TrimSpace(scope.PolicyID))
	runID, runErr := uuid.Parse(strings.TrimSpace(scope.TrainRunID))
	seatClass := strings.ToLower(strings.TrimSpace(scope.SeatClass))
	if policyErr != nil || runErr != nil || !validSeatClass(seatClass) ||
		scope.Version < 1 || scope.Version > MaxPolicyVersion {
		return "", ErrInvalidInput
	}
	return fmt.Sprintf("%s|%s|%s|%d", policyID.String(), runID.String(), seatClass, scope.Version), nil
}

func decodeLocatorScope(s *Store, value string) (PolicyScope, error) {
	if s == nil || len(value) > 256 {
		return PolicyScope{}, ErrBackendUnavailable
	}
	parts := strings.Split(value, "|")
	if len(parts) != 4 {
		return PolicyScope{}, ErrBackendUnavailable
	}
	policyID, policyErr := uuid.Parse(parts[0])
	runID, runErr := uuid.Parse(parts[1])
	version, versionErr := strconv.ParseInt(parts[3], 10, 64)
	if policyErr != nil || runErr != nil || versionErr != nil {
		return PolicyScope{}, ErrBackendUnavailable
	}
	scope := PolicyScope{
		PolicyID: policyID.String(), TrainRunID: runID.String(),
		SeatClass: parts[2], Version: version,
	}
	if parts[0] != scope.PolicyID || parts[1] != scope.TrainRunID ||
		parts[2] != strings.ToLower(parts[2]) {
		return PolicyScope{}, ErrBackendUnavailable
	}
	if _, keyErr := s.policyKeys(scope); keyErr != nil {
		return PolicyScope{}, ErrBackendUnavailable
	}
	return scope, nil
}
