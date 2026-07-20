package admissionredis

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// ListLiveGenerations uses bounded SCAN to discover every still-live
// generation, including disabled and superseded policy versions. It is
// intended only for the read-only reconciliation CLI; request and worker paths
// continue to use exact keys.
func (s *Store) ListLiveGenerations(
	ctx context.Context,
	cursor uint64,
	limit int,
) ([]PolicyScope, uint64, error) {
	if s == nil || s.client == nil || limit < 1 || limit > MaxAdmissionBatch {
		return nil, 0, ErrInvalidInput
	}
	pattern := s.keys.namespace + ":wr:{*}:v*:continuity"
	keys, next, err := s.client.Scan(ctx, cursor, pattern, int64(limit)).Result()
	if err != nil {
		return nil, 0, backendError(err)
	}
	scopes := make([]PolicyScope, 0, len(keys))
	for _, key := range keys {
		scope, parseErr := parseGenerationContinuityKey(s.keys.namespace, key)
		if parseErr != nil {
			return nil, 0, parseErr
		}
		scopes = append(scopes, scope)
	}
	return scopes, next, nil
}

// ValidateCurrentGeneration performs an exact, read-only marker-pair check for
// a PostgreSQL-current generation that was already initialized. Historical
// reconciliation remains continuity-only so a newer shared marker does not
// make bounded inspection of a previous generation impossible.
func (s *Store) ValidateCurrentGeneration(ctx context.Context, scope PolicyScope) error {
	keys, err := s.policyKeys(scope)
	if err != nil {
		return ErrInvalidInput
	}
	values, err := s.client.MGet(ctx, keys.PolicyVersion, keys.Continuity).Result()
	if err != nil {
		return backendError(err)
	}
	if len(values) != 2 {
		return ErrBackendUnavailable
	}
	expected := strconv.FormatInt(scope.Version, 10)
	if stringValue(values[0]) != expected {
		return ErrPolicyMismatch
	}
	if stringValue(values[1]) != expected {
		return ErrContinuityLost
	}
	return nil
}

func parseGenerationContinuityKey(namespace, key string) (PolicyScope, error) {
	if !namespacePattern.MatchString(namespace) || len(key) > 256 {
		return PolicyScope{}, ErrBackendUnavailable
	}
	pattern := regexp.MustCompile(
		`^` + regexp.QuoteMeta(namespace) +
			`:wr:\{([0-9a-f-]{36})\|(standard|business|first)\}:v([1-9][0-9]{0,9}):continuity$`,
	)
	matches := pattern.FindStringSubmatch(key)
	if len(matches) != 4 {
		return PolicyScope{}, fmt.Errorf("%w: malformed admission generation key", ErrBackendUnavailable)
	}
	runID, runErr := uuid.Parse(matches[1])
	version, versionErr := strconv.ParseInt(matches[3], 10, 64)
	if runErr != nil || runID.String() != matches[1] || versionErr != nil ||
		version < 1 || version > MaxPolicyVersion || strings.ToLower(matches[2]) != matches[2] {
		return PolicyScope{}, fmt.Errorf("%w: malformed admission generation key", ErrBackendUnavailable)
	}
	return PolicyScope{
		PolicyID:   "reconciliation",
		TrainRunID: runID.String(),
		SeatClass:  matches[2],
		Version:    version,
	}, nil
}
