package admissionredis

import (
	"context"
	"errors"
)

// StateCounts is an O(1) exact snapshot for one policy generation.
type StateCounts struct {
	QueueDepth         int64
	InflightAdmissions int64
}

func (s *Store) StateCounts(ctx context.Context, scope PolicyScope) (StateCounts, error) {
	keys, err := s.policyKeys(scope)
	if err != nil {
		return StateCounts{}, ErrInvalidInput
	}
	value, err := s.client.Eval(ctx, stateCountsScript, []string{
		keys.PolicyVersion, keys.Continuity, keys.Queue, keys.Inflight,
	}, scope.Version).Slice()
	if err != nil {
		return StateCounts{}, backendError(err)
	}
	if len(value) != 3 || stringValue(value[0]) != "ok" {
		if len(value) > 0 {
			return StateCounts{}, resultError(stringValue(value[0]))
		}
		return StateCounts{}, ErrBackendUnavailable
	}
	queueDepth, queueErr := int64Value(value[1])
	inflight, inflightErr := int64Value(value[2])
	if errors.Join(queueErr, inflightErr) != nil || queueDepth < 0 || inflight < 0 {
		return StateCounts{}, ErrBackendUnavailable
	}
	return StateCounts{QueueDepth: queueDepth, InflightAdmissions: inflight}, nil
}

const stateCountsScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return {'policy_mismatch'} end
if redis.call('GET', KEYS[2]) ~= expected then return {'continuity_lost'} end
return {'ok', redis.call('ZCARD', KEYS[3]), redis.call('ZCARD', KEYS[4])}
`
