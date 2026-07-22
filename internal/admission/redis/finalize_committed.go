package admissionredis

import (
	"context"
	"crypto/sha256"
	"errors"

	goredis "github.com/redis/go-redis/v9"
)

// CommittedMutation carries only identities already proved by the completed
// PostgreSQL idempotency record. It intentionally omits a Redis lease owner:
// the durable commit is the authority used to repair a finalize that was lost
// after commit.
type CommittedMutation struct {
	TokenHash          [sha256.Size]byte
	OwnerHash          [sha256.Size]byte
	BookingFingerprint [sha256.Size]byte
	IdempotencyKeyHash [sha256.Size]byte
}

// FinalizeCommitted repairs Redis after a matching durable PostgreSQL replay.
// The exact token-hash locator selects the original policy generation without
// SCAN, including after a policy version change. The Lua script then rechecks
// every immutable booking binding before persisting a bounded consumed
// tombstone. The locator remains available through its original TTL so later
// matching durable replays can resolve the same generation without a scan.
func (s *Store) FinalizeCommitted(ctx context.Context, mutation CommittedMutation) error {
	if s == nil || s.client == nil || zeroHash(mutation.TokenHash) ||
		zeroHash(mutation.OwnerHash) || zeroHash(mutation.BookingFingerprint) ||
		zeroHash(mutation.IdempotencyKeyHash) {
		return ErrInvalidInput
	}
	scope, err := s.ResolveTokenLocator(ctx, mutation.TokenHash)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	keys, err := s.policyKeys(scope)
	if err != nil {
		return ErrInvalidInput
	}
	result, err := goredis.NewScript(finalizeCommittedScript).Run(ctx, s.client, []string{
		keys.Continuity, keys.Tokens, keys.Inflight, keys.Leases, keys.Entries, keys.Users,
	}, scope.Version, hex32(mutation.TokenHash), hex32(mutation.OwnerHash),
		hex32(mutation.BookingFingerprint), hex32(mutation.IdempotencyKeyHash)).Text()
	if err != nil {
		return backendError(err)
	}
	switch result {
	case "finalized":
		return nil
	case "not_found", "continuity_lost":
		return s.DeleteTokenLocator(ctx, mutation.TokenHash)
	default:
		return resultError(result)
	}
}

func zeroHash(value [sha256.Size]byte) bool {
	var zero [sha256.Size]byte
	return value == zero
}

const finalizeCommittedScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return 'continuity_lost' end
local token = ARGV[2]
local function field(id, suffix) return id .. '|' .. suffix end
local status = redis.call('HGET', KEYS[2], field(token, 's'))
if not status then return 'not_found' end
if redis.call('HGET', KEYS[2], field(token, 'o')) ~= ARGV[3] or
   redis.call('HGET', KEYS[2], field(token, 'bf')) ~= ARGV[4] or
   redis.call('HGET', KEYS[2], field(token, 'ih')) ~= ARGV[5] then return 'mismatch' end
if status ~= 'issued' and status ~= 'processing' and status ~= 'consumed' and
   status ~= 'expired' and status ~= 'cancelled' then return 'terminal' end
local entry = redis.call('HGET', KEYS[2], field(token, 'e'))
local owner = redis.call('HGET', KEYS[2], field(token, 'o'))
local expires = tonumber(redis.call('HGET', KEYS[2], field(token, 'x')) or '0')
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
redis.call('ZREM', KEYS[3], token)
redis.call('ZREM', KEYS[4], token)
if owner and entry and redis.call('HGET', KEYS[6], owner) == entry then
  redis.call('HDEL', KEYS[6], owner)
end
if entry then
  redis.call('HSET', KEYS[5], field(entry, 's'), 'admitted')
end
redis.call('HSET', KEYS[2], field(token, 's'), 'consumed')
redis.call('HDEL', KEYS[2], field(token, 'lo'), field(token, 'le'), field(token, 'cr'))
if expires <= now then expires = now + 60000 end
redis.call('ZADD', KEYS[4], expires, token)
return 'finalized'
`
