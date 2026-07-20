package admissionredis

const installPolicyScript = `
local current = redis.call('GET', KEYS[1])
local expected = tostring(ARGV[1])
local allow_create = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
if not current then
  if allow_create ~= 1 then return 'continuity_lost' end
  redis.call('SET', KEYS[1], expected, 'PX', ttl)
  redis.call('SET', KEYS[2], expected, 'PX', ttl)
  return 'ok'
end
if current == expected then
  if redis.call('GET', KEYS[2]) ~= expected then return 'continuity_lost' end
  redis.call('PEXPIRE', KEYS[1], ttl)
  redis.call('PEXPIRE', KEYS[2], ttl)
  return 'ok'
end
if allow_create ~= 1 or tonumber(current) >= tonumber(expected) then return 'policy_mismatch' end
redis.call('SET', KEYS[1], expected, 'PX', ttl)
redis.call('SET', KEYS[2], expected, 'PX', ttl)
return 'ok'
`

const putLocatorScript = `
local current = redis.call('GET', KEYS[1])
if current and current ~= ARGV[1] then return 'join_conflict' end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 'ok'
`

const joinScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return {'policy_mismatch'} end
if redis.call('GET', KEYS[2]) ~= expected then return {'continuity_lost'} end
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local owner = ARGV[2]
local entry = ARGV[3]
local fingerprint = ARGV[4]
local max_queue = tonumber(ARGV[11])
local entry_ttl = tonumber(ARGV[12])
local key_ttl = tonumber(ARGV[13])
local function field(id, suffix) return id .. '|' .. suffix end
local function expire_entry(id)
  local previous_owner = redis.call('HGET', KEYS[5], field(id, 'o'))
  redis.call('ZREM', KEYS[3], id)
  redis.call('HSET', KEYS[5], field(id, 's'), 'expired')
  if previous_owner and redis.call('HGET', KEYS[6], previous_owner) == id then
    redis.call('HDEL', KEYS[6], previous_owner)
  end
  redis.call('ZADD', KEYS[7], now + 60000, 'entry:' .. id)
end
local stale = redis.call('ZRANGE', KEYS[3], 0, 63)
for _, id in ipairs(stale) do
  local expiry = tonumber(redis.call('HGET', KEYS[5], field(id, 'x')) or '0')
  if expiry > 0 and expiry <= now then expire_entry(id) end
end
local existing = redis.call('HGET', KEYS[6], owner)
if existing then
  local status = redis.call('HGET', KEYS[5], field(existing, 's'))
  local expiry = tonumber(redis.call('HGET', KEYS[5], field(existing, 'x')) or '0')
  if (status == 'queued' or status == 'admitted') and expiry > now then
    if redis.call('HGET', KEYS[5], field(existing, 'f')) ~= fingerprint then
      return {'join_conflict'}
    end
    local rank = redis.call('ZRANK', KEYS[3], existing)
    if not rank then rank = -1 else rank = rank + 1 end
    return {'duplicate', existing,
      redis.call('HGET', KEYS[5], field(existing, 'q')),
      owner, fingerprint, status,
      redis.call('HGET', KEYS[5], field(existing, 'p')),
      redis.call('HGET', KEYS[5], field(existing, 'r')),
      redis.call('HGET', KEYS[5], field(existing, 'j')),
      redis.call('HGET', KEYS[5], field(existing, 'x')),
      rank,
      redis.call('HGET', KEYS[5], field(existing, 'a')),
      redis.call('HGET', KEYS[5], field(existing, 'b')),
      redis.call('HGET', KEYS[5], field(existing, 'n')),
      redis.call('HGET', KEYS[5], field(existing, 'c')),
      expected,
      redis.call('HGET', KEYS[5], field(existing, 'd')) or ''}
  end
  redis.call('HDEL', KEYS[6], owner)
end
if redis.call('ZCARD', KEYS[3]) >= max_queue then return {'queue_full'} end
local sequence = redis.call('INCR', KEYS[4])
local expires = now + entry_ttl
redis.call('ZADD', KEYS[3], sequence, entry)
redis.call('HSET', KEYS[5],
  field(entry, 's'), 'queued', field(entry, 'q'), sequence,
  field(entry, 'o'), owner, field(entry, 'f'), fingerprint,
  field(entry, 'p'), ARGV[5], field(entry, 'r'), ARGV[6],
  field(entry, 'c'), ARGV[7], field(entry, 'a'), ARGV[8],
  field(entry, 'b'), ARGV[9], field(entry, 'n'), ARGV[10],
  field(entry, 'j'), now, field(entry, 'x'), expires)
redis.call('HSET', KEYS[6], owner, entry)
for i=1,#KEYS do redis.call('PEXPIRE', KEYS[i], key_ttl) end
return {'joined', entry, sequence, owner, fingerprint, 'queued', ARGV[5], ARGV[6],
  now, expires, redis.call('ZCARD', KEYS[3]), ARGV[8], ARGV[9], ARGV[10], ARGV[7], expected, ''}
`

const getScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return {'policy_mismatch'} end
if redis.call('GET', KEYS[2]) ~= expected then return {'continuity_lost'} end
local entry = ARGV[2]
local owner = ARGV[3]
local claim = tonumber(ARGV[4] or '0')
local function field(id, suffix) return id .. '|' .. suffix end
local function delete_entry(id)
  redis.call('HDEL', KEYS[4],
    field(id, 's'), field(id, 'q'), field(id, 'o'), field(id, 'f'),
    field(id, 'p'), field(id, 'r'), field(id, 'c'), field(id, 'a'),
    field(id, 'b'), field(id, 'n'), field(id, 'j'), field(id, 'x'),
    field(id, 'd'), field(id, 't'))
end
local function delete_token(id)
  redis.call('HDEL', KEYS[6],
    field(id, 's'), field(id, 'e'), field(id, 'o'), field(id, 'f'),
    field(id, 'p'), field(id, 'v'), field(id, 'r'), field(id, 'c'),
    field(id, 'a'), field(id, 'b'), field(id, 'pc'), field(id, 'k'),
    field(id, 'n'), field(id, 'i'), field(id, 'x'), field(id, 'd'),
    field(id, 'g'), field(id, 'bf'), field(id, 'ih'), field(id, 'lo'),
    field(id, 'le'), field(id, 'cr'))
end
local function terminalize_expired(id, current)
  local token_entry = redis.call('HGET', KEYS[6], field(id, 'e'))
  local token_owner = redis.call('HGET', KEYS[6], field(id, 'o'))
  redis.call('ZREM', KEYS[7], id)
  redis.call('ZREM', KEYS[8], id)
  if token_owner and token_entry and redis.call('HGET', KEYS[5], token_owner) == token_entry then
    redis.call('HDEL', KEYS[5], token_owner)
  end
  if token_entry then
    redis.call('ZREM', KEYS[3], token_entry)
    redis.call('HSET', KEYS[4], field(token_entry, 's'), 'expired')
  end
  redis.call('HSET', KEYS[6], field(id, 's'), 'expired')
  redis.call('HDEL', KEYS[6], field(id, 'lo'), field(id, 'le'), field(id, 'cr'))
  redis.call('ZADD', KEYS[8], current + 60000, id)
end
local status = redis.call('HGET', KEYS[4], field(entry, 's'))
if not status then return {'not_found'} end
if redis.call('HGET', KEYS[4], field(entry, 'o')) ~= owner then return {'owner_mismatch'} end
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local expiry = tonumber(redis.call('HGET', KEYS[4], field(entry, 'x')) or '0')
if status == 'queued' and expiry <= now then
  redis.call('ZREM', KEYS[3], entry)
  if redis.call('HGET', KEYS[5], owner) == entry then
    redis.call('HDEL', KEYS[5], owner)
  end
  redis.call('HSET', KEYS[4], field(entry, 's'), 'expired')
  redis.call('ZADD', KEYS[8], now + 60000, 'entry:' .. entry)
  return {'expired'}
end
if status == 'admitted' then
  local active_token = redis.call('HGET', KEYS[4], field(entry, 't'))
  if active_token then
    local token_expiry = tonumber(redis.call('HGET', KEYS[6], field(active_token, 'x')) or '0')
    if token_expiry <= now then
      terminalize_expired(active_token, now)
      return {'expired'}
    end
  end
end
if claim == 1 then
  if status ~= 'admitted' then return {'not_admitted'} end
  local token = redis.call('HGET', KEYS[4], field(entry, 't'))
  if not token then return {'not_admitted'} end
  if redis.call('HGET', KEYS[6], field(token, 'd')) ~= '0' then return {'already_delivered'} end
  local token_expiry = tonumber(redis.call('HGET', KEYS[6], field(token, 'x')) or '0')
  if token_expiry <= now then return {'expired'} end
  redis.call('HSET', KEYS[6], field(token, 'd'), '1')
  return {'delivery',
    token,
    redis.call('HGET', KEYS[6], field(token, 'k')),
    expected,
    redis.call('HGET', KEYS[4], field(entry, 'p')),
    entry, owner,
    redis.call('HGET', KEYS[4], field(entry, 'f')),
    redis.call('HGET', KEYS[6], field(token, 'i')),
    token_expiry,
    redis.call('HGET', KEYS[6], field(token, 'n'))}
end
local rank = redis.call('ZRANK', KEYS[3], entry)
if not rank then rank = -1 else rank = rank + 1 end
return {'ok', entry,
  redis.call('HGET', KEYS[4], field(entry, 'q')), owner,
  redis.call('HGET', KEYS[4], field(entry, 'f')), status,
  redis.call('HGET', KEYS[4], field(entry, 'p')),
  redis.call('HGET', KEYS[4], field(entry, 'r')),
  redis.call('HGET', KEYS[4], field(entry, 'j')), expiry, rank,
  redis.call('HGET', KEYS[4], field(entry, 'a')),
  redis.call('HGET', KEYS[4], field(entry, 'b')),
  redis.call('HGET', KEYS[4], field(entry, 'n')),
  redis.call('HGET', KEYS[4], field(entry, 'c')), expected,
  redis.call('HGET', KEYS[4], field(entry, 'd')) or ''}
`

const inspectDeliveryScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return {'policy_mismatch'} end
if redis.call('GET', KEYS[2]) ~= expected then return {'continuity_lost'} end
local entry = ARGV[2]
local owner = ARGV[3]
local function field(id, suffix) return id .. '|' .. suffix end
local status = redis.call('HGET', KEYS[3], field(entry, 's'))
if not status then return {'not_found'} end
if redis.call('HGET', KEYS[3], field(entry, 'o')) ~= owner then return {'owner_mismatch'} end
if status == 'expired' then return {'expired'} end
if status ~= 'admitted' then return {'not_admitted'} end
local token = redis.call('HGET', KEYS[3], field(entry, 't'))
if not token then return {'not_admitted'} end
if redis.call('HGET', KEYS[4], field(token, 'd')) ~= '0' then return {'already_delivered'} end
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local token_expiry = tonumber(redis.call('HGET', KEYS[4], field(token, 'x')) or '0')
if token_expiry <= now then return {'expired'} end
return {'delivery',
  token,
  redis.call('HGET', KEYS[4], field(token, 'k')),
  expected,
  redis.call('HGET', KEYS[3], field(entry, 'p')),
  entry, owner,
  redis.call('HGET', KEYS[3], field(entry, 'f')),
  redis.call('HGET', KEYS[4], field(token, 'i')),
  token_expiry,
  redis.call('HGET', KEYS[4], field(token, 'n'))}
`

const cancelScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return 'policy_mismatch' end
if redis.call('GET', KEYS[2]) ~= expected then return 'continuity_lost' end
local entry = ARGV[2]
local owner = ARGV[3]
local function field(id, suffix) return id .. '|' .. suffix end
local function delete_entry(id)
  redis.call('HDEL', KEYS[4],
    field(id, 's'), field(id, 'q'), field(id, 'o'), field(id, 'f'),
    field(id, 'p'), field(id, 'r'), field(id, 'c'), field(id, 'a'),
    field(id, 'b'), field(id, 'n'), field(id, 'j'), field(id, 'x'),
    field(id, 'd'), field(id, 't'))
end
local status = redis.call('HGET', KEYS[4], field(entry, 's'))
if not status then return 'not_found' end
if redis.call('HGET', KEYS[4], field(entry, 'o')) ~= owner then return 'owner_mismatch' end
if status == 'cancelled' or status == 'expired' then return 'terminal' end
local token = redis.call('HGET', KEYS[4], field(entry, 't'))
if token then
  local token_status = redis.call('HGET', KEYS[6], field(token, 's'))
  if token_status == 'processing' then
    redis.call('HSET', KEYS[6], field(token, 'cr'), '1')
    return 'in_progress'
  end
  if token_status == 'consumed' or token_status == 'expired' or token_status == 'cancelled' then
    return 'terminal'
  end
end
redis.call('ZREM', KEYS[3], entry)
if redis.call('HGET', KEYS[5], owner) == entry then
  redis.call('HDEL', KEYS[5], owner)
end
redis.call('HSET', KEYS[4], field(entry, 's'), 'cancelled')
if token then
  redis.call('ZREM', KEYS[7], token)
  redis.call('ZREM', KEYS[8], token)
  redis.call('HSET', KEYS[6], field(token, 's'), 'cancelled')
  redis.call('HDEL', KEYS[6], field(token, 'lo'), field(token, 'le'), field(token, 'cr'))
  local token_expiry = tonumber(redis.call('HGET', KEYS[6], field(token, 'x')) or '0')
  if token_expiry > 0 then redis.call('ZADD', KEYS[8], token_expiry, token) end
else
  redis.call('ZREM', KEYS[8], 'entry:' .. entry)
  delete_entry(entry)
  return 'cancelled_deleted'
end
return 'cancelled'
`

const peekScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return {'policy_mismatch'} end
if redis.call('GET', KEYS[2]) ~= expected then return {'continuity_lost'} end
local limit = tonumber(ARGV[2])
local function field(id, suffix) return id .. '|' .. suffix end
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local ids = redis.call('ZRANGE', KEYS[3], 0, limit * 2 - 1)
local result = {'ok'}
local emitted = 0
for _, entry in ipairs(ids) do
  if emitted >= limit then break end
  local status = redis.call('HGET', KEYS[4], field(entry, 's'))
  local expiry = tonumber(redis.call('HGET', KEYS[4], field(entry, 'x')) or '0')
  if status == 'queued' and expiry > now then
    local rank = redis.call('ZRANK', KEYS[3], entry)
    table.insert(result, entry)
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'q')))
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'o')))
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'f')))
    table.insert(result, status)
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'p')))
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'r')))
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'j')))
    table.insert(result, expiry)
    table.insert(result, rank + 1)
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'a')))
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'b')))
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'n')))
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'c')))
    table.insert(result, expected)
    table.insert(result, redis.call('HGET', KEYS[4], field(entry, 'd')) or '')
    emitted = emitted + 1
  end
end
return result
`

const inspectTokenScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return {'policy_mismatch'} end
if redis.call('GET', KEYS[2]) ~= expected then return {'continuity_lost'} end
local token = ARGV[2]
local function field(id, suffix) return id .. '|' .. suffix end
if redis.call('HEXISTS', KEYS[3], field(token, 's')) == 0 then return {'not_found'} end
return {'token',
  token,
  redis.call('HGET', KEYS[3], field(token, 'k')),
  redis.call('HGET', KEYS[3], field(token, 'v')),
  redis.call('HGET', KEYS[3], field(token, 'p')),
  redis.call('HGET', KEYS[3], field(token, 'e')),
  redis.call('HGET', KEYS[3], field(token, 'o')),
  redis.call('HGET', KEYS[3], field(token, 'f')),
  redis.call('HGET', KEYS[3], field(token, 'i')),
  redis.call('HGET', KEYS[3], field(token, 'x')),
  redis.call('HGET', KEYS[3], field(token, 'n'))}
`

const inspectStateScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[2]) ~= expected then return {'continuity_lost'} end
local entry_cursor = ARGV[2]
local inflight_cursor = ARGV[3]
local lease_cursor = ARGV[4]
local entry_done = tonumber(ARGV[5]) == 1
local inflight_done = tonumber(ARGV[6]) == 1
local lease_done = tonumber(ARGV[7]) == 1
local limit = tonumber(ARGV[8])
local function field(id, suffix) return id .. '|' .. suffix end
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local duplicate_users = 0
local inflight_mismatch = 0
local expired_inflight = 0
local expired_leases = 0
local owner_mismatch = 0

local entry_page = {entry_cursor, {}}
if not entry_done then
  entry_page = redis.call('HSCAN', KEYS[3], entry_cursor, 'MATCH', '*|s', 'COUNT', limit)
end
local entry_values = entry_page[2]
for index=1,#entry_values,2 do
  local status_field = entry_values[index]
  local status = entry_values[index + 1]
  local entry = string.sub(status_field, 1, -3)
  if status == 'queued' then
    local owner = redis.call('HGET', KEYS[3], field(entry, 'o'))
    if owner and redis.call('HGET', KEYS[4], owner) ~= entry then
      duplicate_users = duplicate_users + 1
    end
  elseif status == 'admitted' then
    local owner = redis.call('HGET', KEYS[3], field(entry, 'o'))
    local token = redis.call('HGET', KEYS[3], field(entry, 't'))
    local token_status = token and redis.call('HGET', KEYS[5], field(token, 's')) or false
    if not token or redis.call('HGET', KEYS[5], field(token, 'e')) ~= entry or
       redis.call('HGET', KEYS[5], field(token, 'o')) ~= owner then
      owner_mismatch = owner_mismatch + 1
    elseif token_status == 'issued' or token_status == 'processing' then
      if owner and redis.call('HGET', KEYS[4], owner) ~= entry then
        duplicate_users = duplicate_users + 1
      end
      if not redis.call('ZSCORE', KEYS[6], token) then
        inflight_mismatch = inflight_mismatch + 1
      end
    elseif token_status == 'consumed' or token_status == 'cancelled' or token_status == 'expired' then
      if owner and redis.call('HGET', KEYS[4], owner) == entry then
        duplicate_users = duplicate_users + 1
      end
    else
      owner_mismatch = owner_mismatch + 1
    end
  end
end

local inflight_page = {inflight_cursor, {}}
if not inflight_done then
  inflight_page = redis.call('ZSCAN', KEYS[6], inflight_cursor, 'COUNT', limit)
end
local inflight_values = inflight_page[2]
for index=1,#inflight_values,2 do
  local token = inflight_values[index]
  local score = tonumber(inflight_values[index + 1])
  local status = redis.call('HGET', KEYS[5], field(token, 's'))
  local expires = tonumber(redis.call('HGET', KEYS[5], field(token, 'x')) or '-1')
  if (status ~= 'issued' and status ~= 'processing') or expires ~= score then
    inflight_mismatch = inflight_mismatch + 1
  end
  if score <= now then expired_inflight = expired_inflight + 1 end
end

local lease_page = {lease_cursor, {}}
if not lease_done then
  lease_page = redis.call('ZSCAN', KEYS[7], lease_cursor, 'COUNT', limit)
end
local lease_values = lease_page[2]
for index=1,#lease_values,2 do
  local token = lease_values[index]
  local score = tonumber(lease_values[index + 1])
  if score <= now and redis.call('HGET', KEYS[5], field(token, 's')) == 'processing' then
    expired_leases = expired_leases + 1
  end
end
return {'ok', duplicate_users, inflight_mismatch, expired_inflight, expired_leases,
  owner_mismatch, entry_page[1], inflight_page[1], lease_page[1]}
`

const issueScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return {'policy_mismatch'} end
if redis.call('GET', KEYS[2]) ~= expected then return {'continuity_lost'} end
local rate_limit = tonumber(ARGV[2])
local inflight_limit = tonumber(ARGV[3])
local token_ttl = tonumber(ARGV[4])
local generation_ttl = tonumber(ARGV[5])
local requested = tonumber(ARGV[6])
local cleanup_limit = tonumber(ARGV[7])
local function field(id, suffix) return id .. '|' .. suffix end
local function delete_entry(id)
  redis.call('HDEL', KEYS[4],
    field(id, 's'), field(id, 'q'), field(id, 'o'), field(id, 'f'),
    field(id, 'p'), field(id, 'r'), field(id, 'c'), field(id, 'a'),
    field(id, 'b'), field(id, 'n'), field(id, 'j'), field(id, 'x'),
    field(id, 'd'), field(id, 't'))
end
local function delete_token(id)
  redis.call('HDEL', KEYS[6],
    field(id, 's'), field(id, 'e'), field(id, 'o'), field(id, 'f'),
    field(id, 'p'), field(id, 'v'), field(id, 'r'), field(id, 'c'),
    field(id, 'a'), field(id, 'b'), field(id, 'pc'), field(id, 'k'),
    field(id, 'n'), field(id, 'i'), field(id, 'x'), field(id, 'd'),
    field(id, 'g'), field(id, 'bf'), field(id, 'ih'), field(id, 'lo'),
    field(id, 'le'), field(id, 'cr'))
end
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local expired_retention = math.max(token_ttl, 60000)
local recovered_count = 0
local expired_count = 0
local expired_entry_count = 0
local function terminalize_entry_expired(entry)
  local owner = redis.call('HGET', KEYS[4], field(entry, 'o'))
  redis.call('ZREM', KEYS[3], entry)
  if owner and redis.call('HGET', KEYS[5], owner) == entry then
    redis.call('HDEL', KEYS[5], owner)
  end
  redis.call('HSET', KEYS[4], field(entry, 's'), 'expired')
  redis.call('ZADD', KEYS[9], now + 60000, 'entry:' .. entry)
end
local function delete_token_state(token)
  local entry = redis.call('HGET', KEYS[6], field(token, 'e'))
  local owner = redis.call('HGET', KEYS[6], field(token, 'o'))
  if owner and entry and redis.call('HGET', KEYS[5], owner) == entry then
    redis.call('HDEL', KEYS[5], owner)
  end
  if entry then
    redis.call('ZREM', KEYS[3], entry)
    delete_entry(entry)
  end
  redis.call('ZREM', KEYS[7], token)
  redis.call('ZREM', KEYS[9], token)
  delete_token(token)
end
local function terminalize_cancelled(token)
  local entry = redis.call('HGET', KEYS[6], field(token, 'e'))
  local owner = redis.call('HGET', KEYS[6], field(token, 'o'))
  local token_expiry = tonumber(redis.call('HGET', KEYS[6], field(token, 'x')) or '0')
  redis.call('ZREM', KEYS[7], token)
  redis.call('ZREM', KEYS[9], token)
  if owner and entry and redis.call('HGET', KEYS[5], owner) == entry then
    redis.call('HDEL', KEYS[5], owner)
  end
  if entry then
    redis.call('ZREM', KEYS[3], entry)
    redis.call('HSET', KEYS[4], field(entry, 's'), 'cancelled')
  end
  redis.call('HSET', KEYS[6], field(token, 's'), 'cancelled')
  redis.call('HDEL', KEYS[6], field(token, 'lo'), field(token, 'le'), field(token, 'cr'))
  if token_expiry > now then
    redis.call('ZADD', KEYS[9], token_expiry, token)
  else
    delete_token_state(token)
  end
end
local function terminalize_expired(token)
  local entry = redis.call('HGET', KEYS[6], field(token, 'e'))
  local owner = redis.call('HGET', KEYS[6], field(token, 'o'))
  redis.call('ZREM', KEYS[7], token)
  redis.call('ZREM', KEYS[9], token)
  if owner and entry and redis.call('HGET', KEYS[5], owner) == entry then
    redis.call('HDEL', KEYS[5], owner)
  end
  if entry then
    redis.call('ZREM', KEYS[3], entry)
    redis.call('HSET', KEYS[4], field(entry, 's'), 'expired')
  end
  redis.call('HSET', KEYS[6], field(token, 's'), 'expired')
  redis.call('HDEL', KEYS[6], field(token, 'lo'), field(token, 'le'), field(token, 'cr'))
  redis.call('ZADD', KEYS[9], now + expired_retention, token)
end
local expired_leases = redis.call('ZRANGEBYSCORE', KEYS[9], '-inf', now, 'LIMIT', 0, cleanup_limit)
for _, member in ipairs(expired_leases) do
  redis.call('ZREM', KEYS[9], member)
  if string.sub(member, 1, 6) == 'entry:' then
    local entry = string.sub(member, 7)
    local owner = redis.call('HGET', KEYS[4], field(entry, 'o'))
    if owner and redis.call('HGET', KEYS[5], owner) == entry then
      redis.call('HDEL', KEYS[5], owner)
    end
    redis.call('ZREM', KEYS[3], entry)
    delete_entry(entry)
  else
    local status = redis.call('HGET', KEYS[6], field(member, 's'))
    if status == 'processing' then
      local token_expiry = tonumber(redis.call('HGET', KEYS[6], field(member, 'x')) or '0')
      if token_expiry <= now then
        expired_count = expired_count + 1
        terminalize_expired(member)
      elseif redis.call('HGET', KEYS[6], field(member, 'cr')) == '1' then
        terminalize_cancelled(member)
      else
        redis.call('HSET', KEYS[6], field(member, 's'), 'issued')
        redis.call('HDEL', KEYS[6], field(member, 'lo'), field(member, 'le'))
        recovered_count = recovered_count + 1
      end
    elseif status == 'consumed' or status == 'cancelled' or status == 'expired' then
      delete_token_state(member)
    end
  end
end
local expired_tokens = redis.call('ZRANGEBYSCORE', KEYS[7], '-inf', now, 'LIMIT', 0, cleanup_limit)
for _, token in ipairs(expired_tokens) do
  local status = redis.call('HGET', KEYS[6], field(token, 's'))
  if status == 'issued' or status == 'processing' then
    expired_count = expired_count + 1
    terminalize_expired(token)
  else
    delete_token_state(token)
  end
end
local queue_head = redis.call('ZRANGE', KEYS[3], 0, cleanup_limit - 1)
for _, entry in ipairs(queue_head) do
  local status = redis.call('HGET', KEYS[4], field(entry, 's'))
  local entry_expiry = tonumber(redis.call('HGET', KEYS[4], field(entry, 'x')) or '0')
  if status == 'queued' and entry_expiry <= now then
    terminalize_entry_expired(entry)
    expired_entry_count = expired_entry_count + 1
  end
end
redis.call('ZREMRANGEBYSCORE', KEYS[8], '-inf', now - 1000)
local rate_remaining = rate_limit - redis.call('ZCARD', KEYS[8])
local inflight_remaining = inflight_limit - redis.call('ZCARD', KEYS[7])
local capacity = math.min(requested, rate_remaining, inflight_remaining)
if capacity <= 0 then
  for i=1,#KEYS do redis.call('PEXPIRE', KEYS[i], generation_ttl) end
  return {'ok', recovered_count, expired_count, expired_entry_count}
end
local queue = redis.call('ZRANGE', KEYS[3], 0, capacity - 1)
local result = {'ok', recovered_count, expired_count, expired_entry_count}
for index, entry in ipairs(queue) do
  local offset = 8 + (index - 1) * 8
  local candidate_entry = ARGV[offset]
  if candidate_entry ~= entry then break end
  local token = ARGV[offset + 1]
  local kid = ARGV[offset + 2]
  local nonce = ARGV[offset + 3]
  local issued_at = tonumber(ARGV[offset + 4])
  local expires_at = tonumber(ARGV[offset + 5])
  local candidate_owner = ARGV[offset + 6]
  local candidate_fingerprint = ARGV[offset + 7]
  local status = redis.call('HGET', KEYS[4], field(entry, 's'))
  local entry_expiry = tonumber(redis.call('HGET', KEYS[4], field(entry, 'x')) or '0')
  if status == 'queued' and entry_expiry <= now then
    terminalize_entry_expired(entry)
    expired_entry_count = expired_entry_count + 1
    result[4] = expired_entry_count
  elseif status == 'queued' and entry_expiry > now and
      issued_at <= now + 5000 and
      expires_at > now and expires_at - issued_at <= token_ttl + 1000 and
      candidate_owner == redis.call('HGET', KEYS[4], field(entry, 'o')) and
      candidate_fingerprint == redis.call('HGET', KEYS[4], field(entry, 'f')) and
      redis.call('HEXISTS', KEYS[6], field(token, 's')) == 0 then
    local entry_retention = entry_expiry
    if expires_at > entry_retention then entry_retention = expires_at end
    redis.call('HSET', KEYS[6],
      field(token, 's'), 'issued', field(token, 'e'), entry,
      field(token, 'o'), redis.call('HGET', KEYS[4], field(entry, 'o')),
      field(token, 'f'), redis.call('HGET', KEYS[4], field(entry, 'f')),
      field(token, 'p'), redis.call('HGET', KEYS[4], field(entry, 'p')),
      field(token, 'v'), expected,
      field(token, 'r'), redis.call('HGET', KEYS[4], field(entry, 'r')),
      field(token, 'c'), redis.call('HGET', KEYS[4], field(entry, 'c')),
      field(token, 'a'), redis.call('HGET', KEYS[4], field(entry, 'a')),
      field(token, 'b'), redis.call('HGET', KEYS[4], field(entry, 'b')),
      field(token, 'pc'), redis.call('HGET', KEYS[4], field(entry, 'n')),
      field(token, 'k'), kid, field(token, 'n'), nonce,
      field(token, 'i'), issued_at, field(token, 'x'), expires_at,
      field(token, 'd'), '0', field(token, 'g'), '0')
    redis.call('HSET', KEYS[4], field(entry, 's'), 'admitted', field(entry, 't'), token,
      field(entry, 'd'), now, field(entry, 'x'), entry_retention)
    redis.call('ZREM', KEYS[3], entry)
    redis.call('ZADD', KEYS[7], expires_at, token)
    redis.call('ZADD', KEYS[8], now, token)
    table.insert(result, entry)
  end
end
for i=1,#KEYS do redis.call('PEXPIRE', KEYS[i], generation_ttl) end
return result
`

const acquireScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return {'policy_mismatch'} end
if redis.call('GET', KEYS[2]) ~= expected then return {'continuity_lost'} end
local token = ARGV[2]
local owner = ARGV[3]
local admission_fingerprint = ARGV[4]
local booking_fingerprint = ARGV[5]
local idempotency_hash = ARGV[6]
local function field(id, suffix) return id .. '|' .. suffix end
local function expire_token(id, current)
  local entry = redis.call('HGET', KEYS[3], field(id, 'e'))
  local entry_owner = redis.call('HGET', KEYS[3], field(id, 'o'))
  redis.call('ZREM', KEYS[4], id)
  redis.call('ZREM', KEYS[5], id)
  if entry_owner and entry and redis.call('HGET', KEYS[7], entry_owner) == entry then
    redis.call('HDEL', KEYS[7], entry_owner)
  end
  if entry then
    redis.call('HSET', KEYS[6], field(entry, 's'), 'expired')
  end
  redis.call('HSET', KEYS[3], field(id, 's'), 'expired')
  redis.call('HDEL', KEYS[3], field(id, 'lo'), field(id, 'le'), field(id, 'cr'))
  redis.call('ZADD', KEYS[5], current + 60000, id)
end
local function cancel_token(id, current)
  local entry = redis.call('HGET', KEYS[3], field(id, 'e'))
  local entry_owner = redis.call('HGET', KEYS[3], field(id, 'o'))
  local expires = tonumber(redis.call('HGET', KEYS[3], field(id, 'x')) or '0')
  redis.call('ZREM', KEYS[4], id)
  redis.call('ZREM', KEYS[5], id)
  if entry_owner and entry and redis.call('HGET', KEYS[7], entry_owner) == entry then
    redis.call('HDEL', KEYS[7], entry_owner)
  end
  if entry then
    redis.call('HSET', KEYS[6], field(entry, 's'), 'cancelled')
  end
  redis.call('HSET', KEYS[3], field(id, 's'), 'cancelled')
  redis.call('HDEL', KEYS[3], field(id, 'lo'), field(id, 'le'), field(id, 'cr'))
  if expires <= current then expires = current + 60000 end
  redis.call('ZADD', KEYS[5], expires, id)
end
local status = redis.call('HGET', KEYS[3], field(token, 's'))
if not status then return {'not_found'} end
if redis.call('HGET', KEYS[3], field(token, 'o')) ~= owner or
   redis.call('HGET', KEYS[3], field(token, 'f')) ~= admission_fingerprint or
   redis.call('HGET', KEYS[3], field(token, 'a')) ~= ARGV[7] or
   redis.call('HGET', KEYS[3], field(token, 'b')) ~= ARGV[8] or
   redis.call('HGET', KEYS[3], field(token, 'c')) ~= ARGV[9] or
   redis.call('HGET', KEYS[3], field(token, 'pc')) ~= ARGV[10] then return {'mismatch'} end
local existing_booking = redis.call('HGET', KEYS[3], field(token, 'bf'))
local existing_idempotency = redis.call('HGET', KEYS[3], field(token, 'ih'))
if existing_booking and (existing_booking ~= booking_fingerprint or existing_idempotency ~= idempotency_hash) then
  return {'mismatch'}
end
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local expires = tonumber(redis.call('HGET', KEYS[3], field(token, 'x')) or '0')
if status == 'processing' then
  local lease_expires = tonumber(redis.call('HGET', KEYS[3], field(token, 'le')) or '0')
  if lease_expires > now then return {'retry_allowed', lease_expires - now} end
  if redis.call('HGET', KEYS[3], field(token, 'cr')) == '1' then
    cancel_token(token, now)
    return {'terminal'}
  end
  status = 'issued'
  redis.call('HSET', KEYS[3], field(token, 's'), 'issued')
  redis.call('HDEL', KEYS[3], field(token, 'lo'), field(token, 'le'))
  redis.call('ZREM', KEYS[5], token)
end
if status == 'consumed' then
  if expires <= now then
    expire_token(token, now)
    return {'expired'}
  end
  return {'replay_allowed'}
end
if status ~= 'issued' then return {'terminal'} end
local processing_lease = tonumber(ARGV[12])
if expires - now < processing_lease then
  expire_token(token, now)
  return {'expired'}
end
if not existing_booking then
  redis.call('HSET', KEYS[3], field(token, 'bf'), booking_fingerprint, field(token, 'ih'), idempotency_hash)
end
local generation = redis.call('HINCRBY', KEYS[3], field(token, 'g'), 1)
local lease_expires = now + tonumber(ARGV[12])
redis.call('HSET', KEYS[3], field(token, 's'), 'processing',
  field(token, 'lo'), ARGV[11], field(token, 'le'), lease_expires)
redis.call('ZADD', KEYS[5], lease_expires, token)
return {'acquired', ARGV[11], generation}
`

const releaseScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return 'policy_mismatch' end
if redis.call('GET', KEYS[2]) ~= expected then return 'continuity_lost' end
local token = ARGV[2]
local function field(id, suffix) return id .. '|' .. suffix end
local status = redis.call('HGET', KEYS[3], field(token, 's'))
if not status then return 'not_found' end
if redis.call('HGET', KEYS[3], field(token, 'o')) ~= ARGV[3] or
   redis.call('HGET', KEYS[3], field(token, 'bf')) ~= ARGV[4] or
   redis.call('HGET', KEYS[3], field(token, 'ih')) ~= ARGV[5] then return 'mismatch' end
if status ~= 'processing' or redis.call('HGET', KEYS[3], field(token, 'lo')) ~= ARGV[6] or
   redis.call('HGET', KEYS[3], field(token, 'g')) ~= ARGV[7] then return 'stale_lease' end
redis.call('ZREM', KEYS[5], token)
local cancel_requested = redis.call('HGET', KEYS[3], field(token, 'cr')) == '1'
if tonumber(ARGV[8]) == 1 or cancel_requested then
  local entry = redis.call('HGET', KEYS[3], field(token, 'e'))
  local entry_owner = redis.call('HGET', KEYS[3], field(token, 'o'))
  local expires = tonumber(redis.call('HGET', KEYS[3], field(token, 'x')) or '0')
  local now_parts = redis.call('TIME')
  local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
  redis.call('ZREM', KEYS[4], token)
  if entry_owner and entry and redis.call('HGET', KEYS[7], entry_owner) == entry then
    redis.call('HDEL', KEYS[7], entry_owner)
  end
  if entry then redis.call('HSET', KEYS[6], field(entry, 's'), 'cancelled') end
  redis.call('HSET', KEYS[3], field(token, 's'), 'cancelled')
  redis.call('HDEL', KEYS[3], field(token, 'lo'), field(token, 'le'), field(token, 'cr'))
  if expires <= now then expires = now + 60000 end
  redis.call('ZADD', KEYS[5], expires, token)
  return 'cancelled'
else
  redis.call('HSET', KEYS[3], field(token, 's'), 'issued')
  redis.call('HDEL', KEYS[3], field(token, 'lo'), field(token, 'le'), field(token, 'cr'))
end
return 'released'
`

const finalizeScript = `
local expected = tostring(ARGV[1])
if redis.call('GET', KEYS[1]) ~= expected then return 'policy_mismatch' end
if redis.call('GET', KEYS[2]) ~= expected then return 'continuity_lost' end
local token = ARGV[2]
local function field(id, suffix) return id .. '|' .. suffix end
local status = redis.call('HGET', KEYS[3], field(token, 's'))
if not status then return 'not_found' end
if redis.call('HGET', KEYS[3], field(token, 'o')) ~= ARGV[3] or
   redis.call('HGET', KEYS[3], field(token, 'bf')) ~= ARGV[4] or
   redis.call('HGET', KEYS[3], field(token, 'ih')) ~= ARGV[5] then return 'mismatch' end
if status ~= 'processing' or redis.call('HGET', KEYS[3], field(token, 'lo')) ~= ARGV[6] or
   redis.call('HGET', KEYS[3], field(token, 'g')) ~= ARGV[7] then return 'stale_lease' end
local entry = redis.call('HGET', KEYS[3], field(token, 'e'))
local entry_owner = redis.call('HGET', KEYS[3], field(token, 'o'))
local expires = tonumber(redis.call('HGET', KEYS[3], field(token, 'x')) or '0')
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
redis.call('ZREM', KEYS[4], token)
redis.call('ZREM', KEYS[5], token)
if entry_owner and entry and redis.call('HGET', KEYS[7], entry_owner) == entry then
  redis.call('HDEL', KEYS[7], entry_owner)
end
redis.call('HSET', KEYS[3], field(token, 's'), 'consumed')
redis.call('HDEL', KEYS[3], field(token, 'lo'), field(token, 'le'), field(token, 'cr'))
if expires <= now then expires = now + 60000 end
redis.call('ZADD', KEYS[5], expires, token)
return 'finalized'
`
