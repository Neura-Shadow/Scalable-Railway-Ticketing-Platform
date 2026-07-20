# Waiting-Room Design

Milestone 2 adds a bounded Redis control plane in front of selected hot
train-run and seat-class combinations. PostgreSQL remains the only authority
for reservations, durable idempotency, quotas, and segment seat allocation.
Admission permits a booking attempt; it does not reserve or guarantee a seat.

## Scope and state

An enabled PostgreSQL `hot_train_policies` row defines one policy generation.
The Redis key builder accepts only a canonical train-run UUID, the bounded
`standard`, `business`, or `first` seat-class enum, and a positive bounded
policy version. It produces one Redis Cluster hash tag:

```text
{canonical-train-run-uuid|seat-class}
```

Generation-scoped keys are:

```text
railway:wr:{scope}:policy-version
railway:wr:{scope}:v{version}:continuity
railway:wr:{scope}:v{version}:queue
railway:wr:{scope}:v{version}:sequence
railway:wr:{scope}:v{version}:entries
railway:wr:{scope}:v{version}:users
railway:wr:{scope}:v{version}:tokens
railway:wr:{scope}:v{version}:inflight
railway:wr:{scope}:v{version}:rate
railway:wr:{scope}:v{version}:leases
```

An exact, server-generated entry locator is outside a multi-key script and
contains only a canonical entry UUID. Arbitrary station input, user IDs,
passenger IDs, tokens, token hashes, and idempotency keys never appear in key
names. Scripts receive exact keys; production code must not use Redis `KEYS`.
Bounded maintenance may use `SCAN`.

## Atomic join and fairness

One Lua invocation verifies the policy version and continuity marker, reads
Redis `TIME`, removes a bounded stale prefix, checks the user mapping, enforces
capacity, increments a monotonic sequence, inserts the sorted-set member,
stores metadata, and refreshes bounded TTLs.

The admission fingerprint binds train run, ordered stop indexes, seat class,
and passenger count. For one authenticated owner and policy:

- the same fingerprint returns the existing active entry;
- a different fingerprint conflicts;
- no second active entry is created;
- cancelled or expired entries cannot be admitted; and
- queue capacity is checked atomically after bounded stale cleanup.

Fairness means FIFO by the sequence assigned by Redis for one policy
generation. It is not fairness across policies, train runs, Redis deployments,
regions, network arrival times, or identities controlled by one actor. Queue
position is a current sorted-set rank and therefore approximate: cancellation,
expiry, and admission can change it immediately.

## Entry lifecycle and ownership

Entry states are:

```text
queued -> admitted
queued -> expired (short bounded tombstone)
queued -> cancelled (immediate entry and locator deletion)
admitted -> expired
admitted -> cancelled (until the token is terminal)
```

All status and cancellation operations compare the JWT-derived owner hash
inside the Redis script. Entry IDs are opaque server UUIDs. The status response
uses `Cache-Control: no-store, private`. When an admitted credential is
available, the raw credential is returned once in `X-Admission-Token`, never in
the JSON body. A queued cancellation physically deletes its hash fields,
queue/user/cleanup members, and then exact-UNLINKs its external locator under a
bounded best-effort cleanup context. If that second command is interrupted, the
committed cancellation remains authoritative, a bounded metric marks cleanup
pending, and the locator retains only its original finite TTL.

## Capacity and failure

`max_queue_size` is a hard per-policy queue bound. The waiting room is the
queue; API instances do not add a hidden in-memory queue. Admission rate and
inflight admission count are separate global policy bounds enforced by the
issuance script. API booking concurrency is a separate local instance bound.

An enabled policy requires Redis for join, status, cancellation, issuance, and
token operations. Redis timeout, missing continuity state, or version mismatch
fails closed with a bounded `503` and `Retry-After`; the API never bypasses the
queue. A newly created durable generation can be installed deliberately. A
missing marker for a generation already initialized in PostgreSQL is a
continuity incident and is not silently recreated.

Redis AOF or equivalent managed persistence is required, but total Redis loss
can still lose order, entries, and tokens. It cannot change PostgreSQL seat
inventory or create a reservation.

## Customer surface

```text
POST   /api/v1/waiting-room/entries
GET    /api/v1/waiting-room/entries/:id
DELETE /api/v1/waiting-room/entries/:id
```

Join accepts train-run ID, origin/destination station code, seat class, and
passenger count. Identity comes only from the validated JWT. Responses expose
entry ID, state, timestamps, approximate position, and bounded retry guidance.

See ADR 012 for the decision record and
[admission-token-lifecycle.md](admission-token-lifecycle.md) for the credential
state machine.
