# ADR 012: Redis Owns Bounded Waiting-Room State

- Status: Accepted
- Date: 2026-07-18

## Context

A hot train can receive a burst far larger than the reservation write path should execute concurrently. The system needs a bounded, shared, approximately observable queue across API replicas without turning ephemeral queue churn into authoritative PostgreSQL inventory writes.

Duplicate joins, capacity checks, fairness ordering, cancellation, expiry, and admission must agree atomically. They cannot be assembled from client-side check-then-mutate commands.

## Decision

Redis owns ephemeral waiting-room and admission-token control-plane state. PostgreSQL owns the durable policy and every booking invariant. Waiting-room operations never read or mutate `seat_inventory`.

The Admission domain defines:

- `WaitingRoomEntry`;
- `AdmissionToken`;
- `AdmissionPolicy`;
- `AdmissionDecision`; and
- `QueuePosition`.

Waiting-room entry states are `queued`, `admitted`, `expired`, and `cancelled`. Logical entry metadata contains an opaque server-generated entry ID, owner, train run, seat class, ordered stop indexes, passenger count, versioned admission fingerprint, monotonic sequence, status, and Redis-time-derived join, expiry, and admission instants.

Token states are `issued`, `processing`, `consumed`, `expired`, and `cancelled`. Token binding and lifecycle are defined by ADRs 013 and 017.

Every key used by one atomic script is produced by one validated key builder. Its hash tag is:

```text
{canonical-train-run-uuid|seat-class-enum}
```

Generation-scoped keys use a server-read PostgreSQL policy version:

```text
wr:{scope}:policy-version
wr:{scope}:v{version}:queue
wr:{scope}:v{version}:sequence
wr:{scope}:v{version}:entries
wr:{scope}:v{version}:users
wr:{scope}:v{version}:tokens
wr:{scope}:v{version}:inflight
wr:{scope}:v{version}:rate
```

The train-run value is a parsed canonical UUID, seat class is a domain enumeration, and version is a bounded positive integer. Raw user input, user IDs, station codes, tokens, token hashes, passenger IDs, and idempotency keys never appear in Redis key names.

The queue is a sorted set scored by a Redis-generated monotonic integer sequence. Entry metadata and user-to-entry ownership mappings are hashes. Token and inflight/rate indexes remain in the same Cluster slot. Scripts receive exact keys. Maintenance uses bounded `SCAN`; `KEYS` is prohibited.

One join Lua script:

1. verifies the current enabled policy generation;
2. uses Redis `TIME`, never a client timestamp;
3. checks the active user mapping;
4. returns the existing entry for the same admission fingerprint;
5. returns conflict for a changed fingerprint;
6. expires stale active mappings;
7. checks queue capacity;
8. increments the sequence;
9. inserts the queue member and metadata;
10. records the owner mapping; and
11. applies bounded TTLs to every generation key.

The versioned admission fingerprint canonicalizes train-run ID, ordered origin and destination stop indexes, seat class, and passenger count. JWT identity remains a separate ownership field. Passenger IDs and booking idempotency are intentionally absent at join and bind on first token acquire under ADR 017.

Fairness is first-come order within one policy generation, as observed by the atomic Redis sequence. It does not promise cross-policy, cross-region, or network-arrival fairness. Expired or cancelled entries are skipped. Approximate position is computed from bounded sorted-set rank and can change immediately because of cancellation, expiry, or admission.

Cancellation and status operations validate JWT-derived ownership inside their atomic script. Cancellation removes the entry from the queue and active user mapping and marks bounded terminal metadata. A terminal entry cannot be admitted.

All state has a TTL derived from the durable policy and a small documented cleanup margin. Old policy generations expire naturally. Complete Redis loss can lose queue order, entries, and tokens, but cannot alter a PostgreSQL seat mask or durable reservation.

## Consequences

- Duplicate identical joins converge on one stable active entry across replicas.
- A changed join for the same user and policy conflicts rather than silently replacing intent.
- Queue capacity and ordering are atomic and bounded.
- All keys required by one script share one Redis Cluster slot.
- Position is intentionally approximate and must not be presented as a guaranteed place or wait time.
- PostgreSQL does not receive one write per queue join and remains the seat authority.

## Rejected alternatives

- PostgreSQL row per queue entry: rejected because ephemeral burst traffic would contend with the authoritative booking store.
- In-process queues: rejected because capacity and ordering would fragment across replicas and disappear on termination.
- Client timestamps as scores: rejected because skew and retries can reorder customers.
- One key per arbitrary request value: rejected because it creates injection, privacy, and cardinality risks.
- Redis inventory counters: rejected because admission state cannot authoritatively prevent segment overlap.
