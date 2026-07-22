# ADR 016: Redis Lua Scripts Enforce Admission Limits Across Replicas

- Status: Accepted
- Date: 2026-07-18

## Context

Milestone 2 runs multiple stateless API and admission-worker replicas. Per-process mutexes, semaphores, and tickers cannot provide a policy-wide queue order, admission rate, or inflight limit. A check followed by a separate mutation would also allow two workers to admit the same entry or exceed capacity.

The limits are global to one hot-train policy, identified by train run and seat class. They are not a claim of fairness between policies, regions, or all customers on the platform.

## Decision

Redis Lua scripts are the atomic adapter for queue joins, queue cancellation, admission issuance, token acquisition, lease recovery, token release, and token finalization. Every state read needed for a decision and every corresponding mutation occur in the same script invocation. Workers do not acquire a separate distributed lock.

All keys touched by one script share this Redis Cluster-compatible hash tag:

```text
{canonical-train-run-uuid|seat-class-enum}
```

The train-run component is a server-validated canonical UUID and the seat-class component is a bounded domain enumeration. Arbitrary request values never become key components. Policy-scoped keys cover the queue, monotonic sequence, entries, user mapping, tokens, inflight expiries, rate window, and policy version. Scripts receive exact keys; operational traversal uses bounded `SCAN`, never `KEYS`.

Queue join uses an atomic monotonic sequence as the sorted-set score. The sequence, not an API clock, establishes first-come ordering within a policy. Duplicate detection, capacity checking, sequence allocation, metadata storage, user mapping, and TTL application happen together.

Admission workers load at most 100 enabled policies per pass from PostgreSQL using an immutable-policy-ID keyset cursor. Each worker rotates its in-memory cursor to the beginning after reaching the tail; it never uses an unbounded list or an `OFFSET` scan. Concurrent `RunOnce` calls on the same worker instance are serialized so cursor advancement and per-policy side effects remain deterministic. Replicas keep independent cursors because Redis, not cursor coordination, enforces the policy-wide limits.

For each selected policy the worker passes a bounded batch of cryptographically generated token candidates with SHA-256 bearer commitments, nonces, and immutable claims to the policy script. Redis never receives an issuance MAC; API and worker processes recompute it from their keyring. For each policy the script:

1. validates the expected enabled policy version and continuity sentinel;
2. reads Redis `TIME` as the common time source;
3. reclaims expired token and inflight records and safely recovers expired processing leases;
4. trims the sliding admission-rate window;
5. computes the remaining rate, inflight, and batch capacity;
6. selects the earliest still-queued, unexpired entries;
7. creates each token hash, immutable signed issuance record, and bounded metadata;
8. marks each selected entry admitted;
9. records the token in the inflight expiry index; and
10. records issuance in the rate window.

The script admits no more than the minimum remaining capacity across the configured batch size, `admission_rate_per_second`, and `max_inflight_admissions`. Redis `TIME` prevents replica clock skew from changing rate windows or expiry decisions. A unique server-generated member makes issuance records distinct within the sliding window.

An entry changes from `queued` to `admitted` in the same invocation that creates its one token record. Consequently, concurrent workers cannot both select it. Token consume, cancel, expiry, and permanent-release scripts remove inflight capacity atomically. An expired processing lease may return a still-valid token to `issued`; it does not create a second token or a second inflight member. Lease release and finalization compare a monotonically increasing lease generation so an obsolete API request cannot change a reclaimed token.

A worker crash before the issuance script has no effect. A crash after the script leaves shared Redis state that another worker or API replica can observe and recover. No correctness rule depends only on an in-process ticker, local queue, sticky session, or replica identity.

Failure for one policy is bounded and does not stop a worker from processing other policies. Policy scripts never modify PostgreSQL seat inventory. Admission permits a booking attempt; PostgreSQL can still reject the attempt because inventory has changed.

The admission worker is another executable role over the modular monolith's shared admission modules. Its application interface is `RunOnce(ctx)` and its Redis and PostgreSQL adapters remain behind existing module seams. This deployment role does not create a network microservice interface.

Worker lifecycle metrics expose bounded success/failure pass counts, pass duration, and the last successful pass timestamp. They intentionally carry no policy, train-run, customer, queue-entry, or token identifiers.

## Consequences

- Queue order, issuance rate, and inflight capacity are consistent across API and worker replicas for one policy.
- Redis Cluster can place every atomic policy operation in one slot without cross-slot scripting.
- Limits survive individual API or worker termination while Redis remains available.
- Policy throughput is deliberately bounded by one Redis slot per train-run/seat-class policy.
- Tests can exercise the deep `RunOnce` and script interfaces with concurrent replicas without relying on arbitrary sleeps.
- Cross-policy or global fairness is outside the guarantee.

## Rejected alternatives

- Per-worker tickers or semaphores: rejected because each replica would enforce only a fraction of the shared limit.
- Check-then-mutate Redis commands: rejected because concurrent workers could oversubscribe or double-admit.
- A Redis distributed lock around client-side logic: rejected because lock expiry and partial client failure create a larger, shallower interface than one atomic script.
- Cross-slot policy keys: rejected because Redis Cluster cannot atomically execute the required scripts over them.
- Client timestamps for ordering or rate enforcement: rejected because replica skew can violate fairness and configured rate.
- Extracting admission into a microservice: rejected because a network interface adds failure modes without improving the Redis atomicity or PostgreSQL authority invariants.
