# Multi-Replica Admission Design

`docker-compose.multi-replica.yml` is a local production-like evidence
topology, not a production deployment manifest:

```text
client
  -> nginx (round robin, no sticky sessions)
       -> api-1
       -> api-2
       -> api-3

admission-worker-1 ----\
admission-worker-2 -----+-> shared Redis (AOF every second)
api replicas ----------/              |
                                      + control-plane state only

all API/workers -> one PostgreSQL primary (durable authority)
hold-expirer ---/
outbox-worker --/
```

The APIs and workers are stateless with respect to queue and token state. Every
replica reads the same durable policy rows and uses the same Redis policy
generation. The bounded evidence Nginx uses deterministic round robin and sets
no affinity cookie or hash rule, so every healthy replica remains observable
without depending on transient connection counts. The local evidence proxy
emits `X-Upstream-Addr` so k6 can demonstrate distribution; production ingress
must not expose internal topology.

## Global limits

One Lua issuance operation uses Redis `TIME` and keys in a single Cluster hash
slot to:

- reclaim a bounded set of expired processing leases and tokens;
- trim the global one-second rate window;
- calculate remaining rate and inflight capacity;
- select the earliest eligible queue entries;
- issue each entry no more than once; and
- update entry, token, rate, inflight, and lease state atomically.

Worker tick frequency is not an authority. Starting two workers can increase
availability and scheduling frequency but cannot increase the policy's Redis
rate or inflight limit. Batches and cleanup work are bounded.

API replicas verify and acquire the same token record atomically. Durable
PostgreSQL idempotency and quotas serialize the corresponding booking result
across API replicas. PostgreSQL remains the only seat authority.

## Failure experiments

The evidence plan must validate:

1. request responses contain at least two and preferably all three bounded
   `X-Upstream-Addr` values;
2. duplicate identical joins through different APIs return one entry;
3. queue and token state is shared;
4. two workers do not double-issue and remain within rate/inflight bounds;
5. terminating one API leaves other replicas usable and preserves the shared
   queue entry;
6. a deterministic request routed directly to `api-1`, while PostgreSQL proves
   it is blocked inside the hot reservation transaction, rolls back completely
   when that replica is terminated;
7. after bounded Redis processing-lease recovery, the exact same token,
   idempotency identity, owner, and request can commit once through the shared
   topology and then replay the same durable reservation without duplicate
   seat, idempotency, or outbox state;
8. terminating one worker allows lease recovery and does not double-admit; and
9. stopping Redis makes enabled-hot admission fail closed while a clean
   non-hot reservation still executes the PostgreSQL-authoritative path;
10. Redis restart restores API/worker readiness without silently resetting the
    initialized hot generation; and
11. final hot/non-hot seat, quota, and admission reconciliation is clean.

Use `docker compose -f docker-compose.multi-replica.yml ...` with Compose v2,
or `docker-compose` on hosts where that compatibility binary provides Compose
v2 behavior. The committed admission key is obviously synthetic local material.
Never reuse it outside this isolated environment.

The evidence-only load balancer and direct `api-1` endpoint bind to
Compose-assigned ephemeral `127.0.0.1` ports. The harness resolves those
published ports at runtime; it does not expose the other API replicas directly
or reserve a fixed host port.

This topology does not prove multi-region behavior, regional Redis
replication, global fairness, national-scale capacity, or production sizing.
