# Deployment Baseline

`kubernetes/base` is a single-region Kustomize baseline for the Milestone 4 API,
hold-expirer, outbox-worker, admission-worker, and read-model-worker. Admission
and read-model workers start disabled with one replica and no Service for their
private health/metrics ports. Enabling or scaling either requires an explicit
reviewed overlay; this baseline adds no multi-region writer or operator.

Milestone 4's `legacy`, `shard-0`, and `shard-1` storages are fixed schemas in
one PostgreSQL database. This baseline does not provision independent physical
shards, a second writer, or a public shard-administration service. Migration and
cutover remain private operator CLI workflows.

`runtime-secrets.example.yaml` is a placeholder-only template. It is deliberately
excluded from `kustomization.yaml`; never apply the example or commit populated
secret material.

Before applying a production overlay:

1. pin the application image by immutable registry digest;
2. provision PostgreSQL and Redis in the same region with TLS/network isolation;
3. create `railway-runtime-secrets` through the platform secret manager with
   `database-url`, `redis-address`, `redis-password`, `jwt-secret`, and
   `admission-token-keyring` keys;
4. add ingress/TLS and topology-specific NetworkPolicies;
5. keep a single regional writer; explicitly enable the read-model worker only
   after Migration 7, bounded backfill, reconciliation, and Redis/PostgreSQL
   readiness; its Redis dependency is mandatory;
6. explicitly enable admission only after its PostgreSQL and Redis dependency,
   policy-generation, and key-rotation checks pass; and
7. apply migrations as a separate reviewed release step and set only measured
   resources/replicas.

Keep `BOOKING_SHARD_MODE=legacy` while applying and validating Migration 8.
Before an overlay opts into `schema_poc`, drain every incompatible writer,
prove the minimum fencing-protocol gate, configure only the fixed logical shard
IDs, set the explicit production acknowledgement, and follow
[the Migration 8 rollout](../docs/migrations/migration-8-production-rollout.md).
The quiesced cutover may reject writes for a selected train run, never dual
writes, and retains its source for a bounded rollback window. Do not mount JWT
or admission keyring secrets into `shard-admin`.

Render without changing the cluster:

```bash
kubectl kustomize deploy/kubernetes/base
```

The base image name, schema topology, replica counts, and resource settings are
non-production evidence defaults. They are not physical-shard, zero-downtime,
benchmark, or sizing claims. See
[production deployment](../docs/production-deployment.md).

## Milestone 5 local physical-shard topology

`docker-compose.physical-shards.yml` is the bounded, single-region local
evidence topology. It runs one control PostgreSQL database, two independent
booking PostgreSQL databases, Redis, three stateless API replicas, two
admission workers, two read-model workers, the hold expirer, the outbox worker,
the booking-command reconciler, and a round-robin reverse proxy. PostgreSQL
instances have separate volumes and health checks; no database port is
published to the host. The proxy uses the existing no-affinity upstream and
publishes only to loopback.

The committed password and key defaults are synthetic local values. Override
all `*_PASSWORD`, DSN, JWT, and admission keyring variables before using a
shared environment. The fixed `BOOKING_SHARD_0_DATABASE_URL` and
`BOOKING_SHARD_1_DATABASE_URL` values are application configuration, not
control-catalog data, and use distinct hosts and credentials. Per-process
physical-shard pools are bounded at three connections per shard and six total.
Every control-using process is capped at four connections. The physical-mode
startup contract records ten control pools, three API replicas, three
shard-aware workers with two fixed shard pools each, eight migration/admin reserve and
sixteen operational reserve connections. The resulting combined ceiling is
100; increasing any term without increasing and reviewing
`POSTGRES_MAX_CONNECTIONS_LIMIT` fails startup.

The booking-command reconciler also repairs durable physical operator commands.
It reads only fixed catalog connection references, rotates boundedly across the
two configured shards, validates receipts against the recorded operation,
generation and fingerprint, and atomically finalizes the control projection.
It does not probe arbitrary endpoints or create a second shard write.

Render and validate the fully interpolated configuration without starting it:

```bash
docker compose -f docker-compose.physical-shards.yml config --quiet
```

Start the local topology only after the control and booking-shard migrations
have been reviewed:

```bash
docker compose -f docker-compose.physical-shards.yml up --build --wait
```

The two shard containers are independent failure domains. The following local
test stops only shard 0, leaving the control database and shard 1 running; use
`start` to restore it. Repeat with `booking-shard-1-postgres` for the opposite
case. This is failure-injection evidence, not a production availability claim.

```bash
docker compose -f docker-compose.physical-shards.yml stop booking-shard-0-postgres
docker compose -f docker-compose.physical-shards.yml ps
docker compose -f docker-compose.physical-shards.yml start booking-shard-0-postgres
```

Remove the evidence topology while retaining database volumes with `down`.
Use `down --volumes` only when intentionally discarding all local evidence
data. A source retained for rollback or reverse migration must not be deleted.
