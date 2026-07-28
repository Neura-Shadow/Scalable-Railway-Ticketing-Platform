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
