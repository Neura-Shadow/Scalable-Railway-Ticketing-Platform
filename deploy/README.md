# Deployment Baseline

`kubernetes/base` is a single-region Kustomize baseline for the Milestone 3 API,
hold-expirer, outbox-worker, admission-worker, and read-model-worker. Admission
and read-model workers start disabled with one replica and no Service for their
private health/metrics ports. Enabling or scaling either requires an explicit
reviewed overlay; this baseline adds no multi-region writer or operator.

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

Render without changing the cluster:

```bash
kubectl kustomize deploy/kubernetes/base
```

The base image name and resource settings are non-production defaults. They are not a benchmark or sizing recommendation. See [production deployment](../docs/production-deployment.md).
