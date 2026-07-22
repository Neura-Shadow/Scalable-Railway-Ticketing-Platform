# Deployment Baseline

`kubernetes/base` is a single-region Kustomize baseline for the Milestone 2 API,
hold-expirer, outbox-worker, and admission-worker. The admission-worker starts
with `ADMISSION_WORKER_ENABLED=false`, one replica, and no Service for its
private health/metrics port. Enabling or scaling admission requires an explicit
reviewed overlay; this baseline does not introduce multi-region writers or an
operator.

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
5. keep a single regional writer, explicitly enable the admission-worker only
   after its Redis/PostgreSQL dependencies and key rotation are ready, and set
   measured resources/replicas;
6. apply migrations as a separate reviewed release step.

Render without changing the cluster:

```bash
kubectl kustomize deploy/kubernetes/base
```

The base image name and resource settings are non-production defaults. They are not a benchmark or sizing recommendation. See [production deployment](../docs/production-deployment.md).
