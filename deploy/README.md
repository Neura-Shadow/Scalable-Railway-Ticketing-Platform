# Deployment Baseline

`kubernetes/base` is a single-region Kustomize baseline for the API, hold-expirer, and outbox-worker. It contains no Secret object and no real credential.

Before applying a production overlay:

1. pin the application image by immutable registry digest;
2. provision PostgreSQL and Redis in the same region with TLS/network isolation;
3. create `railway-runtime-secrets` through the platform secret manager with `database-url`, `redis-address`, `redis-password`, and `jwt-secret` keys;
4. add ingress/TLS and topology-specific NetworkPolicies;
5. set measured resources/replicas and monitoring annotations; and
6. apply migrations as a separate reviewed release step.

Render without changing the cluster:

```bash
kubectl kustomize deploy/kubernetes/base
```

The base image name and resource settings are non-production defaults. They are not a benchmark or sizing recommendation. See [production deployment](../docs/production-deployment.md).
