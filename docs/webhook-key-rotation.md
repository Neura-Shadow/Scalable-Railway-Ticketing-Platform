# Webhook Key Rotation

## Durable keyring model

Each provider account has exactly one durable `primary` key version and at most
one additional `accepted` version. Secret material stays in the deployment
secret source. PostgreSQL stores only provider/account/key identities,
lifecycle timestamps, and a domain-separated HMAC material proof. Readiness
compares that proof with the locally provisioned secret, so a passive region or
stale replica fails closed when its ID-to-secret binding differs. Reusing the
same secret under two key IDs is rejected.

The retirement grace starts when the old primary is demoted, not when a new key
is merely staged. Each verified Stripe request is checked against current
durable metadata before inbox persistence. A stale process therefore cannot
accept a removed key after its durable grace deadline. Signature verification
also requires exactly one configured key to match; an ambiguous multi-key match
is rejected.

Lifecycle transitions are serialized per provider account and appended to the
immutable rotation audit with region and epoch actor evidence. The hot table
retains the eight most recent retired versions. Older retired versions are
copied to the immutable archive and deleted from the hot table only after an
exact archive match. This keeps runtime work bounded without erasing operator
evidence.

## Rotation runbook

1. Provision the new secret in every intended region outside PostgreSQL, logs,
   metrics, evidence artifacts, and the repository.
2. Configure old and new IDs as accepted while the old ID remains primary.
   Confirm readiness in active and passive regions. No retirement clock starts
   during this staging step.
3. Send provider-shaped test signatures for both IDs and confirm exactly one
   key matches each request.
4. Change the provider endpoint and application primary to the new ID. Confirm
   the old version is now `accepted` with a future durable deadline.
5. During the bounded overlap, monitor fixed-state lifecycle counts and audit
   transitions. Never label metrics with secret material or unbounded event IDs.
6. After the durable deadline and the provider retry horizon, prove an old-key
   request receives 401 while the current key still commits.
7. Remove the old ID and secret from accepted configuration. Confirm its durable
   state is `retired`, the transition audit is append-only, and active readiness
   remains healthy.
8. Replay the metadata to the passive region. Prove the retired key receives
   401 and the current key reaches fenced persistence but receives 5xx; passive
   replicas must not acknowledge delivery before promotion.

Stripe overlap duration must be selected from the endpoint rotation and retry
policy in use. Providers without a signed timestamp or equivalent replay
contract must not inherit Stripe's verifier or freshness window.

Startup/readiness fails for no primary, more than two accepted IDs, duplicate
IDs or material proofs, missing or swapped secret material, expired accepted
metadata, a retired key reintroduced into configuration, or durable metadata
that differs from the local generation.
