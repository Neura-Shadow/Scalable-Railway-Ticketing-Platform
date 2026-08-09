# Payment Security

## Boundary and claims

Milestone 6 uses hosted/tokenized synthetic semantics and a deterministic local
sandbox. It does not integrate a live gateway or establish PCI certification.
Its strongest data-minimization rule is that card numbers, CVV, PIN,
magnetic-stripe data, raw payment credentials, and real payment methods never
enter the application, sandbox, databases, logs, metrics, events, fixtures, or
evidence.

## Threats and controls

| Threat | Required control |
|---|---|
| client amount/currency manipulation | derive immutable integer money from owner-scoped reservation; request schema rejects money |
| ownership bypass | current authenticated owner filters every intent/ticket query and command |
| forged/replayed webhook | provider allowlist, bounded key ID, timestamp window, HMAC-SHA256, constant-time compare, unique inbox |
| event ID collision with changed content | payload hash comparison, conflict record/metric, no state advance, manual review |
| webhook body exhaustion | strict content type, body/request timeout and byte limit before parse |
| provider credential leakage | process-specific secret injection; never store/log/return/label; shard-only workers receive no provider secret |
| provider URL SSRF/redirect | configuration-only endpoint, HTTPS production policy, SSRF-safe DNS/IP allow policy, redirects disabled/tightly allowlisted |
| oversized/malicious response | bounded connect/request/header timeouts, strict response bytes, normalized schema validation |
| blind retry/double charge or refund | one durable operation and stable idempotency identity; query after unknown outcome |
| refund greater than capture | server-derived full amount and immutable integer invariant |
| ticket without capture | shard issuance requires durable exact captured command snapshot and unique receipt |
| captured payment without ticket | bounded retry, compensation/manual review, detect-only mismatch |
| unsafe seat release | normal expiry only for `held`; release only after proven no capture/void/refund and fenced local command |
| state regression/out-of-order events | expected-state transitions; conflicting terminal events query current provider status |
| stale physical route | current assignment resolution and database-local generation fence before mutation |
| command replay | globally unique receipt plus immutable fingerprint; changed replay conflicts |
| sandbox in production | allowed provider types only; production rejects sandbox unless explicit disposable override |
| PII in provider metadata | bounded allowlisted non-PII metadata; no passenger names/email/associations |
| metric cardinality attack | every label value from bounded allowlists; never label by any resource/customer/provider event ID or endpoint |
| operator refund misuse | current database role, bounded scope, audit, dry-run, explicit confirmation, invariant checks |
| manual-review bypass | no completion without provider proof and shard receipt; no direct ticket/seat mutation |
| migration data loss | base-copy/journal/replay/validation coverage for all payment/ticket state and receipts |

## Webhook verification

The webhook endpoint is outside customer JWT auth but is not unauthenticated.
It verifies the provider, content type, byte/time bounds, accepted key ID,
timestamp window, and HMAC over exact documented raw bytes before parsing.
Keyring rotation permits overlapping accepted keys. Invalid requests return
bounded errors without revealing whether a key, signature fragment, or event
exists. Exact duplicates are success/no-op; changed-hash collisions are
security conflicts. Valid unknown event types are durable ignored rows and do
not mutate payment state.

The handler stores only normalized bounded fields and payload hash, then
returns. It does no provider call, ticket issuance, or saga advancement inline.

## Provider client

Callers cannot select provider, endpoint, callback URL, operation ID, or shard.
Production adapters require HTTPS and an explicit egress policy that resolves
and validates targets against private, loopback, link-local, metadata, and
rebinding risk. Redirects are disabled or revalidated against a tight allowlist.
Clients use bounded connection, TLS, header, request and response-body limits.
Authentication failures stop and alert rather than retrying indefinitely.

The sandbox is a separate test/development process. Fault/admin endpoints exist
only in a disposable profile and must not appear in production manifests or
readiness. It rejects raw card/authentication fields even in test requests.
Its project-scoped volume retains only bounded synthetic provider state and
hashed stable-key identities across process restart; corrupt or unavailable
state fails closed. The checksum detects accidental corruption, not a malicious
host able to rewrite both state and digest. Sandbox host/volume compromise is
therefore outside customer-facing trust and is never production evidence.

## Secrets and sensitive output

Provider API keys and webhook keyrings are process-specific runtime secrets.
Control migrations, source code, evidence, Compose defaults, command arguments,
public health, logs, traces, metrics and admin output contain no real secret.
Webhook bodies/signatures and raw provider responses are not logged. Public
errors exclude provider detail, operation/request IDs unless explicitly proven
safe, SQL, DSNs, connection references, shard identity, and stack traces.

## Financial and operator audit

Immutable operation records capture globally unique intent/operation identity,
bounded provider name/type, operation, amount/currency, state, attempts,
response fingerprint, provider reference where required, timestamps, bounded
error/actor/reason, and manual-review decisions. They do not store raw
idempotency keys, payloads, card data, secrets, or passenger PII.

Provider-side admin actions require current operator authorization, explicit
confirmation and nonzero error exits. They cannot change money, exceed capture,
mark completion without a current-shard receipt, activate tickets, or mutate
inventory directly. Default reconciliation is read-only.

## Metrics policy

Allowed labels are bounded values from `provider`, `operation`, `result`,
`reason`, `state`, `database_role`, and allowlisted `shard_id`. Forbidden labels
include all payment/reservation/ticket/operation/provider-event/user/passenger
identities, email, idempotency keys, signatures, DSN, host, port, and
`connection_ref`. Review must reject dynamic error text as a label.

## Residual risks

Conservative unknown-outcome handling can retain inventory and needs staffed
manual review. A compromised provider credential, secret system, runtime host,
database superuser, or authorized operator remains high impact and requires
deployment controls outside this codebase. A real adapter can add different
webhook, idempotency, authorization-expiry, data residency, and settlement
risks; it requires a fresh threat model and cannot inherit sandbox acceptance.
