# ADR 054: Payment Security and Sensitive-Data Boundary

- Status: Accepted for Milestone 6 implementation
- Date: 2026-08-05

## Context

Payment integration introduces signed internet-facing messages, provider
credentials, hosted-session references, financial identities, administrative
repair actions, and egress to a configured provider. Expanding the platform to
handle raw account or sensitive authentication data would create a materially
different security and compliance boundary that Milestone 6 neither needs nor
claims to satisfy.

## Decision

The platform never accepts, transmits, stores, logs, traces, or places in test
fixtures a primary account number, full card number, CVV/CVC/CID, PIN/PIN block,
magnetic-stripe/track data, raw bank credential, or equivalent sensitive
authentication data. Customer payment entry occurs only on a hosted provider
surface or through a provider SDK that returns a bounded opaque token. Platform
APIs reject card-like fields and accept no arbitrary provider request body.

Control persistence is limited to globally unique platform identities, bounded
provider and operation identifiers, hosted-session reference needed for the
customer journey, integer minor-unit amount, three-character currency, state,
hashes/fingerprints, bounded error categories, timestamps, and retry metadata.
No raw idempotency key, provider secret, signing secret, or full webhook payload
is stored by default. Physical shards store only the immutable financial snapshot
and receipts required to validate local reservation/ticket transitions.

Webhook handlers enforce strict method, route, content type, request/body size,
parsing depth, and deadline limits. They authenticate the exact bounded bytes
using HMAC-SHA256, constant-time comparison, a timestamp replay window, and an
allowlisted rotating key ring. Duplicate event identity with a changed hash is a
security conflict. Logs never contain signature headers or bodies, and error
responses do not expose verification detail.

Provider base URLs and webhook provider names come only from startup-validated
configuration, never request input, catalog rows, webhook content, or redirect
targets. Production-mode provider egress requires HTTPS, an allowlisted host and
port, DNS/IP safety appropriate to deployment, explicit connect and total
timeouts, strict response/header/body limits, and redirects disabled or limited
to an exact allowlist. These controls prevent the provider adapter from becoming
a general SSRF client.

Provider API and webhook secrets enter only through the process secret mechanism
and are redacted from config dumps, errors, logs, traces, metrics, health, and
admin output. Access is separated by purpose and least privilege. Rotation keeps
only a bounded active/previous webhook key window, and a missing or ambiguous key
identifier fails closed. The deterministic sandbox uses synthetic tokens and
keys and is rejected when production mode is enabled.

Customer APIs require authenticated ownership and use opaque platform IDs.
Webhook authentication is separate from customer authentication. Payment admin
and manual-review actions require an explicit privileged role, bounded request
schemas, reason categories, append-only audit records, and the same idempotent
provider/shard command paths; operators cannot edit financial, reservation, or
ticket rows directly.

Metrics use fixed operation/state/result/reason labels. Payment intent,
reservation, user, provider event, operation, ticket, DSN, host, port, and
connection-reference values are prohibited labels. Logs use bounded correlation
identities only where operationally necessary and access controlled; customer-
facing errors remain topology- and provider-safe.

Amounts are server-derived and immutable, use checked integer minor-unit
arithmetic, and are revalidated at each command. Currency and provider values
are allowlisted. Responses expose only the hosted-session fields needed by the
client; raw provider responses are normalized and size bounded.

## Invariants

- No raw card or sensitive authentication data crosses the platform boundary,
  including sandbox fixtures, logs, traces, queues, databases, and outboxes.
- A signed webhook is only an authenticated observation and cannot bypass
  idempotency, state, amount/currency, ordering, or reconciliation checks.
- Request-controlled input cannot choose an egress URL, shard DSN, topology
  label, redirect destination, or secret identifier outside an allowlist.
- Security conflicts and manual actions are auditable without recording secrets
  or sensitive payloads.
- Milestone evidence describes the implemented boundary and does not assert PCI
  certification or compliance assessment.

## Consequences

- Hosted/tokenized collection narrows platform exposure but leaves deployment-
  specific provider, frontend, operational, and compliance responsibilities for
  a future production adapter.
- Full webhook bodies are unavailable for casual debugging; payload hashes,
  normalized fields, synthetic replay fixtures, and provider queries provide the
  supported diagnostic path.
- Strict egress and response limits require provider-specific validation before
  production onboarding.
- Manual review is safer but intentionally less convenient than direct database
  repair.

## Rejected alternatives

- Add card number or CVV fields for the sandbox: rejected because deterministic
  tests need only synthetic opaque tokens and such fields expand the boundary.
- Persist full webhook payloads by default: rejected because normalized fields
  and a hash satisfy replay/conflict needs with less sensitive data exposure.
- Allow a per-request provider URL: rejected because it creates SSRF and bypasses
  deployment allowlisting.
- Log provider responses and signatures for debugging: rejected because they may
  contain secrets, hosted tokens, or payment data.
- Let operators repair rows directly: rejected because it bypasses invariants,
  receipts, fences, idempotency, and auditability.
