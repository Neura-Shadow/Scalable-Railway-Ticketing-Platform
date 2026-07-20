# ADR 013: Admission Tokens Are One-Time, Bound, and Recoverable

- Status: Accepted
- Date: 2026-07-18

## Context

An admitted customer polls an API replica after an independent worker issues the token. The raw token must be returned once, but it must never be stored, logged, or exposed in metrics. Token acquisition must be atomic across replicas and must survive retry, worker failure, and the PostgreSQL-commit/Redis-finalize gap.

The waiting-room join does not contain passenger IDs or a booking idempotency key, so complete booking identity cannot be known when the worker admits an entry.

## Decision

Admission tokens use these states:

```text
issued -> processing -> consumed
issued -> expired
processing -> issued       (safe lease recovery)
processing -> expired
issued|processing -> cancelled
```

`consumed`, `expired`, and `cancelled` are terminal. One token authorizes one bounded booking attempt; admission does not guarantee a seat.

The worker generates a fresh 32-byte cryptographically secure delivery nonce for every admission. All signed material uses a versioned, length-prefixed canonical binary encoding with the domain separator `railway-admission-token/v1`; concatenation without length framing is prohibited.

The worker computes an immutable issuance MAC:

```text
issuance_mac = HMAC-SHA-256(
  process-owned derivation key,
  domain || key ID || policy ID || policy version || entry ID ||
  user ID || admission fingerprint || issued at || expires at || delivery nonce
)
```

The opaque raw token is the versioned base64url encoding of the key ID and issuance MAC. The random nonce is deliberately absent from the bearer. The derivation key is exactly 32 bytes of external secret material shared only by API and admission-worker processes. It has no development or production default, is never logged, and is validated as process-owned configuration. The worker stores only `SHA-256(raw token)`, the random nonce, immutable signed context, binding metadata, state, and expiry indexes. Redis stores neither the raw token, its issuance MAC, nor an encrypted raw-token copy; a Redis snapshot therefore lacks both the process key and the bearer MAC needed to reconstruct the credential.

The API validates the issuance MAC in constant time before trusting a delivery or acquire record. This prevents an actor with active Redis write access but no derivation key from minting a usable token or changing immutable ownership, policy, fingerprint, or expiry fields. Such an actor can still corrupt queue order or availability, which remains an operational security incident.

Before claiming delivery, the customer status path performs a read-only ownership, state, expiry, and delivery preflight. The API uses the configured key to recompute the issuance MAC and raw bearer from the returned claims and nonce, then compares `SHA-256(raw token)` with the stored commitment. Unknown keys, invalid claims, and hash mismatches therefore fail before the at-most-once marker changes. A second atomic script rechecks ownership and state, marks delivery complete, and returns the metadata; the API repeats the reconstruction and hash verification before returning the raw token. Later status calls cannot return it. The nonce remains only as signed verification metadata until token TTL; the delivery flag, not nonce deletion, enforces one response. Nonces, raw tokens, token hashes, and issuance MACs are never logged or used as metric labels.

Delivery is deliberately **at most once**, not exactly once over the network. If the API terminates or the connection is lost after the atomic claim but before the response arrives, the credential cannot be redelivered. The customer can cancel the admitted entry/token to release inflight capacity and rejoin; otherwise bounded expiry reclaims it. Tests must terminate the API at this seam. A missing key ID is detected before the delivery claim and fails readiness, so key configuration cannot burn the delivery.

The process uses a keyring with explicit key IDs and separate `issue` and `accept` sets. Rotation deploys the new accept key to every API and proves readiness before workers issue with it. The previous accept key remains only through the maximum token TTL plus a bounded safety margin, or operators deliberately invalidate affected tokens and require rejoin. Missing or unknown key IDs fail closed.

The raw token is sent only through `X-Admission-Token`. The application hashes it before crossing the Redis seam. Atomic acquire validates:

- token hash exists and is unexpired;
- authenticated user;
- train run and current policy version;
- origin and destination stop indexes;
- seat class;
- passenger count;
- admission fingerprint; and
- first-acquire or matching durable booking fingerprint and idempotency-key hash under ADR 017.

`issued` transitions to `processing` with a bounded Redis-time lease, a fresh unguessable lease owner, and a monotonically increasing lease generation, then returns `acquired`. Release, recovery, and finalization compare the lease generation so an obsolete request cannot change a reclaimed token.

A matching request during an active `processing` lease returns `retry_allowed`, but does not receive lease ownership and must not execute another PostgreSQL create. It may perform only a read-only durable-idempotency lookup: a completed match returns the reservation and repairs finalization; an incomplete match returns bounded `in_progress` with `Retry-After`. Changed data conflicts. A matching request after `consumed` returns `replay_allowed` so PostgreSQL durable idempotency can return the reservation; changed data conflicts.

After PostgreSQL commit, an idempotent finalize script changes the matching token to `consumed` and removes it from inflight capacity. Before commit:

- bounded local backpressure or a transient dependency failure releases the matching lease back to `issued` when safe;
- otherwise Redis-time lease recovery performs that release;
- permanent inventory, quota, or booking conflict changes it to `cancelled` and releases inflight capacity; and
- expiry changes it to `expired` and releases inflight capacity.

A finalize failure after PostgreSQL commit does not permit a second reservation. The same bound idempotency key replays the durable PostgreSQL result and retries finalize. Reconciliation detects a processing lease past its timeout and token/inflight drift; it does not auto-repair production state.

Token delivery responses use `Cache-Control: no-store, private`. The raw token is accepted only in `X-Admission-Token`, never a URL, query value, redirect, cookie, or body log. TLS is required; reverse-proxy, APM, tracing, and access-log configuration must redact the admission and idempotency headers. Public invalid, missing, mismatch, and unknown-key errors remain bounded and do not expose token-record existence.

## Consequences

- At least 256 bits of fresh cryptographic randomness feed each token.
- Redis stores no raw token and the raw value is delivered once to the authenticated owner.
- A Redis snapshot or active writer without the derivation key cannot reconstruct or forge a usable token.
- Token theft, route/class/count changes, passenger changes, and idempotency substitution fail before inventory mutation.
- Same-identity retries converge on PostgreSQL durable idempotency.
- Losing Redis can lose undelivered or issued tokens, but it cannot corrupt durable inventory.
- A lost delivery response can require rejoin; no exactly-once network-delivery claim is made.

## Rejected alternatives

- Store the raw token until polling: rejected because Redis or backup exposure would reveal a booking credential.
- Store an encrypted raw-token envelope: rejected because deterministic derivation provides the same one-time delivery without persisting ciphertext.
- Generate the token only on the first status request: rejected because issuance and inflight accounting must be atomic in the admission worker.
- Put passenger IDs or the raw idempotency key in the queue: rejected because those values are unavailable at join and unnecessarily increase retention.
- Mark consumed before PostgreSQL commit: rejected because a rollback could strand a token without a reservation.
