# Admission-Token Lifecycle

An admission token authorizes one matching hot-run booking attempt. It is not a
seat hold and does not guarantee inventory.

## Issuance and storage

The admission worker creates a fresh 32-byte cryptographically secure nonce.
It signs versioned, length-prefixed immutable claims using HMAC-SHA-256 and an
externally injected 32-byte derivation key. The opaque raw token contains only
the version, key ID, and MAC; the nonce is deliberately absent. Redis stores:

- `SHA-256(raw token)`, never the raw token;
- nonce and key ID, but neither the issuance MAC nor another reconstructable
  bearer representation;
- policy and entry identity;
- an owner hash and admission fingerprint;
- issue and expiry times; and
- bounded lifecycle and lease metadata.

The API performs a read-only delivery preflight, reconstructs the raw value
using the process key, and compares its SHA-256 digest with Redis's stored
commitment before changing delivery state. Redis then atomically rechecks and
marks delivery; the API repeats reconstruction and hash verification before
returning `X-Admission-Token`. The
response is at-most-once: a connection loss after that claim can require
cancellation or expiry and rejoin. No exactly-once network-delivery claim is
made.

Never log or emit as a metric label the raw token, token hash, nonce, MAC,
idempotency key, booking payload, passenger ID, or owner identity. TLS is
mandatory. Reverse proxies, tracing, crash reporting, and APM must redact both
`X-Admission-Token` and `Idempotency-Key`.

## Binding

Before trusting a Redis token record, the API verifies its self-authenticating
claims with a configured accept key. Atomic acquire then validates:

- authenticated owner;
- current policy and train run;
- ordered origin and destination;
- seat class and passenger count;
- admission fingerprint;
- durable booking fingerprint, including passenger IDs; and
- SHA-256 hash of the durable booking idempotency key.

The durable booking fingerprint and idempotency hash bind on first acquire and
cannot be replaced. A stolen token or changed request fails before the local
execution slot and before any PostgreSQL inventory mutation.

## State machine

```text
issued -> processing -> consumed
issued -> expired
processing -> issued       (safe lease recovery)
processing -> expired
issued|processing -> cancelled
```

`consumed`, `expired`, and `cancelled` are terminal. An acquire from `issued`
creates a bounded Redis-time processing lease with a random lease owner and
monotonic lease generation. Release and finalize compare both values so a stale
request cannot mutate a recovered lease.

The minimum processing lease is five seconds. The API rejects a
`DATABASE_TIMEOUT` of five seconds or longer and applies that shorter deadline
to each reservation transaction. Token acquisition also requires at least one
full configured lease of remaining token lifetime. These bounds ensure a lease
cannot be reclaimed while the preceding PostgreSQL transaction is still
running.

A same-identity retry during an active processing lease is read-only: it may
look up a completed PostgreSQL idempotency record, but it must not execute a
second create transaction. A completed match returns the original reservation
and repairs finalization. Otherwise it returns bounded `admission_in_progress`
with `Retry-After`. Changed identity conflicts.

A same-identity retry after `consumed` uses PostgreSQL durable idempotency to
return the original reservation. PostgreSQL, not Redis, is the recovery
authority.

## Booking outcomes

- After PostgreSQL commit, finalize changes the matching processing token to
  `consumed` and removes it from inflight tracking.
- Local backpressure or a transient failure before commit releases the matching
  lease to `issued` when safe; otherwise expiry recovery does so.
- A permanent quota, inventory, or booking conflict applies the documented
  one-attempt rule: cancel the token, release inflight capacity, and require the
  customer to rejoin.
- If PostgreSQL commits but Redis finalize fails, the same idempotency key
  returns the durable reservation and an idempotent finalize repairs control
  state. Before invoking atomic issue, the worker writes exact, bounded-TTL
  entry and token locators for every bounded candidate. Only after every
  write-ahead locator succeeds may the Lua script admit an entry. After a
  successful Issue response, token locators for candidates that definitively
  lost a rate, inflight, or concurrent-worker race are deleted immediately;
  an ambiguous Issue failure retains the bounded write-ahead locators for
  repair. No nonce, MAC, raw bearer, or deliverable token record is exposed.
  This ordering means a crash after issue cannot strand an admitted token
  without its locator. Durable replay uses the locator without `KEYS` or
  `SCAN`, including after a policy version change, and rechecks owner, booking
  fingerprint, and idempotency-key hash in Lua before marking the token
  consumed, releasing inflight capacity, and scheduling bounded cleanup. The
  locator expires under its own bounded TTL and is deleted earlier when repair
  proves the generation state is gone. Issuance accepts older candidates that
  remain unexpired after bounded locator work while rejecting future-skewed
  claims. A duplicate reservation cannot be created.
- PostgreSQL failure never reports the token consumed unless the durable booking
  exists.

## Key rotation

`ADMISSION_TOKEN_KEYRING` contains `key-id=base64url` entries whose decoded
material is exactly 32 bytes. `ADMISSION_TOKEN_ISSUE_KEY_ID` selects the worker
issue key; `ADMISSION_TOKEN_ACCEPT_KEY_IDS` is the explicit API/worker accept
set.

Rotation is API first:

1. inject the new key into every API accept set and prove readiness;
2. add it to workers and switch worker issuance;
3. keep the previous accept key through the maximum token TTL plus a bounded
   safety margin; and
4. remove the old key only after affected credentials cannot remain valid.

Unknown or missing key IDs fail closed. The committed Compose key is synthetic
local-test material only. Production must inject independent secret-managed
key material.
