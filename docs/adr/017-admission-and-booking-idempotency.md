# ADR 017: Admission Identity and Durable Booking Idempotency Remain Distinct

- Status: Accepted
- Date: 2026-07-18

## Context

A waiting-room join identifies an intended booking shape: train run, travel interval, seat class, and passenger count. It intentionally does not accept passenger IDs or a booking idempotency key. The later reservation request contains those additional fields and uses PostgreSQL-backed durable idempotency from ADR 005.

Treating the join identity as the complete booking identity would allow a token to be reused with different passengers. Requiring the complete booking identity at join time would retain passenger-derived data and an idempotency credential for the potentially much longer queue wait.

Redis token finalization is not atomic with the PostgreSQL booking transaction. The design must tolerate retries while processing and the gap in which PostgreSQL committed but Redis did not record consumption.

## Decision

Milestone 2 uses three versioned, separately hashed identities:

1. The **admission request fingerprint** canonicalizes the train-run ID, origin and destination stop indexes, seat class, and passenger count. User ownership is a separate required token field.
2. The **durable booking fingerprint** remains ADR 005's canonical reservation-create fingerprint, including the stable ordered passenger identity set.
3. The **idempotency-key hash** is SHA-256 over the exact raw `Idempotency-Key` bytes as defined by ADR 005.

The queue entry and issued token contain the admission request fingerprint and bounded request metadata, but no passenger IDs and no raw idempotency key. Raw admission tokens and raw idempotency keys exist only at the transport/application edge long enough to hash and are never logged.

The first atomic token-acquire operation uses set-if-empty binding. It validates the token's authenticated user, train run, interval, seat class, passenger count, admission request fingerprint, status, and expiry. If the durable booking fingerprint and idempotency-key hash fields are empty, the script stores both hashes and transitions `issued` to `processing` with a bounded lease. If either field is already present, both must match exactly; a token can never be rebound.

Before applying the current hot-policy gate, the application performs a bounded, read-only PostgreSQL lookup by `(user, reservation.create, idempotency-key hash, durable booking fingerprint)`. A completed same-fingerprint command returns its owner-scoped durable reservation immediately. A completed different fingerprint conflicts. No new or in-progress command can use this lookup to bypass admission.

This replay-first order is required because a policy update or Redis loss can occur after PostgreSQL committed but before the original response/finalize completed. A newer policy generation must not hide an already durable reservation.

The token lifecycle behavior for a command without a completed durable result is:

- `issued` plus a valid first or matching binding becomes `processing` and returns `acquired`;
- `processing` plus the same admission, booking, and idempotency identities returns `retry_allowed`;
- `processing` plus any different identity returns conflict;
- `consumed` plus the same identities returns `replay_allowed`;
- `consumed` plus any different identity returns conflict; and
- expired or cancelled tokens cannot be rebound or acquired.

Only `acquired` invokes the PostgreSQL create transaction. It carries the current lease owner and generation. `retry_allowed` performs the read-only completed lookup; if no completed result exists, the API returns bounded `in_progress` plus `Retry-After` rather than creating another database contender. `replay_allowed` performs the same read-only lookup and repairs finalization.

PostgreSQL, not Redis, owns command uniqueness and returns the original durable reservation for a completed same-fingerprint replay. It returns a typed conflict if the same key was used for a different durable booking fingerprint.

The application marks a token `consumed` and removes its inflight capacity only after the reservation, durable idempotency completion, and outbox event commit in PostgreSQL. Finalization is idempotent for the bound identities.

If PostgreSQL commits and Redis finalization fails, a retry with the same booking fingerprint and idempotency key first finds the already committed reservation. The application returns it even if the policy generation has since changed, then attempts best-effort idempotent finalization of the old-generation token. This repairs Redis when state remains available without allocating another seat or creating another reservation.

Failures before PostgreSQL commit are classified:

- a bounded local-backpressure rejection or transient dependency failure releases `processing` to `issued` when the matching lease owner can do so safely; otherwise the bounded lease recovers it;
- a permanent booking conflict, including unavailable inventory or a durable quota rejection, follows the one-attempt policy by transitioning the token to `cancelled` and removing inflight capacity; the customer must rejoin; and
- token expiry removes inflight capacity and cannot be reversed.

`consumed` therefore means a durable reservation exists. Permanent rejections do not masquerade as consumed reservations. Admission still does not guarantee a seat.

Complete loss of the Redis token record remains a fail-closed queue-continuity limitation under ADR 015. Durable PostgreSQL state stays correct, but a token that no longer exists cannot itself authorize a hot-run retry until operational recovery.

## Consequences

- A queue entry remains small and excludes passenger IDs and booking credentials.
- A token is bound to the authenticated user, admitted trip shape, exact booking fingerprint, and one idempotency identity.
- Same-identity concurrent retries converge on one PostgreSQL reservation.
- The PostgreSQL-commit/Redis-finalize gap is safely repairable while the token record remains available.
- A policy update cannot hide a reservation that already committed.
- Active processing retries cannot create an unbounded set of PostgreSQL idempotency-lock waiters.
- A customer must rejoin after a permanent booking conflict under the explicit one-attempt policy.
- Canonicalization changes require explicit fingerprint versions and compatibility tests.

## Rejected alternatives

- Use only the admission fingerprint: rejected because passenger substitutions would not change the identity.
- Collect passenger IDs and the idempotency key at queue join: rejected because the queue wait would unnecessarily retain passenger-derived data and a replay credential.
- Allow a token binding to be overwritten: rejected because it would permit request or idempotency substitution.
- Use Redis as durable command idempotency: rejected because it cannot commit atomically with PostgreSQL booking state.
- Mark consumed before PostgreSQL commit: rejected because a rollback could leave a token claiming a reservation that does not exist.
- Reuse a token after a permanent inventory or quota conflict: rejected because it would make a single admission an unbounded series of booking attempts.
- Send every `processing` retry into PostgreSQL: rejected because one credential could amplify into a cross-replica database burst.
- Require current-generation admission before a completed replay: rejected because a policy update could make a committed reservation unreachable to its original retry.
