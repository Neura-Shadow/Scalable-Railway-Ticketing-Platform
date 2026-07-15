# ADR 005: PostgreSQL-Backed Durable Command Idempotency

- Status: Accepted
- Date: 2026-07-15

## Context

Customers and infrastructure retry requests after timeouts. A Redis-only key can disappear or expire independently of a committed reservation. Raw idempotency keys are credentials for replay behavior and must not be logged or stored.

## Decision

PostgreSQL owns idempotency for:

- `reservation.create`;
- `reservation.confirm`; and
- `reservation.cancel`.

Uniqueness is `(user_id, operation, key_hash)`, where `key_hash` is SHA-256 over the exact client key bytes. The raw key exists only at the HTTP/application edge long enough to hash it and is never logged, metric-labeled, cached as a raw Redis key, persisted, or returned.

The request fingerprint is SHA-256 over a versioned canonical binary representation. Create-hold canonicalizes train-run ID, normalized origin and destination codes, seat class, and passenger IDs sorted into a stable order after duplicate rejection. Confirm/cancel canonicalize the target reservation and command version. Length-prefixing prevents ambiguous concatenation.

The command transaction inserts an `in_progress` record. It is not committed separately. The database unique constraint establishes ownership. A conflicting transaction waits for the owning transaction's unique-key outcome and then:

- returns the completed resource for the same fingerprint;
- returns a typed conflict for a different fingerprint;
- reevaluates after the owning database transaction commits or rolls back.

Resource creation, idempotency completion, and the outbox event commit in the same transaction. A rollback removes the uncommitted record and every domain mutation, so a committed abandoned `in_progress` state is not part of the normal design. Completion stores only `resource_type` and `resource_id`, never a full response containing sensitive data.

Records have bounded retention. Database time allows an expired key to be atomically reacquired, and every hold-expiration pass deletes at most 1,000 expired rows through the expiry index with `SKIP LOCKED`. Cleanup never removes a record before the documented retry window and does not affect already committed resource uniqueness.

Redis may cache a completed lookup keyed by the hashed tuple with a bounded TTL. A miss or outage always falls back to PostgreSQL. Redis never grants command ownership.

## Consequences

- Same-key concurrent requests create one resource.
- Redis loss cannot duplicate reservations.
- Canonicalization is a versioned contract and has dedicated tests.
- Durable records add a database write and lock to each command, accepted for correctness.

## Rejected alternatives

- Raw key storage: rejected because database/log exposure would reveal replay credentials.
- Redis `SET NX` as authority: rejected because it is not atomic with booking state.
- Fingerprint only, without caller key: rejected because semantically repeated but intentionally separate requests could collapse.
- Caching full response bodies durably: rejected to minimize passenger/ticket data retention.
