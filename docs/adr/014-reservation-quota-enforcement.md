# ADR 014: PostgreSQL Serializes Durable Active-Hold Quotas

- Status: Accepted
- Date: 2026-07-18

## Context

A waiting room bounds admission attempts but does not stop one account from accumulating many temporary holds over time. Redis counters can disappear or diverge from committed reservation state and therefore cannot be the durable quota authority.

The existing Booking transaction runs at Read Committed. Passenger row locks serialize two requests for the same passenger, but two concurrent requests by one user with disjoint passenger sets can both observe an under-limit count and exceed a user-wide quota.

## Decision

PostgreSQL enforces three process-owned positive bounded settings:

- `reservation_max_active_holds_per_user`;
- `reservation_max_active_holds_per_user_per_train_run`; and
- `reservation_max_active_passengers_per_user`.

All three count only reservations currently in `held` state. Confirmation releases active-hold quota while retaining authoritative seat occupancy. Cancellation and expiration release quota by leaving `held`. No Redis quota counter grants or denies a booking.

Create-hold uses this order inside its existing PostgreSQL transaction:

```text
durable idempotency acquire/replay
-> per-user advisory transaction lock
-> indexed authoritative held-reservation counts
-> existing train-run/passenger/fare checks
-> existing VARBIT seat allocation
-> reservation rows
-> durable idempotency completion
-> outbox event
-> commit
```

Completed same-fingerprint idempotency replay returns before quota evaluation, so an existing reservation does not reject its own retry.

The per-user serialization point is `pg_advisory_xact_lock` over a stable 64-bit digest of the canonical user UUID in a Booking-owned namespace. A digest collision can only serialize unrelated users; it cannot let either user bypass a quota. The lock is transaction-scoped and is always acquired in the same position before passenger or inventory locks.

While holding it, Booking counts authoritative `held` reservations for the user, counts the subset for the requested train run, and sums their `reservation_seats`. Supporting partial indexes cover held reservations by `(user_id, train_run_id)` and reservation-seat lookup by reservation ID. The pending request is accepted only if adding one hold and its passenger count stays within every configured limit.

Quota rejection returns typed `reservation_quota_exceeded` and HTTP `429 Too Many Requests` with a bounded `Retry-After`. It rolls back the uncommitted idempotency claim and occurs before train-run, passenger, fare, inventory, reservation, idempotency-completion, or outbox mutation. It therefore creates no reservation row, does not complete durable idempotency, writes no domain event, and changes no seat mask.

The following alternatives were evaluated:

- **Advisory transaction lock plus indexed counts**: selected; one serialization point and authoritative derived state provide the smallest correct implementation.
- **Explicit quota-counter row**: correct if every create, confirm, cancel, and expire transaction updates it, but adds another durable state machine and reconciliation burden.
- **Serializable transaction**: correct with retries, but broadens serialization failures across the entire allocation transaction and is larger than the one user-scoped conflict.
- **Partial unique constraints**: can enforce one row for a fixed key, not configurable counts or passenger totals.
- **Indexed counts without serialization**: rejected because Read Committed permits write skew.

Quota reconciliation reads the same authoritative held-reservation queries and configuration and reports users whose current state exceeds a configured bound. Because no counter table exists, there is no counter drift to repair. Existing seat reconciliation remains unchanged.

## Consequences

- Concurrent API replicas cannot exceed a user's durable active-hold quotas.
- Redis failure cannot bypass quota.
- Confirmation immediately frees hold quota, while the confirmed seat remains occupied until cancellation.
- The design adds one short user-scoped serialization point and indexed count queries to create-hold.
- No quota counter table or lifecycle update is added.
- The existing PostgreSQL `VARBIT` allocation and release SQL remain unchanged.

## Rejected alternatives

- Redis-only quota: rejected because loss or expiry could permit durable hoarding.
- Count without a lock: rejected because concurrent disjoint requests can both pass.
- Counter rows: rejected because the derived-state design is smaller and avoids drift.
- Global transaction isolation escalation: rejected because the required conflict scope is one user.
- Quota checks before durable idempotency replay: rejected because a retry could be denied by the reservation it already created.
