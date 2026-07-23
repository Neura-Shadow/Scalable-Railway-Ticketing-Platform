# ADR 022: Availability Cache Is a Short-Lived Hint

- Status: Accepted
- Date: 2026-07-22

## Context

Counting seats whose `occupied_segments` do not overlap a requested journey is
read-heavy. The count can change immediately after a response because another
transaction may hold, confirm, cancel, or expire inventory. Redis cannot commit
that count atomically with PostgreSQL reservation and outbox state.

## Decision

Cache availability only as a point-in-time hint. A value contains the bounded
non-negative count, observation timestamp, and an allowlisted source marker.
It uses a short TTL, bounded jitter, and the per-train-run version namespace
from ADR 021. The response contract and documentation do not call it a seat
guarantee or exact real-time availability.

On a hit, the read endpoint may return the hint. On a miss, it computes the
requested `[from_stop_index,to_stop_index)` mask and counts available seats from
authoritative `seat_inventory` rows, then attempts a bounded Redis fill. Redis
failure falls back to that PostgreSQL computation. PostgreSQL failure returns a
safe service error; stale-if-error is disabled by default.

Reservation creation ignores the cached count. It repeats train-run status,
journey, fare, quota, admission, and overlap checks inside the authoritative
PostgreSQL booking transaction. A stale positive hint can therefore become a
normal inventory conflict but cannot create an invalid reservation or
overselling.

Reservation held, confirmed, cancelled, and expired events conservatively
rotate the affected train run's availability generation. Seat, coach,
inventory-affecting train-run, and cancellation changes do the same. Rotation
failure retries asynchronously; it never rolls back or bypasses a booking.

## Consequences

- Hot availability reads can avoid repeated segment-mask counts.
- Responses may be stale within the configured TTL/invalidation window.
- Duplicate conservative invalidations may reduce hit ratio but remain correct.
- Redis outage increases PostgreSQL read load while leaving booking authority
  unchanged.

## Rejected alternatives

- Cache-authoritative quantity reservation: rejected because it cannot commit
  physical segment assignments with PostgreSQL domain state.
- Stale-if-error by default: rejected because an unboundedly old count is a poor
  operational signal and is not needed for booking continuity.
- Synchronous invalidation inside booking: rejected because Redis is not part
  of the booking transaction and must not decide command success.
