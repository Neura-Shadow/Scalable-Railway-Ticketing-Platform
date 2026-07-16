# ADR 008: UTC Instants and Operator-Local Service Dates

- Status: Accepted
- Date: 2026-07-15

## Context

Railway services are marketed by an operating date in a local timezone, while routes may continue after midnight. Reservation expiry and event/lock timestamps require unambiguous instants.

## Decision

- Store all instants as PostgreSQL `timestamptz` and operate in UTC at application/database connections.
- Store `train_runs.service_date` as a date interpreted in the route/operator IANA timezone.
- Store each station's IANA timezone for display and validation, but a Milestone 1 route has one configured operating timezone that defines its service date.
- Store route-stop arrival/departure offsets as non-negative minutes from the service-date origin. Offsets are non-decreasing and may exceed 1,440 for overnight or multi-day journeys.
- Materialize `scheduled_departure_at` as the first stop's UTC departure instant when creating the train run, and reject a caller-supplied instant that does not equal `service_date + first departure offset` in the route timezone. Later arrival/departure instants add the difference from that first departure offset.
- Reject ambiguous/nonexistent local departure times caused by daylight-saving transitions unless an explicit offset-disambiguation policy resolves them. The default conservative policy is to reject ambiguous schedules during creation.
- Use an injected Clock for application comparisons and deterministic tests. A command captures one `now` value per use case. Database claims and transitions use a transaction-consistent database timestamp when the predicate itself must be authoritative.

Hold expiry is an absolute UTC instant. Confirmation requires the locked reservation to remain `held` and PostgreSQL `clock_timestamp() < expires_at`. Expiration requires `held` and PostgreSQL `clock_timestamp() >= expires_at`; application clocks schedule work but do not decide the authoritative transition.

## Consequences

- Overnight route offsets remain ordered without changing the service date after midnight.
- APIs can present station-local times while persisting unambiguous instants.
- Timezone database updates can affect future materialization; the stored UTC schedule preserves already created train-run instants.
- Deterministic clocks eliminate arbitrary sleeps from lifecycle tests.

## Rejected alternatives

- Store local timestamps without timezone: rejected as ambiguous.
- Derive service date from UTC date: rejected because it does not match railway operations.
- Wrap offsets at midnight: rejected because ordering and journey duration would be lost.
