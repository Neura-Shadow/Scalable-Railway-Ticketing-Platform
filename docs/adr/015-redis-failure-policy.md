# ADR 015: Hot-Train Admission Fails Closed When Redis Is Unavailable

- Status: Accepted
- Date: 2026-07-18

## Context

Redis owns the ephemeral waiting-room and admission control plane for an enabled hot-train policy. A missing queue, stale policy version, or unavailable token record cannot prove that a customer was admitted. Allowing the request to continue in that state would let a Redis incident bypass the waiting room and transfer an uncontrolled burst to PostgreSQL.

Redis is not seat-inventory authority. PostgreSQL remains authoritative for train-run bookability, durable quotas, reservation state, segment overlap, and seat allocation. Redis loss therefore has different consequences for admission continuity and inventory correctness.

ADR 006 deliberately permits the existing reservation-create rate limiter to fail open because PostgreSQL still enforces the Milestone 1 booking invariants. That policy does not answer whether a booking may bypass an enabled Milestone 2 waiting room.

## Decision

The PostgreSQL `hot_train_policies` record determines whether admission is required. An enabled policy may not be treated as disabled merely because its Redis state is missing.

For an enabled hot-train policy, all of the following fail closed:

- joining the waiting room;
- reading, cancelling, or otherwise mutating a waiting-room entry;
- issuing or reclaiming admissions;
- acquiring, releasing, or finalizing an admission token; and
- observing a missing or stale Redis policy version when the operation requires the current version.

Redis timeout, connection failure, script failure, missing required state, and policy-version mismatch return a bounded `503 Service Unavailable` response with a bounded `Retry-After`. The application does not create a substitute in-memory queue, bypass admission, or retry without a finite attempt and time budget.

For a train run and seat class with no enabled PostgreSQL policy, the existing Milestone 1 booking path remains available. Its existing reservation rate limiter and documented fail-open behavior remain unchanged. A Redis failure therefore does not turn a non-hot reservation into a hot one and does not make Redis a universal prerequisite for PostgreSQL booking.

PostgreSQL failure makes reservation processing and readiness fail. A token transitions to `consumed` only after the durable reservation transaction commits. If PostgreSQL fails before commit, the application safely releases `processing` back to `issued` when possible or relies on the bounded processing lease. If PostgreSQL commits and Redis finalization fails, the durable idempotency record is authoritative; a same-identity retry replays the reservation and repairs finalization. The application never reports a token as consumed unless a durable booking exists.

Complete Redis data loss does not change PostgreSQL seat masks, reservations, or tickets, so it cannot create an overlapping allocation. It can, however, lose:

- queue order and active queue entries;
- issued tokens and one-time token delivery state;
- inflight and admission-rate windows; and
- processing leases.

Each policy records the last successfully initialized Redis version in PostgreSQL. Redis stores the matching policy-generation marker and a continuity sentinel. A missing marker/sentinel for a version PostgreSQL already records as initialized is treated as data loss; workers do not silently recreate it under the same version because doing so would reset inflight and rate state while old PostgreSQL work may still execute.

After detected total loss, enabled hot policies remain fail closed until Redis is healthy and an operator deliberately opens a new policy generation after the prior maximum token/processing window has drained or been accounted for. A fresh generation receives a new marker and resets the durable initialized version only through the policy workflow. Affected customers must rejoin. The system does not reconstruct ephemeral entries or tokens from PostgreSQL and does not claim perfect queue continuity.

Production deployment requires Redis AOF persistence with an explicitly reviewed durability policy, or a managed Redis offering with equivalent persistence and restore procedures. Persistence reduces routine restart loss; it is not treated as a correctness proof or a guarantee that the waiting room survives every disaster.

## Consequences

- A Redis incident can temporarily make an enabled hot train unavailable for joining and booking, which is accepted to prevent an admission bypass.
- Non-hot PostgreSQL booking remains independent of the waiting-room control plane.
- Redis restart or total loss may require customers to rejoin, but PostgreSQL inventory remains correct.
- Readiness and alerts can distinguish Redis health, policy-version readiness, and PostgreSQL health without treating queue depth as a readiness failure.
- Detect-only admission reconciliation reports both an absent current continuity key and a missing or mismatched shared policy-version/current-continuity marker pair for every initialized enabled policy.
- Operators must provision, monitor, back up, and test recovery of Redis persistence.
- Recovery alerts distinguish ordinary Redis unavailability from a missing continuity sentinel or generation marker.

## Rejected alternatives

- Fail open for enabled hot trains: rejected because it removes the burst-control seam precisely during a control-plane incident.
- Infer hot/non-hot status from Redis alone: rejected because missing Redis state could silently disable a durable PostgreSQL policy.
- Persist every queue entry in PostgreSQL: rejected because it would put ephemeral burst traffic on the inventory authority and still would not make token transitions atomic with Redis admission.
- Move seat allocation into Redis: rejected because Redis recovery and eviction are not PostgreSQL reservation transactions.
- Claim lossless queue recovery from AOF: rejected because persistence configurations and disaster modes have non-zero data-loss windows.
