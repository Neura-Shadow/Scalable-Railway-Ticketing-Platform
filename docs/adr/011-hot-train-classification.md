# ADR 011: PostgreSQL Classifies Hot Train Runs

- Status: Accepted
- Date: 2026-07-18

## Context

Milestone 2 must apply a waiting room only to selected train-run and seat-class combinations. Redis must not decide whether admission is required: missing Redis state during an outage would then silently turn a protected run into an unprotected run.

Policy management also needs durable authorization, validation, audit events, and an ordering rule for changes that race reservation creation.

## Decision

Add the Admission bounded context inside the modular monolith. Admission owns the PostgreSQL-backed `HotTrainPolicy` aggregate, Redis-backed waiting-room control plane, admission-token lifecycle, and admission reconciliation. It is a module, not a network service.

`hot_train_policies` stores:

- `id`;
- `train_run_id`;
- `seat_class`;
- `enabled`;
- `version`;
- `redis_initialized_version`;
- `max_queue_size`;
- `admission_rate_per_second`;
- `max_inflight_admissions`;
- `admission_token_ttl_seconds`;
- `processing_lease_seconds`;
- `queue_entry_ttl_seconds`; and
- creation and update timestamps.

`(train_run_id, seat_class)` is unique. PostgreSQL foreign keys and bounded check constraints reject invalid state. Application validation uses the same limits:

| Setting | Accepted range |
|---|---:|
| `max_queue_size` | 1–100,000 |
| `admission_rate_per_second` | 1–10,000 |
| `max_inflight_admissions` | 1–10,000 |
| `admission_token_ttl_seconds` | 6–900 |
| `processing_lease_seconds` | 5–120 and less than token TTL |
| `queue_entry_ttl_seconds` | 60–86,400 |

The documented safe maximum for inflight admission is 10,000 per policy. A policy may select a lower operational bound after load testing.

Only authenticated `operator` or `admin` roles may list, create, update, or disable policies. Delete is a durable soft disable rather than row deletion. Updates use optimistic concurrency (`expected_version`) so concurrent operators cannot silently overwrite limits. Every create, update, and disable increments `version`, clears `redis_initialized_version`, and appends one bounded outbox event in the same PostgreSQL transaction:

- `hot_train_policy.created`;
- `hot_train_policy.updated`; or
- `hot_train_policy.disabled`.

PostgreSQL is authoritative for the enabled decision. An absent or disabled policy follows the existing non-hot booking flow and does not require an admission token. An enabled policy requires an acquired token for the current policy version.

Every reservation command performs two policy checks:

1. the application loads the policy before Redis token acquisition; and
2. the Booking transaction rechecks the PostgreSQL row after durable-idempotency replay resolution and before quota or inventory mutation.

The application passes a bounded admission decision containing the acquired policy ID and version into Booking. If the transaction sees a newly enabled or newer policy version, it rejects before inventory mutation. This closes the disabled-to-enabled activation race without making a Redis call inside a PostgreSQL transaction.

The final recheck and every policy mutation use one versioned PostgreSQL advisory-lock tuple derived from `train_run_id` and canonical `seat_class`. Booking takes the shared transaction-scoped mode, while policy create, update, and disable take the exclusive mode. The exclusive side covers the absent-row creation race that row locks cannot represent; the shared side allows same-scope bookings to remain concurrent rather than serializing them behind one another.

Redis state is generation-scoped by policy version. A worker installs only the PostgreSQL-current version with an atomic monotonic compare operation; an older version can never replace a newer marker. After successful first install, it records the matching `redis_initialized_version` durably. A missing Redis marker when PostgreSQL already records that version as initialized is treated as continuity loss, not a fresh bootstrap, and remains fail closed until an operator deliberately reopens a new generation.

A policy change starts a fresh generation; old entries and tokens become unusable and expire under bounded TTLs. This deliberate customer disruption keeps update semantics fail closed and avoids a blocking deletion of a large old generation. Policy mutations are separately rate-limited and record actor and correlation metadata in the audit/outbox envelope. The current mutation response returns the durable policy and new version only. A point-in-time queue/inflight impact preview is deliberately deferred: it requires a separate bounded read of the previous Redis generation with explicit partial-failure semantics. Operators currently have bounded issue/cancel/expiry counters and detect-only admission-state reconciliation for drift; neither is represented as a current-impact preview. Generation churn and stale/downgrade attempts are monitored.

Admission permits one booking attempt. It does not reserve a seat. PostgreSQL remains authoritative for train-run bookability, durable quotas, passenger ownership, fares, segment overlap, reservation state, tickets, idempotency, and outbox intent.

## Consequences

- Redis loss cannot disable a durable hot-train classification.
- Policy activation races fail before quota or seat mutation.
- A policy update or disable/re-enable cycle invalidates prior queue and token generations; customers may need to rejoin.
- Redis data loss cannot be mistaken for a harmless first initialization of an already active generation.
- Policy mutation stays auditable through the existing transactional outbox.
- Booking retains its current PostgreSQL transaction and `VARBIT` seat-allocation implementation.
- Admission remains a feature-owned deep module with customer, operator, worker, and Booking interfaces.

## Rejected alternatives

- Redis-only classification: rejected because missing state could bypass admission.
- Hard-delete policy: rejected because it weakens auditability and makes disable/re-enable races ambiguous.
- Reuse old entries across policy versions: rejected because changed rate, TTL, or inflight rules would have no safe deterministic interpretation.
- Blind last-write-wins policy updates: rejected because concurrent operators could silently weaken protection.
- Automatically recreate a previously initialized missing Redis generation: rejected because data loss would reset rate and inflight state while old booking work may still run.
- Call Redis from the Booking transaction: rejected because network I/O would extend locks and still would not be atomic with PostgreSQL.
- Extract Admission as a microservice: rejected because it adds a network seam without changing either Redis atomicity or PostgreSQL authority.
