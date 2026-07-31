# Milestone 5: Physical PostgreSQL Shard Pilot and Online Rebalancing

Status: Planned for implementation on `feat/milestone-5-physical-shard-rebalancing`

Target: Milestone 5

Last updated: 2026-07-29

## Problem Statement

Milestone 4 proves train-run routing, monotonic fencing, migration, and rollback
inside one PostgreSQL database. Its logical schemas still share a transaction
manager, cross-schema foreign keys, connection pool, failure domain, and atomic
commit. Those properties cannot be assumed after booking authority is moved to
independent PostgreSQL instances.

Milestone 5 must prove a bounded physical-shard design without disguising
partial failure as a distributed transaction. A selected train run must have
exactly one physical writer, stale routers must be rejected by a database-local
fence, and booking correctness must survive loss of atomicity between the
control database and the authoritative booking shard. Online migration means
that a source keeps serving during base copy and journal catch-up; final
cutover still has a measured, bounded write pause.

The milestone is a reversible, single-region pilot using synthetic evidence.
It is not production-certified sharding, zero-downtime rebalancing,
multi-region active-active booking, distributed serializability, exactly-once
distributed processing, or national-scale capacity evidence.

## Solution

Run one control PostgreSQL instance and two independent booking PostgreSQL
instances. Keep global identity, offering, search/read-model, assignment,
booking-command, quota-lease, reservation-directory, and migration-control
state in the control database. Put all state needed for one booking transaction
in the selected physical booking shard, including local reference snapshots,
inventory, reservations, tickets, idempotency execution, command receipts,
outbox intent, write fences, migration capture, apply receipts, and local
reconciliation state.

Extend the current router with an allowlisted connection registry. The catalog
stores a bounded `connection_ref`, never a DSN. Configuration maps approved
references to bounded `pgxpool` instances. A validated route and `ShardHandle`
carry only opaque shard identity, storage kind, generation, protocol/schema
versions, health, and the approved pool.

Replace same-database create-reservation atomicity with a durable command saga:
reserve an idempotent command, conservative quota lease, globally unique
reservation ID, and pending directory in control; execute exactly once on the
physical shard using a local command receipt; then finalize control state.
Shard commit is authoritative proof. Delayed control finalization is repaired
idempotently and never repeats seat mutation.

Move train runs with source-local mutation capture. Enable capture before a
bounded, resumable base copy; replay journal entries idempotently through target
apply receipts while source writes continue; validate source and target beyond
row counts; then drain, fence the source, catch up to zero lag, validate again,
enable the target under a newer generation, and switch the control assignment.
The order may create a bounded zero-writer interval but never two writers.

Retain the source read-only for a rollback window. Direct rollback is allowed
only when target-write evidence is verifiably zero. Any successful target write
requires the same migration protocol in reverse with a new migration ID and a
newer generation. Source cleanup is explicit, guarded, resumable, and never
automatic.

## Actors and Journeys

### Customer booking-command journey

1. Authenticate the current user and validate passenger ownership in control.
2. Resolve the authoritative train-run assignment; no request value selects a shard.
3. Create or replay the global booking command from a hashed idempotency identity and request fingerprint.
4. Acquire a conservative quota lease, reserve a globally unique reservation ID, and create a pending directory entry in one control transaction.
5. Route to one validated physical `ShardHandle`.
6. In one local shard transaction, validate route generation, local fence, migration permission, and booking snapshot; acquire a unique command receipt; allocate inventory; write reservation state and local outbox intent; commit the successful receipt.
7. In a second control transaction, finalize the command, activate the directory, and convert the pending quota lease to an active hold.
8. On retry, converge from the global command and shard receipt to the same reservation without repeating seat mutation.

### Reservation lookup journey

1. Resolve reservation ID to train run and owner through the stable global directory.
2. Enforce owner access before returning protected data.
3. Resolve the train run's current assignment; treat stored shard/generation fields only as refreshable hints.
4. Read exactly one authoritative shard and verify local existence.
5. Never scan all shards when the directory is missing or stale.

### Global quota journey

1. Lock the user's control-plane quota scope.
2. Count both pending and active leases against configured limits.
3. Acquire exactly one lease for one command before shard execution.
4. Convert it after a durable shard receipt or release it only after failure is proven.
5. Permit conservative false rejection during uncertainty; never undercount and never release based only on Redis.

### Shard-failure journey

1. Fail only affected train-run writes with a bounded topology-neutral response.
2. Keep the healthy booking shard and global search/read model available.
3. Preserve uncertain quota and directory state until the reconciler can prove the shard outcome.
4. Never route to another shard randomly or from Redis alone.

### Base-copy and journal journey

1. Validate source authority and target compatibility.
2. Bootstrap target booking snapshots and a disabled target fence.
3. Enable source-local capture and record the journal start sequence.
4. Copy the approved train-run boundary in deterministic bounded batches while source writes continue.
5. Replay bounded journal batches through unique target apply receipts.
6. Persist copy/replay checkpoints only after the target commit is durable.
7. Stop safely on source, target, protocol, schema, or validation failure.

### Cutover journey

1. Mark assignment draining and return retryable migration responses for new writes.
2. Wait for in-flight work using bounded database-visible mechanisms.
3. Disable the source fence and record durable evidence.
4. Record the final source journal sequence, replay to zero lag, and run final validation.
5. Enable the target fence with a newer generation.
6. Switch the control assignment and emit control-plane cutover intent.
7. Rotate route and availability generations, then enter the rollback window.

### Rollback and reverse-migration journey

- Before control switch, disable any target fence and re-enable source only with a newer generation.
- After control switch, allow direct rollback only when target writes, receipts, and outbox evidence are all verifiably absent.
- After any target write, plan a reverse migration from the current target to the retained source, repeat capture/copy/catch-up/quiesce/validation, and cut over under a newer generation.
- Execute one reverse migration after successful target writes in a disposable acceptance environment.

### Worker journey

- The hold-expirer resolves the current assignment and handles each shard in bounded batches.
- The outbox worker enumerates configured shard handles fairly; one failed shard cannot starve another.
- The read-model worker consumes at-least-once events and reloads current routed state.
- The admission worker remains queue control, not shard authority.
- The booking-command reconciler finalizes control state from shard receipts and never mutates seat inventory.

### Degraded-read journey

- Search remains a global control-database projection and never fans out booking shards.
- Availability resolves the current physical assignment and rejects old-generation cache envelopes.
- Cached availability never authorizes booking.
- Control-database loss fails new commands closed; explicitly documented shard-local reads may continue only where ownership can still be checked safely.

### Operator journey

- Operators use `physical-shard-admin` with bounded output, dry-run, explicit confirmation for destructive actions, and nonzero failure exits.
- Commands cannot expose DSNs, credentials, passenger PII, raw payloads, or unrestricted SQL.
- Migration, rollback, reverse migration, repair, reconciliation, and cleanup revalidate all durable prerequisites and are idempotently resumable.

## User Stories

1. As a customer, I want one authoritative physical shard per train run so overlapping seat allocations cannot commit on different databases.
2. As a customer, I want the same idempotency identity to return one reservation after timeouts, retries, and control-finalization failure.
3. As a customer, I want a different request fingerprint with the same identity rejected.
4. As a customer, I want shard topology hidden from requests, responses, logs, and tokens.
5. As a customer, I want retryable migration rejection to preserve my admission token and avoid leaking quota.
6. As a customer, I want reservation lookup to use one stable directory and never scan every shard.
7. As a customer, I want healthy-shard bookings to continue when another physical shard is unavailable.
8. As an API replica, I want stale routes rejected by the destination database before any mutation.
9. As an API replica, I want one bounded refresh retry and no random fallback.
10. As a booking repository, I want a `ShardHandle` that encapsulates the approved pool, route, generation, and local transaction boundary.
11. As a booking repository, I want all booking-critical reference state local so a shard transaction never synchronously reads control.
12. As a control-plane operator, I want connection references allowlisted and DSNs supplied only from secrets/configuration.
13. As a control-plane operator, I want total connection-pool demand validated before startup.
14. As a control-plane operator, I want every global command ID and resource ID collision-safe across databases.
15. As a quota operator, I want pending work counted conservatively so physical sharding cannot bypass user limits.
16. As a quota operator, I want proven shard failure to release its pending lease idempotently.
17. As a recovery operator, I want a committed shard receipt to finalize control state without changing inventory again.
18. As a recovery operator, I want unknown shard outcomes retained as uncertainty rather than misclassified as failure.
19. As a railway operator, I want cancellation, fare, seat-status, and booking-policy changes enforced locally before the global projection announces them.
20. As a railway operator, I want source writes to continue during bounded base copy and journal catch-up.
21. As a railway operator, I want journal capture in the same source transaction as every train-run mutation.
22. As a railway operator, I want target replay idempotent under duplicate and out-of-order delivery attempts.
23. As a railway operator, I want validation to compare identities, masks, relationships, fingerprints, snapshots, receipts, outbox, and fences, not just row counts.
24. As a railway operator, I want final write-pause duration measured without a zero-downtime claim.
25. As a railway operator, I want source disabled before target enable and target enabled before assignment switch.
26. As a railway operator, I want every cutover crash window to have at most one writer and a deterministic recovery path.
27. As a railway operator, I want successful target-write evidence to forbid a direct mapping rollback.
28. As a railway operator, I want reverse migration to preserve all target-era writes under a newer generation.
29. As a railway operator, I want source data retained read-only and cleanup explicitly confirmed after the rollback window.
30. As a railway operator, I want source/target copy and replay resumable after process or database failure.
31. As an outbox operator, I want booking intent committed on the authoritative shard and relayed fairly with at-least-once semantics.
32. As a read-model operator, I want global projections and cache generations correct across forward and reverse cutover.
33. As a worker operator, I want one failed shard unable to starve healthy shard work.
34. As a security reviewer, I want malicious catalog rows unable to inject endpoints, DSNs, SQL identifiers, or metric labels.
35. As a security reviewer, I want command fingerprints checked and raw idempotency keys and passenger PII absent from receipts and journals.
36. As a security reviewer, I want admin fanout, retry counts, pool counts, and output accumulation bounded.
37. As a platform operator, I want liveness independent of one shard and readiness to report bounded degraded status without topology secrets.
38. As a platform operator, I want each process to receive only the database credentials it actually needs.
39. As a migration operator, I want separate control and booking-shard migration histories with dirty/version checks.
40. As a migration operator, I want incompatible shard schema or protocol versions to fail closed before writes.
41. As a reviewer, I want real three-PostgreSQL integration evidence for isolation, sagas, fencing, copy, cutover, outage, and reverse migration.
42. As a reviewer, I want deterministic barriers and failure hooks instead of arbitrary sleeps.
43. As a reviewer, I want race, security, migration, Compose, container, and all prior-milestone regressions green.
44. As a reviewer, I want bounded load evidence with honest host limits and no production-capacity extrapolation.
45. As a maintainer, I want legacy and logical-schema paths preserved while selected train runs opt into physical storage.
46. As a maintainer, I want the modular monolith retained rather than replaced by network microservices.
47. As a maintainer, I want detect-only reconciliation by default and no automatic seat repair.
48. As a release owner, I want a reviewed, non-draft, green PR opened but not merged and no tag created.

## Implementation Decisions

### Deep modules

- **Physical connection registry:** parses bounded configuration, maps approved connection references to lazily initialized pools, enforces per-pool and global budgets, and exposes sanitized health.
- **Physical shard router:** resolves control assignments to validated shard handles and performs exactly one stale-route refresh without exposing DSNs or SQL identifiers.
- **Shard-local transaction gate:** validates local snapshot, storage identity, generation, assignment epoch, migration phase, and write enablement in the same transaction as mutation.
- **Booking command coordinator:** owns control command, quota lease, and pending directory creation/finalization without pretending that shard execution is atomic with control.
- **Booking command reconciler:** classifies durable shard receipts and repairs only control state; it never edits inventory.
- **Booking snapshot service:** installs versioned train-run, seat/coach, fare, and policy snapshots idempotently and fails booking closed on missing/stale state.
- **Physical migration engine:** owns explicit durable states, bounded checkpoints, copy, replay, validation, final quiesce, ordered cutover, rollback eligibility, and reverse migration.
- **Mutation capture and apply modules:** capture resource identities transactionally at source and converge target rows idempotently through apply receipts.
- **Shard-local outbox enumerator:** polls control plus configured shard outboxes with bounded fair scheduling and duplicate-safe publication.
- **Cross-database reconciliation:** detects command/quota/directory/receipt, fence, inventory, outbox, journal, apply, and source/target divergence without automatic seat repair.

### Storage boundaries

- The control database owns users, passengers, global catalog and search/read model, hot-train policies, shard registry, assignments, command ledger, quota leases, reservation directory, migration control, control outbox, receipts, and global operational state.
- Each booking database owns local booking snapshots, inventory, reservations, reservation seats, orders, tickets, idempotency execution, command receipts, shard outbox, write fences, migration capture/journal/apply state, target-write evidence, and local reconciliation state.
- No transaction, foreign key, query callback, or hidden repository call spans control and a booking database.
- PostgreSQL booking state remains authoritative for seat allocation; Redis remains admission/cache control only.

### Identity and protocol

- Cross-database resource, command, event, migration, and externally referenced journal IDs use the existing globally unique UUID representation; generation and source-journal sequence remain scoped monotonic values.
- Global command uniqueness is `(owner_user_id, operation, idempotency_key_hash)` and the request fingerprint distinguishes replay from conflict.
- Shard receipts and target apply receipts commit with their local effects.
- Event delivery remains at least once; no exactly-once distributed claim is made.

### Migration and cutover

- Select an application-written, source-local mutation journal unless research or coverage analysis proves a database trigger is safer. Every mutation path must have a capture test.
- Reject source/target dual writes, generic workflow engines, XA, two-phase commit, and silent logical-replication adoption.
- Copy deterministic dependency-ordered batches and replay current source row state or tombstones; partial targets are never routable.
- Cutover ordering is source fence off, final replay/validation, target fence on, control assignment switch.
- A bounded zero-writer interval is acceptable and measured; two writers are never acceptable.

### Compatibility and rollout

- Physical sharding is disabled by default and requires explicit production opt-in.
- Existing legacy/logical routes continue through their proven adapter while physical routes use independent pools.
- The catalog stores storage kind and allowlisted connection reference, not credentials or network endpoints.
- Control migration history advances independently from booking-shard schema version 1.
- Source data is never deleted automatically.

## Security

- Treat catalog storage kind, shard ID, protocol/schema version, health, and connection reference as untrusted until validated against bounded configuration.
- Never log, persist in catalog, expose in public readiness, or label metrics with DSNs, passwords, hosts, ports, or connection references.
- Customer and operator HTTP input cannot create a shard, connection, or SQL identifier.
- Command IDs are cryptographically unguessable UUIDs; fingerprints are verified before replay.
- Admin commands require current operator authorization, bounded scopes, dry-run, and explicit confirmation for cutover, rollback, reverse migration, or cleanup.
- Mutation journals, receipts, metrics, and evidence contain no passenger PII, raw idempotency keys, JWTs, admission tokens, or database credentials.

## Observability

- Record bounded metrics for command state/recovery, quota/directory repair, route outcome, fence rejection, pool failure, migration phase/copy/replay/lag/validation/write-pause/cutover/rollback/reverse, and reconciliation mismatch.
- Labels are limited to bounded operation, result, reason, phase, shard ID, and storage kind.
- Health reports per-process dependencies and bounded shard IDs only.
- Evidence artifacts are strict canonical JSON or honestly named raw logs; accepted Milestone 4 benchmark values remain unchanged.

## Load Testing

- Measure route and command overhead, booking latency, quota and receipt latency, finalization repair, copy/replay rates, lag, write-pause duration, stale-route recovery, outage isolation, reverse-migration duration, connection counts, unexpected 5xx, and reconciliation.
- Use bounded synthetic workloads and record environment/host limits.
- Acceptance is correctness-first: no split brain, duplicate reservation, overlapping allocation, quota breach, missing writes, or failed reconciliation.
- Results do not certify production capacity or zero downtime.

## Testing Decisions

- Unit tests assert external state-machine, registry, fingerprint, quota, directory, receipt, fence, journal, and apply-receipt behavior.
- PostgreSQL integration tests use one control and two independent booking instances; they prove physical isolation and the absence of cross-database assumptions.
- Deterministic barriers inject failures before/after shard commit, control finalization, source fencing, target enable, assignment switch, replay checkpoint, and cleanup batches.
- Concurrency tests cover 100 same-idempotency requests, cross-shard global quota, snapshot races, stale replicas, and worker fairness.
- Migration tests cover fresh/repeated/dirty control and shard migrations, populated v8-to-v9 upgrade, compatible down/up rehearsal, and readiness version rejection.
- Acceptance executes a complete forward cutover, successful target-era writes, direct-rollback rejection, and a complete reverse migration.
- Full prior-milestone tests, `go test -race`, vet, staticcheck, govulncheck, actionlint, gitleaks, Trivy, Compose validation, image build/scan, and focused three-database CI remain required.
- Tests verify observable durable outcomes and invariants rather than private helper call order.

## Acceptance Criteria

- Control and two independent booking PostgreSQL instances run with separate data directories, migrations, health checks, and credentials.
- Connection references and pool budgets are bounded; catalog data cannot introduce a DSN or endpoint.
- Selected train runs route to physical shards while legacy/logical compatibility remains.
- Every physical write checks a database-local monotonic fence and booking snapshot in its transaction.
- The booking command saga, shard receipt, conservative quota lease, and stable directory converge safely after each partial-failure case.
- No control/shard transaction or foreign key crosses databases.
- All required booking state and reference snapshots are local to the authoritative booking shard.
- Mutation capture covers every approved mutation path and commits with source mutation.
- Base copy runs while source writes continue; replay is idempotent and target remains disabled.
- Online and final validation prove source/target equality and booking invariants beyond row counts.
- Final cutover has a measured bounded write pause and preserves at most one writer in every crash window.
- Target writes create durable generation-bound evidence and prohibit direct rollback.
- One reverse migration after successful target writes completes in disposable acceptance evidence.
- Source remains read-only through retention and cleanup is never automatic.
- One shard outage does not corrupt or block healthy-shard service.
- Read model, cache, admission, workers, outbox, and reconciliation remain correct.
- All prior tests, race detector, CI/security/container gates, and independent reviews pass with zero unresolved Critical or High findings.
- A non-draft mergeable PR is opened, not merged, and no tag or release is created.

## Data-Loss Boundaries and Pilot RPO/RTO

- RPO for an acknowledged booking is zero within the authoritative shard: the reservation, inventory mutation, receipt, idempotency result, and outbox intent commit locally together.
- Control finalization may lag; recovery uses the shard receipt and therefore may temporarily delay directory/quota visibility without losing the acknowledged booking.
- During an unresolved shard outage, the affected train run is unavailable rather than failed over to stale retained data.
- Pilot RTO is measured from failure injection to bounded recovery or explicit operator action; no production RTO is promised.
- Source and target backups, restore assumptions, journal retention, and recovery limits are recorded in runbooks and evidence.

## Out of Scope

- Milestone 6, payment, refund, ticket rescheduling, identity verification, or anti-bot platforms
- Zero-downtime claims, production certification, or national-scale capacity evidence
- Multi-region ownership, active-active writers, distributed serializable transactions, XA, or two-phase commit
- Kafka, Debezium, external CDC, Service Mesh, Kubernetes Operators, or a generic workflow engine
- Separate booking/router network microservices or splitting the modular monolith
- Source/target dual writes, automatic balancing/split/merge/autoscaling, or automatic source deletion
- User-, passenger-, or reservation-hash sharding
- Generic cross-shard joins, distributed foreign keys, arbitrary DSNs, unbounded shard discovery, or dynamic untrusted connections
- Redesign of PostgreSQL VARBIT segment-mask inventory

## Further Notes

- Online rebalancing in this PRD means online base copy and journal catch-up followed by bounded quiescence; it does not mean uninterrupted writes.
- The fixed two-booking-shard topology is intentionally small enough to audit connection, failure, and recovery behavior completely.
- Conservative pending control state may cause temporary false rejection. That is safer than quota undercount or a directory that asserts nonexistent data.
- The milestone ends after a green non-draft PR is opened. Merge and tagging require separate instructions.
