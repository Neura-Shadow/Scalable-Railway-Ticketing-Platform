# Shard Failure Policy

## Principle

Ownership failures fail closed. Availability is reduced rather than allowing a
write whose current owner cannot be proven. A route cache, Redis value,
process-local observation, or previous success cannot replace the public
assignment and local fence checks inside PostgreSQL.

Because `legacy`, `shard-0`, and `shard-1` are schemas in one database, these
policies demonstrate logical routing isolation only. They do not establish
independent physical failure domains, physical-shard RTO/RPO, or database
failover behavior.

## Failure matrix

| Failure | Write behavior | Read/worker/admin behavior | Recovery condition |
|---|---|---|---|
| Catalog/control PostgreSQL unavailable | All new train-run mutations fail closed | API unready; no cached-route authorization | Catalog lock/read and schema-version checks pass |
| Stale assignment generation | Mutation rolls back before any booking side effect | Refresh authority once, retry once, then stop | Current route succeeds or caller receives bounded retryable error |
| Unknown/disabled shard | No fallback or probing | Bounded internal failure; topology hidden | Fixed catalog/config repaired and revalidated |
| One logical storage degraded | Assigned requests fail boundedly | Healthy assignments continue where shared DB permits; admin reports partial | Storage and fence health pass |
| Shared PostgreSQL outage | All authoritative booking writes fail | Projection/cache may serve only documented hints; no speculative booking | Primary and control/schema checks pass |
| Target unavailable before cutover | Source remains authoritative | Migration resumes or fails safely | Target restored, copy/validation rerun |
| Source unavailable before validation | No target promotion | Preserve partial target and incident evidence | Source restored and full validation passes |
| Cutover timeout/failure | Atomic transaction rolls back | Source assignment remains visible; no partial locator switch | Preconditions and bounds pass on retry |
| Source unavailable after cutover | Target remains authoritative | Direct rollback evidence/source audit may be unavailable | Restore source for audit; do not demote target blindly |
| Redis read-cache outage | No ownership change | Milestone 3 bounded PostgreSQL fallback where defined | Redis recovery creates/uses safe namespace |
| Redis admission outage | No ownership change | Enabled hot-run admission remains fail closed | Continuity and policy-generation checks pass |
| Worker timeout/crash | No authority change | Other bounded shard work may continue; cursor/lease enables retry | Worker readiness and reconciliation pass |
| Reconciliation mismatch | Stop affected writes by incident policy | Preserve evidence; no automatic repair | Reviewed repair and clean rerun |

## Public behavior

Requests assigned to unavailable, fenced, or migrating storage return a safe
bounded response, normally a `503`-class result with an allowlisted
`Retry-After`. Public output does not include shard ID, schema, generation,
migration ID, database host, DSN, SQL, or whether another logical shard remains
healthy.

Retries are bounded. The application does not create an in-memory queue, loop
until migration ends, or retry another storage without a fresh authoritative
lookup.

## Health policy

- `/livez` reports process responsiveness and remains independent of optional
  logical-storage health.
- API `/readyz` requires catalog/control PostgreSQL, the expected Migration 8
  schema, valid fixed topology/configuration, compatible fencing protocol, and
  the documented minimum serving set.
- Catalog loss makes the API unready because no mutation can prove ownership.
- One optional logical storage may leave the API ready but explicitly
  degraded, while requests assigned there fail safely.
- Each worker checks only its owned dependencies/configuration and exposes
  private `/livez`, `/readyz`, and `/metrics` endpoints.
- Health detail is bounded and sanitized; it does not expose physical or schema
  topology.

Readiness is a serving gate, not a reconciliation or capacity claim. A backlog,
migration in progress, or one partial operator result is generally an alert,
not proof that the process is dead.

## Migration failure rules

- Source remains authority until atomic cutover commits.
- Partial target copies are never routable.
- A timeout, cap breach, cancellation, or mismatch does not enable target.
- No error path enables both source and target fences.
- After a target write, source recovery cannot authorize a direct mapping
  rollback; a reverse migration is required.
- Cleanup is never an automatic recovery action.

## Observability

Use bounded operation, result, reason, phase, and fixed shard-ID labels. Never
label or log train-run, reservation, ticket, user, passenger, migration,
generation, schema, host, DSN, idempotency, admission-token, or cache-key data.

Alert on catalog/fence rejection, logical-storage unavailability, repeated
stale refresh, migration timeout/failure, target-write rollback rejection,
fanout partial status, reconciliation mismatch, PostgreSQL lock/deadlock/pool
pressure, and unexpected customer 5xx.

See [ADR 034](adr/034-shard-failure-and-degradation.md) and
[production deployment](production-deployment.md).
