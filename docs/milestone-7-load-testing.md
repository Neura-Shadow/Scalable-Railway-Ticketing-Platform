# Milestone 7 Load and Recovery Testing

Status: `not_run` until the canonical project-scoped evidence runner completes.

The source-inventory fixed point excludes exactly this publication document
and `docs/benchmark-report-milestone-7.md`, so measured bundle values can be
published without invalidating the run that produced them. The verifier
rejects any broader exclusion; code, topology, tests, PRD, and runner inputs
remain source-digest bound.

The ten bounded k6 modules exercise provider contract behavior, settlement
import, whole-ticket partial refunds, idempotency, webhook commit failure and
key rotation, regional failover, payment/refund during failover, and regional
failback. Each module has finite VUs, iterations, duration, fixture count, and
static allowlisted metric tags. Provider object IDs, ticket IDs, customer IDs,
URLs, signatures, and credentials are never metric labels.

The suite measures provider classification, settlement rate/lag/mismatches,
refund latency/duplicates, webhook durable-ack/retry/outage, replication lag,
RTO/RPO, stale-region rejections, saga and ticket recovery, pool pressure,
unexpected 5xx, duplicate financial/ticket effects, ledger imbalance, and DR
reconciliation mismatch count.

The settlement module fails unless it observes a positive durable
successful-import counter and a non-negative durable settlement-lag sample
from the reconciler while the settlement worker remains healthy. Its rate is
the observed imported-record count
divided by the evidence driver's bounded end-to-end convergence window; the k6
summary and final evidence bundle retain the record count, derived rate, and
average lag. These are bounded-run observations, not sustained-throughput or
capacity claims.

Passing requires zero duplicate charge/refund/ticket and double seat release,
a balanced operational ledger, visible settlement mismatches, exactly one
writer region, no old-region write after promotion, recovered payment/ticket
state, independently booted backup restore, and failback from reseeded data.
Reported values describe only the bounded disposable topology; they are not a
production capacity, SLO, zero-RPO, or zero-RTO claim.
