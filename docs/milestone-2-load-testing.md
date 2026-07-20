# Milestone 2 Load Testing

The eight k6 scenarios in `loadtest/k6` exercise the Milestone 2 control and
booking paths. They contain no credentials, admission tokens, passenger IDs, or
idempotency keys. All such values are injected through environment variables.
Use synthetic disposable accounts and passengers only.

## Evidence levels

- **Syntax validation** proves only that the script parses.
- **Functional smoke** proves a small flow and selected invariants in one
  environment.
- **Concurrency evidence** uses controlled replica counts, policy bounds, and
  post-run reconciliation.
- **Sustained capacity evidence** requires a documented host, duration,
  dataset, dependency telemetry, repeated runs, and accepted results.

A smoke pass is not a throughput or production-sizing result. No script may be
used to claim national-scale or 12306-equivalent capacity.

## Setup contract

Every scenario validates its required environment in `setup()` and refuses to
run when its fixture contract is incomplete. Prepare:

- a live API URL, synthetic JWTs, canonical train-run UUIDs, station codes,
  seat class, and passenger fixtures;
- an enabled hot policy initialized by an admission worker;
- distinct identities for multi-user admission scenarios;
- one separately admitted token per independent booking attempt;
- a known already-committed reservation for the replay scenario; and
- a clean baseline for quotas, outbox/DLQ, Redis, PostgreSQL connections, and
  reconciliation.

Never put real values in command history on shared hosts. Prefer a short-lived
secret-injection wrapper or CI secret store and clear the process environment
after the run.

## Scenarios

| Script | Primary evidence | Important environment |
|---|---|---|
| `waiting-room-join.js` | Join RPS, latency, duplicate/capacity responses | `BASE_URL`, `CUSTOMER_TOKEN`, run/route/class/count |
| `waiting-room-status.js` | Status RPS and latency | `ENTRY_IDS`, owning `CUSTOMER_TOKEN` |
| `hot-train-admission.js` | Bounded join-to-issue wait and one-time header delivery | `CUSTOMER_TOKENS`, policy request fields |
| `hot-train-reservation.js` | Reservation p50/p95/p99 and bounded conflicts | `RESERVATION_CASES_JSON` |
| `admission-idempotency.js` | Same-token/key replay converges on one durable ID | committed `EXPECTED_RESERVATION_ID` and matching credential |
| `reservation-quota.js` | Concurrent durable quota never exceeds configured hold bound | admitted case matrix, `MAX_ACTIVE_HOLDS` |
| `redis-outage.js` | Hot fail-closed and non-hot PostgreSQL path | externally stopped Redis, `CONFIRM_REDIS_IS_DOWN=yes` |
| `multi-replica-hot-train.js` | Shared duplicate state and load-balancer distribution | multi-replica LB URL and distinct customer tokens |

`RESERVATION_CASES_JSON` is an environment-provided JSON array. Each object
contains `customer_token`, `admission_token`, `idempotency_key`, and a
`passenger_ids` array. Do not save the expanded value to the repository or
benchmark report.

Example with placeholders only:

```powershell
$env:BASE_URL = 'http://localhost:8080'
$env:TRAIN_RUN_ID = '<synthetic-train-run-uuid>'
$env:ORIGIN_CODE = 'AAA'
$env:DESTINATION_CODE = 'BBB'
$env:SEAT_CLASS = 'standard'
$env:CUSTOMER_TOKEN = '<injected-synthetic-jwt>'
k6 run loadtest/k6/waiting-room-join.js
```

## Bounded local multi-replica evidence

The repository includes an isolated PowerShell harness for the local evidence
topology. It creates a uniquely named Compose project, loads only the committed
synthetic fixture, provisions disposable customers without printing their
credentials, runs k6 inside the Compose network, injects the shared-entry API
termination, a separate API termination during an actual booking transaction,
one worker termination, and a real bounded Redis stop/restart. During the Redis
outage it proves an enabled-hot join returns bounded fail-closed guidance while
a clean non-hot reservation still creates exactly one PostgreSQL reservation.
It then restores Redis, waits for API and both workers, runs hot and non-hot
seat reconciliation plus quota/admission reconciliation, writes sanitized
artifacts under the operating-system temporary directory, and removes the
project and its volumes by default. The load balancer and the direct `api-1`
booking probe use Compose-assigned ephemeral loopback ports, so parallel or
unrelated local services do not compete for a fixed host port.

Nginx may report a comma-delimited `$upstream_addr` retry chain. The harness
counts only the final successful address, requires exactly three addresses in
the initial phase, exactly two while one replica is stopped, and exactly three
in a separate post-restart probe within a 30-second bounded recovery window.
Phase-local sets allow a restarted container to receive a new private address
without being miscounted as a fourth replica.
It also checks both surviving services' `/readyz` endpoints directly before
the load-balancer probe. Retry-chain strings are never counted as additional
replicas.

The bounded evidence Nginx configuration deliberately avoids upstream
connection reuse so each `X-Upstream-Addr` value represents a fresh
round-robin decision. Production ingress balancing and connection-pooling
policy remain deployment concerns. The evidence image is digest-pinned and
sets `keepalive 0` explicitly because recent Nginx releases otherwise enable a
local upstream keepalive cache by default. Its passive failure window is fixed
at 10 seconds; the harness allows up to 30 seconds for exact three-replica
recovery. A shared upstream zone and Docker DNS resolver let the proxy discover
a restarted Compose container even if its private address changes. The proxy
image upgrades the pinned slim base packages during its build, and CI scans the
resulting image for fixed Critical/High findings.

Run from the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-milestone-2-multi-replica-evidence.ps1
```

The default is a 30-VU, 30-second duplicate-join steady-state smoke followed by
a 10-VU, 15-second status smoke. The harness first proves the policy generation
initialized, then pauses both admission workers during these two read-only
queue phases so repeated status/join requests cannot claim and discard a
one-time admission credential. It is a **functional smoke**, not a capacity benchmark. Use
`-SteadyStateDuration 2m` or a different bounded `-CustomerCount` only when the
host and disposable dataset are recorded. The multi-replica k6 scenario keeps
its iteration-based mode when `DURATION` is unset and switches to
`constant-vus` only when `DURATION` is supplied.

The harness asserts and records:

- all three API upstreams are reached without sticky sessions;
- after one API stops, both surviving replicas return the same shared entry;
- while a distinctive bounded PostgreSQL session holds the synthetic train-run
  row, a request sent directly to `api-1` is observed waiting on that lock;
  `api-1` is then terminated with no grace period, the blocker is always
  terminated, and PostgreSQL is checked for zero reservation, seat,
  idempotency, outbox, and inventory residue;
- after the five-second processing lease has a bounded recovery interval, the
  exact same in-memory admission token, idempotency key, identity, and request
  are retried through the shared load balancer, commit one reservation, and
  replay the same durable identifier without another row, seat bit,
  idempotency record, or outbox event;
- one entry exists per disposable customer despite sustained duplicate joins;
- the two workers never exceed the fixture's global five-per-second admission
  rate or five-token inflight bound;
- after one worker stops, the survivor continues admitting as capacity is
  released, and the stopped worker returns ready after restart;
- each delivered token creates exactly one durable reservation;
- with Redis stopped, an enabled-hot join returns `503` with bounded
  `Retry-After`, the expected hot response is not counted as an unexpected
  healthy-state 5xx, and the hot probe creates no PostgreSQL reservation;
- during the same outage, one clean non-hot request creates exactly one
  PostgreSQL-authoritative reservation without an admission token;
- after Redis restarts, both admission workers return ready and hot/non-hot
  seat-inventory, reservation-quota, and admission-state reconciliation pass;
  and
- bounded PostgreSQL, Redis persistence/statistics, outbox/DLQ, API metrics,
  worker metrics, join/status/Redis-outage k6 summaries, and Compose topology
  snapshots are retained outside git.

If `-KeepEnvironment` is intentionally used, or cleanup reports a failure, use
the exact project name printed by the harness:

```powershell
docker compose -p <printed-project-name> -f docker-compose.multi-replica.yml down -v --remove-orphans
```

Do not commit the generated evidence directory. Admission credentials,
customer JWTs, passenger identifiers, and idempotency keys remain in process
memory only, are never written to evidence, and are cleared before the script
exits. The booking blocker has a bounded database sleep and an independent
`finally` cleanup path.

## Required telemetry

Capture, without sensitive labels:

- join and status RPS;
- queue depth, issue rate, and inflight count;
- wait and reservation p50/p95/p99;
- token conflicts, quota rejects, local backpressure rejects, and seat
  conflicts;
- unexpected 5xx;
- PostgreSQL connections, lock waits, and transaction latency;
- Redis command latency/errors and persistence status;
- outbox backlog and DLQ growth; and
- final seat, quota, and admission reconciliation.

The API metrics endpoint uses bounded labels only. Queue depth and connection
state may come from deployment telemetry rather than the k6 client.

## Run order and correctness gates

1. Record commit SHA, configuration, replica counts, dataset size, and machine
   resources.
2. Apply migrations twice and prove clean version 6.
3. Start the multi-replica topology and prove readiness.
4. Record baseline metrics and reconciliation.
5. Run a short functional smoke for each script.
6. Run concurrency scenarios separately so one fixture does not contaminate
   another.
7. Conduct the planned Redis/API/worker termination experiments.
8. Restore dependencies, wait for bounded recovery, and run reconciliation.
9. Compare final outbox/DLQ/backlog and PostgreSQL/Redis state with baseline.
10. Enter only observed results in
    [benchmark-report-milestone-2.md](benchmark-report-milestone-2.md).

Acceptance requires zero unexpected healthy-state 5xx, no duplicate active
entry, no wrong-owner/request token acceptance, no configured admission or
inflight breach, no durable quota breach, no overlapping seat allocation, a
bounded queue, and clean reconciliation. A threshold pass is necessary but not
sufficient; post-run server evidence is authoritative.
