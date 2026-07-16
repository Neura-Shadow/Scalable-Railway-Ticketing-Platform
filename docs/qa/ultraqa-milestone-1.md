# Milestone 1 UltraQA Evidence

Date: 2026-07-16
Branch: `feat/milestone-1-core-ticketing`
Scope: production-minded single-region Milestone 1 only

## Decision

The implemented Milestone 1 correctness and delivery gates pass locally. Independent domain, architecture/operations, and security review lanes report zero unresolved Critical or High findings. This evidence does not establish sustained capacity, multi-region behavior, or national-scale throughput.

## Verification matrix

| Lane | Command or scenario | Result |
|---|---|---|
| Formatting and module graph | `gofmt`, `go mod tidy`, `git diff --check` | Passed |
| Unit and integration | `go test ./... -count=1 -timeout 300s` with PostgreSQL and Redis | Passed |
| Race detector | `go test -race ./... -count=1 -timeout 420s` | Passed |
| Static analysis | `go vet ./...`, `staticcheck ./...` | Passed |
| Go vulnerability reachability | `govulncheck ./...` | 0 reachable vulnerabilities |
| Migration, fresh database | `up`, second `up`, `version` | Passed; version 5, dirty false |
| Migration, upgrade database | Existing branch version 4 with data -> version 5 | Passed; backfill, old-writer bridge, and existing-data preflight verified |
| Migration reversibility | Version 5 -> version 4 -> version 5 on disposable database | Passed |
| PostgreSQL race focus | booking, offering, query, and outbox adapters under `-race` | Passed |
| Secret history | `gitleaks git --redact` over full branch history | Passed; 0 leaks |
| Workflow syntax | `actionlint` | Passed |
| Filesystem security | Trivy vulnerability, secret, and misconfiguration scanners | 0 Critical/High after one path-scoped false-positive exception for numeric settings |
| Image security | Trivy over API, hold-expirer, and outbox-worker images | 0 Critical/High |
| Compose/Kubernetes | `docker compose config`, `kubectl kustomize deploy/kubernetes/base` | Passed |
| Load harness syntax | k6 `inspect` with required non-secret placeholders | Passed for all 9 scripts |

## Correctness scenarios

The automated suite covers variable-length segment-mask algebra, routes longer than 64 segments, equal-length database bit operations, all-or-nothing multi-seat allocation, scarce-seat contention, same-passenger contention, create/confirm/cancel idempotency, idempotency-key reuse conflicts and expiry, database-clock hold confirmation/expiration, exact mask release, invalid inventory relationships, orphan detection, outbox rollback, stale claim recovery, retry/dead-letter behavior, refresh-token rotation/replay, authorization, pagination bounds, and terminal train-run transitions.

Focused `-race` integration runs passed for Booking, Railway Offering, Query, and Event Relay. The reconciliation checks found no overlapping active allocations and no difference between stored inventory masks and the union of held/confirmed reservation-seat masks.

## Container end-to-end scenario

A fresh version-5 PostgreSQL database and Redis instance were exercised through the built non-root API and worker images:

1. Register customer, admin, and operator identities; issue role-specific tokens.
2. Create two IANA-timezone stations, an ordered route, train, coach, two seats, a train run, and fare.
3. Observe two available standard seats.
4. Create an owned passenger and reservation hold.
5. Replay the same idempotency key and receive the same reservation.
6. Confirm the hold and observe one ticket order.
7. Request an extreme ticket-order page and observe the bounded page value 10,000.
8. Cancel the confirmed reservation and verify all inventory masks return to zero.

The API, hold-expirer, and outbox-worker health endpoints all returned 200. SIGTERM stopped each process with exit code 0, after which each restarted healthy.

## Dependency-failure scenario

With an access token already issued, Redis was stopped while PostgreSQL remained available:

- registration returned 503 and therefore failed closed;
- reservation creation returned 201 because this admission hint intentionally fails open while the PostgreSQL transaction remains authoritative;
- cancellation returned 200 and released inventory;
- API readiness returned 503 during the outage and 200 after Redis recovery.

No inventory authority moved to Redis.

## Defects found and closed during UltraQA

1. The Alpine runtime lacked IANA timezone data, so container-only station creation rejected `Asia/Taipei`. The image now installs `tzdata`, and CI verifies the runtime is non-root and contains the required zoneinfo file.
2. Retrofitting integrity constraints into already committed migration files left version-4 development databases stale. The changes now live in migration 5 with a data backfill, named foreign keys, an upgrade test from version 4, and a disposable down/up verification.
3. The initial migration-5 draft was not rolling-deploy compatible with a version-4 writer. A compatibility trigger now derives the new train-run column for the old INSERT shape, and a real PostgreSQL regression executes that exact old statement under the version-5 schema.
4. Version 4 could leave the source route invalid when moving a stop between routes and did not bind an inventory seat to its train. Version 5 now preflights every existing route and inventory row before installing the stronger triggers; negative upgrade regressions cover both corrupt shapes.
5. Trivy's ConfigMap rule interpreted `PASS` inside two numeric setting names as secret material. A structured exception is restricted to that rule and file; all other vulnerability, secret, and misconfiguration checks remain active.

## Security audit result

The security-audit artifact set is stored outside the repository at `C:\Users\zongx\security-audit-skill\Scalable-Railway-Ticketing-Platform\run-1`. Its `findings.json` passed the skill schema validator. Final counts are Critical 0, High 0, Medium implementation defects 0, and Low 1. The Low finding is the bounded registration email-existence oracle; durable account/train-run reservation quotas remain a documented Medium Milestone 2 residual rather than a claimed Milestone 1 control.

## Load evidence boundary

All nine k6 scripts inspect successfully. Only two local one-VU, five-second read-path smoke runs have measured values:

| Scenario | Requests/s | Median | p95 | p99 | Failure rate |
|---|---:|---:|---:|---:|---:|
| Station browse | 174.08 | 4.98 ms | 8.68 ms | 13.97 ms | 0% |
| Availability read | 140.24 | 6.36 ms | 9.96 ms | 14.94 ms | 0% |

These measurements validate the local harness and request paths only. Sustained write load, hot-train correctness under k6, multi-replica behavior, and production capacity remain unmeasured.
