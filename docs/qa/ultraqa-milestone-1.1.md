# UltraQA Report: Milestone 1.1 Hardening

`ULTRAQA COMPLETE: Goal met after 4 cycles`

## Goal and success criteria

- Goal: adversarially verify the six Milestone 1.1 residual-hardening changes without starting Milestone 2 work.
- Stop condition: normal, malformed, retry, stale-state, concurrency, secret-leak, deployment, and cleanup scenarios pass with no unresolved Critical or High review finding.
- Safety bounds: disposable PostgreSQL/Redis/container state only; synthetic identities; bounded command timeouts; no production writes, real credentials, captured tokens, destructive repository operations, or throughput claims.

## Scenario matrix

| ID | User/attacker model | Scenario | Command or harness | Expected signal | Actual result | Status | Evidence | Cleanup |
|---|---|---|---|---|---|---|---|---|
| UQ-01 | Search client | Zero/non-zero first offset, intermediate origin, same-day and overnight destination, cross-surface equality | Focused query integration tests, repeated ten times | Search and availability return identical correctly anchored instants | All repeated cases passed | Pass | `internal/query/postgres/store_integration_test.go` | Isolated schemas removed by tests |
| UQ-02 | CLI operator with malformed input | Malformed URL contains password, query token, and client-key path | `go run ./cmd/migrate ... version` with a synthetic malformed DSN | Non-zero exit, bounded category, no supplied marker | Bounded migration connection failure; every marker absent | Pass | `cmd/migrate/main_test.go`, dynamic CLI probe | No artifact created |
| UQ-03 | Unauthenticated remote client | New/duplicate registration, invalid Unicode and credential boundaries, subsequent login, malformed JSON | HTTP/application/domain tests, disposable PostgreSQL, and API container requests | Valid new/duplicate responses are byte-identical `202`; every invalid shape is rejected before account lookup; login succeeds; malformed JSON is `400`; logs contain no secrets | All signals matched | Pass | Registration, account-domain, and PostgreSQL auth tests plus container probe | Container stopped; synthetic accounts deleted |
| UQ-04 | Misconfigured operator | Production log publisher, explicit override, Redis Streams, and disabled publication | Four disposable outbox-worker container modes | Default log rejected; override warns; Streams checks Redis; disabled mode stays healthy without Redis | All modes matched the policy | Pass | `internal/platform/config/config_test.go`, `cmd/outbox-worker/main_test.go`, container probes | All smoke containers removed |
| UQ-05 | Compromised workload | Worker receives or logs unused JWT/Redis/database secrets | Rendered Compose/Kubernetes assertions and bounded-log scans | Hold has database only; log outbox has database only; Streams outbox has database/Redis; no secret markers in logs | Assertions and scans passed | Pass | Process-specific ConfigMaps/deployments and runtime smokes | Render output not persisted |
| UQ-06 | Concurrent customers/workers | Overlap, exact-count allocation, confirm/expire, cancel/confirm, idempotency, reconciliation | Selected booking PostgreSQL tests, repeated three times; full race suite | No double allocation, partial batch, invalid terminal state, or reconciliation drift | All tests and race detector passed | Pass | `internal/booking/postgres/store_integration_test.go` | Isolated schemas removed by tests |
| UQ-07 | Retrying/stale outbox workers | Publish failure, stale lease, obsolete owner, concurrent claims, dead letter | Event-relay application/PostgreSQL tests, repeated five times | At-least-once retry remains bounded; stale owner cannot finalize; one claim per event | All repeated scenarios passed | Pass | Event-relay worker/store tests | Isolated schemas removed by tests |
| UQ-08 | Release operator | Initial migration, repeat `up`, clean version, pre/post/rollback SQL | Migration CLI plus PostgreSQL 16 version-4/version-5 scripts | First apply succeeds, repeat is no change, version is clean; every incompatibility count is zero | All catalog/data checks passed | Pass | `docs/migrations/sql/` | Disposable validation database removed |
| UQ-09 | Flaky or hung dependency/test | False green, long-running command, and lucky single pass | Explicit exit-code checks, bounded test timeouts, repeated focused tests | Non-zero failures cannot look successful; commands terminate; repeated results remain green | All final gates passed; no flake cluster | Pass | Full tests, repeated lanes, race/static/security gates | No child process remained |
| UQ-10 | Registration boundary attacker | Overlong CJK display names, embedded controls, malformed UTF-8, too-short multibyte passwords, bcrypt byte overflow, and duplicate email separators | Shared domain validation plus HTTP/application/PostgreSQL regression tests, repeated ten times | Invalid requests fail before the uniqueness-sensitive account path; valid 100-rune passenger names remain accepted | All boundary and new/existing-account cases passed | Pass | `internal/accounts/domain/`, auth/passenger HTTP and application tests | Isolated schemas removed by tests |
| UQ-11 | Developer with a dirty tree | Generated harnesses or scans hide/overwrite source work | Status checks before commits and after every disposable probe | Only intentional source/docs remain, then clean after commit; no test output or local config enters Git | Worktree returned clean; Gitleaks found no leak | Pass | Git status/diff gates | Temporary state external or removed |
| UQ-12 | Operator reusing development settings | A production process receives the committed local database password in userinfo or query settings, the development JWT value, or a universal/mapped universal trusted-proxy CIDR | Focused production configuration tests and rendered Kubernetes baseline | Startup rejects known development credentials and effective universal trust; baseline trusts loopback until an overlay supplies exact ingress addresses | All misuse cases were rejected without echoing credentials | Pass | `internal/platform/config/config_test.go`, `deploy/kubernetes/base/configmap.yaml` | No artifact created |
| UQ-13 | Operator disabling expiration | `HOLD_EXPIRER_ENABLED=false` at process start | Focused hold-expirer startup and lifecycle seams | No expiration pass or database mutation is invoked; health process remains alive until cancellation or server failure | Disabled mode made zero pass calls and waited for lifecycle termination | Pass | `cmd/hold-expirer/main_test.go` | No artifact created |
| UQ-14 | Generated client/user with Unicode input | OpenAPI registration constraints for Unicode email/password and whitespace-only display name, plus legacy Unicode login | Contract/source comparison plus HTTP/domain boundary tests | Registration contract matches the one-at-sign Unicode email rule, 72 UTF-8-byte password ceiling, trimming, and non-whitespace display-name requirement; login retains its legacy lookup syntax | Contract and runtime boundaries agree | Pass | `docs/openapi.yaml`, `internal/accounts/domain/`, auth HTTP tests | No artifact created |

## Commands run

- Exit status 0: `go mod tidy`, changed-file `gofmt`, `go vet ./...`, and `go test ./... -count=1 -timeout 300s`.
- Exit status 0: `go test -race ./... -count=1 -timeout 420s`.
- Exit status 0: repeated query, configuration/auth, booking/reconciliation, and event-relay focused tests.
- Exit status 0: `staticcheck ./...`, `govulncheck ./...`, `actionlint .github/workflows/ci.yml`, and Gitleaks working-tree/history scans.
- Exit status 0: migration `up`, repeat `up`, `version`, and all three read-only Migration 5 operator scripts against disposable PostgreSQL.
- Exit status 0: `docker compose config`, `kubectl kustomize deploy/kubernetes/base`, main/migration image builds, runtime identity/timezone checks, and worker/API container probes.
- `[expected non-zero]` production enabled log publisher without override and malformed migration DSN; harness asserted both failure categories and absence of secret markers.

Connection strings, passwords, JWTs, and issued tokens were kept in process-local disposable test state and are intentionally omitted from this report.

## Failures found

Independent review after Cycle 1 found three in-scope product gaps:

- Registration validated display-name persistence limits after the uniqueness-sensitive user insert. Overlong or PostgreSQL-invalid control input could therefore distinguish a new email from an existing email.
- The Makefile globally exported `DATABASE_URL`, unnecessarily coupling dependency-free targets to the integration database.
- Valid migration CLI help returned argument-error status 2.

The registration investigation also exposed inconsistent byte/rune handling in passenger CRUD and registration email/password validation. These were treated as the same validation-boundary class and closed before Cycle 2.

The security attack pass after Cycle 2 found two additional defense-in-depth gaps:

- Production validation accepted the known development JWT/database credential values and universal trusted-proxy CIDRs when an operator manually combined them with `APP_ENV=production`.
- A disabled hold-expirer performed its initial mutation pass before checking the enable flag.

Both were reproduced by failing tests and closed in Cycle 3. Review also confirmed that synchronous immediate account activation permits a rate-limited follow-up login inference even though the direct registration responses are identical. That intentional Milestone 1 limitation is now explicitly documented; no email-verification or other Milestone 2 flow was added.

The final fixed-point review after Cycle 3 found nine consistency and deployment
gaps: query-string database passwords could bypass the exact local-credential
guard, an IPv4-mapped universal proxy CIDR passed validation, disabled
hold-expirer lifecycle exited into Kubernetes restart/backoff, and six OpenAPI
registration/login constraints did not precisely describe the Unicode, byte,
control, normalization, and legacy-login rules. Focused failing tests
reproduced the code paths; all nine were closed in Cycle 4 without changing
login or adding an activation workflow.

Four harness/setup mistakes were classified separately and corrected before product conclusions:

- Windows did not expand an `actionlint` wildcard; the exact workflow path then passed.
- A repository-wide format probe included unchanged baseline files; the required changed-Go-file format gate then passed.
- A shell identity assertion was interpolated by the host shell; direct `id -u` and timezone checks then passed.
- One full race run exhausted a 30-second isolated-schema migration setup context while many packages were migrating concurrently. The affected cancellation/hold test then passed ten consecutive race runs, and the unchanged full race command passed on repeat; no race or product assertion failed.

## Fixes applied

- Added shared registration email, password, and passenger display-name validation before account lookup, including UTF-8, control-rune, rune-count, and bcrypt byte boundaries.
- Reused the passenger-name rule across HTTP, application, and PostgreSQL CRUD paths, with new/duplicate and 100/101-rune regressions.
- Removed the Makefile-wide database export while preserving environment-based migration targets.
- Made valid migration CLI help return success without exposing `DATABASE_URL`.
- Rejected committed development credentials and universal trusted-proxy CIDRs in production, and changed the Kubernetes baseline to loopback-only trust until an overlay supplies exact ingress addresses.
- Extended the credential guard to query-string password settings and the proxy guard to IPv4-mapped universal trust.
- Made the disabled hold-expirer skip its initial expiration pass and remain healthy until lifecycle termination.
- Documented the direct-response boundary and the remaining follow-up login inference for immediate account activation.
- Aligned the auth-only OpenAPI contract with the runtime Unicode email, UTF-8 byte, trimming, and non-whitespace rules.
- Repeated all affected tests before the final Cycle 4 gates.

## Cleanup and rollback

- API, log-publisher, override, Redis Streams, and disabled-publisher smoke containers were stopped and removed.
- Synthetic registration rows and the standalone Migration 5 validation database were removed.
- No generated harness, captured log, coverage file, benchmark output, `.env`, database data, Redis data, or workflow state was added to the repository.
- The isolated dependency containers are retained only until all review and CI gates finish, then removed.

## Residual risks

- The local environment has no Trivy executable; filesystem and image Trivy gates remain mandatory in GitHub Actions.
- Migration 5 was validated functionally, not at production table scale. The runbook therefore requires a production-like rehearsal and maintenance-window decision and makes no zero-downtime claim.
- Registration direct responses are indistinguishable for new/existing valid emails, but immediate activation still allows a rate-limited follow-up login inference and database timing is not constant-time. Verified activation is outside Milestone 1.1.
- Role changes are not exposed by this repository; any future administrative role-change procedure must atomically increment `token_version`.
- No sustained throughput or national-scale capacity claim is supported.

## Evidence

- Full unit/integration/race/static/security command exit codes were checked directly.
- Repeated focused tests guard against lucky single-run success.
- Negative scenarios required the expected non-zero status and bounded failure text; success-looking output alone was never treated as a pass.
- Dynamic HTTP/container checks verified behavior through built artifacts, not only mocks.
