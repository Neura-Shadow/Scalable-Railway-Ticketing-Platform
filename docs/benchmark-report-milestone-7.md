# Milestone 7 Benchmark and DR Evidence Report

Status: `not_run`

No Milestone 7 runtime, load, failover, failback, or restore measurements have
been published yet. This file is intentionally a gate, not a placeholder for
fabricated values.

To permit this report to be populated after a successful fixed-source run,
the source inventory excludes exactly this file and
`docs/milestone-7-load-testing.md`. The manifest and strict verifier disclose
and enforce that exact two-file exclusion; all code, configuration, PRD,
runner, migrations, and other documentation remain digest-bound.

The canonical run must record the exact source commit and source inventory
digest, rendered Compose digest, provider adapter/API versions, PostgreSQL and
pgBackRest versions, replication mode, pre/post WAL positions, typed failover
steps, observed per-database and aggregate RPO, observed RTO, webhook outage,
payment/ticket/refund recovery, ledger and settlement reconciliation, backup
checksum and isolated restore, failback, final region/epoch, final non-empty
reconciliation, secret scan, and project-scoped teardown.

Only strict JSON summaries are treated as structured evidence. Human progress
and command transcripts use `.log` or `.txt`. A future passed report will state
whether source-built or prebuilt images were used and whether optional Stripe
test mode ran. It will not claim live production readiness, PCI certification,
statutory accounting, active-active operation, production capacity, or zero
RPO/RTO.
