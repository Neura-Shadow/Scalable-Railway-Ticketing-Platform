# Milestone 5 Benchmark Report

## Status

**Runtime and load evidence: pending.**

This document intentionally contains no throughput, latency, copy rate,
journal-lag, write-pause, recovery-time, or resource-utilization numbers. No
reviewed artifact proving the complete Milestone 5 three-database scenario was
available when this report was created. Documentation, unit tests, SQL review,
or a successful container build cannot substitute for runtime evidence.

## Required evidence before publication

- exact commit and immutable container/migration artifacts;
- isolated control PostgreSQL plus two independently persisted booking
  PostgreSQL instances, configuration and pool budgets;
- host and database limits, fixture/seed, k6 version and exact commands;
- canonical JSON summaries plus raw `.log` transcripts for all ten scenarios
  in [the load-test plan](milestone-5-load-testing.md);
- before/after database invariants and untruncated reconciliation;
- migration fresh/repeat/populated/down/up evidence for both histories;
- fault evidence for control/shard outage and every cutover crash window;
- at least one successful target write followed by physical reverse migration;
- measured final write pause, explicitly labelled as a pause; and
- race, security, Compose, container and prior-milestone regression results.

## Result table

| Scenario | Status | Artifact | Observed result |
| --- | --- | --- | --- |
| Physical routing | not run | pending | no claim |
| Cross-shard quota | not run | pending | no claim |
| Command recovery | not run | pending | no claim |
| Shard outage | not run | pending | no claim |
| Online base copy | not run | pending | no claim |
| Journal catch-up | not run | pending | no claim |
| Physical cutover | not run | pending | no claim |
| Stale router | not run | pending | no claim |
| Reverse migration | not run | pending | no claim |
| Legacy comparison | not run | pending | no claim |

When evidence is produced, replace each row with its artifact link and bounded
observations. Do not infer production capacity, zero downtime, national scale,
exactly-once delivery, or multi-region correctness from this pilot.
