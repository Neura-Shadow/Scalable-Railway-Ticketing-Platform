# Physical Shard Reconciliation

## Policy

Reconciliation is detect-only by default. It reads bounded pages at recorded
watermarks, emits sanitized counts and stable mismatch categories, and exits
nonzero on mismatch, partial observation, truncation, or dependency failure.
It never automatically repairs seat inventory, masks, fences, assignment, or
migration authority.

## Required checks

- command, quota lease, pending/active directory, local receipt and reservation
  identity/fingerprint/state convergence;
- control assignment versus source/target local fence identity, generation and
  write enablement, including the at-most-one-writer invariant;
- snapshots, inventory masks, reservation seats, lifecycle, orders, tickets,
  idempotency records and relationship fingerprints;
- local and control outbox identities, publication/accounting and duplicates;
- capture start/final sequence, journal continuity, target apply receipts,
  copy/replay checkpoints and validation watermark;
- target successful-write evidence versus direct-rollback eligibility; and
- retained-source, rollback-window, reverse-migration and cleanup gates.

## Repair boundary

The command reconciler may idempotently finalize control command, quota and
directory state when a verified successful shard receipt exists, or mark a
proven rejection. Other repairs require a named, narrowly scoped operator
action with dry-run, explicit confirmation, before/after evidence, limits, and
an audit result. Uncertainty preserves conservative state.

Acceptance evidence must include a clean run and intentional mismatches for
each critical category against independent PostgreSQL instances. Until those
tests run, the result is **pending runtime evidence**, not a production
certification.
