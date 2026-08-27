# Disaster-Recovery Reconciliation

DR reconciliation is bounded and detect-first. It runs after database
promotion and pool reset, before customer-write activation, and again after
worker recovery. It reads only the fixed promoted control/shard set and never
falls back to an unrelated shard.

Checks include regional authority and epoch agreement; database roles,
timelines, schema, and migration cleanliness; reservation directory and
generation fences; payment/provider/ledger totals; issued/refunded tickets and
seat masks; shard command and refund receipts; ticket-code uniqueness;
settlement and payout evidence; webhook conflicts; backup/restore metadata; and
physical migration journal gaps.

Every run persists a checkpoint, bounded mismatch records, truncation flag,
rows examined, and sanitized category counts. The activation gate requires a
non-empty, non-truncated run with no blocking mismatch or unresolved required
manual review. Detect-only reconciliation never calls a provider mutation,
issues/cancels tickets, changes inventory, repairs a ledger history, promotes a
database, or switches ingress.

Explicit operator repairs may replay only an existing durable command under
expected-state and lease guards. A repair failure becomes visible manual review
and cannot be silently retried forever.
