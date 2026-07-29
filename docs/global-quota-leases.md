# Global Quota Leases

## Invariant

Global user reservation limits remain control-database authority after booking
state moves to independent databases. A control transaction locks the user's
scope and counts both pending and active leases. It acquires exactly one lease
per command before shard execution, so parallel commands routed to different
shards cannot each observe unused quota.

`booking_quota_leases` binds the command, owner, train run, reservation, and
passenger count. Its bounded states distinguish pending uncertainty, active
holds, repair, release, and expiry. Command identity makes acquire, activate,
and release idempotent.

## Failure rules

- A committed shard receipt converts the lease to an active hold during normal
  finalization or reconciliation.
- A proven shard rejection/failure can release the lease once, with a bounded
  reason and durable evidence.
- A timeout, unavailable shard, control-finalization failure, or missing reply
  is not proof of failure. The lease remains conservative.
- Redis loss, cache expiry, customer retry, or operator health status cannot
  release or bypass quota.

This can temporarily reject a valid request while an outcome is uncertain. The
pilot chooses bounded false rejection over quota undercount and duplicate seat
allocation. Monitoring must report pending age, repair backlog, releases by
bounded reason, and limit rejections without user or topology identifiers.

See [ADR 039](adr/039-global-quota-and-reservation-directory.md) and
[command recovery](control-plane-booking-command.md).
