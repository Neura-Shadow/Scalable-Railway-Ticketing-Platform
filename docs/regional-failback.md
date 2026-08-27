# Regional Failback and Reseeding

Failback is another authority transfer, not reversal of a flag. The old primary
may contain divergent writes and is never reused directly. While it remains
externally fenced, its data is discarded or retained only as an isolated
incident artifact; fresh control, shard 0, and shard 1 standbys are restored or
reseeded from the current active region.

The target copies verify schema, database identity, timeline, backup source,
regional authority, and payment/ticket/ledger/settlement invariants, then catch
up WAL under measured lag. Only after full reconciliation may an operator fence
the current active region and invoke the same typed promotion protocol with a
strictly greater epoch.

Recovery starts only APIs; workers and the target regional proxy remain stopped
until control and both shard authorities are active under the newer epoch.
Ingress and workers switch only after the promoted databases and fresh pools
pass active authority and primary-identity readiness. The formerly active
region remains fenced and passive.
Evidence records the reseed source, positions, reconciliation, new epoch,
failback RTO/RPO, and zero post-promotion old-region writes. No automatic
failback, multi-primary interval, or zero-loss claim is permitted.
