# Physical Shard Failure Policy

## Authority never follows health

A database failure does not change assignment, generation, fence, or migration
phase. There is no automatic physical-shard failover in Milestone 5. Health,
Redis, route cache, hash, previous success, or operator guess cannot promote a
database or route a train run elsewhere.

## Request behavior

| Failure | Required behavior |
| --- | --- |
| Control unavailable | Fail new booking commands closed; preserve committed shard results for later reconciliation |
| Assigned shard unavailable | Return bounded topology-neutral retryable error; preserve uncertain command/quota state |
| Unassigned shard unavailable | Continue healthy assignments through independent pools; report degraded readiness |
| Stale local generation | Reject before mutation; refresh control route at most once; never fan out or fall back |
| Redis unavailable | Follow admission/cache degradation rules; never change booking authority or quota |

Customer responses and public health omit shard IDs, DSNs, hosts, connection
references, generations, migration IDs, SQL, credentials, and raw payloads.

## Worker and migration behavior

Workers use per-shard batches/timeouts and fair bounded enumeration, so a failed
shard cannot monopolize connections or retries. Control loss stops new saga
reservation. A target failure before cutover leaves source authoritative; a
source failure before final validation forbids promotion. Failure after source
disable may intentionally leave zero writers. Failure after assignment switch
leaves target authoritative and repair converges delayed metadata.

Liveness measures process responsiveness. Readiness is role-specific and may be
degraded when optional shard work is unavailable. Per-database and total pool
caps are startup gates. This is fixed-topology, single-region failure isolation,
not certified RPO/RTO, replica failover, disaster recovery, or production
availability. See [ADR 045](adr/045-physical-shard-failure-policy.md).
