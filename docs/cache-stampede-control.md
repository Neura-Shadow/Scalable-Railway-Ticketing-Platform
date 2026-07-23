# Cache Stampede Control

Each API replica uses `x/sync/singleflight` keyed by the exact data key. When
many goroutines miss the same current generation, one leader queries
PostgreSQL and the other callers share its result. Different cache keys proceed
independently.

The leader performs a second cache read after entering the flight, covering a
fill completed between the first miss and leadership. Successful source data
is returned even when Redis fill fails. Errors are propagated to all current
waiters, the flight is removed, and a later request may retry. Contexts and
database/Redis timeouts remain bounded.

TTL jitter spreads ordinary expiry. Version rotation can still cold-start a
popular key across multiple API replicas; local singleflight bounds work per
replica, while shared Redis lets the first completed fill serve other replicas.
No distributed lock is enabled because it would add lease-expiry and ownership
failure modes without measured need.

Tests cover concurrent identical misses, warm reuse, unrelated-key progress,
failed-fill retry, and multi-replica shared cache behavior. Load evidence must
compare `cache_singleflight_shared_total`, source-query/fallback metrics,
PostgreSQL pool use, and Redis latency; request latency alone cannot prove
stampede control.
