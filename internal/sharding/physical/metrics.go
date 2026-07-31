package physical

import (
	"reflect"
	"time"
)

// Metrics is the bounded observability seam for physical catalog routing.
// Implementations must normalize labels again at the exporter boundary. The
// router supplies only fixed operations, outcomes, reasons, shard IDs, and
// storage kinds; it never supplies resource IDs, connection references, DSNs,
// or database error strings.
type Metrics interface {
	RecordPhysicalShardRoute(operation, result, reason, shardID, storageKind string, duration time.Duration)
	RecordPhysicalShardRouteRefresh(result, reason, shardID string)
	RecordPhysicalShardUnavailable(operation, reason, shardID string)
}

type RouterOption func(*CatalogRouter)

// WithMetrics installs an optional bounded metrics recorder.
func WithMetrics(metrics Metrics) RouterOption {
	return func(router *CatalogRouter) {
		if router != nil && !nilMetrics(metrics) {
			router.metrics = metrics
		}
	}
}

func nilMetrics(value Metrics) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
