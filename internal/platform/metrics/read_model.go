package metrics

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	allowedCacheTypes      = set("stations", "train_search", "availability")
	allowedCacheOperations = set("read", "fill", "invalidate", "version_get", "version_rotate")
	allowedReadResults     = set("success", "failure", "duplicate", "skipped")
	allowedReadReasons     = set(
		"none", "redis", "database", "projection", "invalid_event", "handler_failure",
		"timeout", "missing", "extra", "duplicate", "stale", "mismatch", "invalid", "unknown",
	)
)

type ReadModelMetrics struct {
	readModelEvent           *prometheus.CounterVec
	readModelDuplicateEvent  *prometheus.CounterVec
	readModelRebuild         *prometheus.CounterVec
	readModelRebuildFailure  *prometheus.CounterVec
	readModelRebuildDuration prometheus.Histogram
	readModelRowsWritten     prometheus.Counter
	readModelFallback        *prometheus.CounterVec
	readModelReconciliation  *prometheus.CounterVec
	readModelProjectionLag   prometheus.Gauge
	cacheRequest             *prometheus.CounterVec
	cacheHit                 *prometheus.CounterVec
	cacheMiss                *prometheus.CounterVec
	cacheFailure             *prometheus.CounterVec
	cacheInvalidation        *prometheus.CounterVec
	cacheInvalidationFailure *prometheus.CounterVec
	cacheFill                *prometheus.CounterVec
	cacheFillFailure         *prometheus.CounterVec
	cacheSingleflightShared  *prometheus.CounterVec
	cacheFillDuration        *prometheus.HistogramVec
	cacheSourceQuery         *prometheus.CounterVec
}

func NewReadModelMetrics(registerer prometheus.Registerer) (*ReadModelMetrics, error) {
	if registerer == nil {
		return nil, errors.New("read-model metrics: nil Prometheus registerer")
	}
	metrics := &ReadModelMetrics{
		readModelEvent:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "read_model_event_total", Help: "Read-model events by bounded event type and result."}, []string{"event_type", "result"}),
		readModelDuplicateEvent:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "read_model_duplicate_event_total", Help: "Duplicate read-model events by bounded event type."}, []string{"event_type"}),
		readModelRebuild:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "read_model_rebuild_total", Help: "Read-model rebuilds by bounded result."}, []string{"result"}),
		readModelRebuildFailure:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "read_model_rebuild_failure_total", Help: "Read-model rebuild failures by bounded reason."}, []string{"reason"}),
		readModelRebuildDuration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "read_model_rebuild_duration_seconds", Help: "Read-model rebuild duration.", Buckets: prometheus.DefBuckets}),
		readModelRowsWritten:     prometheus.NewCounter(prometheus.CounterOpts{Name: "read_model_rows_written_total", Help: "Rows written to the read model."}),
		readModelFallback:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "read_model_fallback_total", Help: "Read-model fallbacks by bounded reason."}, []string{"reason"}),
		readModelReconciliation:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "read_model_reconciliation_mismatch_total", Help: "Read-model reconciliation mismatches by bounded reason."}, []string{"reason"}),
		readModelProjectionLag:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "read_model_projection_lag_seconds", Help: "Observed read-model projection lag in seconds."}),
		cacheRequest:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_request_total", Help: "Cache requests by bounded cache type and operation."}, []string{"cache_type", "operation"}),
		cacheHit:                 prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_hit_total", Help: "Cache hits by bounded cache type."}, []string{"cache_type"}),
		cacheMiss:                prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_miss_total", Help: "Cache misses by bounded cache type."}, []string{"cache_type"}),
		cacheFailure:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_failure_total", Help: "Cache failures by bounded cache type, operation, and reason."}, []string{"cache_type", "operation", "reason"}),
		cacheInvalidation:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_invalidation_total", Help: "Cache invalidations by bounded cache type and event type."}, []string{"cache_type", "event_type"}),
		cacheInvalidationFailure: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_invalidation_failure_total", Help: "Cache invalidation failures by bounded cache type and reason."}, []string{"cache_type", "reason"}),
		cacheFill:                prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_fill_total", Help: "Successful cache fills by bounded cache type."}, []string{"cache_type"}),
		cacheFillFailure:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_fill_failure_total", Help: "Cache fill failures by bounded cache type and reason."}, []string{"cache_type", "reason"}),
		cacheSingleflightShared:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_singleflight_shared_total", Help: "Cache singleflight shared results by bounded cache type."}, []string{"cache_type"}),
		cacheFillDuration:        prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "cache_fill_duration_seconds", Help: "Cache fill duration by bounded cache type.", Buckets: prometheus.DefBuckets}, []string{"cache_type"}),
		cacheSourceQuery:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_source_query_total", Help: "Authoritative source queries issued by bounded read-cache type."}, []string{"cache_type"}),
	}
	for _, collector := range []prometheus.Collector{
		metrics.readModelEvent, metrics.readModelDuplicateEvent, metrics.readModelRebuild,
		metrics.readModelRebuildFailure, metrics.readModelRebuildDuration, metrics.readModelRowsWritten,
		metrics.readModelFallback, metrics.readModelReconciliation, metrics.readModelProjectionLag,
		metrics.cacheRequest, metrics.cacheHit, metrics.cacheMiss, metrics.cacheFailure,
		metrics.cacheInvalidation, metrics.cacheInvalidationFailure, metrics.cacheFill,
		metrics.cacheFillFailure, metrics.cacheSingleflightShared, metrics.cacheFillDuration,
		metrics.cacheSourceQuery,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("read-model metrics: register collector: %w", err)
		}
	}
	for _, reason := range []string{"missing", "extra", "duplicate", "stale", "mismatch", "invalid"} {
		metrics.readModelReconciliation.WithLabelValues(reason).Add(0)
	}
	return metrics, nil
}

func (metrics *ReadModelMetrics) RecordEvent(eventType, result string) {
	metrics.readModelEvent.WithLabelValues(NormalizeEventType(eventType), normalize(result, allowedReadResults, "failure")).Inc()
}

func (metrics *ReadModelMetrics) RecordDuplicateEvent(eventType string) {
	metrics.readModelDuplicateEvent.WithLabelValues(NormalizeEventType(eventType)).Inc()
}

func (metrics *ReadModelMetrics) RecordRebuild(result, reason string, rows int64, duration time.Duration) {
	metrics.readModelRebuild.WithLabelValues(normalize(result, allowedReadResults, "failure")).Inc()
	if result != "success" {
		metrics.readModelRebuildFailure.WithLabelValues(normalize(reason, allowedReadReasons, "unknown")).Inc()
	}
	if rows > 0 {
		metrics.readModelRowsWritten.Add(float64(rows))
	}
	metrics.readModelRebuildDuration.Observe(nonNegativeSeconds(duration))
}

func (metrics *ReadModelMetrics) RecordFallback(reason string) {
	metrics.readModelFallback.WithLabelValues(normalize(reason, allowedReadReasons, "unknown")).Inc()
}

func (metrics *ReadModelMetrics) RecordReconciliationMismatch(reason string) {
	metrics.readModelReconciliation.WithLabelValues(normalize(reason, allowedReadReasons, "unknown")).Inc()
}

func (metrics *ReadModelMetrics) AddReconciliationMismatches(reason string, count int) {
	if count <= 0 {
		return
	}
	metrics.readModelReconciliation.WithLabelValues(normalize(reason, allowedReadReasons, "unknown")).Add(float64(count))
}

func (metrics *ReadModelMetrics) SetProjectionLag(duration time.Duration) {
	metrics.readModelProjectionLag.Set(nonNegativeSeconds(duration))
}

func (metrics *ReadModelMetrics) RecordCacheRequest(cacheType, operation, result, reason string) {
	cacheType = normalize(cacheType, allowedCacheTypes, "unknown")
	operation = normalize(operation, allowedCacheOperations, "unknown")
	metrics.cacheRequest.WithLabelValues(cacheType, operation).Inc()
	switch result {
	case "hit":
		metrics.cacheHit.WithLabelValues(cacheType).Inc()
	case "miss":
		metrics.cacheMiss.WithLabelValues(cacheType).Inc()
	case "failure":
		metrics.cacheFailure.WithLabelValues(cacheType, operation, normalize(reason, allowedReadReasons, "unknown")).Inc()
	}
}

func (metrics *ReadModelMetrics) RecordCacheFill(cacheType, result, reason string, duration time.Duration, shared bool) {
	cacheType = normalize(cacheType, allowedCacheTypes, "unknown")
	if result == "success" {
		metrics.cacheFill.WithLabelValues(cacheType).Inc()
	} else {
		metrics.cacheFillFailure.WithLabelValues(cacheType, normalize(reason, allowedReadReasons, "unknown")).Inc()
	}
	if shared {
		metrics.cacheSingleflightShared.WithLabelValues(cacheType).Inc()
	}
	metrics.cacheFillDuration.WithLabelValues(cacheType).Observe(nonNegativeSeconds(duration))
}

func (metrics *ReadModelMetrics) RecordCacheSingleflightShared(cacheType string) {
	metrics.cacheSingleflightShared.WithLabelValues(normalize(cacheType, allowedCacheTypes, "unknown")).Inc()
}

func (metrics *ReadModelMetrics) RecordCacheSourceQuery(cacheType string) {
	metrics.cacheSourceQuery.WithLabelValues(normalize(cacheType, allowedCacheTypes, "unknown")).Inc()
}

func (metrics *ReadModelMetrics) RecordCacheInvalidation(cacheType, eventType, result, reason string) {
	cacheType = normalize(cacheType, allowedCacheTypes, "unknown")
	if result == "success" {
		metrics.cacheInvalidation.WithLabelValues(cacheType, NormalizeEventType(eventType)).Inc()
		return
	}
	metrics.cacheInvalidationFailure.WithLabelValues(cacheType, normalize(reason, allowedReadReasons, "unknown")).Inc()
}
