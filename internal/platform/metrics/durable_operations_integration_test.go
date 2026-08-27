package metrics_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type unavailableMetricsDatabase struct{}

func (unavailableMetricsDatabase) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errors.New("test replica unavailable")
}

func TestDurableOperationsCollectorQueriesLiveMilestone7Schema(t *testing.T) {
	dsn := os.Getenv("M7_METRICS_DATABASE_URL")
	if dsn == "" {
		t.Skip("M7_METRICS_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	collector, err := platformmetrics.NewDurableOperationsCollector(
		pool,
		"region-a",
		5*time.Second,
		platformmetrics.WithProviderCapabilityProfile("stripe", map[string]bool{"partial_refund": false}),
		platformmetrics.WithDurableReplicationSource("booking_shard", "shard-0", unavailableMetricsDatabase{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	if _, err := platformmetrics.NewEventMetrics(registry); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(collector); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather live M7 durable metrics: %v", err)
	}
	observed := make(map[string]bool, len(families))
	for _, family := range families {
		observed[family.GetName()] = true
	}
	if !observed["regional_active_epoch"] {
		t.Fatal("live control evidence was not gathered")
	}
	if !observed["provider_capability_failure_total"] {
		t.Fatal("one unavailable shard silently emptied the otherwise-valid scrape")
	}
	var hasStripePayoutFinding bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(
SELECT 1 FROM public.settlement_reconciliation_mismatches mismatch
JOIN public.provider_payouts payout
  ON mismatch.correlation='payout:'||COALESCE(payout.provider_payout_id,payout.provider_record_id)
WHERE mismatch.evidence_kind='payout' AND payout.provider='stripe'
)`).Scan(&hasStripePayoutFinding); err != nil {
		t.Fatal(err)
	}
	if hasStripePayoutFinding && !hasMetricLabels(families, "payout_reconciliation_mismatch_total", map[string]string{
		"provider": "stripe", "currency": "twd",
	}) {
		t.Fatal("live payout mismatch metrics lost provider or currency attribution")
	}
}

func hasMetricLabels(families []*dto.MetricFamily, name string, labels map[string]string) bool {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matched := true
			for key, want := range labels {
				found := false
				for _, label := range metric.Label {
					if label.GetName() == key && label.GetValue() == want {
						found = true
						break
					}
				}
				matched = matched && found
			}
			if matched {
				return true
			}
		}
	}
	return false
}
