package conformance

import (
	"context"
	"math"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

type EvidenceHarness struct {
	NewClient func(*testing.T) provider.Described
}

// RunEvidence verifies optional settlement capabilities through provider-
// neutral interfaces. A provider that does not advertise a capability is
// skipped; an advertised capability must expose and exercise its reader.
func RunEvidence(t *testing.T, harness EvidenceHarness) {
	t.Helper()
	if harness.NewClient == nil {
		t.Fatal("evidence conformance harness is incomplete")
	}

	t.Run("settlement transaction capability is executable", func(t *testing.T) {
		client, descriptor := evidenceClient(t, harness)
		if !descriptor.Capabilities.SettlementTransactions {
			t.Skip("provider does not advertise settlement transactions")
		}
		reader, ok := client.(provider.BalanceTransactionReader)
		if !ok {
			t.Fatal("advertised settlement reader is unavailable")
		}
		page, err := reader.ListBalanceTransactions(context.Background(), provider.EvidenceListOptions{Limit: 1})
		if err != nil || len(page.Items) != 1 {
			t.Fatalf("balance page = %#v, %v", page, err)
		}
		item := page.Items[0]
		if item.ID == "" || item.Currency == "" || item.FeeMinor < 0 || item.GrossMinor < math.MinInt64+item.FeeMinor || item.GrossMinor-item.FeeMinor != item.NetMinor || item.CreatedAt.IsZero() || item.AvailableAt.IsZero() {
			t.Fatalf("balance item = %#v", item)
		}
		if page.HasMore != (page.NextStartingAfter != "") {
			t.Fatalf("balance cursor = %#v", page)
		}
		replayed, err := reader.ListBalanceTransactions(context.Background(), provider.EvidenceListOptions{Limit: 1})
		if err != nil || len(replayed.Items) != len(page.Items) || replayed.HasMore != page.HasMore ||
			replayed.NextStartingAfter != page.NextStartingAfter || replayed.Items[0] != page.Items[0] {
			t.Fatalf("balance checkpoint replay = %#v, %v; want %#v", replayed, err, page)
		}
		if page.HasMore {
			next, err := reader.ListBalanceTransactions(context.Background(), provider.EvidenceListOptions{Limit: 1, StartingAfter: page.NextStartingAfter})
			if err != nil || len(next.Items) != 1 || next.Items[0].ID == page.Items[0].ID {
				t.Fatalf("balance checkpoint resume = %#v, %v", next, err)
			}
		}
	})

	t.Run("payout report capability is executable", func(t *testing.T) {
		client, descriptor := evidenceClient(t, harness)
		if !descriptor.Capabilities.PayoutReports {
			t.Skip("provider does not advertise payout reports")
		}
		reader, ok := client.(provider.PayoutReader)
		if !ok {
			t.Fatal("advertised payout reader is unavailable")
		}
		page, err := reader.ListPayouts(context.Background(), provider.EvidenceListOptions{Limit: 1})
		if err != nil || len(page.Items) != 1 {
			t.Fatalf("payout page = %#v, %v", page, err)
		}
		item := page.Items[0]
		if item.ID == "" || item.AmountMinor <= 0 || item.Currency == "" || item.Status == "" || item.CreatedAt.IsZero() || item.ArrivalAt.IsZero() {
			t.Fatalf("payout item = %#v", item)
		}
		if page.HasMore != (page.NextStartingAfter != "") {
			t.Fatalf("payout cursor = %#v", page)
		}
		replayed, err := reader.ListPayouts(context.Background(), provider.EvidenceListOptions{Limit: 1})
		if err != nil || len(replayed.Items) != len(page.Items) || replayed.HasMore != page.HasMore ||
			replayed.NextStartingAfter != page.NextStartingAfter || replayed.Items[0] != page.Items[0] {
			t.Fatalf("payout checkpoint replay = %#v, %v; want %#v", replayed, err, page)
		}
		if page.HasMore {
			next, err := reader.ListPayouts(context.Background(), provider.EvidenceListOptions{Limit: 1, StartingAfter: page.NextStartingAfter})
			if err != nil || len(next.Items) != 1 || next.Items[0].ID == page.Items[0].ID {
				t.Fatalf("payout checkpoint resume = %#v, %v", next, err)
			}
		}
	})
}

func evidenceClient(t *testing.T, harness EvidenceHarness) (provider.Described, provider.Descriptor) {
	t.Helper()
	client := harness.NewClient(t)
	if client == nil {
		t.Fatal("evidence conformance client is nil")
	}
	descriptor := client.Descriptor()
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("provider descriptor: %v", err)
	}
	return client, descriptor
}
