package postgres_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	settlementpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
)

func TestPostgresReconciliationPayoutLifecycleAndConflict(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("SETTLEMENT_POSTGRES_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("SETTLEMENT_POSTGRES_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := postgresx.NewRegionalBoundedPool(ctx, databaseURL, 4, postgresx.RegionalSession{
		Region: "region-a", Role: "active", Epoch: 1, WritesEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	region, _ := authority.ParseRegion("region-a")
	epoch, _ := authority.NewEpoch(1)
	deployment, _ := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	store, err := settlementpostgres.New(pool, settlementpostgres.WithRegionalAuthority(deployment))
	if err != nil {
		t.Fatal(err)
	}
	prefix := strings.ReplaceAll(uuid.NewString(), "-", "")
	paidID, failedID, agedID := "po_paid_"+prefix, "po_failed_"+prefix, "po_aged_"+prefix
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck -- rollback after commit is intentionally harmless
	if _, err = tx.Exec(ctx, `SELECT set_config('railway.deployment_region','region-a',true),set_config('railway.deployment_role','active',true),set_config('railway.region_epoch','1',true),set_config('railway.regional_writes_enabled','true',true)`); err != nil {
		t.Fatal(err)
	}
	insertPayout := func(id, status string, createdAt time.Time, hashByte string) {
		t.Helper()
		_, err = tx.Exec(ctx, `INSERT INTO public.provider_payouts(
provider,provider_account_id,provider_record_id,operation_type,gross_minor,fee_minor,net_minor,currency,
available_at,provider_created_at,provider_payout_id,payout_status,payload_hash,imported_at
) VALUES('stripe','acct_integration',$1,'payout',670,0,670,'TWD',$2,$2,$1,$3,decode(repeat($4,32),'hex'),clock_timestamp())`, id, createdAt, status, hashByte)
		if err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	insertPayout(paidID, "pending", now.Add(-8*24*time.Hour), "11")
	insertPayout(paidID, "paid", now.Add(-time.Hour), "12")
	insertPayout(failedID, "failed", now.Add(-time.Hour), "13")
	insertPayout(agedID, "pending", now.Add(-8*24*time.Hour), "14")
	if _, err = tx.Exec(ctx, `INSERT INTO public.provider_balance_transactions(
provider,provider_account_id,provider_record_id,operation_type,gross_minor,fee_minor,net_minor,currency,
available_at,provider_created_at,provider_payout_id,payout_status,payload_hash,imported_at
) VALUES('stripe','acct_integration',$1,'payout',-670,0,-670,'TWD',$2,$2,$3,'available',decode(repeat('15',32),'hex'),clock_timestamp())`, "txn_"+prefix, now.Add(-time.Hour), paidID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO public.provider_payout_lines(
provider,provider_account_id,provider_record_id,operation_type,gross_minor,fee_minor,net_minor,currency,
available_at,provider_created_at,provider_payout_id,payout_status,payload_hash,imported_at
) VALUES('stripe','acct_integration',$1,'payout',-123,0,-123,'USD',$2,$2,$3,'paid',decode(repeat('17',32),'hex'),clock_timestamp())`, "line_"+prefix, now.Add(-time.Hour), paidID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO public.provider_settlement_import_conflicts(
conflict_id,provider,provider_account_id,record_kind,provider_record_id,stored_hash,incoming_hash,detected_at
) VALUES($1,'stripe','acct_integration','payout',$2,decode(repeat('12',32),'hex'),decode(repeat('16',32),'hex'),clock_timestamp())`, uuid.New(), paidID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	detector, err := settlement.NewDetector(store, settlement.DetectorConfig{PageSize: 1, MaxPages: 20})
	if err != nil {
		t.Fatal(err)
	}
	paid, err := detector.RunOnce(ctx, settlement.DetectionScope{Kind: settlement.ScopePayout, Value: paidID})
	if err != nil {
		t.Fatal(err)
	}
	if hasFinding(paid.Findings, settlement.FindingDuplicate) || hasFinding(paid.Findings, settlement.FindingPayoutLifecycle) || !hasFinding(paid.Findings, settlement.FindingEventConflict) {
		t.Fatalf("paid lifecycle findings = %+v", paid.Findings)
	}
	failed, err := detector.RunOnce(ctx, settlement.DetectionScope{Kind: settlement.ScopePayout, Value: failedID})
	if err != nil || !hasFinding(failed.Findings, settlement.FindingPayoutLifecycle) {
		t.Fatalf("failed lifecycle report=%+v err=%v", failed, err)
	}
	aged, err := detector.RunOnce(ctx, settlement.DetectionScope{Kind: settlement.ScopePayout, Value: agedID})
	if err != nil || !hasFinding(aged.Findings, settlement.FindingAge) {
		t.Fatalf("aged lifecycle report=%+v err=%v", aged, err)
	}
}

func hasFinding(findings []settlement.Finding, reason settlement.FindingReason) bool {
	for _, finding := range findings {
		if finding.Reason == reason {
			return true
		}
	}
	return false
}
