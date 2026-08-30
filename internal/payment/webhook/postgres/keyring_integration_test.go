package postgres_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook"
	webhookpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
)

func TestPostgresKeyringLifecycleProofRuntimeFenceAndHistoryBound(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("WEBHOOK_POSTGRES_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("WEBHOOK_POSTGRES_TEST_DATABASE_URL is not set")
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
	repository, err := webhookpostgres.NewRepository(pool, webhookpostgres.WithRegionalAuthority(deployment))
	if err != nil {
		t.Fatal(err)
	}

	accountID := "acct_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	oldProof := webhook.KeyProof("stripe", accountID, "old", "whsec_old_contract")
	newProof := webhook.KeyProof("stripe", accountID, "new", "whsec_new_contract")
	now := time.Now().UTC().Truncate(time.Microsecond)
	sync := func(primary string, ids []string, proofs map[string][32]byte, at time.Time) webhook.KeyringPlan {
		t.Helper()
		plan, syncErr := repository.SynchronizeKeyring(ctx, "stripe", accountID, webhook.DesiredKeyring{
			PrimaryKeyID: primary, AcceptedKeyIDs: ids, SecretProofs: proofs, Grace: time.Hour, Now: at,
		})
		if syncErr != nil {
			t.Fatal(syncErr)
		}
		return plan
	}

	sync("old", []string{"old"}, map[string][32]byte{"old": oldProof}, now)
	staged := sync("old", []string{"old", "new"}, map[string][32]byte{"old": oldProof, "new": newProof}, now.Add(time.Minute))
	if staged.ByID["new"].RetirementNotBefore != nil {
		t.Fatalf("staged replacement started grace: %+v", staged.ByID["new"])
	}
	activatedAt := now.Add(2 * time.Minute)
	activated := sync("new", []string{"new", "old"}, map[string][32]byte{"new": newProof, "old": oldProof}, activatedAt)
	deadline := activatedAt.Add(time.Hour)
	if got := activated.ByID["old"].RetirementNotBefore; got == nil || !got.Equal(deadline) {
		t.Fatalf("old deadline = %v, want %v", got, deadline)
	}
	if err := repository.ValidateVerifiedKey(ctx, "stripe", accountID, "old", deadline.Add(-time.Nanosecond)); err != nil {
		t.Fatalf("old key before deadline: %v", err)
	}
	if err := repository.ValidateVerifiedKey(ctx, "stripe", accountID, "old", deadline); !errors.Is(err, webhook.ErrKeyringConflict) {
		t.Fatalf("old key at deadline = %v", err)
	}
	sync("new", []string{"new"}, map[string][32]byte{"new": newProof}, deadline)

	for index := 0; index < 12; index++ {
		keyID := "retired-" + time.Unix(int64(index), 0).UTC().Format("150405")
		proof := webhook.KeyProof("stripe", accountID, keyID, "whsec_retired_contract_"+keyID)
		if _, err := pool.Exec(ctx, `INSERT INTO public.payment_webhook_key_versions(
provider,provider_account_id,key_id,state,activated_at,material_proof,retirement_not_before,retired_at
) VALUES('stripe',$1,$2,'retired',$3,$4,$3,$3)`, accountID, keyID, now.Add(-2*time.Hour), proof[:]); err != nil {
			t.Fatal(err)
		}
	}
	desired := webhook.DesiredKeyring{PrimaryKeyID: "new", AcceptedKeyIDs: []string{"new"}, SecretProofs: map[string][32]byte{"new": newProof}, Grace: time.Hour, Now: deadline.Add(time.Minute)}
	sync("new", []string{"new"}, map[string][32]byte{"new": newProof}, desired.Now)
	if err := repository.ValidateKeyring(ctx, "stripe", accountID, desired); err != nil {
		t.Fatalf("retired history blocked readiness: %v", err)
	}
	var retained, archived int
	if err := pool.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM public.payment_webhook_key_versions WHERE provider='stripe' AND provider_account_id=$1 AND state='retired'),
 (SELECT count(*) FROM public.payment_webhook_key_version_archive WHERE provider='stripe' AND provider_account_id=$1)`, accountID).Scan(&retained, &archived); err != nil {
		t.Fatal(err)
	}
	if retained != 8 || archived != 5 {
		t.Fatalf("retired hot/archive history = %d/%d, want 8/5", retained, archived)
	}
	reusedKeyID := "reused-archived"
	reusedArchivedProof := webhook.KeyProof("stripe", accountID, reusedKeyID, "whsec_retired_contract_retired-000000")
	if _, err := repository.SynchronizeKeyring(ctx, "stripe", accountID, webhook.DesiredKeyring{
		PrimaryKeyID: "new", AcceptedKeyIDs: []string{"new", reusedKeyID},
		SecretProofs: map[string][32]byte{"new": newProof, reusedKeyID: reusedArchivedProof},
		Grace:        time.Hour, Now: desired.Now.Add(time.Minute),
	}); !errors.Is(err, webhook.ErrPersistence) {
		t.Fatalf("archived secret material was reusable under a new key ID: %v", err)
	}
	wrong := desired
	wrong.SecretProofs = map[string][32]byte{"new": webhook.KeyProof("stripe", accountID, "new", "whsec_wrong_contract")}
	if err := repository.ValidateKeyring(ctx, "stripe", accountID, wrong); !errors.Is(err, webhook.ErrKeyringConflict) {
		t.Fatalf("wrong independently provisioned secret proof = %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE public.payment_webhook_key_rotation_audit SET reason='tampered' WHERE provider_account_id=$1`, accountID); err == nil {
		t.Fatal("immutable rotation audit accepted update")
	}
	if _, err := pool.Exec(ctx, `UPDATE public.payment_webhook_key_version_archive SET archived_by='tampered' WHERE provider_account_id=$1`, accountID); err == nil {
		t.Fatal("immutable retired-key archive accepted update")
	}
}
