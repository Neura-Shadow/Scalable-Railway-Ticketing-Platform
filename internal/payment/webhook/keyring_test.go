package webhook_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook"
)

func TestPlanKeyRotationDemotesPrimaryAndBoundsPreviousSecret(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	oldProof := webhook.KeyProof("stripe", "acct_contract", "old", "whsec_old_contract")
	newProof := webhook.KeyProof("stripe", "acct_contract", "new", "whsec_new_contract")
	current := []webhook.KeyVersion{{KeyID: "old", State: webhook.KeyPrimary, ActivatedAt: now.Add(-time.Hour), SecretProof: oldProof}}
	plan, err := webhook.PlanKeyring(current, webhook.DesiredKeyring{
		PrimaryKeyID: "new", AcceptedKeyIDs: []string{"new", "old"},
		SecretProofs: map[string][32]byte{"new": newProof, "old": oldProof}, Grace: 24 * time.Hour, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	old := plan.ByID["old"]
	newKey := plan.ByID["new"]
	if old.State != webhook.KeyAccepted || old.RetirementNotBefore == nil || !old.RetirementNotBefore.Equal(now.Add(24*time.Hour)) {
		t.Fatalf("old key after rotation = %+v", old)
	}
	if newKey.State != webhook.KeyPrimary || !newKey.ActivatedAt.Equal(now) || newKey.RetirementNotBefore != nil {
		t.Fatalf("new key after rotation = %+v", newKey)
	}
}

func TestPlanKeyRotationStagesReplacementWithoutStartingRetirementGrace(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	oldProof := webhook.KeyProof("stripe", "acct_contract", "old", "whsec_old_contract")
	newProof := webhook.KeyProof("stripe", "acct_contract", "new", "whsec_new_contract")
	plan, err := webhook.PlanKeyring([]webhook.KeyVersion{{
		KeyID: "old", State: webhook.KeyPrimary, ActivatedAt: now.Add(-time.Hour), SecretProof: oldProof,
	}}, webhook.DesiredKeyring{
		PrimaryKeyID: "old", AcceptedKeyIDs: []string{"old", "new"},
		SecretProofs: map[string][32]byte{"old": oldProof, "new": newProof}, Grace: time.Hour, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	staged := plan.ByID["new"]
	if staged.State != webhook.KeyAccepted || staged.RetirementNotBefore != nil || staged.SecretProof != newProof {
		t.Fatalf("staged replacement = %+v", staged)
	}
}

func TestPlanKeyRotationCanRetireStagedReplacementThatWasNeverPrimary(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	oldProof := webhook.KeyProof("stripe", "acct_contract", "old", "whsec_old_contract")
	stagedProof := webhook.KeyProof("stripe", "acct_contract", "staged", "whsec_staged_contract")
	current := []webhook.KeyVersion{
		{KeyID: "old", State: webhook.KeyPrimary, ActivatedAt: now.Add(-time.Hour), SecretProof: oldProof},
		{KeyID: "staged", State: webhook.KeyAccepted, ActivatedAt: now.Add(-30 * time.Minute), SecretProof: stagedProof},
	}
	desired := webhook.DesiredKeyring{
		PrimaryKeyID: "old", AcceptedKeyIDs: []string{"old"},
		SecretProofs: map[string][32]byte{"old": oldProof}, Grace: time.Hour, Now: now,
	}
	plan, err := webhook.PlanKeyring(current, desired)
	if err != nil {
		t.Fatal(err)
	}
	staged := plan.ByID["staged"]
	if staged.State != webhook.KeyAccepted || staged.RetirementNotBefore == nil ||
		!staged.RetirementNotBefore.Equal(now.Add(time.Hour)) {
		t.Fatalf("staged key retirement plan = %+v", staged)
	}

	desired.Now = now.Add(time.Hour)
	plan, err = webhook.PlanKeyring(plan.Versions, desired)
	if err != nil {
		t.Fatal(err)
	}
	staged = plan.ByID["staged"]
	if staged.State != webhook.KeyRetired || staged.RetiredAt == nil || !staged.RetiredAt.Equal(desired.Now) {
		t.Fatalf("retired staged key = %+v", staged)
	}
}

func TestPlanKeyRotationRejectsEarlyRemovalThenRetiresAtDeadline(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)
	current := []webhook.KeyVersion{
		{KeyID: "new", State: webhook.KeyPrimary, ActivatedAt: now.Add(-time.Hour), SecretProof: webhook.KeyProof("stripe", "acct_contract", "new", "whsec_new_contract")},
		{KeyID: "old", State: webhook.KeyAccepted, ActivatedAt: now.Add(-2 * time.Hour), RetirementNotBefore: &deadline, SecretProof: webhook.KeyProof("stripe", "acct_contract", "old", "whsec_old_contract")},
	}
	proofs := map[string][32]byte{"new": current[0].SecretProof}
	_, err := webhook.PlanKeyring(current, webhook.DesiredKeyring{
		PrimaryKeyID: "new", AcceptedKeyIDs: []string{"new"}, SecretProofs: proofs, Grace: 24 * time.Hour, Now: now,
	})
	if !errors.Is(err, webhook.ErrKeyRetirementGrace) {
		t.Fatalf("early removal error = %v", err)
	}
	plan, err := webhook.PlanKeyring(current, webhook.DesiredKeyring{
		PrimaryKeyID: "new", AcceptedKeyIDs: []string{"new"}, SecretProofs: proofs, Grace: 24 * time.Hour, Now: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	old := plan.ByID["old"]
	if old.State != webhook.KeyRetired || old.RetiredAt == nil || !old.RetiredAt.Equal(deadline) {
		t.Fatalf("retired key = %+v", old)
	}
}

func TestPlanKeyRotationRejectsRetiredKeyReactivationAndUnboundedInput(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	retiredAt := now.Add(-time.Hour)
	proof := webhook.KeyProof("stripe", "acct_contract", "old", "whsec_old_contract")
	current := []webhook.KeyVersion{{KeyID: "old", State: webhook.KeyRetired, ActivatedAt: now.Add(-2 * time.Hour), RetiredAt: &retiredAt, SecretProof: proof}}
	_, err := webhook.PlanKeyring(current, webhook.DesiredKeyring{
		PrimaryKeyID: "old", AcceptedKeyIDs: []string{"old"}, SecretProofs: map[string][32]byte{"old": proof}, Grace: time.Hour, Now: now,
	})
	if !errors.Is(err, webhook.ErrKeyringConflict) {
		t.Fatalf("retired key reactivation error = %v", err)
	}
	_, err = webhook.PlanKeyring(nil, webhook.DesiredKeyring{
		PrimaryKeyID: "bad key", AcceptedKeyIDs: []string{"bad key"}, Grace: 0, Now: now,
	})
	if !errors.Is(err, webhook.ErrInvalidKeyring) {
		t.Fatalf("invalid keyring error = %v", err)
	}
}

func TestPlanKeyRotationRejectsOneSecretMappedToTwoKeyIDs(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	oldProof := webhook.KeyProof("stripe", "acct_contract", "old", "whsec_reused_contract")
	newProof := webhook.KeyProof("stripe", "acct_contract", "new", "whsec_reused_contract")
	if oldProof != newProof {
		t.Fatal("material identity proof must expose duplicate secret reuse")
	}
	_, err := webhook.PlanKeyring(nil, webhook.DesiredKeyring{
		PrimaryKeyID: "new", AcceptedKeyIDs: []string{"new", "old"},
		SecretProofs: map[string][32]byte{"new": newProof, "old": oldProof},
		Grace:        time.Hour, Now: now,
	})
	if !errors.Is(err, webhook.ErrInvalidKeyring) {
		t.Fatalf("reused secret error = %v", err)
	}
}
