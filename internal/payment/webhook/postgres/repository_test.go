package postgres

import "testing"

func TestWebhookConflictQuarantinesEveryClaimableState(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"received", "processing", "failed_retryable"} {
		if !webhookConflictQuarantinable(state) {
			t.Fatalf("state %q must be quarantinable", state)
		}
	}
	for _, state := range []string{"processed", "ignored", "failed_permanent", "security_conflict"} {
		if webhookConflictQuarantinable(state) {
			t.Fatalf("terminal state %q must remain immutable", state)
		}
	}
}
