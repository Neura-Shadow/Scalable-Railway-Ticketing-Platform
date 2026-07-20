package admissionredis

import (
	"strings"
	"testing"
)

func TestAdmissionScriptsAreAtomicBoundedAndClusterSafe(t *testing.T) {
	scripts := map[string]string{
		"install": installPolicyScript, "join": joinScript, "get": getScript,
		"preflight": inspectDeliveryScript, "cancel": cancelScript, "peek": peekScript, "issue": issueScript,
		"inspect": inspectTokenScript, "acquire": acquireScript, "release": releaseScript, "finalize": finalizeScript,
	}
	for name, script := range scripts {
		normalized := strings.ToUpper(script)
		if strings.Contains(normalized, "REDIS.CALL('KEYS'") || strings.Contains(normalized, `REDIS.CALL("KEYS"`) {
			t.Fatalf("%s script uses prohibited KEYS traversal", name)
		}
		if !strings.Contains(script, "KEYS[1]") || !strings.Contains(script, "policy_mismatch") {
			t.Fatalf("%s script does not validate the explicit policy-generation marker", name)
		}
	}
	if strings.Contains(strings.ToUpper(finalizeCommittedScript), "REDIS.CALL('KEYS'") ||
		!strings.Contains(finalizeCommittedScript, "redis.call('GET', KEYS[1])") {
		t.Fatal("durable finalize repair must remain exact-scope and cluster-safe")
	}
	for name, script := range map[string]string{
		"join": joinScript, "get": getScript, "preflight": inspectDeliveryScript,
		"peek": peekScript, "issue": issueScript, "acquire": acquireScript,
	} {
		if !strings.Contains(script, "redis.call('TIME')") {
			t.Fatalf("%s script does not use Redis TIME", name)
		}
	}
	if !strings.Contains(issueScript, "math.min(requested, rate_remaining, inflight_remaining)") {
		t.Fatal("issue script does not apply one bounded global capacity calculation")
	}
	if !strings.Contains(issueScript, "cleanup_limit") ||
		!strings.Contains(issueScript, "if capacity <= 0 then") ||
		!strings.Contains(issueScript, "return {'ok', recovered_count, expired_count, expired_entry_count}") {
		t.Fatal("issue maintenance must reclaim a bounded batch even with no queue candidates")
	}
	if !strings.Contains(issueScript, "entry_retention") ||
		!strings.Contains(issueScript, "field(entry, 'x'), entry_retention") {
		t.Fatal("admission must retain the entry through the issued token lifetime")
	}
	if !strings.Contains(joinScript, "field(id, 's'), 'expired'") ||
		!strings.Contains(joinScript, "'entry:' .. id") ||
		!strings.Contains(joinScript, "redis.call('ZADD', KEYS[7]") {
		t.Fatal("join stale-head cleanup must retain a bounded expired tombstone")
	}
	if strings.Contains(peekScript, "redis.call('ZREM', KEYS[3], entry)") ||
		strings.Contains(peekScript, "delete_entry(entry)") {
		t.Fatal("peek must leave expiry cleanup to counted bounded maintenance")
	}
	if !strings.Contains(issueScript, "terminalize_entry_expired(entry)") ||
		!strings.Contains(issueScript, "expired_entry_count = expired_entry_count + 1") {
		t.Fatal("maintenance must count and tombstone expired waiting-room entries")
	}
	if !strings.Contains(getScript, "terminalize_expired(active_token, now)") ||
		!strings.Contains(issueScript, "terminalize_expired(token)") ||
		!strings.Contains(issueScript, "status == 'consumed' or status == 'cancelled' or status == 'expired'") ||
		!strings.Contains(issueScript, "delete_token_state(member)") {
		t.Fatal("token expiry must release inflight state, persist a bounded tombstone, then physically GC it")
	}
	if !strings.Contains(acquireScript, "return {'retry_allowed'") ||
		strings.Contains(acquireScript, "return {'retry_allowed', ARGV[11]") {
		t.Fatal("processing retry must not receive the database lease owner")
	}
	if strings.Contains(inspectTokenScript, "HSET") || strings.Contains(inspectTokenScript, "DEL") {
		t.Fatal("signed token inspection must not mutate delivery or lifecycle state")
	}
	if strings.Contains(inspectDeliveryScript, "HSET") || strings.Contains(inspectDeliveryScript, "DEL") ||
		strings.Contains(inspectDeliveryScript, "ZREM") {
		t.Fatal("delivery preflight must not mutate delivery or lifecycle state")
	}
	if strings.Contains(issueScript, "field(token, 'm')") ||
		strings.Contains(getScript, "field(token, 'm')") ||
		strings.Contains(inspectTokenScript, "field(token, 'm')") {
		t.Fatal("Redis scripts must never store or return the raw bearer MAC")
	}
	if strings.Contains(inspectStateScript, "HSET") || strings.Contains(inspectStateScript, "ZREM") ||
		strings.Contains(inspectStateScript, "DEL") || strings.Contains(strings.ToUpper(inspectStateScript), "REDIS.CALL('KEYS'") {
		t.Fatal("state reconciliation must remain exact-scope, bounded, and detect-only")
	}
	if !strings.Contains(inspectStateScript, "'HSCAN'") || !strings.Contains(inspectStateScript, "'ZSCAN'") ||
		!strings.Contains(inspectStateScript, "'COUNT', limit") {
		t.Fatal("state reconciliation must expose bounded cursor-based pagination")
	}
	if strings.Contains(inspectStateScript, "redis.call('GET', KEYS[1])") ||
		!strings.Contains(inspectStateScript, "redis.call('GET', KEYS[2]) ~= expected") {
		t.Fatal("state reconciliation must inspect a live historical generation by continuity, not current-policy equality")
	}
	if !strings.Contains(cancelScript, "field(token, 'cr'), '1'") ||
		!strings.Contains(cancelScript, "return 'in_progress'") ||
		!strings.Contains(releaseScript, "or cancel_requested then") ||
		!strings.Contains(issueScript, "field(member, 'cr')) == '1'") {
		t.Fatal("processing cancellation must preserve the active lease and terminalize after release or recovery")
	}
	if !strings.Contains(acquireScript, "expires - now < processing_lease") ||
		!strings.Contains(acquireScript, "expire_token(token, now)") {
		t.Fatal("acquire must require enough token lifetime for the complete processing lease")
	}
	for name, script := range map[string]string{
		"cancel": cancelScript, "release": releaseScript,
		"finalize": finalizeScript, "finalize_committed": finalizeCommittedScript,
	} {
		if !strings.Contains(script, "'ZADD'") ||
			(!strings.Contains(script, "'cancelled'") && !strings.Contains(script, "'consumed'")) {
			t.Fatalf("%s script does not persist and schedule bounded terminal state", name)
		}
	}
	if !strings.Contains(finalizeScript, "field(token, 's'), 'consumed'") ||
		!strings.Contains(finalizeCommittedScript, "field(token, 's'), 'consumed'") ||
		!strings.Contains(finalizeCommittedScript, "status ~= 'expired'") ||
		!strings.Contains(finalizeCommittedScript, "status ~= 'cancelled'") {
		t.Fatal("normal and durable finalize must persist consumed state, including tombstone repair")
	}
}
