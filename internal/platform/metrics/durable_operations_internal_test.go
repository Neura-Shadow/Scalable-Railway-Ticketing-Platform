package metrics

import (
	"strings"
	"testing"
)

func TestReplicationMetricsKeepUnknownAbsentAndIdleCurrent(t *testing.T) {
	if strings.Contains(replicationMetricsSQL, "COALESCE") {
		t.Fatal("replication metrics must not collapse an unknown LSN or replay timestamp to zero")
	}
	for _, required := range []string{
		"receive_lsn=replay_lsn THEN 0::float8",
		"receive_lsn IS NOT NULL",
		"replay_lsn IS NOT NULL",
		"replayed_at IS NOT NULL",
	} {
		if !strings.Contains(replicationMetricsSQL, required) {
			t.Fatalf("replication metrics SQL omits %q", required)
		}
	}
}
