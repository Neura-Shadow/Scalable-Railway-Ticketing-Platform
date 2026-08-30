package authority_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
)

func TestRequireNewerEpochRejectsReuseAndDecrease(t *testing.T) {
	t.Parallel()

	current := mustEpoch(t, 11)
	for _, candidate := range []authority.Epoch{mustEpoch(t, 10), current} {
		if err := authority.RequireNewerEpoch(current, candidate); !errors.Is(err, authority.ErrEpochNotNewer) {
			t.Fatalf("RequireNewerEpoch(11, %d) error = %v, want ErrEpochNotNewer", candidate.Uint64(), err)
		}
	}
	if err := authority.RequireNewerEpoch(current, mustEpoch(t, 12)); err != nil {
		t.Fatalf("RequireNewerEpoch(11, 12) error = %v", err)
	}
}
