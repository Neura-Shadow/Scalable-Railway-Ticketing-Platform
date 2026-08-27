package settlement

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidImportLease = errors.New("invalid settlement import lease")
	ErrImportLeaseLost    = errors.New("settlement import lease lost")
)

// ImportLease is the durable ownership token for one provider-account pass.
// The token, rather than the human-readable owner, prevents an expired worker
// from committing after another replica has taken over the account.
type ImportLease struct {
	Scope      AccountScope
	Owner      string
	Token      uuid.UUID
	Cursor     string
	LeaseUntil time.Time
}

// ImportLeaser claims due account work in a short transaction and returns an
// ImportStore bound to the resulting token. Provider I/O is performed only
// after ClaimDue returns and therefore never runs inside the claim transaction.
type ImportLeaser interface {
	ClaimDue(context.Context, AccountScope, string, time.Time, time.Duration) (ImportStore, ImportLease, bool, error)
	// FinishLease schedules from the durable store's clock. A zero delay makes
	// failed work immediately eligible without trusting a worker host clock.
	FinishLease(context.Context, ImportLease, time.Duration) error
}
