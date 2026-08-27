package postgresx

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
)

var ErrRegionalAuthorityUnavailable = errors.New("regional database authority unavailable")

// CheckRegionalReadiness binds a process session to the durable authority row
// and the database primary identity. It is intentionally safe for readiness
// and pass gates: errors contain no DSN or authority contents.
func CheckRegionalReadiness(ctx context.Context, db authoritypostgres.QueryRower, session RegionalSession) error {
	if ValidateRegionalSession(session) != nil || !session.WritesEnabled {
		return ErrRegionalAuthorityUnavailable
	}
	region, err := authority.ParseRegion(session.Region)
	if err != nil {
		return ErrRegionalAuthorityUnavailable
	}
	epoch, err := authority.NewEpoch(uint64(session.Epoch))
	if err != nil {
		return ErrRegionalAuthorityUnavailable
	}
	deployment, err := authority.NewDeployment(region, authority.Role(session.Role), epoch, session.WritesEnabled)
	if err != nil || authoritypostgres.CheckActiveReadiness(ctx, db, deployment) != nil {
		return ErrRegionalAuthorityUnavailable
	}
	return nil
}
