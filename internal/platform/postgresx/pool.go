// Package postgresx constructs bounded PostgreSQL pools without returning DSN
// parser details that could disclose configured credentials.
package postgresx

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInvalidPoolConfig = errors.New("postgres pool configuration invalid")

const maxOpenConnections = 1_000

// RegionalSession is the bounded PostgreSQL session identity consumed by the
// v11/v3 database-local write guards. It contains no host or credential data.
type RegionalSession struct {
	Region        string
	Role          string
	Epoch         int64
	WritesEnabled bool
}

// ParseRegionalSession converts the four deployment environment values used by
// operator binaries into one bounded session identity. Invalid input is never
// included in the returned error because environment values may be supplied by
// a secret manager even though this identity itself is not secret.
func ParseRegionalSession(region, role, epoch, writesEnabled string) (RegionalSession, error) {
	parsedEpoch, epochErr := strconv.ParseInt(strings.TrimSpace(epoch), 10, 64)
	parsedWritesEnabled, writesErr := strconv.ParseBool(strings.TrimSpace(writesEnabled))
	session := RegionalSession{
		Region:        strings.ToLower(strings.TrimSpace(region)),
		Role:          strings.ToLower(strings.TrimSpace(role)),
		Epoch:         parsedEpoch,
		WritesEnabled: parsedWritesEnabled,
	}
	if epochErr != nil || writesErr != nil || ValidateRegionalSession(session) != nil {
		return RegionalSession{}, ErrInvalidPoolConfig
	}
	return session, nil
}

func NewBoundedPool(ctx context.Context, dsn string, maxOpen int) (*pgxpool.Pool, error) {
	return newBoundedPool(ctx, dsn, maxOpen, nil)
}

// NewRegionalBoundedPool installs immutable connection runtime parameters for
// every transaction. Database triggers still lock and compare the durable
// regional authority row; these parameters are not an external fence.
func NewRegionalBoundedPool(ctx context.Context, dsn string, maxOpen int, session RegionalSession) (*pgxpool.Pool, error) {
	if !validRegionalSession(session) {
		return nil, ErrInvalidPoolConfig
	}
	return newBoundedPool(ctx, dsn, maxOpen, &session)
}

func newBoundedPool(ctx context.Context, dsn string, maxOpen int, session *RegionalSession) (*pgxpool.Pool, error) {
	if ctx == nil || maxOpen < 1 || maxOpen > maxOpenConnections {
		return nil, ErrInvalidPoolConfig
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, ErrInvalidPoolConfig
	}
	config.MaxConns = int32(maxOpen)
	if config.MinConns > config.MaxConns {
		config.MinConns = 0
	}
	if session != nil {
		if err := ApplyRegionalSession(config.ConnConfig, *session); err != nil {
			return nil, err
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, ErrInvalidPoolConfig
	}
	return pool, nil
}

// ApplyRegionalSession installs the bounded runtime identity on a pgx config.
// It is shared by control and independently pooled physical shard adapters.
func ApplyRegionalSession(config *pgx.ConnConfig, session RegionalSession) error {
	if config == nil || ValidateRegionalSession(session) != nil {
		return ErrInvalidPoolConfig
	}
	if config.RuntimeParams == nil {
		config.RuntimeParams = make(map[string]string, 4)
	}
	config.RuntimeParams["railway.deployment_region"] = session.Region
	config.RuntimeParams["railway.deployment_role"] = session.Role
	config.RuntimeParams["railway.region_epoch"] = strconv.FormatInt(session.Epoch, 10)
	config.RuntimeParams["railway.regional_writes_enabled"] = strconv.FormatBool(session.WritesEnabled)
	return nil
}

func ValidateRegionalSession(session RegionalSession) error {
	if !validRegionalSession(session) {
		return ErrInvalidPoolConfig
	}
	return nil
}

func validRegionalSession(session RegionalSession) bool {
	if session.Region != "region-a" && session.Region != "region-b" || session.Epoch < 1 {
		return false
	}
	if session.Role != "active" && session.Role != "passive" && session.Role != "recovery" {
		return false
	}
	return !session.WritesEnabled || session.Role == "active"
}
