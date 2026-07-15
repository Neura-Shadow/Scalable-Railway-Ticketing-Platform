package app

import (
	"context"
	"errors"
	"time"

	accountsapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	accountsdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	accountspostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresAuthAccounts struct {
	pool      *pgxpool.Pool
	passwords accountsapp.PasswordHasher
	login     *accountspostgres.Store
}

func (p *postgresAuthAccounts) RegisterCustomer(ctx context.Context, input registerCustomerInput) (accountspostgres.User, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return accountspostgres.User{}, accountspostgres.ErrPersistence
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	store, err := accountspostgres.NewStore(tx, p.passwords)
	if err != nil {
		return accountspostgres.User{}, err
	}
	user, err := store.RegisterUser(ctx, accountspostgres.RegisterUserParams{Email: input.Email, Password: input.Password, Role: accountsdomain.RoleCustomer})
	if err != nil {
		return accountspostgres.User{}, err
	}
	if _, err = store.CreatePassenger(ctx, accountspostgres.CreatePassengerParams{UserID: user.ID, DisplayName: input.DisplayName}); err != nil {
		return accountspostgres.User{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return accountspostgres.User{}, accountspostgres.ErrPersistence
	}
	return user, nil
}
func (p *postgresAuthAccounts) Login(ctx context.Context, input httpapi.LoginCommand) (accountspostgres.User, error) {
	return p.login.LookupUserForLogin(ctx, accountspostgres.LoginLookup{Email: input.Email, Password: input.Password})
}

type postgresRefreshTokens struct {
	pool       *pgxpool.Pool
	repository *accountspostgres.RefreshTokenRepository
}

func (p *postgresRefreshTokens) Register(ctx context.Context, r accountspostgres.RefreshTokenRecord) error {
	return p.repository.Register(ctx, r)
}
func (p *postgresRefreshTokens) Rotate(ctx context.Context, r accountspostgres.RotateRefreshTokenParams) error {
	return p.repository.Rotate(ctx, r)
}
func (p *postgresRefreshTokens) RevokeFamily(ctx context.Context, user uuid.UUID, hash []byte, now time.Time) error {
	return p.repository.RevokeFamily(ctx, user, hash, now)
}
func (p *postgresRefreshTokens) FamilyID(ctx context.Context, hash []byte) (uuid.UUID, error) {
	if len(hash) != 32 {
		return uuid.Nil, accountspostgres.ErrInvalidRefreshToken
	}
	var family uuid.UUID
	err := p.pool.QueryRow(ctx, "SELECT family_id FROM refresh_tokens WHERE jti_hash=$1", hash).Scan(&family)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, accountspostgres.ErrInvalidRefreshToken
	}
	if err != nil {
		return uuid.Nil, accountspostgres.ErrPersistence
	}
	return family, nil
}

// NewPostgresAuth builds the credential, JWT, bearer, and durable refresh
// adapters against one pgx pool. Registration creates the customer and their
// initial passenger atomically.
func NewPostgresAuth(pool *pgxpool.Pool, cfg config.Config, clock appClock) (*AuthService, *BearerTokenParser, error) {
	if pool == nil {
		return nil, nil, accountspostgres.ErrInvalidStoreConfiguration
	}
	passwords, err := accountsapp.NewBcryptPasswordHasher(cfg.BcryptCost)
	if err != nil {
		return nil, nil, err
	}
	store, err := accountspostgres.NewStore(pool, passwords)
	if err != nil {
		return nil, nil, err
	}
	if clock == nil {
		clock = wallClock{}
	}
	jwtService, err := accountsapp.NewJWTService(accountsapp.JWTConfig{Secret: []byte(cfg.JWTSecret), Issuer: cfg.JWTIssuer, Audience: cfg.JWTAudience, AccessTTL: cfg.AccessTokenTTL, RefreshTTL: cfg.RefreshTokenTTL, Clock: clock}, store)
	if err != nil {
		return nil, nil, err
	}
	repository, err := accountspostgres.NewRefreshTokenRepository(pool)
	if err != nil {
		return nil, nil, err
	}
	auth := NewAuthService(&postgresAuthAccounts{pool: pool, passwords: passwords, login: store}, jwtService, &postgresRefreshTokens{pool: pool, repository: repository}, clock, cfg.AccessTokenTTL, uuid.New)
	return auth, NewBearerTokenParser(jwtService), nil
}

func NewPostgresPassengerService(pool *pgxpool.Pool, bcryptCost int) (*PassengerService, error) {
	passwords, err := accountsapp.NewBcryptPasswordHasher(bcryptCost)
	if err != nil {
		return nil, err
	}
	store, err := accountspostgres.NewStore(pool, passwords)
	if err != nil {
		return nil, err
	}
	return NewPassengerService(store), nil
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }
