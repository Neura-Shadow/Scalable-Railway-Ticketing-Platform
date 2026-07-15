package app

import (
	"context"
	"errors"
	"strings"
	"time"

	accountsapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	accountsdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	accountspostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type registerCustomerInput struct {
	Email       string
	Password    string
	DisplayName string
}

type authAccounts interface {
	RegisterCustomer(context.Context, registerCustomerInput) (accountspostgres.User, error)
	Login(context.Context, httpapi.LoginCommand) (accountspostgres.User, error)
}

type authTokens interface {
	IssueTokenPair(string, accountsdomain.Role, int64) (accountsapp.TokenPair, error)
	ParseRefreshToken(context.Context, string) (accountsapp.TokenClaims, error)
}

type refreshTokens interface {
	Register(context.Context, accountspostgres.RefreshTokenRecord) error
	FamilyID(context.Context, []byte) (uuid.UUID, error)
	Rotate(context.Context, accountspostgres.RotateRefreshTokenParams) error
	RevokeFamily(context.Context, uuid.UUID, []byte, time.Time) error
}

type appClock interface {
	Now() time.Time
}

type tokenSubject struct {
	subject string
	role    accountsdomain.Role
	version int64
}

// AuthService coordinates account credentials, signed JWTs, and durable
// refresh-token families. Only the JTI digest crosses the persistence seam.
type AuthService struct {
	accounts  authAccounts
	tokens    authTokens
	refreshes refreshTokens
	clock     appClock
	accessTTL time.Duration
	newFamily func() uuid.UUID
}

func NewAuthService(accounts authAccounts, tokens authTokens, refreshes refreshTokens, clock appClock, accessTTL time.Duration, newFamily func() uuid.UUID) *AuthService {
	return &AuthService{accounts: accounts, tokens: tokens, refreshes: refreshes, clock: clock, accessTTL: accessTTL, newFamily: newFamily}
}

func (s *AuthService) Register(ctx context.Context, command httpapi.RegisterCommand) (httpapi.TokenPairView, error) {
	if s == nil || s.accounts == nil || strings.TrimSpace(command.DisplayName) == "" {
		return httpapi.TokenPairView{}, httpapi.ErrInvalidInput
	}
	user, err := s.accounts.RegisterCustomer(ctx, registerCustomerInput(command))
	if err != nil {
		return httpapi.TokenPairView{}, mapAccountError(err)
	}
	return s.issueAndRegister(ctx, user, s.newFamilyID())
}

func (s *AuthService) Login(ctx context.Context, command httpapi.LoginCommand) (httpapi.TokenPairView, error) {
	if s == nil || s.accounts == nil {
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	user, err := s.accounts.Login(ctx, command)
	if err != nil {
		return httpapi.TokenPairView{}, mapAccountError(err)
	}
	return s.issueAndRegister(ctx, user, s.newFamilyID())
}

func (s *AuthService) Refresh(ctx context.Context, raw string) (httpapi.TokenPairView, error) {
	if s == nil || s.tokens == nil || s.refreshes == nil || strings.TrimSpace(raw) == "" {
		return httpapi.TokenPairView{}, httpapi.ErrUnauthenticated
	}
	presented, err := s.tokens.ParseRefreshToken(ctx, raw)
	if err != nil {
		return httpapi.TokenPairView{}, httpapi.ErrUnauthenticated
	}
	userID, presentedHash, ok := refreshIdentity(presented)
	if !ok {
		return httpapi.TokenPairView{}, httpapi.ErrUnauthenticated
	}
	familyID, err := s.refreshes.FamilyID(ctx, presentedHash)
	if err != nil || familyID == uuid.Nil {
		if errors.Is(err, accountspostgres.ErrInvalidRefreshToken) {
			return httpapi.TokenPairView{}, httpapi.ErrUnauthenticated
		}
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	pair, err := s.tokens.IssueTokenPair(presented.Subject, presented.Role, presented.TokenVersion)
	if err != nil {
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	replacementClaims, err := s.tokens.ParseRefreshToken(ctx, pair.RefreshToken)
	if err != nil {
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	replacement, ok := refreshRecord(replacementClaims, familyID)
	if !ok || replacement.UserID != userID || replacement.TokenVersion != presented.TokenVersion {
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	err = s.refreshes.Rotate(ctx, accountspostgres.RotateRefreshTokenParams{
		PresentedJTIHash: presentedHash,
		Replacement:      replacement,
		Now:              s.now(),
	})
	if err != nil {
		if errors.Is(err, accountspostgres.ErrInvalidRefreshToken) || errors.Is(err, accountspostgres.ErrRefreshTokenReplay) {
			return httpapi.TokenPairView{}, httpapi.ErrUnauthenticated
		}
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	return s.tokenView(pair), nil
}

func (s *AuthService) Logout(ctx context.Context, ownerID, raw string) error {
	if s == nil || s.tokens == nil || s.refreshes == nil || strings.TrimSpace(raw) == "" {
		return httpapi.ErrUnauthenticated
	}
	claims, err := s.tokens.ParseRefreshToken(ctx, raw)
	if err != nil {
		return httpapi.ErrUnauthenticated
	}
	userID, hash, ok := refreshIdentity(claims)
	if !ok {
		return httpapi.ErrUnauthenticated
	}
	if claims.Subject != ownerID {
		return httpapi.ErrForbidden
	}
	if err := s.refreshes.RevokeFamily(ctx, userID, hash, s.now()); err != nil {
		if errors.Is(err, accountspostgres.ErrInvalidRefreshToken) {
			return httpapi.ErrUnauthenticated
		}
		return httpapi.ErrUnavailable
	}
	return nil
}

func (s *AuthService) issueAndRegister(ctx context.Context, user accountspostgres.User, familyID uuid.UUID) (httpapi.TokenPairView, error) {
	if s.tokens == nil || s.refreshes == nil || familyID == uuid.Nil || user.ID == "" || user.TokenVersion <= 0 {
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	pair, err := s.tokens.IssueTokenPair(user.ID, user.Role, user.TokenVersion)
	if err != nil {
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	claims, err := s.tokens.ParseRefreshToken(ctx, pair.RefreshToken)
	if err != nil {
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	record, ok := refreshRecord(claims, familyID)
	if !ok || record.UserID.String() != user.ID || record.TokenVersion != user.TokenVersion || claims.Role != user.Role {
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	if err := s.refreshes.Register(ctx, record); err != nil {
		return httpapi.TokenPairView{}, httpapi.ErrUnavailable
	}
	return s.tokenView(pair), nil
}

func refreshRecord(claims accountsapp.TokenClaims, familyID uuid.UUID) (accountspostgres.RefreshTokenRecord, bool) {
	userID, hash, ok := refreshIdentity(claims)
	if !ok || familyID == uuid.Nil || claims.ExpiresAt == nil {
		return accountspostgres.RefreshTokenRecord{}, false
	}
	return accountspostgres.RefreshTokenRecord{
		UserID: userID, FamilyID: familyID, JTIHash: hash,
		TokenVersion: claims.TokenVersion, ExpiresAt: claims.ExpiresAt.Time.UTC(),
	}, true
}

func refreshIdentity(claims accountsapp.TokenClaims) (uuid.UUID, []byte, bool) {
	if claims.TokenType != accountsapp.TokenTypeRefresh || claims.TokenVersion <= 0 {
		return uuid.Nil, nil, false
	}
	userID, err := uuid.Parse(claims.Subject)
	if err != nil {
		return uuid.Nil, nil, false
	}
	hash, err := accountsapp.HashTokenID(claims.ID)
	if err != nil {
		return uuid.Nil, nil, false
	}
	return userID, append([]byte(nil), hash[:]...), true
}

func (s *AuthService) tokenView(pair accountsapp.TokenPair) httpapi.TokenPairView {
	expires := int64(s.accessTTL / time.Second)
	return httpapi.TokenPairView{AccessToken: pair.AccessToken, RefreshToken: pair.RefreshToken, TokenType: "Bearer", ExpiresIn: expires}
}

func (s *AuthService) newFamilyID() uuid.UUID {
	if s.newFamily == nil {
		return uuid.Nil
	}
	return s.newFamily()
}

func (s *AuthService) now() time.Time {
	if s.clock == nil {
		return time.Time{}
	}
	return s.clock.Now().UTC()
}

func mapAccountError(err error) error {
	switch {
	case errors.Is(err, accountspostgres.ErrEmailAlreadyRegistered):
		return httpapi.ErrConflict
	case errors.Is(err, accountspostgres.ErrInvalidInput):
		return httpapi.ErrInvalidInput
	case errors.Is(err, accountspostgres.ErrInvalidCredentials), errors.Is(err, accountspostgres.ErrUserNotFound):
		return httpapi.ErrUnauthenticated
	default:
		return httpapi.ErrUnavailable
	}
}

var _ httpapi.AuthService = (*AuthService)(nil)
