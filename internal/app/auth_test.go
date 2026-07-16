package app

import (
	"context"
	"errors"
	"testing"
	"time"

	accountsapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	accountsdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	accountspostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type authAccountsFake struct {
	registerInput registerCustomerInput
	registerUser  accountspostgres.User
	registerErr   error
	loginInput    httpapi.LoginCommand
	loginUser     accountspostgres.User
	loginErr      error
}

func (f *authAccountsFake) RegisterCustomer(_ context.Context, input registerCustomerInput) (accountspostgres.User, error) {
	f.registerInput = input
	return f.registerUser, f.registerErr
}

func (f *authAccountsFake) Login(_ context.Context, input httpapi.LoginCommand) (accountspostgres.User, error) {
	f.loginInput = input
	return f.loginUser, f.loginErr
}

type authTokensFake struct {
	pairs       []accountsapp.TokenPair
	issueInputs []tokenSubject
	claims      map[string]accountsapp.TokenClaims
	parseErr    map[string]error
}

func (f *authTokensFake) IssueTokenPair(subject string, role accountsdomain.Role, version int64) (accountsapp.TokenPair, error) {
	f.issueInputs = append(f.issueInputs, tokenSubject{subject: subject, role: role, version: version})
	pair := f.pairs[0]
	f.pairs = f.pairs[1:]
	return pair, nil
}

func (f *authTokensFake) ParseRefreshToken(_ context.Context, raw string) (accountsapp.TokenClaims, error) {
	if err := f.parseErr[raw]; err != nil {
		return accountsapp.TokenClaims{}, err
	}
	return f.claims[raw], nil
}

type refreshTokensFake struct {
	registered  accountspostgres.RefreshTokenRecord
	registerErr error
	family      uuid.UUID
	familyErr   error
	rotated     accountspostgres.RotateRefreshTokenParams
	rotateErr   error
	revokedUser uuid.UUID
	revokedHash []byte
	revokeErr   error
}

func (f *refreshTokensFake) Register(_ context.Context, record accountspostgres.RefreshTokenRecord) error {
	f.registered = record
	return f.registerErr
}

func (f *refreshTokensFake) FamilyID(_ context.Context, _ []byte) (uuid.UUID, error) {
	return f.family, f.familyErr
}

func (f *refreshTokensFake) Rotate(_ context.Context, params accountspostgres.RotateRefreshTokenParams) error {
	f.rotated = params
	return f.rotateErr
}

func (f *refreshTokensFake) RevokeFamily(_ context.Context, userID uuid.UUID, hash []byte, _ time.Time) error {
	f.revokedUser, f.revokedHash = userID, append([]byte(nil), hash...)
	return f.revokeErr
}

func refreshClaims(subject, id string, role accountsdomain.Role, version int64, expires time.Time) accountsapp.TokenClaims {
	return accountsapp.TokenClaims{
		Role: role, TokenType: accountsapp.TokenTypeRefresh, TokenVersion: version,
		RegisteredClaims: jwt.RegisteredClaims{Subject: subject, ID: id, ExpiresAt: jwt.NewNumericDate(expires)},
	}
}

func TestAuthRegisterCreatesCustomerAndPersistsOnlyHashedRefreshIdentifier(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	userID, refreshID, familyID := uuid.New(), uuid.New(), uuid.New()
	accounts := &authAccountsFake{registerUser: accountspostgres.User{ID: userID.String(), Role: accountsdomain.RoleCustomer, TokenVersion: 3}}
	tokens := &authTokensFake{
		pairs:  []accountsapp.TokenPair{{AccessToken: "access", RefreshToken: "refresh-raw"}},
		claims: map[string]accountsapp.TokenClaims{"refresh-raw": refreshClaims(userID.String(), refreshID.String(), accountsdomain.RoleCustomer, 3, now.Add(7*24*time.Hour))},
	}
	refreshes := &refreshTokensFake{}
	service := NewAuthService(accounts, tokens, refreshes, fixedClock{now}, 15*time.Minute, func() uuid.UUID { return familyID })

	got, err := service.Register(context.Background(), httpapi.RegisterCommand{Email: "user@example.com", Password: "long-password", DisplayName: "Rider"})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if accounts.registerInput != (registerCustomerInput{Email: "user@example.com", Password: "long-password", DisplayName: "Rider"}) {
		t.Fatalf("register input = %#v", accounts.registerInput)
	}
	if got.AccessToken != "access" || got.RefreshToken != "refresh-raw" || got.TokenType != "Bearer" || got.ExpiresIn != 900 {
		t.Fatalf("token view = %#v", got)
	}
	wantHash, _ := accountsapp.HashTokenID(refreshID.String())
	if refreshes.registered.UserID != userID || refreshes.registered.FamilyID != familyID ||
		string(refreshes.registered.JTIHash) != string(wantHash[:]) || refreshes.registered.TokenVersion != 3 {
		t.Fatalf("persisted refresh record = %#v", refreshes.registered)
	}
	if string(refreshes.registered.JTIHash) == "refresh-raw" {
		t.Fatal("raw refresh token was persisted")
	}
}

func TestAuthRefreshRotatesWithinPersistedFamily(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	userID, oldID, newID, familyID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	oldClaims := refreshClaims(userID.String(), oldID.String(), accountsdomain.RoleCustomer, 2, now.Add(time.Hour))
	newClaims := refreshClaims(userID.String(), newID.String(), accountsdomain.RoleCustomer, 2, now.Add(2*time.Hour))
	tokens := &authTokensFake{
		pairs:  []accountsapp.TokenPair{{AccessToken: "new-access", RefreshToken: "new-refresh"}},
		claims: map[string]accountsapp.TokenClaims{"old-refresh": oldClaims, "new-refresh": newClaims},
	}
	refreshes := &refreshTokensFake{family: familyID}
	service := NewAuthService(&authAccountsFake{}, tokens, refreshes, fixedClock{now}, 15*time.Minute, uuid.New)

	got, err := service.Refresh(context.Background(), "old-refresh")
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if got.AccessToken != "new-access" || got.RefreshToken != "new-refresh" {
		t.Fatalf("token view = %#v", got)
	}
	wantOldHash, _ := accountsapp.HashTokenID(oldID.String())
	wantNewHash, _ := accountsapp.HashTokenID(newID.String())
	if string(refreshes.rotated.PresentedJTIHash) != string(wantOldHash[:]) {
		t.Fatal("presented JTI hash was not rotated")
	}
	replacement := refreshes.rotated.Replacement
	if replacement.FamilyID != familyID || replacement.UserID != userID || string(replacement.JTIHash) != string(wantNewHash[:]) {
		t.Fatalf("replacement = %#v", replacement)
	}
}

func TestAuthRefreshMapsReplayToSafeUnauthenticatedError(t *testing.T) {
	now := time.Now().UTC()
	userID, oldID, newID := uuid.New(), uuid.New(), uuid.New()
	tokens := &authTokensFake{
		pairs: []accountsapp.TokenPair{{AccessToken: "access", RefreshToken: "new"}},
		claims: map[string]accountsapp.TokenClaims{
			"old": refreshClaims(userID.String(), oldID.String(), accountsdomain.RoleCustomer, 1, now.Add(time.Hour)),
			"new": refreshClaims(userID.String(), newID.String(), accountsdomain.RoleCustomer, 1, now.Add(time.Hour)),
		},
	}
	refreshes := &refreshTokensFake{family: uuid.New(), rotateErr: accountspostgres.ErrRefreshTokenReplay}
	_, err := NewAuthService(&authAccountsFake{}, tokens, refreshes, fixedClock{now}, time.Minute, uuid.New).Refresh(context.Background(), "old")
	if err != httpapi.ErrUnauthenticated {
		t.Fatalf("error = %v, want exact safe sentinel", err)
	}
}

func TestAuthLogoutRequiresRefreshSubjectToMatchAccessSubject(t *testing.T) {
	now := time.Now().UTC()
	refreshID := uuid.New()
	tokens := &authTokensFake{claims: map[string]accountsapp.TokenClaims{
		"refresh": refreshClaims(uuid.NewString(), refreshID.String(), accountsdomain.RoleCustomer, 1, now.Add(time.Hour)),
	}}
	refreshes := &refreshTokensFake{}
	err := NewAuthService(&authAccountsFake{}, tokens, refreshes, fixedClock{now}, time.Minute, uuid.New).
		Logout(context.Background(), uuid.NewString(), "refresh")
	if err != httpapi.ErrForbidden {
		t.Fatalf("Logout() error = %v", err)
	}
	if refreshes.revokedUser != uuid.Nil {
		t.Fatal("mismatched family was revoked")
	}
}

func TestAuthMapsAccountErrorsToSafeSentinels(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"duplicate", accountspostgres.ErrEmailAlreadyRegistered, httpapi.ErrConflict},
		{"invalid", accountspostgres.ErrInvalidInput, httpapi.ErrInvalidInput},
		{"credentials", accountspostgres.ErrInvalidCredentials, httpapi.ErrUnauthenticated},
		{"persistence", errors.New("database host secret"), httpapi.ErrUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accounts := &authAccountsFake{loginErr: test.err}
			_, err := NewAuthService(accounts, &authTokensFake{}, &refreshTokensFake{}, fixedClock{time.Now()}, time.Minute, uuid.New).
				Login(context.Background(), httpapi.LoginCommand{Email: "a@b.c", Password: "password"})
			if err != test.want {
				t.Fatalf("error = %v, want exact %v", err, test.want)
			}
		})
	}
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }
