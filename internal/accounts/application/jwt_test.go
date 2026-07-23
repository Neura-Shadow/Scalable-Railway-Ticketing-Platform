package application_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	"github.com/golang-jwt/jwt/v5"
)

type fixedClock struct {
	now time.Time
}

func TestJWTServiceRejectsInvalidTokenContracts(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 15, 8, 30, 0, 0, time.UTC)
	readerFailure := errors.New("state store unavailable")
	tests := []struct {
		name         string
		mutate       func(*application.TokenClaims)
		method       jwt.SigningMethod
		signingKey   any
		state        application.SubjectState
		stateErr     error
		parseRefresh bool
		omitVersion  bool
		wantErr      error
	}{
		{
			name:       "HS512 is not accepted as HS256",
			method:     jwt.SigningMethodHS512,
			signingKey: testJWTSecret,
			state:      activeSubjectState,
			wantErr:    application.ErrUnexpectedSigningMethod,
		},
		{
			name:       "unsigned token is rejected",
			method:     jwt.SigningMethodNone,
			signingKey: jwt.UnsafeAllowNoneSignatureType,
			state:      activeSubjectState,
			wantErr:    application.ErrUnexpectedSigningMethod,
		},
		{
			name:  "access token rejected by refresh parser",
			state: activeSubjectState, parseRefresh: true,
			wantErr: application.ErrInvalidTokenType,
		},
		{
			name: "unknown token type",
			mutate: func(claims *application.TokenClaims) {
				claims.TokenType = "bearer"
			},
			state: activeSubjectState, wantErr: application.ErrInvalidTokenType,
		},
		{
			name: "wrong issuer",
			mutate: func(claims *application.TokenClaims) {
				claims.Issuer = "another-issuer"
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenInvalidIssuer,
		},
		{
			name: "missing issuer",
			mutate: func(claims *application.TokenClaims) {
				claims.Issuer = ""
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenRequiredClaimMissing,
		},
		{
			name: "wrong audience",
			mutate: func(claims *application.TokenClaims) {
				claims.Audience = jwt.ClaimStrings{"another-audience"}
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenInvalidAudience,
		},
		{
			name: "missing audience",
			mutate: func(claims *application.TokenClaims) {
				claims.Audience = nil
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenRequiredClaimMissing,
		},
		{
			name: "additional audience",
			mutate: func(claims *application.TokenClaims) {
				claims.Audience = append(claims.Audience, "unexpected-audience")
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenInvalidAudience,
		},
		{
			name: "expired",
			mutate: func(claims *application.TokenClaims) {
				claims.ExpiresAt = jwt.NewNumericDate(now.Add(-time.Second))
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenExpired,
		},
		{
			name: "not active yet",
			mutate: func(claims *application.TokenClaims) {
				claims.NotBefore = jwt.NewNumericDate(now.Add(time.Minute))
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenNotValidYet,
		},
		{
			name: "issued in future",
			mutate: func(claims *application.TokenClaims) {
				claims.IssuedAt = jwt.NewNumericDate(now.Add(time.Minute))
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenUsedBeforeIssued,
		},
		{
			name: "missing expiry",
			mutate: func(claims *application.TokenClaims) {
				claims.ExpiresAt = nil
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenRequiredClaimMissing,
		},
		{
			name: "missing not-before",
			mutate: func(claims *application.TokenClaims) {
				claims.NotBefore = nil
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenRequiredClaimMissing,
		},
		{
			name: "missing issued-at",
			mutate: func(claims *application.TokenClaims) {
				claims.IssuedAt = nil
			},
			state: activeSubjectState, wantErr: jwt.ErrTokenRequiredClaimMissing,
		},
		{
			name: "missing subject",
			mutate: func(claims *application.TokenClaims) {
				claims.Subject = ""
			},
			state: activeSubjectState, wantErr: application.ErrInvalidTokenSubject,
		},
		{
			name: "invalid role",
			mutate: func(claims *application.TokenClaims) {
				claims.Role = "super-admin"
			},
			state: activeSubjectState, wantErr: domain.ErrInvalidRole,
		},
		{
			name: "negative token version",
			mutate: func(claims *application.TokenClaims) {
				claims.TokenVersion = -1
			},
			state: activeSubjectState, wantErr: application.ErrInvalidTokenVersion,
		},
		{
			name:        "missing token version",
			omitVersion: true,
			state:       activeSubjectState,
			wantErr:     application.ErrInvalidTokenVersion,
		},
		{
			name:    "inactive user",
			state:   application.SubjectState{Active: false, TokenVersion: 7},
			wantErr: application.ErrUserInactive,
		},
		{
			name:    "revoked token version",
			state:   application.SubjectState{Active: true, TokenVersion: 8},
			wantErr: application.ErrTokenVersionMismatch,
		},
		{
			name: "signed role differs from authoritative role",
			mutate: func(claims *application.TokenClaims) {
				claims.Role = domain.RoleAdmin
			},
			state:   activeSubjectState,
			wantErr: application.ErrTokenRoleMismatch,
		},
		{
			name:     "subject state lookup failure",
			state:    activeSubjectState,
			stateErr: readerFailure,
			wantErr:  readerFailure,
		},
		{
			name:       "signature from another key",
			signingKey: bytes.Repeat([]byte{0x5a}, application.MinJWTSecretBytes),
			state:      activeSubjectState,
			wantErr:    jwt.ErrTokenSignatureInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service := newTestJWTService(t, now, stubSubjectStateReader{state: tt.state, err: tt.stateErr})
			claims := validAccessClaims(now)
			if tt.mutate != nil {
				tt.mutate(&claims)
			}
			method := tt.method
			if method == nil {
				method = jwt.SigningMethodHS256
			}
			key := tt.signingKey
			if key == nil {
				key = testJWTSecret
			}
			claimsToSign := jwt.Claims(claims)
			if tt.omitVersion {
				claimsToSign = accessMapClaimsWithoutVersion(now)
			}
			raw := signTestToken(t, method, claimsToSign, key)

			var err error
			if tt.parseRefresh {
				_, err = service.ParseRefreshToken(context.Background(), raw)
			} else {
				_, err = service.ParseAccessToken(context.Background(), raw)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Parse token error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestJWTServiceRejectsInvalidConfigurationAndIssueClaims(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 15, 8, 30, 0, 0, time.UTC)
	validConfig := application.JWTConfig{
		Secret:     testJWTSecret,
		Issuer:     testJWTIssuer,
		Audience:   testJWTAudience,
		AccessTTL:  15 * time.Minute,
		RefreshTTL: 24 * time.Hour,
		Clock:      fixedClock{now: now},
	}

	configTests := []struct {
		name   string
		mutate func(*application.JWTConfig)
		reader application.SubjectStateReader
	}{
		{name: "short secret", mutate: func(config *application.JWTConfig) { config.Secret = []byte("short") }, reader: stubSubjectStateReader{}},
		{name: "missing issuer", mutate: func(config *application.JWTConfig) { config.Issuer = "" }, reader: stubSubjectStateReader{}},
		{name: "missing audience", mutate: func(config *application.JWTConfig) { config.Audience = "" }, reader: stubSubjectStateReader{}},
		{name: "invalid access TTL", mutate: func(config *application.JWTConfig) { config.AccessTTL = 0 }, reader: stubSubjectStateReader{}},
		{name: "invalid refresh TTL", mutate: func(config *application.JWTConfig) { config.RefreshTTL = 0 }, reader: stubSubjectStateReader{}},
		{name: "missing state reader", mutate: func(*application.JWTConfig) {}, reader: nil},
	}
	for _, tt := range configTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := validConfig
			tt.mutate(&config)
			if _, err := application.NewJWTService(config, tt.reader); !errors.Is(err, application.ErrInvalidJWTConfiguration) {
				t.Fatalf("NewJWTService() error = %v, want %v", err, application.ErrInvalidJWTConfiguration)
			}
		})
	}

	service := newTestJWTService(t, now, stubSubjectStateReader{state: activeSubjectState})
	issueTests := []struct {
		name         string
		subject      string
		role         domain.Role
		tokenVersion int64
		wantErr      error
	}{
		{name: "missing subject", subject: "", role: domain.RoleCustomer, tokenVersion: 7, wantErr: application.ErrInvalidTokenSubject},
		{name: "invalid role", subject: "user-123", role: "super-admin", tokenVersion: 7, wantErr: domain.ErrInvalidRole},
		{name: "negative token version", subject: "user-123", role: domain.RoleCustomer, tokenVersion: -1, wantErr: application.ErrInvalidTokenVersion},
	}
	for _, tt := range issueTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := service.IssueTokenPair(tt.subject, tt.role, tt.tokenVersion); !errors.Is(err, tt.wantErr) {
				t.Fatalf("IssueTokenPair() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

var (
	testJWTSecret      = bytes.Repeat([]byte{0xa5}, application.MinJWTSecretBytes)
	activeSubjectState = application.SubjectState{Active: true, Role: domain.RoleCustomer, TokenVersion: 7}
)

const (
	testJWTIssuer   = "railway-ticketing-api"
	testJWTAudience = "railway-ticketing-clients"
)

func newTestJWTService(t *testing.T, now time.Time, reader application.SubjectStateReader) *application.JWTService {
	t.Helper()

	service, err := application.NewJWTService(application.JWTConfig{
		Secret:     testJWTSecret,
		Issuer:     testJWTIssuer,
		Audience:   testJWTAudience,
		AccessTTL:  15 * time.Minute,
		RefreshTTL: 24 * time.Hour,
		Clock:      fixedClock{now: now},
	}, reader)
	if err != nil {
		t.Fatalf("NewJWTService() error = %v", err)
	}
	return service
}

func validAccessClaims(now time.Time) application.TokenClaims {
	return application.TokenClaims{
		Role:         domain.RoleCustomer,
		TokenType:    application.TokenTypeAccess,
		TokenVersion: 7,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    testJWTIssuer,
			Subject:   "user-123",
			ID:        "313aef88-e233-4bb9-9575-9b4c3ba358ee",
			Audience:  jwt.ClaimStrings{testJWTAudience},
			ExpiresAt: jwt.NewNumericDate(now.Add(15 * time.Minute)),
			NotBefore: jwt.NewNumericDate(now),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
}

func accessMapClaimsWithoutVersion(now time.Time) jwt.MapClaims {
	return jwt.MapClaims{
		"iss":        testJWTIssuer,
		"sub":        "user-123",
		"jti":        "313aef88-e233-4bb9-9575-9b4c3ba358ee",
		"aud":        []string{testJWTAudience},
		"exp":        now.Add(15 * time.Minute).Unix(),
		"nbf":        now.Unix(),
		"iat":        now.Unix(),
		"role":       domain.RoleCustomer,
		"token_type": application.TokenTypeAccess,
	}
}

func signTestToken(t *testing.T, method jwt.SigningMethod, claims jwt.Claims, key any) string {
	t.Helper()

	raw, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}
	return raw
}

func (c fixedClock) Now() time.Time { return c.now }

type stubSubjectStateReader struct {
	state application.SubjectState
	err   error
}

func (r stubSubjectStateReader) AuthenticationState(context.Context, string) (application.SubjectState, error) {
	return r.state, r.err
}

func TestJWTServiceIssuesAndParsesTypedTokenPair(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 15, 8, 30, 0, 0, time.UTC)
	service, err := application.NewJWTService(application.JWTConfig{
		Secret:     testJWTSecret,
		Issuer:     "railway-ticketing-api",
		Audience:   "railway-ticketing-clients",
		AccessTTL:  15 * time.Minute,
		RefreshTTL: 24 * time.Hour,
		Clock:      fixedClock{now: now},
	}, stubSubjectStateReader{state: application.SubjectState{Active: true, Role: domain.RoleCustomer, TokenVersion: 0}})
	if err != nil {
		t.Fatalf("NewJWTService() error = %v", err)
	}

	pair, err := service.IssueTokenPair("user-123", domain.RoleCustomer, 0)
	if err != nil {
		t.Fatalf("IssueTokenPair() error = %v", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatal("IssueTokenPair() returned an empty token")
	}
	if pair.AccessToken == pair.RefreshToken {
		t.Fatal("IssueTokenPair() returned identical access and refresh tokens")
	}

	tests := []struct {
		name       string
		raw        string
		parse      func(context.Context, string) (application.TokenClaims, error)
		wantType   application.TokenType
		wantExpiry time.Time
	}{
		{
			name:       "access",
			raw:        pair.AccessToken,
			parse:      service.ParseAccessToken,
			wantType:   application.TokenTypeAccess,
			wantExpiry: now.Add(15 * time.Minute),
		},
		{
			name:       "refresh",
			raw:        pair.RefreshToken,
			parse:      service.ParseRefreshToken,
			wantType:   application.TokenTypeRefresh,
			wantExpiry: now.Add(24 * time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			claims, err := tt.parse(context.Background(), tt.raw)
			if err != nil {
				t.Fatalf("Parse token error = %v", err)
			}
			if claims.Subject != "user-123" {
				t.Fatalf("Subject = %q, want user-123", claims.Subject)
			}
			if claims.Role != domain.RoleCustomer {
				t.Fatalf("Role = %q, want %q", claims.Role, domain.RoleCustomer)
			}
			if claims.TokenType != tt.wantType {
				t.Fatalf("TokenType = %q, want %q", claims.TokenType, tt.wantType)
			}
			if claims.TokenVersion != 0 {
				t.Fatalf("TokenVersion = %d, want 0", claims.TokenVersion)
			}
			if claims.ID == "" {
				t.Fatal("token JTI is empty")
			}
			if claims.Issuer != "railway-ticketing-api" {
				t.Fatalf("Issuer = %q", claims.Issuer)
			}
			if len(claims.Audience) != 1 || claims.Audience[0] != "railway-ticketing-clients" {
				t.Fatalf("Audience = %v", claims.Audience)
			}
			if claims.IssuedAt == nil || !claims.IssuedAt.Time.Equal(now) {
				t.Fatalf("IssuedAt = %v, want %v", claims.IssuedAt, now)
			}
			if claims.NotBefore == nil || !claims.NotBefore.Time.Equal(now) {
				t.Fatalf("NotBefore = %v, want %v", claims.NotBefore, now)
			}
			if claims.ExpiresAt == nil || !claims.ExpiresAt.Time.Equal(tt.wantExpiry) {
				t.Fatalf("ExpiresAt = %v, want %v", claims.ExpiresAt, tt.wantExpiry)
			}
		})
	}
}
