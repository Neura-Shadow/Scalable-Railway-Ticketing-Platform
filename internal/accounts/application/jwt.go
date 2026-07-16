package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const MinJWTSecretBytes = 32

var (
	ErrInvalidJWTConfiguration = errors.New("invalid JWT configuration")
	ErrInvalidTokenType        = errors.New("invalid token type")
	ErrInvalidTokenSubject     = errors.New("invalid token subject")
	ErrInvalidTokenID          = errors.New("invalid token id")
	ErrInvalidTokenVersion     = errors.New("invalid token version")
	ErrUnexpectedSigningMethod = errors.New("unexpected JWT signing method")
	ErrUserInactive            = errors.New("user is inactive")
	ErrTokenVersionMismatch    = errors.New("token version mismatch")
)

type TokenType string

const (
	TokenTypeAccess  TokenType = "access"
	TokenTypeRefresh TokenType = "refresh"
)

func (t TokenType) Validate() error {
	switch t {
	case TokenTypeAccess, TokenTypeRefresh:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidTokenType, t)
	}
}

type Clock interface {
	Now() time.Time
}

type SubjectState struct {
	Active       bool
	TokenVersion int64
}

type SubjectStateReader interface {
	AuthenticationState(ctx context.Context, subject string) (SubjectState, error)
}

type JWTConfig struct {
	Secret     []byte
	Issuer     string
	Audience   string
	AccessTTL  time.Duration
	RefreshTTL time.Duration
	Clock      Clock
}

type TokenClaims struct {
	Role         domain.Role `json:"role"`
	TokenType    TokenType   `json:"token_type"`
	TokenVersion int64       `json:"token_version"`
	jwt.RegisteredClaims
}

type wireTokenClaims struct {
	Role         domain.Role `json:"role"`
	TokenType    TokenType   `json:"token_type"`
	TokenVersion *int64      `json:"token_version"`
	jwt.RegisteredClaims
}

func (c wireTokenClaims) Validate() error {
	if c.Subject == "" {
		return ErrInvalidTokenSubject
	}
	if _, err := uuid.Parse(c.ID); err != nil {
		return ErrInvalidTokenID
	}
	if c.ExpiresAt == nil || c.NotBefore == nil || c.IssuedAt == nil {
		return jwt.ErrTokenRequiredClaimMissing
	}
	if err := c.Role.Validate(); err != nil {
		return err
	}
	if err := c.TokenType.Validate(); err != nil {
		return err
	}
	if c.TokenVersion == nil || *c.TokenVersion < 0 {
		return ErrInvalidTokenVersion
	}
	return nil
}

func (c wireTokenClaims) public() TokenClaims {
	return TokenClaims{
		Role:             c.Role,
		TokenType:        c.TokenType,
		TokenVersion:     *c.TokenVersion,
		RegisteredClaims: c.RegisteredClaims,
	}
}

type TokenPair struct {
	AccessToken  string
	RefreshToken string
}

type JWTService struct {
	secret     []byte
	issuer     string
	audience   string
	accessTTL  time.Duration
	refreshTTL time.Duration
	clock      Clock
	subjects   SubjectStateReader
}

func NewJWTService(config JWTConfig, subjects SubjectStateReader) (*JWTService, error) {
	if len(config.Secret) < MinJWTSecretBytes {
		return nil, fmt.Errorf("%w: secret must contain at least %d bytes", ErrInvalidJWTConfiguration, MinJWTSecretBytes)
	}
	if strings.TrimSpace(config.Issuer) == "" {
		return nil, fmt.Errorf("%w: issuer is required", ErrInvalidJWTConfiguration)
	}
	if strings.TrimSpace(config.Audience) == "" {
		return nil, fmt.Errorf("%w: audience is required", ErrInvalidJWTConfiguration)
	}
	if config.AccessTTL <= 0 || config.RefreshTTL <= 0 {
		return nil, fmt.Errorf("%w: token TTLs must be positive", ErrInvalidJWTConfiguration)
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	if subjects == nil {
		return nil, fmt.Errorf("%w: subject state reader is required", ErrInvalidJWTConfiguration)
	}

	return &JWTService{
		secret:     append([]byte(nil), config.Secret...),
		issuer:     config.Issuer,
		audience:   config.Audience,
		accessTTL:  config.AccessTTL,
		refreshTTL: config.RefreshTTL,
		clock:      config.Clock,
		subjects:   subjects,
	}, nil
}

func (s *JWTService) IssueTokenPair(subject string, role domain.Role, tokenVersion int64) (TokenPair, error) {
	accessToken, err := s.issueToken(subject, role, TokenTypeAccess, tokenVersion, s.accessTTL)
	if err != nil {
		return TokenPair{}, err
	}
	refreshToken, err := s.issueToken(subject, role, TokenTypeRefresh, tokenVersion, s.refreshTTL)
	if err != nil {
		return TokenPair{}, err
	}
	return TokenPair{AccessToken: accessToken, RefreshToken: refreshToken}, nil
}

func (s *JWTService) ParseAccessToken(ctx context.Context, raw string) (TokenClaims, error) {
	return s.parseToken(ctx, raw, TokenTypeAccess)
}

func (s *JWTService) ParseRefreshToken(ctx context.Context, raw string) (TokenClaims, error) {
	return s.parseToken(ctx, raw, TokenTypeRefresh)
}

func (s *JWTService) issueToken(subject string, role domain.Role, tokenType TokenType, tokenVersion int64, ttl time.Duration) (string, error) {
	now := s.clock.Now().UTC()
	claims := wireTokenClaims{
		Role:         role,
		TokenType:    tokenType,
		TokenVersion: &tokenVersion,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   subject,
			ID:        uuid.NewString(),
			Audience:  jwt.ClaimStrings{s.audience},
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			NotBefore: jwt.NewNumericDate(now),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
	if err := claims.Validate(); err != nil {
		return "", fmt.Errorf("issue JWT: %w", err)
	}

	raw, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
	if err != nil {
		return "", fmt.Errorf("sign JWT: %w", err)
	}
	return raw, nil
}

func (s *JWTService) parseToken(ctx context.Context, raw string, expectedType TokenType) (TokenClaims, error) {
	claims := wireTokenClaims{}
	_, err := jwt.ParseWithClaims(raw, &claims, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("%w: %s", ErrUnexpectedSigningMethod, token.Method.Alg())
		}
		return s.secret, nil
	},
		jwt.WithAudience(s.audience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithIssuer(s.issuer),
		jwt.WithTimeFunc(s.clock.Now),
	)
	if err != nil {
		return TokenClaims{}, fmt.Errorf("parse JWT: %w", err)
	}
	if claims.TokenType != expectedType {
		return TokenClaims{}, fmt.Errorf("%w: got %q, want %q", ErrInvalidTokenType, claims.TokenType, expectedType)
	}
	if len(claims.Audience) != 1 || claims.Audience[0] != s.audience {
		return TokenClaims{}, jwt.ErrTokenInvalidAudience
	}

	state, err := s.subjects.AuthenticationState(ctx, claims.Subject)
	if err != nil {
		return TokenClaims{}, fmt.Errorf("read authentication state: %w", err)
	}
	if !state.Active {
		return TokenClaims{}, ErrUserInactive
	}
	if state.TokenVersion != *claims.TokenVersion {
		return TokenClaims{}, ErrTokenVersionMismatch
	}
	return claims.public(), nil
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
