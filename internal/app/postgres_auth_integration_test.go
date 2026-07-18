package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

func TestPostgresAuthRegistrationDoesNotIssueCredentialsAndLoginStillDoes(t *testing.T) {
	auth, pool := openPostgresAuthIntegration(t)
	ctx := context.Background()
	command := httpapi.RegisterCommand{
		Email:       "oracle-regression@example.test",
		Password:    "correct horse battery staple",
		DisplayName: "Rider",
	}

	if err := auth.Register(ctx, command); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	assertAuthRowCounts(t, pool, command.Email, 1, 1, 0)

	if err := auth.Register(ctx, command); !errors.Is(err, httpapi.ErrConflict) {
		t.Fatalf("duplicate Register() error = %v, want %v", err, httpapi.ErrConflict)
	}
	assertAuthRowCounts(t, pool, command.Email, 1, 1, 0)

	for label, invalidName := range map[string]string{
		"overlong": strings.Repeat("界", 101),
		"control":  "Rider\x00",
	} {
		for _, email := range []string{command.Email, label + "-new@example.test"} {
			invalid := command
			invalid.Email = email
			invalid.DisplayName = invalidName
			if err := auth.Register(ctx, invalid); !errors.Is(err, httpapi.ErrInvalidInput) {
				t.Fatalf("Register(%q, %s display name) error = %v, want %v", email, label, err, httpapi.ErrInvalidInput)
			}
		}
	}
	assertAuthRowCounts(t, pool, command.Email, 1, 1, 0)
	assertAuthRowCounts(t, pool, "overlong-new@example.test", 0, 0, 0)
	assertAuthRowCounts(t, pool, "control-new@example.test", 0, 0, 0)

	for label, invalid := range map[string]httpapi.RegisterCommand{
		"short-password": {
			Email: "short-password-new@example.test", Password: strings.Repeat("界", 4), DisplayName: "Rider",
		},
		"long-password": {
			Email: "long-password-new@example.test", Password: strings.Repeat("界", 25), DisplayName: "Rider",
		},
		"double-at-email": {
			Email: "invalid@@example.test", Password: command.Password, DisplayName: "Rider",
		},
	} {
		if err := auth.Register(ctx, invalid); !errors.Is(err, httpapi.ErrInvalidInput) {
			t.Fatalf("Register(%s invalid credentials) error = %v, want %v", label, err, httpapi.ErrInvalidInput)
		}
		assertAuthRowCounts(t, pool, invalid.Email, 0, 0, 0)
	}
	for label, invalidPassword := range map[string]string{
		"short": strings.Repeat("界", 4),
		"long":  strings.Repeat("界", 25),
	} {
		invalid := command
		invalid.Password = invalidPassword
		if err := auth.Register(ctx, invalid); !errors.Is(err, httpapi.ErrInvalidInput) {
			t.Fatalf("Register(existing email, %s password) error = %v, want %v", label, err, httpapi.ErrInvalidInput)
		}
	}

	pair, err := auth.Login(ctx, httpapi.LoginCommand{Email: command.Email, Password: command.Password})
	if err != nil {
		t.Fatalf("Login() after registration error = %v", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" || pair.TokenType != "Bearer" || pair.ExpiresIn <= 0 {
		t.Fatal("Login() returned an incomplete token pair")
	}
	assertAuthRowCounts(t, pool, command.Email, 1, 1, 1)
}

func assertAuthRowCounts(t *testing.T, pool *pgxpool.Pool, email string, wantUsers, wantPassengers, wantRefreshTokens int) {
	t.Helper()

	var users, passengers, refreshTokens int
	err := pool.QueryRow(context.Background(), `
		SELECT
			(SELECT count(*) FROM users WHERE email = $1),
			(SELECT count(*) FROM passengers p JOIN users u ON u.id = p.user_id WHERE u.email = $1),
			(SELECT count(*) FROM refresh_tokens r JOIN users u ON u.id = r.user_id WHERE u.email = $1)
	`, strings.ToLower(strings.TrimSpace(email))).Scan(&users, &passengers, &refreshTokens)
	if err != nil {
		t.Fatalf("query auth row counts: %v", err)
	}
	if users != wantUsers || passengers != wantPassengers || refreshTokens != wantRefreshTokens {
		t.Fatalf(
			"auth row counts = users %d, passengers %d, refresh tokens %d; want %d, %d, %d",
			users, passengers, refreshTokens, wantUsers, wantPassengers, wantRefreshTokens,
		)
	}
}

func openPostgresAuthIntegration(t *testing.T) (*AuthService, *pgxpool.Pool) {
	t.Helper()

	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect PostgreSQL integration database: %v", err)
	}
	schema := "app_auth_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("create auth integration schema: %v", err)
	}

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("parse PostgreSQL integration configuration: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("open auth integration pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop auth integration schema: %v", err)
		}
		if err := admin.Close(cleanupCtx); err != nil {
			t.Errorf("close PostgreSQL integration connection: %v", err)
		}
	})

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve auth integration test path")
	}
	migrationPath := filepath.Join(filepath.Dir(currentFile), "..", "..", "migrations", "000001_accounts.up.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read accounts migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply accounts migration: %v", err)
	}

	cfg := config.Defaults()
	cfg.JWTSecret = strings.Repeat("auth-integration-secret-", 2)
	cfg.BcryptCost = bcrypt.MinCost
	now := time.Date(2100, 1, 1, 12, 0, 0, 0, time.UTC)
	auth, _, err := NewPostgresAuth(pool, cfg, fixedClock{now})
	if err != nil {
		t.Fatalf("NewPostgresAuth() error = %v", err)
	}
	return auth, pool
}
