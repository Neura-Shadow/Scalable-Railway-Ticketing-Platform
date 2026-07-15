package postgres

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRefreshTokenRotationRevokesFamilyOnReplay(t *testing.T) {
	pool := newRefreshTokenTestPool(t)
	ctx := context.Background()
	userID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)`,
		userID, userID.String()+"@example.test", "$2a$12$abcdefghijklmnopqrstuv012345678901234567890123456789")
	if err != nil {
		t.Fatal(err)
	}
	repository, err := NewRefreshTokenRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	familyID := uuid.New()
	now := time.Now().UTC()
	oldHash := bytes.Repeat([]byte{0x11}, 32)
	newHash := bytes.Repeat([]byte{0x22}, 32)
	if err := repository.Register(ctx, RefreshTokenRecord{UserID: userID, FamilyID: familyID, JTIHash: oldHash, TokenVersion: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Rotate(ctx, RotateRefreshTokenParams{
		PresentedJTIHash: oldHash,
		Replacement:      RefreshTokenRecord{UserID: userID, FamilyID: familyID, JTIHash: newHash, TokenVersion: 1, ExpiresAt: now.Add(2 * time.Hour)},
		Now:              now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Rotate(ctx, RotateRefreshTokenParams{
		PresentedJTIHash: oldHash,
		Replacement:      RefreshTokenRecord{UserID: userID, FamilyID: familyID, JTIHash: bytes.Repeat([]byte{0x33}, 32), TokenVersion: 1, ExpiresAt: now.Add(2 * time.Hour)},
		Now:              now.Add(time.Second),
	}); !errors.Is(err, ErrRefreshTokenReplay) {
		t.Fatalf("replayed rotation error = %v", err)
	}
	var familyCount, revokedCount int
	if err := pool.QueryRow(ctx, `
SELECT count(*)::integer, count(*) FILTER (WHERE revoked_at IS NOT NULL)::integer
FROM refresh_tokens WHERE user_id = $1 AND family_id = $2`, userID, familyID).Scan(&familyCount, &revokedCount); err != nil {
		t.Fatal(err)
	}
	if familyCount != 2 || revokedCount != 2 {
		t.Fatalf("family rows=%d revoked=%d, want 2/2", familyCount, revokedCount)
	}
}

func newRefreshTokenTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := "refresh_test_" + uuid.NewString()
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000001_accounts.up.sql"))
	if err != nil {
		pool.Close()
		admin.Close()
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		pool.Close()
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
	return pool
}
