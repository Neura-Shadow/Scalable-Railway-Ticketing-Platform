package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidRefreshToken = errors.New("invalid refresh token")
	ErrRefreshTokenReplay  = errors.New("refresh token replay detected")
)

type RefreshTokenRepository struct{ pool *pgxpool.Pool }

func NewRefreshTokenRepository(pool *pgxpool.Pool) (*RefreshTokenRepository, error) {
	if pool == nil {
		return nil, ErrInvalidStoreConfiguration
	}
	return &RefreshTokenRepository{pool: pool}, nil
}

type RefreshTokenRecord struct {
	UserID       uuid.UUID
	FamilyID     uuid.UUID
	JTIHash      []byte
	TokenVersion int64
	ExpiresAt    time.Time
}

func (r *RefreshTokenRepository) Register(ctx context.Context, record RefreshTokenRecord) error {
	if err := validateRefreshRecord(record); err != nil {
		return err
	}
	_, err := r.pool.Exec(ctx, `
INSERT INTO refresh_tokens (user_id, family_id, jti_hash, token_version, expires_at)
VALUES ($1, $2, $3, $4, $5)`, record.UserID, record.FamilyID, record.JTIHash, record.TokenVersion, record.ExpiresAt.UTC())
	if err != nil {
		return ErrPersistence
	}
	return nil
}

type RotateRefreshTokenParams struct {
	PresentedJTIHash []byte
	Replacement      RefreshTokenRecord
	Now              time.Time
}

func (r *RefreshTokenRepository) Rotate(ctx context.Context, params RotateRefreshTokenParams) error {
	if len(params.PresentedJTIHash) != 32 || params.Now.IsZero() || validateRefreshRecord(params.Replacement) != nil {
		return ErrInvalidRefreshToken
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ErrPersistence
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var id, userID, familyID uuid.UUID
	var tokenVersion int64
	var expiresAt time.Time
	var databaseNow time.Time
	var revokedAt *time.Time
	var rotatedTo *uuid.UUID
	err = tx.QueryRow(ctx, `
SELECT id, user_id, family_id, token_version, expires_at, revoked_at, rotated_to_id, clock_timestamp()
FROM refresh_tokens
WHERE jti_hash = $1
FOR UPDATE`, params.PresentedJTIHash).Scan(&id, &userID, &familyID, &tokenVersion, &expiresAt, &revokedAt, &rotatedTo, &databaseNow)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidRefreshToken
	}
	if err != nil {
		return ErrPersistence
	}

	if revokedAt != nil || rotatedTo != nil {
		if _, err := tx.Exec(ctx, `
UPDATE refresh_tokens
SET revoked_at = COALESCE(revoked_at, $2)
WHERE user_id = $1 AND family_id = $3`, userID, databaseNow, familyID); err != nil {
			return ErrPersistence
		}
		if err := tx.Commit(ctx); err != nil {
			return ErrPersistence
		}
		return ErrRefreshTokenReplay
	}
	if !databaseNow.Before(expiresAt) || userID != params.Replacement.UserID || familyID != params.Replacement.FamilyID || tokenVersion != params.Replacement.TokenVersion {
		if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = COALESCE(revoked_at, $2) WHERE id = $1`, id, databaseNow); err != nil {
			return ErrPersistence
		}
		if err := tx.Commit(ctx); err != nil {
			return ErrPersistence
		}
		return ErrInvalidRefreshToken
	}

	var replacementID uuid.UUID
	err = tx.QueryRow(ctx, `
INSERT INTO refresh_tokens (user_id, family_id, jti_hash, token_version, expires_at)
VALUES ($1, $2, $3, $4, $5)
RETURNING id`, params.Replacement.UserID, params.Replacement.FamilyID,
		params.Replacement.JTIHash, params.Replacement.TokenVersion, params.Replacement.ExpiresAt.UTC()).Scan(&replacementID)
	if err != nil {
		return ErrPersistence
	}
	tag, err := tx.Exec(ctx, `
UPDATE refresh_tokens
SET revoked_at = $2, rotated_to_id = $3
WHERE id = $1 AND revoked_at IS NULL AND rotated_to_id IS NULL`, id, databaseNow, replacementID)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrPersistence
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrPersistence
	}
	return nil
}

func (r *RefreshTokenRepository) RevokeFamily(ctx context.Context, userID uuid.UUID, presentedJTIHash []byte, now time.Time) error {
	if userID == uuid.Nil || len(presentedJTIHash) != 32 || now.IsZero() {
		return ErrInvalidRefreshToken
	}
	tag, err := r.pool.Exec(ctx, `
UPDATE refresh_tokens
SET revoked_at = COALESCE(revoked_at, clock_timestamp())
WHERE user_id = $1
  AND family_id = (
      SELECT family_id FROM refresh_tokens WHERE user_id = $1 AND jti_hash = $2
  )`, userID, presentedJTIHash)
	if err != nil {
		return ErrPersistence
	}
	if tag.RowsAffected() == 0 {
		return ErrInvalidRefreshToken
	}
	return nil
}

func validateRefreshRecord(record RefreshTokenRecord) error {
	if record.UserID == uuid.Nil || record.FamilyID == uuid.Nil || len(record.JTIHash) != 32 || record.TokenVersion <= 0 || record.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: malformed record", ErrInvalidRefreshToken)
	}
	return nil
}
