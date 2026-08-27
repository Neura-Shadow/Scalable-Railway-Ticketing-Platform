package postgres

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrInvalidReview = errors.New("invalid settlement reconciliation review")

var reviewerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$`)

type ReviewDisposition string

const (
	ReviewAcknowledged      ReviewDisposition = "acknowledged"
	ReviewInvestigating     ReviewDisposition = "investigating"
	ReviewAcceptedException ReviewDisposition = "accepted_exception"
	ReviewResolvedExternal  ReviewDisposition = "resolved_external"
)

func (disposition ReviewDisposition) Valid() bool {
	switch disposition {
	case ReviewAcknowledged, ReviewInvestigating, ReviewAcceptedException, ReviewResolvedExternal:
		return true
	default:
		return false
	}
}

// Review is append-only operator evidence. It acknowledges a reconciliation
// run without changing the run, its mismatches, or any financial record.
type Review struct {
	ID           uuid.UUID
	RunID        uuid.UUID
	ReviewerID   string
	Disposition  ReviewDisposition
	EvidenceHash [32]byte
	ReviewedAt   time.Time
}

const insertReviewSQL = `
INSERT INTO public.settlement_reconciliation_reviews(
 review_id,run_id,reviewer_id,disposition,evidence_hash,reviewed_at
) VALUES($1,$2,$3,$4,$5,$6)`

func (store *Store) AppendReview(ctx context.Context, review Review) error {
	if store == nil || store.db == nil || store.writer == nil || !validReview(review) {
		return ErrInvalidReview
	}
	return store.writer.Write(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, insertReviewSQL,
			review.ID, review.RunID, review.ReviewerID, review.Disposition,
			review.EvidenceHash[:], review.ReviewedAt.UTC(),
		)
		return err
	})
}

func validReview(review Review) bool {
	if review.ID == uuid.Nil || review.RunID == uuid.Nil || !reviewerPattern.MatchString(review.ReviewerID) ||
		!review.Disposition.Valid() || review.ReviewedAt.IsZero() {
		return false
	}
	var zero [32]byte
	return review.EvidenceHash != zero
}
