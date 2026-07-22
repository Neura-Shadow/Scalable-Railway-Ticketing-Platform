package readmodel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const MaxRebuildAllBatchSize = 100

var ErrInvalidRebuildOptions = errors.New("read-model rebuild options invalid")

type RebuildAllOptions struct {
	After string
	Limit int
}

type RebuildAllResult struct {
	TrainRunsRebuilt int
	RowsWritten      int64
	NextCursor       string
	HasMore          bool
}

type rebuildCandidate struct {
	serviceDate time.Time
	trainRunID  uuid.UUID
}

func (s *Store) RebuildAll(ctx context.Context, options RebuildAllOptions) (RebuildAllResult, error) {
	if options.Limit < 1 || options.Limit > MaxRebuildAllBatchSize {
		return RebuildAllResult{}, ErrInvalidRebuildOptions
	}
	afterDate, afterID, err := parseRebuildCursor(options.After)
	if err != nil {
		return RebuildAllResult{}, err
	}
	candidates, err := s.listRebuildCandidates(ctx, afterDate, afterID, options.Limit+1)
	if err != nil {
		return RebuildAllResult{}, err
	}
	result := RebuildAllResult{HasMore: len(candidates) > options.Limit}
	if result.HasMore {
		candidates = candidates[:options.Limit]
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		rebuild, err := s.RebuildTrainRun(ctx, candidate.trainRunID.String())
		if err != nil {
			return result, fmt.Errorf("rebuild train run after cursor %q: %w", result.NextCursor, err)
		}
		result.TrainRunsRebuilt++
		result.RowsWritten += rebuild.RowsWritten
		result.NextCursor = formatRebuildCursor(candidate.serviceDate, candidate.trainRunID)
	}
	return result, nil
}

func (s *Store) listRebuildCandidates(
	ctx context.Context,
	afterDate *time.Time,
	afterID uuid.UUID,
	limit int,
) ([]rebuildCandidate, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("%w: begin rebuild candidate scan", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var rows pgx.Rows
	if afterDate == nil {
		rows, err = tx.Query(ctx, `
			SELECT service_date, id
			FROM train_runs
			ORDER BY service_date, id
			LIMIT $1
		`, limit)
	} else {
		rows, err = tx.Query(ctx, `
			SELECT service_date, id
			FROM train_runs
			WHERE (service_date, id) > ($1, $2)
			ORDER BY service_date, id
			LIMIT $3
		`, *afterDate, afterID, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: query rebuild candidates", ErrPersistence)
	}
	candidates := make([]rebuildCandidate, 0, limit)
	for rows.Next() {
		var candidate rebuildCandidate
		if err := rows.Scan(&candidate.serviceDate, &candidate.trainRunID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: scan rebuild candidate", ErrPersistence)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("%w: iterate rebuild candidates", ErrPersistence)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%w: commit rebuild candidate scan", ErrPersistence)
	}
	return candidates, nil
}

func parseRebuildCursor(raw string) (*time.Time, uuid.UUID, error) {
	if raw == "" {
		return nil, uuid.Nil, nil
	}
	parts := strings.Split(raw, "|")
	if len(parts) != 2 {
		return nil, uuid.Nil, ErrInvalidRebuildOptions
	}
	serviceDate, err := time.Parse("2006-01-02", parts[0])
	if err != nil {
		return nil, uuid.Nil, ErrInvalidRebuildOptions
	}
	trainRunID, err := uuid.Parse(parts[1])
	if err != nil || trainRunID == uuid.Nil {
		return nil, uuid.Nil, ErrInvalidRebuildOptions
	}
	return &serviceDate, trainRunID, nil
}

func formatRebuildCursor(serviceDate time.Time, trainRunID uuid.UUID) string {
	return serviceDate.Format("2006-01-02") + "|" + trainRunID.String()
}
