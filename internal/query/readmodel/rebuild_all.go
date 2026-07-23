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
	TrainRunsSelected int
	TrainRunsRebuilt  int
	RowsWritten       int64
	NextCursor        string
	HasMore           bool
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
	if err := s.beginRebuildAll(ctx, options.After); err != nil {
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
		result.TrainRunsSelected++
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
	if result.NextCursor == "" {
		result.NextCursor = options.After
	}
	if err := s.checkpointRebuildAll(ctx, options.After, result.NextCursor, !result.HasMore); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Store) beginRebuildAll(ctx context.Context, after string) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin rebuild state", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended('journey_search', 7213007))
	`); err != nil {
		return fmt.Errorf("%w: lock rebuild state", ErrPersistence)
	}
	if after == "" {
		if _, err := tx.Exec(ctx, `
			UPDATE read_model_projection_state
			SET ready = false, rebuild_after = '', updated_at = clock_timestamp()
			WHERE projection_name = 'journey_search'
		`); err != nil {
			return fmt.Errorf("%w: reset rebuild state", ErrPersistence)
		}
	} else {
		var expected string
		if err := tx.QueryRow(ctx, `
			SELECT rebuild_after
			FROM read_model_projection_state
			WHERE projection_name = 'journey_search'
			FOR UPDATE
		`).Scan(&expected); err != nil {
			return fmt.Errorf("%w: read rebuild state", ErrPersistence)
		}
		if expected != after {
			return ErrInvalidRebuildOptions
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit rebuild state", ErrPersistence)
	}
	return nil
}

func (s *Store) checkpointRebuildAll(ctx context.Context, expected, next string, ready bool) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin rebuild checkpoint", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended('journey_search', 7213007))
	`); err != nil {
		return fmt.Errorf("%w: lock rebuild checkpoint", ErrPersistence)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE read_model_projection_state
		SET ready = $1, rebuild_after = $2, updated_at = clock_timestamp()
		WHERE projection_name = 'journey_search'
		  AND rebuild_after = $3
	`, ready, next, expected)
	if err != nil {
		return fmt.Errorf("%w: write rebuild checkpoint", ErrPersistence)
	}
	if tag.RowsAffected() != 1 {
		return ErrInvalidRebuildOptions
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit rebuild checkpoint", ErrPersistence)
	}
	return nil
}

func (s *Store) PreviewRebuildAll(ctx context.Context, options RebuildAllOptions) (RebuildAllResult, error) {
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
	result.TrainRunsSelected = len(candidates)
	if len(candidates) > 0 {
		last := candidates[len(candidates)-1]
		result.NextCursor = formatRebuildCursor(last.serviceDate, last.trainRunID)
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
