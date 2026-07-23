package postgres

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalidConfiguration = errors.New("invalid offering store configuration")
	ErrInvalidInput         = errors.New("invalid offering input")
	ErrNotFound             = errors.New("offering resource not found")
	ErrConflict             = errors.New("offering resource conflict")
	ErrPersistence          = errors.New("offering persistence failure")
)

type DBTX interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type DB interface {
	DBTX
	Begin(context.Context) (pgx.Tx, error)
}

type Store struct {
	db DB
}

func NewStore(db DB) (*Store, error) {
	if db == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Store{db: db}, nil
}

type Station struct {
	ID        string             `json:"id"`
	Code      domain.StationCode `json:"code"`
	Name      string             `json:"name"`
	Timezone  string             `json:"timezone"`
	Active    bool               `json:"active"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
}

type CreateStationParams struct {
	Code     string
	Name     string
	Timezone string
}

type UpdateStationParams struct {
	Code     string
	Name     string
	Timezone string
	Active   bool
}

func (s *Store) CreateStation(ctx context.Context, params CreateStationParams) (Station, error) {
	code, name, timezone, err := normalizeStation(params.Code, params.Name, params.Timezone)
	if err != nil {
		return Station{}, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Station{}, safeError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	station, err := scanStation(tx.QueryRow(ctx, `
		INSERT INTO stations (code, name, timezone)
		VALUES ($1, $2, $3)
		RETURNING id::text, code, name, timezone, active, created_at, updated_at
	`, code.String(), name, timezone), false)
	if err != nil {
		return Station{}, err
	}
	if err := appendReadModelEvent(ctx, tx, "station", station.ID, "station.created"); err != nil {
		return Station{}, safeError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Station{}, safeError(err)
	}
	return station, nil
}

func (s *Store) UpdateStation(ctx context.Context, id string, params UpdateStationParams) (Station, error) {
	if strings.TrimSpace(id) == "" {
		return Station{}, ErrNotFound
	}
	code, name, timezone, err := normalizeStation(params.Code, params.Name, params.Timezone)
	if err != nil {
		return Station{}, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Station{}, safeError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	station, err := scanStation(tx.QueryRow(ctx, `
		UPDATE stations
		SET code = $2, name = $3, timezone = $4, active = $5
		WHERE id = $1
		RETURNING id::text, code, name, timezone, active, created_at, updated_at
	`, id, code.String(), name, timezone, params.Active), true)
	if err != nil {
		return Station{}, err
	}
	eventType := "station.updated"
	if !station.Active {
		eventType = "station.disabled"
	}
	if err := appendReadModelEvent(ctx, tx, "station", station.ID, eventType); err != nil {
		return Station{}, safeError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Station{}, safeError(err)
	}
	return station, nil
}

func (s *Store) ListStations(ctx context.Context, activeOnly bool) ([]Station, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, code, name, timezone, active, created_at, updated_at
		FROM stations
		WHERE NOT $1::boolean OR active
		ORDER BY code, id
	`, activeOnly)
	if err != nil {
		return nil, safeError(err)
	}
	defer rows.Close()

	stations := make([]Station, 0)
	for rows.Next() {
		station, err := scanStation(rows, false)
		if err != nil {
			return nil, err
		}
		stations = append(stations, station)
	}
	if err := rows.Err(); err != nil {
		return nil, safeError(err)
	}
	return stations, nil
}

func normalizeStation(rawCode, rawName, rawTimezone string) (domain.StationCode, string, string, error) {
	code, err := domain.NewStationCode(rawCode)
	if err != nil {
		return "", "", "", err
	}
	name := strings.TrimSpace(rawName)
	timezone := strings.TrimSpace(rawTimezone)
	if runeLengthOutside(name, 1, 120) || runeLengthOutside(timezone, 1, 64) {
		return "", "", "", ErrInvalidInput
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return "", "", "", ErrInvalidInput
	}
	return code, name, timezone, nil
}

func runeLengthOutside(value string, minimum, maximum int) bool {
	length := utf8.RuneCountInString(value)
	return length < minimum || length > maximum
}

func scanStation(row pgx.Row, notFound bool) (Station, error) {
	var station Station
	var code string
	if err := row.Scan(&station.ID, &code, &station.Name, &station.Timezone, &station.Active, &station.CreatedAt, &station.UpdatedAt); err != nil {
		if notFound && errors.Is(err, pgx.ErrNoRows) {
			return Station{}, ErrNotFound
		}
		return Station{}, safeError(err)
	}
	parsedCode, err := domain.NewStationCode(code)
	if err != nil {
		return Station{}, ErrPersistence
	}
	station.Code = parsedCode
	station.CreatedAt = station.CreatedAt.UTC()
	station.UpdatedAt = station.UpdatedAt.UTC()
	return station, nil
}

func safeError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		switch pgError.Code {
		case "23505":
			return ErrConflict
		case "23503":
			return ErrNotFound
		case "23514", "22001", "22P02":
			return ErrInvalidInput
		}
	}
	return ErrPersistence
}
