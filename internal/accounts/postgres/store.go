package postgres

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalidStoreConfiguration = errors.New("invalid accounts store configuration")
	ErrInvalidInput              = errors.New("invalid accounts input")
	ErrEmailAlreadyRegistered    = errors.New("email already registered")
	ErrInvalidCredentials        = errors.New("invalid credentials")
	ErrUserNotFound              = errors.New("user not found")
	ErrPassengerNotFound         = errors.New("passenger not found")
	ErrPersistence               = errors.New("accounts persistence failure")
)

type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Store struct {
	db                DBTX
	passwords         application.PasswordHasher
	dummyPasswordHash string
}

var _ application.SubjectStateReader = (*Store)(nil)

type RegisterUserParams struct {
	Email    string
	Password string
	Role     domain.Role
}

type LoginLookup struct {
	Email    string
	Password string
}

type CreatePassengerParams struct {
	UserID      string
	DisplayName string
}

type UpdatePassengerParams struct {
	UserID      string
	PassengerID string
	DisplayName string
}

type User struct {
	ID           string      `json:"id"`
	Email        string      `json:"email"`
	Role         domain.Role `json:"role"`
	TokenVersion int64       `json:"token_version"`
	Active       bool        `json:"active"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
}

type Passenger struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	DisplayName string    `json:"display_name"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func NewStore(db DBTX, passwords application.PasswordHasher) (*Store, error) {
	if db == nil || passwords == nil {
		return nil, ErrInvalidStoreConfiguration
	}
	dummyPasswordHash, err := passwords.Hash("accounts-login-timing-equalization")
	if err != nil || dummyPasswordHash == "" {
		return nil, ErrInvalidStoreConfiguration
	}
	return &Store{db: db, passwords: passwords, dummyPasswordHash: dummyPasswordHash}, nil
}

func (s *Store) RegisterUser(ctx context.Context, params RegisterUserParams) (User, error) {
	email := canonicalEmail(params.Email)
	if !domain.ValidRegistrationEmail(email) || !domain.ValidRegistrationPassword(params.Password) {
		return User{}, ErrInvalidInput
	}
	if err := params.Role.Validate(); err != nil {
		return User{}, err
	}

	passwordHash, err := s.passwords.Hash(params.Password)
	if err != nil {
		return User{}, ErrInvalidInput
	}

	return scanUser(s.db.QueryRow(ctx, `
		INSERT INTO users (email, password_hash, role)
		VALUES ($1, $2, $3)
		RETURNING id::text, email, role, token_version, active, created_at, updated_at
	`, email, passwordHash, params.Role), ErrEmailAlreadyRegistered)
}

func (s *Store) LookupUserForLogin(ctx context.Context, lookup LoginLookup) (User, error) {
	email := canonicalEmail(lookup.Email)
	if !validEmailLength(email) || lookup.Password == "" {
		return User{}, ErrInvalidCredentials
	}

	var user User
	var role string
	var passwordHash string
	err := s.db.QueryRow(ctx, `
		SELECT id::text, email, password_hash, role, token_version, active, created_at, updated_at
		FROM users
		WHERE email = $1
	`, email).Scan(
		&user.ID,
		&user.Email,
		&passwordHash,
		&role,
		&user.TokenVersion,
		&user.Active,
		&user.CreatedAt,
		&user.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = s.passwords.Verify(lookup.Password, s.dummyPasswordHash)
		return User{}, ErrInvalidCredentials
	}
	if err != nil {
		return User{}, mapDatabaseError(err, nil, nil)
	}
	if err := s.passwords.Verify(lookup.Password, passwordHash); err != nil {
		return User{}, ErrInvalidCredentials
	}
	return finalizeUser(user, role)
}

func (s *Store) CreatePassenger(ctx context.Context, params CreatePassengerParams) (Passenger, error) {
	displayName := strings.TrimSpace(params.DisplayName)
	if params.UserID == "" || !domain.ValidPassengerDisplayName(displayName) {
		return Passenger{}, ErrInvalidInput
	}

	return scanPassenger(s.db.QueryRow(ctx, `
		INSERT INTO passengers (user_id, display_name)
		VALUES ($1, $2)
		RETURNING id::text, user_id::text, display_name, created_at, updated_at
	`, params.UserID, displayName), nil, ErrUserNotFound)
}

func (s *Store) GetPassenger(ctx context.Context, userID, passengerID string) (Passenger, error) {
	if userID == "" || passengerID == "" {
		return Passenger{}, ErrPassengerNotFound
	}

	return scanPassenger(s.db.QueryRow(ctx, `
		SELECT id::text, user_id::text, display_name, created_at, updated_at
		FROM passengers
		WHERE id = $1 AND user_id = $2
	`, passengerID, userID), ErrPassengerNotFound, nil)
}

func (s *Store) ListPassengers(ctx context.Context, userID string) ([]Passenger, error) {
	if userID == "" {
		return nil, ErrInvalidInput
	}

	rows, err := s.db.Query(ctx, `
		SELECT id::text, user_id::text, display_name, created_at, updated_at
		FROM passengers
		WHERE user_id = $1
		ORDER BY created_at, id
	`, userID)
	if err != nil {
		return nil, mapDatabaseError(err, nil, nil)
	}
	defer rows.Close()

	passengers := make([]Passenger, 0)
	for rows.Next() {
		passenger, err := scanPassenger(rows, nil, nil)
		if err != nil {
			return nil, err
		}
		passengers = append(passengers, passenger)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDatabaseError(err, nil, nil)
	}
	return passengers, nil
}

func (s *Store) UpdatePassenger(ctx context.Context, params UpdatePassengerParams) (Passenger, error) {
	displayName := strings.TrimSpace(params.DisplayName)
	if !domain.ValidPassengerDisplayName(displayName) {
		return Passenger{}, ErrInvalidInput
	}
	if params.UserID == "" || params.PassengerID == "" {
		return Passenger{}, ErrPassengerNotFound
	}

	return scanPassenger(s.db.QueryRow(ctx, `
		UPDATE passengers
		SET display_name = $3
		WHERE id = $1 AND user_id = $2
		RETURNING id::text, user_id::text, display_name, created_at, updated_at
	`, params.PassengerID, params.UserID, displayName), ErrPassengerNotFound, nil)
}

func (s *Store) DeletePassenger(ctx context.Context, userID, passengerID string) error {
	if userID == "" || passengerID == "" {
		return ErrPassengerNotFound
	}

	result, err := s.db.Exec(ctx, `
		DELETE FROM passengers
		WHERE id = $1 AND user_id = $2
	`, passengerID, userID)
	if err != nil {
		return mapDatabaseError(err, nil, nil)
	}
	if result.RowsAffected() == 0 {
		return ErrPassengerNotFound
	}
	return nil
}

func (s *Store) AuthenticationState(ctx context.Context, subject string) (application.SubjectState, error) {
	if strings.TrimSpace(subject) == "" {
		return application.SubjectState{}, ErrUserNotFound
	}

	var state application.SubjectState
	err := s.db.QueryRow(ctx, `
		SELECT active, token_version
		FROM users
		WHERE id = $1
	`, subject).Scan(&state.Active, &state.TokenVersion)
	if errors.Is(err, pgx.ErrNoRows) || hasPostgresCode(err, "22P02") {
		return application.SubjectState{}, ErrUserNotFound
	}
	if err != nil {
		return application.SubjectState{}, mapDatabaseError(err, nil, nil)
	}
	return state, nil
}

func canonicalEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func validEmailLength(email string) bool {
	length := utf8.RuneCountInString(email)
	return length >= 3 && length <= 320
}

func scanUser(row pgx.Row, uniqueError error) (User, error) {
	var user User
	var role string
	if err := row.Scan(
		&user.ID,
		&user.Email,
		&role,
		&user.TokenVersion,
		&user.Active,
		&user.CreatedAt,
		&user.UpdatedAt,
	); err != nil {
		return User{}, mapDatabaseError(err, uniqueError, nil)
	}

	return finalizeUser(user, role)
}

func scanPassenger(row pgx.Row, notFoundError, foreignKeyError error) (Passenger, error) {
	var passenger Passenger
	if err := row.Scan(
		&passenger.ID,
		&passenger.UserID,
		&passenger.DisplayName,
		&passenger.CreatedAt,
		&passenger.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) && notFoundError != nil {
			return Passenger{}, notFoundError
		}
		return Passenger{}, mapDatabaseError(err, nil, foreignKeyError)
	}
	passenger.CreatedAt = passenger.CreatedAt.UTC()
	passenger.UpdatedAt = passenger.UpdatedAt.UTC()
	return passenger, nil
}

func finalizeUser(user User, role string) (User, error) {
	parsedRole, err := domain.ParseRole(role)
	if err != nil {
		return User{}, ErrPersistence
	}
	user.Role = parsedRole
	user.CreatedAt = user.CreatedAt.UTC()
	user.UpdatedAt = user.UpdatedAt.UTC()
	return user, nil
}

func mapDatabaseError(err error, uniqueError, foreignKeyError error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}

	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		switch pgError.Code {
		case "23505":
			if uniqueError != nil {
				return uniqueError
			}
		case "23503":
			if foreignKeyError != nil {
				return foreignKeyError
			}
		case "23514", "22001", "22P02":
			return ErrInvalidInput
		}
	}
	return ErrPersistence
}

func hasPostgresCode(err error, code string) bool {
	var pgError *pgconn.PgError
	return errors.As(err, &pgError) && pgError.Code == code
}
