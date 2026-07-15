package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	accountspostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestLoginLookupPerformsPasswordWorkForUnknownEmail(t *testing.T) {
	hasher := &recordingPasswordHasher{}
	store, err := accountspostgres.NewStore(noRowsDB{}, hasher)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	_, err = store.LookupUserForLogin(context.Background(), accountspostgres.LoginLookup{
		Email:    "unknown@example.com",
		Password: "submitted password",
	})
	if !errors.Is(err, accountspostgres.ErrInvalidCredentials) {
		t.Fatalf("LookupUserForLogin() error = %v, want %v", err, accountspostgres.ErrInvalidCredentials)
	}
	if hasher.verifyCalls != 1 || hasher.lastVerifiedHash != "adapter-local-dummy-hash" {
		t.Fatalf("password verification calls = %d, hash = %q", hasher.verifyCalls, hasher.lastVerifiedHash)
	}
}

func TestRegisterUserMapsDatabaseErrorsWithoutLeakingDetails(t *testing.T) {
	tests := []struct {
		name    string
		rowErr  error
		wantErr error
	}{
		{
			name: "duplicate email",
			rowErr: &pgconn.PgError{
				Code:   "23505",
				Detail: "Key (email)=(private@example.com) already exists",
			},
			wantErr: accountspostgres.ErrEmailAlreadyRegistered,
		},
		{
			name:    "unexpected database failure",
			rowErr:  errors.New("database failed for private@example.com"),
			wantErr: accountspostgres.ErrPersistence,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := accountspostgres.NewStore(rowErrorDB{err: tt.rowErr}, &recordingPasswordHasher{})
			if err != nil {
				t.Fatalf("NewStore() error = %v", err)
			}
			_, err = store.RegisterUser(context.Background(), accountspostgres.RegisterUserParams{
				Email:    "private@example.com",
				Password: "submitted password",
				Role:     domain.RoleCustomer,
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("RegisterUser() error = %v, want %v", err, tt.wantErr)
			}
			if err.Error() == tt.rowErr.Error() {
				t.Fatalf("RegisterUser() returned raw database error: %v", err)
			}
		})
	}
}

func TestNewStoreRejectsMissingOrBrokenDependencies(t *testing.T) {
	hasher := &recordingPasswordHasher{}
	if _, err := accountspostgres.NewStore(nil, hasher); !errors.Is(err, accountspostgres.ErrInvalidStoreConfiguration) {
		t.Fatalf("NewStore(nil, hasher) error = %v", err)
	}
	if _, err := accountspostgres.NewStore(noRowsDB{}, nil); !errors.Is(err, accountspostgres.ErrInvalidStoreConfiguration) {
		t.Fatalf("NewStore(db, nil) error = %v", err)
	}
	if _, err := accountspostgres.NewStore(noRowsDB{}, failingPasswordHasher{}); !errors.Is(err, accountspostgres.ErrInvalidStoreConfiguration) {
		t.Fatalf("NewStore(db, failing hasher) error = %v", err)
	}
}

type recordingPasswordHasher struct {
	verifyCalls      int
	lastVerifiedHash string
}

func (*recordingPasswordHasher) Hash(string) (string, error) {
	return "adapter-local-dummy-hash", nil
}

func (h *recordingPasswordHasher) Verify(_ string, encodedHash string) error {
	h.verifyCalls++
	h.lastVerifiedHash = encodedHash
	return application.ErrPasswordMismatch
}

type failingPasswordHasher struct{}

func (failingPasswordHasher) Hash(string) (string, error) {
	return "", errors.New("hash unavailable")
}

func (failingPasswordHasher) Verify(string, string) error { return nil }

type noRowsDB struct{}

func (noRowsDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (noRowsDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected Query call")
}

func (noRowsDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return noRowsRow{}
}

type noRowsRow struct{}

func (noRowsRow) Scan(...any) error { return pgx.ErrNoRows }

type rowErrorDB struct {
	err error
}

func (rowErrorDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (rowErrorDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected Query call")
}

func (db rowErrorDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return rowErrorRow(db)
}

type rowErrorRow struct {
	err error
}

func (row rowErrorRow) Scan(...any) error { return row.err }
