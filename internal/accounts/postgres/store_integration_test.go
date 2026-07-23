package postgres_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	accountspostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/postgres"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

func TestStoreRegistersUserWithoutReturningCredentials(t *testing.T) {
	store := openTestStore(t)

	user, err := store.RegisterUser(context.Background(), accountspostgres.RegisterUserParams{
		Email:    "Alice@Example.COM ",
		Password: "correct horse battery staple",
		Role:     domain.RoleCustomer,
	})
	if err != nil {
		t.Fatalf("RegisterUser() error = %v", err)
	}
	if user.ID == "" {
		t.Fatal("RegisterUser() returned a nil user ID")
	}
	if user.Email != "alice@example.com" {
		t.Fatalf("Email = %q, want alice@example.com", user.Email)
	}
	if user.Role != domain.RoleCustomer || !user.Active || user.TokenVersion != 1 {
		t.Fatalf("User auth state = role %q, active %t, version %d", user.Role, user.Active, user.TokenVersion)
	}
	if user.CreatedAt.IsZero() || user.UpdatedAt.IsZero() {
		t.Fatal("RegisterUser() returned zero timestamps")
	}

	encoded, err := json.Marshal(user)
	if err != nil {
		t.Fatalf("json.Marshal(user) error = %v", err)
	}
	if bytes.Contains(bytes.ToLower(encoded), []byte("password")) {
		t.Fatalf("registered user exposes credential material: %s", encoded)
	}
}

func TestStoreLoginLookupVerifiesCredentialsWithoutLeakingExistence(t *testing.T) {
	store := openTestStore(t)

	registered, err := store.RegisterUser(context.Background(), accountspostgres.RegisterUserParams{
		Email:    "login@example.com",
		Password: "valid login password",
		Role:     domain.RoleOperator,
	})
	if err != nil {
		t.Fatalf("RegisterUser() error = %v", err)
	}

	loggedIn, err := store.LookupUserForLogin(context.Background(), accountspostgres.LoginLookup{
		Email:    " LOGIN@EXAMPLE.COM ",
		Password: "valid login password",
	})
	if err != nil {
		t.Fatalf("LookupUserForLogin() error = %v", err)
	}
	if loggedIn.ID != registered.ID || loggedIn.Role != domain.RoleOperator {
		t.Fatalf("LookupUserForLogin() user = %#v, want registered user", loggedIn)
	}

	tests := []struct {
		name   string
		lookup accountspostgres.LoginLookup
	}{
		{
			name: "wrong password",
			lookup: accountspostgres.LoginLookup{
				Email:    "login@example.com",
				Password: "wrong password",
			},
		},
		{
			name: "unknown email",
			lookup: accountspostgres.LoginLookup{
				Email:    "unknown@example.com",
				Password: "valid login password",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.LookupUserForLogin(context.Background(), tt.lookup)
			if !errors.Is(err, accountspostgres.ErrInvalidCredentials) {
				t.Fatalf("LookupUserForLogin() error = %v, want %v", err, accountspostgres.ErrInvalidCredentials)
			}
		})
	}
}

func TestStoreMapsDuplicateEmailWithoutLeakingDatabaseDetails(t *testing.T) {
	store := openTestStore(t)

	registration := accountspostgres.RegisterUserParams{
		Email:    "duplicate@example.com",
		Password: "valid registration password",
		Role:     domain.RoleCustomer,
	}
	if _, err := store.RegisterUser(context.Background(), registration); err != nil {
		t.Fatalf("first RegisterUser() error = %v", err)
	}
	registration.Email = " DUPLICATE@EXAMPLE.COM "
	_, err := store.RegisterUser(context.Background(), registration)
	if !errors.Is(err, accountspostgres.ErrEmailAlreadyRegistered) {
		t.Fatalf("duplicate RegisterUser() error = %v, want %v", err, accountspostgres.ErrEmailAlreadyRegistered)
	}
	if strings.Contains(strings.ToLower(err.Error()), "duplicate@example.com") {
		t.Fatalf("duplicate error leaked email: %v", err)
	}
}

func TestStoreCreatesAndGetsOnlyOwnedPassenger(t *testing.T) {
	store := openTestStore(t)
	owner := registerTestUser(t, store, "owner@example.com")
	other := registerTestUser(t, store, "other@example.com")

	created, err := store.CreatePassenger(context.Background(), accountspostgres.CreatePassengerParams{
		UserID:      owner.ID,
		DisplayName: "  Lin Ya-Ting  ",
	})
	if err != nil {
		t.Fatalf("CreatePassenger() error = %v", err)
	}
	if created.ID == "" || created.UserID != owner.ID || created.DisplayName != "Lin Ya-Ting" {
		t.Fatalf("CreatePassenger() = %#v", created)
	}
	if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Fatal("CreatePassenger() returned zero timestamps")
	}

	found, err := store.GetPassenger(context.Background(), owner.ID, created.ID)
	if err != nil {
		t.Fatalf("GetPassenger() error = %v", err)
	}
	if found != created {
		t.Fatalf("GetPassenger() = %#v, want %#v", found, created)
	}

	if _, err := store.GetPassenger(context.Background(), other.ID, created.ID); !errors.Is(err, accountspostgres.ErrPassengerNotFound) {
		t.Fatalf("other owner's GetPassenger() error = %v, want %v", err, accountspostgres.ErrPassengerNotFound)
	}
}

func TestStoreListsOnlyOwnedPassengersInStableOrder(t *testing.T) {
	store := openTestStore(t)
	owner := registerTestUser(t, store, "list-owner@example.com")
	other := registerTestUser(t, store, "list-other@example.com")
	emptyOwner := registerTestUser(t, store, "list-empty@example.com")

	first, err := store.CreatePassenger(context.Background(), accountspostgres.CreatePassengerParams{UserID: owner.ID, DisplayName: "First"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.CreatePassenger(context.Background(), accountspostgres.CreatePassengerParams{UserID: owner.ID, DisplayName: "Second"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreatePassenger(context.Background(), accountspostgres.CreatePassengerParams{UserID: other.ID, DisplayName: "Other"}); err != nil {
		t.Fatal(err)
	}

	firstRead, err := store.ListPassengers(context.Background(), owner.ID)
	if err != nil {
		t.Fatalf("ListPassengers() error = %v", err)
	}
	secondRead, err := store.ListPassengers(context.Background(), owner.ID)
	if err != nil {
		t.Fatalf("ListPassengers() second error = %v", err)
	}
	if len(firstRead) != 2 {
		t.Fatalf("ListPassengers() count = %d, want 2", len(firstRead))
	}
	if !reflect.DeepEqual(firstRead, secondRead) {
		t.Fatalf("ListPassengers() order changed: %#v then %#v", firstRead, secondRead)
	}
	seen := map[string]bool{firstRead[0].ID: true, firstRead[1].ID: true}
	if !seen[first.ID] || !seen[second.ID] {
		t.Fatalf("ListPassengers() = %#v, want owned passengers", firstRead)
	}
	for _, passenger := range firstRead {
		if passenger.UserID != owner.ID {
			t.Fatalf("ListPassengers() leaked another owner's passenger: %#v", passenger)
		}
	}

	empty, err := store.ListPassengers(context.Background(), emptyOwner.ID)
	if err != nil {
		t.Fatalf("empty ListPassengers() error = %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty ListPassengers() = %#v, want non-nil empty slice", empty)
	}
}

func TestStoreUpdatesAndDeletesOnlyOwnedPassenger(t *testing.T) {
	store := openTestStore(t)
	owner := registerTestUser(t, store, "mutate-owner@example.com")
	other := registerTestUser(t, store, "mutate-other@example.com")
	created, err := store.CreatePassenger(context.Background(), accountspostgres.CreatePassengerParams{
		UserID: owner.ID, DisplayName: "Before",
	})
	if err != nil {
		t.Fatal(err)
	}

	updated, err := store.UpdatePassenger(context.Background(), accountspostgres.UpdatePassengerParams{
		UserID: owner.ID, PassengerID: created.ID, DisplayName: "  After  ",
	})
	if err != nil {
		t.Fatalf("UpdatePassenger() error = %v", err)
	}
	if updated.ID != created.ID || updated.UserID != owner.ID || updated.DisplayName != "After" {
		t.Fatalf("UpdatePassenger() = %#v", updated)
	}

	_, err = store.UpdatePassenger(context.Background(), accountspostgres.UpdatePassengerParams{
		UserID: other.ID, PassengerID: created.ID, DisplayName: "Stolen",
	})
	if !errors.Is(err, accountspostgres.ErrPassengerNotFound) {
		t.Fatalf("other owner's UpdatePassenger() error = %v, want %v", err, accountspostgres.ErrPassengerNotFound)
	}
	if err := store.DeletePassenger(context.Background(), other.ID, created.ID); !errors.Is(err, accountspostgres.ErrPassengerNotFound) {
		t.Fatalf("other owner's DeletePassenger() error = %v, want %v", err, accountspostgres.ErrPassengerNotFound)
	}
	if err := store.DeletePassenger(context.Background(), owner.ID, created.ID); err != nil {
		t.Fatalf("DeletePassenger() error = %v", err)
	}
	if err := store.DeletePassenger(context.Background(), owner.ID, created.ID); !errors.Is(err, accountspostgres.ErrPassengerNotFound) {
		t.Fatalf("repeated DeletePassenger() error = %v, want %v", err, accountspostgres.ErrPassengerNotFound)
	}
	if _, err := store.GetPassenger(context.Background(), owner.ID, created.ID); !errors.Is(err, accountspostgres.ErrPassengerNotFound) {
		t.Fatalf("GetPassenger() after delete error = %v, want %v", err, accountspostgres.ErrPassengerNotFound)
	}
}

func TestStoreReadsAuthenticationStateWithoutCredentials(t *testing.T) {
	store := openTestStore(t)
	user := registerTestUser(t, store, "state@example.com")
	var reader application.SubjectStateReader = store

	state, err := reader.AuthenticationState(context.Background(), user.ID)
	if err != nil {
		t.Fatalf("AuthenticationState() error = %v", err)
	}
	if !state.Active || state.Role != domain.RoleCustomer || state.TokenVersion != 1 {
		t.Fatalf("AuthenticationState() = %#v, want active customer version 1", state)
	}

	for _, subject := range []string{"not-a-uuid", "00000000-0000-0000-0000-000000000000"} {
		if _, err := reader.AuthenticationState(context.Background(), subject); !errors.Is(err, accountspostgres.ErrUserNotFound) {
			t.Fatalf("AuthenticationState(%q) error = %v, want %v", subject, err, accountspostgres.ErrUserNotFound)
		}
	}
}

func registerTestUser(t *testing.T, store *accountspostgres.Store, email string) accountspostgres.User {
	t.Helper()

	user, err := store.RegisterUser(context.Background(), accountspostgres.RegisterUserParams{
		Email:    email,
		Password: "synthetic test password",
		Role:     domain.RoleCustomer,
	})
	if err != nil {
		t.Fatalf("RegisterUser(%q) error = %v", email, err)
	}
	return user
}

func openTestStore(t *testing.T) *accountspostgres.Store {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if strings.TrimSpace(databaseURL) == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	adminConn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin database: %v", err)
	}
	if err := adminConn.Ping(ctx); err != nil {
		_ = adminConn.Close(ctx)
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	schema := "accounts_test_" + randomHex(t, 16)
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := adminConn.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		_ = adminConn.Close(ctx)
		t.Fatalf("create test schema: %v", err)
	}

	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		_ = adminConn.Close(ctx)
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	config.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		_ = adminConn.Close(ctx)
		t.Fatalf("connect test database: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := conn.Close(cleanupCtx); err != nil {
			t.Errorf("close test database: %v", err)
		}
		if _, err := adminConn.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		if err := adminConn.Close(cleanupCtx); err != nil {
			t.Errorf("close admin database: %v", err)
		}
	})

	applyAccountsMigration(t, ctx, conn)
	hasher, err := application.NewBcryptPasswordHasher(bcrypt.MinCost)
	if err != nil {
		t.Fatalf("create password hasher: %v", err)
	}
	store, err := accountspostgres.NewStore(conn, hasher)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store
}

func randomHex(t *testing.T, byteCount int) string {
	t.Helper()

	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate test schema suffix: %v", err)
	}
	return hex.EncodeToString(value)
}

func applyAccountsMigration(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test path")
	}
	migrationPath := filepath.Join(filepath.Dir(currentFile), "..", "..", "..", "migrations", "000001_accounts.up.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read accounts migration: %v", err)
	}

	if _, err := conn.PgConn().Exec(ctx, string(migration)).ReadAll(); err != nil {
		t.Fatalf("apply accounts migration: %v", err)
	}
}
