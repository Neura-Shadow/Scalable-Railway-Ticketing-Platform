package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNewStoreRejectsNilPool(t *testing.T) {
	t.Parallel()
	store, err := NewStore(nil)
	if store != nil || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("NewStore(nil) = (%v, %v), want (nil, %v)", store, err, ErrInvalidConfiguration)
	}
}

func TestMutationMetadataRequiresBoundedAuditableIdentity(t *testing.T) {
	t.Parallel()
	if validMetadata(MutationMetadata{ActorID: uuid.Nil, CorrelationID: "request"}) {
		t.Fatal("nil actor ID was accepted")
	}
	if validMetadata(MutationMetadata{ActorID: uuid.New(), CorrelationID: "  "}) {
		t.Fatal("blank correlation ID was accepted")
	}
	if !validMetadata(MutationMetadata{ActorID: uuid.New(), CorrelationID: "policy-change-42"}) {
		t.Fatal("bounded actor and correlation metadata was rejected")
	}
}

func TestPolicyOrderByUsesOnlyDeterministicWhitelistedClauses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		field      PolicySortField
		descending bool
		want       string
		ok         bool
	}{
		{PolicySortTrainRunID, false, " ORDER BY train_run_id ASC, seat_class ASC, id ASC", true},
		{PolicySortSeatClass, true, " ORDER BY seat_class DESC, train_run_id DESC, id DESC", true},
		{PolicySortUpdatedAt, false, " ORDER BY updated_at ASC, id ASC", true},
		{PolicySortField("updated_at; DROP TABLE hot_train_policies"), false, "", false},
	}
	for _, test := range tests {
		got, ok := policyOrderBy(test.field, test.descending)
		if got != test.want || ok != test.ok {
			t.Fatalf("policyOrderBy(%q, %v) = (%q, %v), want (%q, %v)",
				test.field, test.descending, got, ok, test.want, test.ok)
		}
	}
}

func TestPolicyMutationPersistsClassificationVersionAndOutboxAtomically(t *testing.T) {
	pool, actorID, trainRunID := newPolicyIntegrationFixture(t)
	store, err := NewStore(pool)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	metadata := MutationMetadata{ActorID: actorID, CorrelationID: "policy-integration-test"}
	created, err := store.CreatePolicy(context.Background(), CreatePolicyParams{
		TrainRunID: trainRunID, SeatClass: offeringdomain.SeatClassStandard,
		Limits: policyTestLimits(t), Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("CreatePolicy() error = %v", err)
	}
	if !created.Enabled || created.Version != 1 || created.RedisInitializedVersion != nil {
		t.Fatalf("created policy = %+v, want enabled version-1 uninitialized policy", created)
	}
	page, err := store.ListPoliciesPage(context.Background(), ListPoliciesParams{
		Limit: 1, Sort: PolicySortUpdatedAt, Descending: true,
	})
	if err != nil {
		t.Fatalf("ListPoliciesPage() error = %v", err)
	}
	if page.Total != 1 || len(page.Policies) != 1 || page.Policies[0].ID != created.ID {
		t.Fatalf("ListPoliciesPage() = %+v, want one created policy", page)
	}
	enabledPage, err := store.ListEnabledPoliciesAfter(context.Background(), uuid.Nil, 1)
	if err != nil || len(enabledPage) != 1 || enabledPage[0].ID != created.ID {
		t.Fatalf("ListEnabledPoliciesAfter(start) = (%+v, %v), want created policy", enabledPage, err)
	}
	afterCreated, err := store.ListEnabledPoliciesAfter(context.Background(), created.ID, 1)
	if err != nil || len(afterCreated) != 0 {
		t.Fatalf("ListEnabledPoliciesAfter(created) = (%+v, %v), want empty tail", afterCreated, err)
	}
	emptyPage, err := store.ListPoliciesPage(context.Background(), ListPoliciesParams{
		Offset: 1, Limit: 1, Sort: PolicySortTrainRunID,
	})
	if err != nil || emptyPage.Total != 1 || len(emptyPage.Policies) != 0 {
		t.Fatalf("offset ListPoliciesPage() = (%+v, %v), want empty page with total 1", emptyPage, err)
	}
	if _, err := store.CreatePolicy(context.Background(), CreatePolicyParams{
		TrainRunID: trainRunID, SeatClass: offeringdomain.SeatClassStandard,
		Limits: policyTestLimits(t), Metadata: metadata,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate CreatePolicy() error = %v, want %v", err, ErrConflict)
	}
	initialized, err := store.MarkRedisInitialized(context.Background(), created.ID, created.Version)
	if err != nil {
		t.Fatalf("MarkRedisInitialized() error = %v", err)
	}
	if initialized.RedisInitializedVersion == nil || *initialized.RedisInitializedVersion != created.Version {
		t.Fatalf("initialized policy = %+v, want continuity marker %d", initialized, created.Version)
	}
	replayedInitialization, err := store.MarkRedisInitialized(context.Background(), created.ID, created.Version)
	if err != nil || replayedInitialization.RedisInitializedVersion == nil ||
		*replayedInitialization.RedisInitializedVersion != created.Version {
		t.Fatalf("idempotent MarkRedisInitialized() = (%+v, %v)", replayedInitialization, err)
	}
	updated, err := store.UpdatePolicy(context.Background(), created.ID, UpdatePolicyParams{
		ExpectedVersion: created.Version,
		Limits:          policyTestLimits(t),
		Metadata:        metadata,
	})
	if err != nil {
		t.Fatalf("UpdatePolicy() error = %v", err)
	}
	if updated.Version != 2 || updated.RedisInitializedVersion != nil || !updated.Enabled {
		t.Fatalf("updated policy = %+v, want version 2 and cleared continuity marker", updated)
	}
	if _, err := store.MarkRedisInitialized(context.Background(), created.ID, created.Version); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale MarkRedisInitialized() error = %v, want %v", err, ErrVersionConflict)
	}
	if _, err := store.UpdatePolicy(context.Background(), created.ID, UpdatePolicyParams{
		ExpectedVersion: created.Version, Limits: policyTestLimits(t), Metadata: metadata,
	}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale UpdatePolicy() error = %v, want %v", err, ErrVersionConflict)
	}
	disabled, err := store.DisablePolicy(context.Background(), created.ID, updated.Version, metadata)
	if err != nil {
		t.Fatalf("DisablePolicy() error = %v", err)
	}
	if disabled.Enabled || disabled.Version != 3 || disabled.RedisInitializedVersion != nil {
		t.Fatalf("disabled policy = %+v, want disabled version-3 uninitialized policy", disabled)
	}
	enabledPage, err = store.ListEnabledPoliciesAfter(context.Background(), uuid.Nil, 1)
	if err != nil || len(enabledPage) != 0 {
		t.Fatalf("ListEnabledPoliciesAfter(disable) = (%+v, %v), want disabled policy excluded", enabledPage, err)
	}
	var events int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM outbox_events
WHERE aggregate_type = 'hot_train_policy' AND aggregate_id = $1`, created.ID).Scan(&events); err != nil {
		t.Fatalf("count policy outbox events: %v", err)
	}
	if events != 3 {
		t.Fatalf("policy outbox events = %d, want 3", events)
	}
}

func policyTestLimits(t *testing.T) admissiondomain.PolicyLimits {
	t.Helper()
	limits, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput{
		MaxQueueSize: 100, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 20,
		AdmissionTokenTTL: time.Minute, ProcessingLease: 10 * time.Second, QueueEntryTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPolicyLimits() error = %v", err)
	}
	return limits
}

func newPolicyIntegrationFixture(t *testing.T) (*pgxpool.Pool, uuid.UUID, uuid.UUID) {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect PostgreSQL admin: %v", err)
	}
	schema := "admission_policy_" + uuid.NewString()[0:12]
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close(ctx)
		t.Fatalf("create isolated schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close(ctx)
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		admin.Close(ctx)
		t.Fatalf("connect isolated pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		admin.Close(context.Background())
	})
	applyAllMigrations(t, ctx, pool)
	return seedPolicyFixture(t, ctx, pool)
}

func applyAllMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("find migrations: %v", err)
	}
	sort.Strings(paths)
	for _, path := range paths {
		migrationName := filepath.Base(path)
		if migrationName == "000009_physical_shard_control_plane.up.sql" {
			var installed bool
			if err := pool.QueryRow(ctx, `SELECT to_regclass('public.physical_shard_migrations') IS NOT NULL`).Scan(&installed); err != nil {
				t.Fatalf("inspect control-plane migration state: %v", err)
			}
			if installed {
				continue
			}
		}
		if migrationName == "000010_payment_control_plane.up.sql" {
			var installed bool
			if err := pool.QueryRow(ctx, `SELECT to_regclass('public.payment_intents') IS NOT NULL`).Scan(&installed); err != nil {
				t.Fatalf("inspect payment control-plane migration state: %v", err)
			}
			if installed {
				continue
			}
		}
		if migrationName == "000011_payment_ops_dr.up.sql" {
			var installed bool
			if err := pool.QueryRow(ctx, `SELECT to_regclass('public.regional_write_authority') IS NOT NULL`).Scan(&installed); err != nil {
				t.Fatalf("inspect payment operations and DR migration state: %v", err)
			}
			if installed {
				continue
			}
		}
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", filepath.Base(path), err)
		}
	}
}

func seedPolicyFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Pool, uuid.UUID, uuid.UUID) {
	t.Helper()
	actorID, routeID, trainID, trainRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	stationA, stationB := uuid.New(), uuid.New()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin policy fixture: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `INSERT INTO users (id, email, password_hash, role) VALUES ($1, 'operator@example.test', $2, 'operator')`, actorID, "$2a$12$abcdefghijklmnopqrstuv012345678901234567890123456789"); err != nil {
		t.Fatalf("seed actor: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO stations (id, code, name, timezone) VALUES ($1, 'PA', 'Policy A', 'UTC'), ($2, 'PB', 'Policy B', 'UTC')`, stationA, stationB); err != nil {
		t.Fatalf("seed stations: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO routes (id, code, name, operating_timezone) VALUES ($1, 'POLICY', 'Policy Route', 'UTC')`, routeID); err != nil {
		t.Fatalf("seed route: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO route_stops (route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes) VALUES ($1, $2, 0, 0, 0), ($1, $3, 1, 10, 10)`, routeID, stationA, stationB); err != nil {
		t.Fatalf("seed route stops: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO trains (id, code, name) VALUES ($1, 'POLICY_TRAIN', 'Policy Train')`, trainID); err != nil {
		t.Fatalf("seed train: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO train_runs (id, train_id, route_id, service_date, scheduled_departure_at, segment_count) VALUES ($1, $2, $3, CURRENT_DATE + 1, clock_timestamp() + interval '1 day', 1)`, trainRunID, trainID, routeID); err != nil {
		t.Fatalf("seed train run: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit policy fixture: %v", err)
	}
	return pool, actorID, trainRunID
}
