package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	admissionapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/application"
	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
)

func TestLiveConcurrentFirstUseAdmissionTokenCreatesOneDurableReservation(t *testing.T) {
	harness := newLiveReservationHarness(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const attempts = 32
	start := make(chan struct{})
	results := make(chan liveReservationCallResult, attempts)
	var group sync.WaitGroup
	for range attempts {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			view, err := harness.service.CreateHold(ctx, harness.command)
			results <- liveReservationCallResult{view: view, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	successfulReservationIDs := make(map[string]struct{})
	for result := range results {
		if result.err == nil {
			successfulReservationIDs[result.view.ID] = struct{}{}
			continue
		}
		if !errors.Is(result.err, httpapi.ErrAdmissionInProgress) {
			t.Fatalf("concurrent CreateHold() error = %v", result.err)
		}
	}
	if len(successfulReservationIDs) != 1 {
		t.Fatalf("successful reservation IDs = %v, want one durable reservation", successfulReservationIDs)
	}

	var reservationID string
	for id := range successfulReservationIDs {
		reservationID = id
	}
	replayed, err := harness.service.CreateHold(ctx, harness.command)
	if err != nil {
		t.Fatalf("completed replay CreateHold() error = %v", err)
	}
	if replayed.ID != reservationID {
		t.Fatalf("completed replay reservation = %q, want %q", replayed.ID, reservationID)
	}
	if count := harness.reservationCount(t); count != 1 {
		t.Fatalf("durable reservation count = %d, want 1", count)
	}
}

func TestLiveCommittedReservationReplaysAfterForcedRedisFinalizeFailure(t *testing.T) {
	var fault *forcedFinalizeFailureControl
	harness := newLiveReservationHarness(t, func(control *admissionredis.Store) reservationAdmissionControl {
		fault = newForcedFinalizeFailureControl(control)
		return fault
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	created, err := harness.service.CreateHold(ctx, harness.command)
	if err != nil {
		t.Fatalf("CreateHold() with forced finalize failure error = %v", err)
	}
	if created.ID == "" {
		t.Fatal("CreateHold() with forced finalize failure returned no reservation ID")
	}
	if fault.FinalizeFailures() != 1 {
		t.Fatalf("forced Redis finalize failures = %d, want 1", fault.FinalizeFailures())
	}

	replayed, err := harness.service.CreateHold(ctx, harness.command)
	if err != nil {
		t.Fatalf("CreateHold() replay after finalize failure error = %v", err)
	}
	if replayed.ID != created.ID {
		t.Fatalf("replayed reservation = %q, want committed %q", replayed.ID, created.ID)
	}
	if count := harness.reservationCount(t); count != 1 {
		t.Fatalf("durable reservation count after replay = %d, want 1", count)
	}
}

type liveReservationCallResult struct {
	view httpapi.ReservationView
	err  error
}

// forcedFinalizeFailureControl keeps every Redis operation on the real adapter
// and injects one transport failure through go-redis while the real Store
// executes Finalize. The embedded store preserves the real FinalizeCommitted
// repair path used by the replay.
type forcedFinalizeFailureControl struct {
	*admissionredis.Store
	mu       sync.Mutex
	armed    bool
	failures int
}

func newForcedFinalizeFailureControl(store *admissionredis.Store) *forcedFinalizeFailureControl {
	return &forcedFinalizeFailureControl{Store: store, armed: true}
}

func (control *forcedFinalizeFailureControl) Finalize(
	ctx context.Context,
	mutation admissionredis.LeaseMutation,
) error {
	control.mu.Lock()
	if control.armed {
		control.armed = false
		control.failures++
		control.mu.Unlock()
		return control.Store.Finalize(
			context.WithValue(ctx, forceRedisFinalizeFailureKey{}, true),
			mutation,
		)
	}
	control.mu.Unlock()
	return control.Store.Finalize(ctx, mutation)
}

func (control *forcedFinalizeFailureControl) FinalizeFailures() int {
	control.mu.Lock()
	defer control.mu.Unlock()
	return control.failures
}

type forceRedisFinalizeFailureKey struct{}

type forcedFinalizeRedisHook struct{}

func (forcedFinalizeRedisHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return next
}

func (forcedFinalizeRedisHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, command goredis.Cmder) error {
		if forced, _ := ctx.Value(forceRedisFinalizeFailureKey{}).(bool); forced {
			return errors.New("forced Redis finalize transport failure")
		}
		return next(ctx, command)
	}
}

func (forcedFinalizeRedisHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, commands []goredis.Cmder) error {
		if forced, _ := ctx.Value(forceRedisFinalizeFailureKey{}).(bool); forced {
			return errors.New("forced Redis finalize transport failure")
		}
		return next(ctx, commands)
	}
}

type liveReservationHarness struct {
	pool       *pgxpool.Pool
	service    *ReservationService
	command    httpapi.CreateReservationCommand
	userID     uuid.UUID
	trainRunID uuid.UUID
}

func newLiveReservationHarness(
	t *testing.T,
	decorateControl func(*admissionredis.Store) reservationAdmissionControl,
) liveReservationHarness {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	redisAddress := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR"))
	if databaseURL == "" || redisAddress == "" {
		t.Skip("DATABASE_URL and TEST_REDIS_ADDR are required; skipping live reservation integration test")
	}

	pool := newLiveReservationDatabase(t, databaseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	redisClient := goredis.NewClient(&goredis.Options{Addr: redisAddress})
	redisClient.AddHook(forcedFinalizeRedisHook{})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		_ = redisClient.Close()
		t.Fatalf("connect disposable Redis: %v", err)
	}
	namespace := "m2live_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		deleteRedisNamespace(cleanupCtx, redisClient, namespace)
		_ = redisClient.Close()
	})
	control, err := admissionredis.NewStore(redisClient, namespace)
	if err != nil {
		t.Fatalf("create admission Redis store: %v", err)
	}

	fixture := seedLiveReservationFixture(t, pool)
	bookingStore := bookingpostgres.New(pool)
	if _, err := bookingStore.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize authoritative seat inventory: %v", err)
	}
	policyStore, err := admissionpostgres.NewStore(pool)
	if err != nil {
		t.Fatalf("create admission policy store: %v", err)
	}
	limits, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput{
		MaxQueueSize:           64,
		AdmissionRatePerSecond: 100,
		MaxInflightAdmissions:  64,
		AdmissionTokenTTL:      time.Minute,
		ProcessingLease:        10 * time.Second,
		QueueEntryTTL:          5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("create admission policy limits: %v", err)
	}
	if _, err := policyStore.CreatePolicy(ctx, admissionpostgres.CreatePolicyParams{
		TrainRunID: fixture.trainRunID,
		SeatClass:  offeringdomain.SeatClassStandard,
		Limits:     limits,
		Metadata: admissionpostgres.MutationMetadata{
			ActorID:       uuid.New(),
			CorrelationID: "live-reservation-integration",
		},
	}); err != nil {
		t.Fatalf("create hot-train policy: %v", err)
	}

	keyring, err := admissiondomain.NewTokenKeyring("integration", map[string][]byte{
		"integration": bytes.Repeat([]byte{0x5a}, 32),
	})
	if err != nil {
		t.Fatalf("create admission token keyring: %v", err)
	}
	worker, err := admissionapp.NewWorker(policyStore, control, keyring, 64)
	if err != nil {
		t.Fatalf("create admission worker: %v", err)
	}
	if _, err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("initialize Redis policy generation: %v", err)
	}

	queryStore, err := querypostgres.NewStore(pool)
	if err != nil {
		t.Fatalf("create journey query store: %v", err)
	}
	waitingRoom := NewWaitingRoomService(policyStore, queryStore, control, keyring)
	joined, err := waitingRoom.JoinWaitingRoom(ctx, httpapi.JoinWaitingRoomCommand{
		OwnerID:                fixture.userID.String(),
		TrainRunID:             fixture.trainRunID.String(),
		OriginStationCode:      fixture.originCode,
		DestinationStationCode: fixture.destinationCode,
		SeatClass:              offeringdomain.SeatClassStandard.String(),
		PassengerCount:         len(fixture.passengerIDs),
	})
	if err != nil {
		t.Fatalf("join live waiting room: %v", err)
	}
	issued, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("issue live admission token: %v", err)
	}
	if issued.Issued != 1 {
		t.Fatalf("issued admission tokens = %d, want 1", issued.Issued)
	}
	delivered, err := waitingRoom.GetWaitingRoomEntry(ctx, fixture.userID.String(), joined.EntryID)
	if err != nil {
		t.Fatalf("claim live admission token: %v", err)
	}
	if delivered.AdmissionToken == "" {
		t.Fatal("live waiting-room delivery omitted admission token")
	}

	reservationControl := reservationAdmissionControl(control)
	if decorateControl != nil {
		reservationControl = decorateControl(control)
	}
	executionSlots, err := NewExecutionSlots(64)
	if err != nil {
		t.Fatalf("create reservation execution slots: %v", err)
	}
	passengerIDs := make([]string, len(fixture.passengerIDs))
	for index, passengerID := range fixture.passengerIDs {
		passengerIDs[index] = passengerID.String()
	}
	command := httpapi.CreateReservationCommand{
		OwnerID:                fixture.userID.String(),
		IdempotencyKey:         "live-" + uuid.NewString(),
		AdmissionToken:         delivered.AdmissionToken,
		TrainRunID:             fixture.trainRunID.String(),
		OriginStationCode:      fixture.originCode,
		DestinationStationCode: fixture.destinationCode,
		SeatClass:              offeringdomain.SeatClassStandard.String(),
		PassengerIDs:           passengerIDs,
	}
	service := NewAdmissionProtectedReservationService(
		bookingStore,
		bookingStore,
		queryStore,
		NewPostgresReads(pool),
		policyStore,
		reservationControl,
		keyring,
		executionSlots,
		clock.RealClock{},
		10*time.Minute,
		6,
	).WithDatabaseCommandTimeout(3 * time.Second)

	return liveReservationHarness{
		pool: pool, service: service, command: command,
		userID: fixture.userID, trainRunID: fixture.trainRunID,
	}
}

type liveReservationFixture struct {
	userID          uuid.UUID
	passengerIDs    []uuid.UUID
	trainRunID      uuid.UUID
	originCode      string
	destinationCode string
}

func seedLiveReservationFixture(t *testing.T, pool *pgxpool.Pool) liveReservationFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	userID := uuid.New()
	routeID, trainID, coachID, trainRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	stationIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	stationCodes := []string{"LVA", "LVB", "LVC"}
	passengerIDs := []uuid.UUID{uuid.New()}

	batch := &pgx.Batch{}
	batch.Queue(
		`INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)`,
		userID,
		userID.String()+"@live.test",
		"$2a$12$abcdefghijklmnopqrstuv012345678901234567890123456789",
	)
	for index, stationID := range stationIDs {
		batch.Queue(
			`INSERT INTO stations (id, code, name, timezone) VALUES ($1, $2, $3, 'Asia/Taipei')`,
			stationID,
			stationCodes[index],
			fmt.Sprintf("Live Station %d", index),
		)
	}
	batch.Queue(
		`INSERT INTO routes (id, code, name, operating_timezone) VALUES ($1, $2, 'Live Route', 'Asia/Taipei')`,
		routeID,
		"LVR"+strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", "")[:8]),
	)
	for index, stationID := range stationIDs {
		batch.Queue(
			`INSERT INTO route_stops (
			    route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes
			) VALUES ($1, $2, $3, $4, $4)`,
			routeID,
			stationID,
			index,
			index*10,
		)
	}
	batch.Queue(
		`INSERT INTO trains (id, code, name) VALUES ($1, $2, 'Live Train')`,
		trainID,
		"LVT"+strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", "")[:8]),
	)
	batch.Queue(
		`INSERT INTO coaches (id, train_id, coach_number, seat_class) VALUES ($1, $2, '1', 'standard')`,
		coachID,
		trainID,
	)
	for index := 0; index < 8; index++ {
		batch.Queue(
			`INSERT INTO seats (id, coach_id, seat_number) VALUES ($1, $2, $3)`,
			uuid.New(),
			coachID,
			fmt.Sprintf("%02dA", index+1),
		)
	}
	batch.Queue(
		`INSERT INTO train_runs (
		    id, train_id, route_id, service_date, scheduled_departure_at, segment_count
		) VALUES ($1, $2, $3, CURRENT_DATE + 1, clock_timestamp() + interval '1 day', 2)`,
		trainRunID,
		trainID,
		routeID,
	)
	batch.Queue(
		`INSERT INTO fares (
		    train_run_id, from_stop_index, to_stop_index, seat_class, amount_minor, currency
		) VALUES ($1, 0, 2, 'standard', 1250, 'TWD')`,
		trainRunID,
	)
	for index, passengerID := range passengerIDs {
		batch.Queue(
			`INSERT INTO passengers (id, user_id, display_name) VALUES ($1, $2, $3)`,
			passengerID,
			userID,
			fmt.Sprintf("Live Passenger %d", index+1),
		)
	}

	results := pool.SendBatch(ctx, batch)
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			t.Fatalf("seed live reservation fixture: %v", err)
		}
	}
	if err := results.Close(); err != nil {
		t.Fatalf("close live reservation fixture batch: %v", err)
	}
	return liveReservationFixture{
		userID: userID, passengerIDs: passengerIDs, trainRunID: trainRunID,
		originCode: stationCodes[0], destinationCode: stationCodes[2],
	}
}

func newLiveReservationDatabase(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect disposable PostgreSQL: %v", err)
	}
	schema := "m2_live_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatalf("create isolated PostgreSQL schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
		t.Fatalf("parse disposable PostgreSQL configuration: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
		t.Fatalf("connect isolated PostgreSQL schema: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	applyLiveReservationMigrations(t, ctx, pool)
	return pool
}

func applyLiveReservationMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("glob live reservation migrations: %v", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		t.Fatal("no migrations found for live reservation integration test")
	}
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
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", filepath.Base(path), err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", filepath.Base(path), err)
		}
	}
}

func deleteRedisNamespace(ctx context.Context, client *goredis.Client, namespace string) {
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, namespace+":*", 100).Result()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

func (h liveReservationHarness) reservationCount(t *testing.T) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var count int
	if err := h.pool.QueryRow(ctx, `
SELECT count(*)
FROM reservations
WHERE user_id = $1 AND train_run_id = $2`, h.userID, h.trainRunID).Scan(&count); err != nil {
		t.Fatalf("count durable reservations: %v", err)
	}
	return count
}
