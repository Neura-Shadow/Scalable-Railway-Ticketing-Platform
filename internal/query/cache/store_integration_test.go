package cache

import (
	"context"
	"crypto/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestCacheStoreCoalescesColdStationMissAndSharesWarmValue(t *testing.T) {
	client := openCacheRedis(t)
	source := &cacheSourceFake{stations: stationFixture(t)}
	store := newCacheStore(t, source, source, client)
	const callers = 24
	ready := make(chan struct{}, callers)
	start := make(chan struct{})
	stationPointers := make(chan *querypostgres.Station, callers)
	var wait sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ready <- struct{}{}
			<-start
			stations, err := store.ListStations(context.Background())
			if err != nil || len(stations) != 1 {
				t.Errorf("ListStations() = %v, %v", stations, err)
				return
			}
			stationPointers <- &stations[0]
		}()
	}
	for caller := 0; caller < callers; caller++ {
		<-ready
	}
	close(start)
	for attempt := 0; attempt < 1024; attempt++ {
		runtime.Gosched()
	}
	wait.Wait()
	close(stationPointers)
	seenPointers := make(map[*querypostgres.Station]struct{}, callers)
	for pointer := range stationPointers {
		if _, duplicate := seenPointers[pointer]; duplicate {
			t.Fatal("cold station callers shared a mutable result slice")
		}
		seenPointers[pointer] = struct{}{}
	}
	if source.stationCalls.Load() != 1 {
		t.Fatalf("cold station source calls = %d, want 1", source.stationCalls.Load())
	}
	if _, err := store.ListStations(context.Background()); err != nil {
		t.Fatalf("ListStations(warm) error = %v", err)
	}
	if source.stationCalls.Load() != 1 {
		t.Fatalf("warm station source calls = %d, want 1", source.stationCalls.Load())
	}
}

func TestCacheStoreUsesProjectionThenFallsBackToSourceForEmptyProjection(t *testing.T) {
	client := openCacheRedis(t)
	request := searchFixture()
	projected := searchResultFixture(1200)
	source := &cacheSourceFake{search: []querypostgres.SearchResult{searchResultFixture(1300)}}
	projection := &cacheSourceFake{search: []querypostgres.SearchResult{projected}}
	store := newCacheStore(t, source, projection, client)

	results, err := store.SearchTrainRuns(context.Background(), request)
	if err != nil || len(results) != 1 || results[0].FareAmountMinor != 1200 {
		t.Fatalf("SearchTrainRuns(projected) = %+v, %v", results, err)
	}
	if source.searchCalls.Load() != 0 || projection.searchCalls.Load() != 1 {
		t.Fatalf("search calls source/projection = %d/%d", source.searchCalls.Load(), projection.searchCalls.Load())
	}
	manager, _ := NewSecureVersionManager(client)
	if _, err := manager.Rotate(context.Background(), SearchVersionKey()); err != nil {
		t.Fatalf("rotate search namespace: %v", err)
	}
	projection.search = []querypostgres.SearchResult{}
	results, err = store.SearchTrainRuns(context.Background(), request)
	if err != nil || len(results) != 1 || results[0].FareAmountMinor != 1300 {
		t.Fatalf("SearchTrainRuns(fallback) = %+v, %v", results, err)
	}
	if source.searchCalls.Load() != 1 {
		t.Fatalf("empty projection source fallback calls = %d, want 1", source.searchCalls.Load())
	}
}

func TestAvailabilityHintIsSharedAcrossReplicasAndRotationReobservesPostgres(t *testing.T) {
	client := openCacheRedis(t)
	trainRunID := uuid.NewString()
	request := querypostgres.AvailabilityRequest{
		TrainRunID: trainRunID, OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard",
	}
	source := &cacheSourceFake{availability: availabilityFixture(trainRunID, 7)}
	replicaA := newCacheStore(t, source, source, client)
	replicaB := newCacheStore(t, source, source, client)
	first, err := replicaA.Availability(context.Background(), request)
	if err != nil || first.AvailableSeats != 7 {
		t.Fatalf("Availability(first) = %+v, %v", first, err)
	}
	source.mu.Lock()
	source.availability = availabilityFixture(trainRunID, 2)
	source.mu.Unlock()
	warm, err := replicaB.Availability(context.Background(), request)
	if err != nil || warm.AvailableSeats != 7 {
		t.Fatalf("Availability(shared warm) = %+v, %v", warm, err)
	}
	manager, _ := NewSecureVersionManager(client)
	versionKey, _ := AvailabilityVersionKey(trainRunID)
	if _, err := manager.Rotate(context.Background(), versionKey); err != nil {
		t.Fatalf("rotate availability namespace: %v", err)
	}
	refreshed, err := replicaB.Availability(context.Background(), request)
	if err != nil || refreshed.AvailableSeats != 2 {
		t.Fatalf("Availability(rotated) = %+v, %v", refreshed, err)
	}
	if source.availabilityCalls.Load() != 2 {
		t.Fatalf("availability source calls = %d, want 2", source.availabilityCalls.Load())
	}
}

func TestAvailabilityHintCanonicalizesUppercaseTrainRunIDForWarmHit(t *testing.T) {
	client := openCacheRedis(t)
	canonicalTrainRunID := uuid.NewString()
	request := querypostgres.AvailabilityRequest{
		TrainRunID: strings.ToUpper(canonicalTrainRunID),
		OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard",
	}
	source := &cacheSourceFake{availability: availabilityFixture(canonicalTrainRunID, 7)}
	store := newCacheStore(t, source, source, client)
	for attempt := 0; attempt < 2; attempt++ {
		availability, err := store.Availability(context.Background(), request)
		if err != nil || availability.AvailableSeats != 7 {
			t.Fatalf("Availability(attempt %d) = %+v, %v", attempt+1, availability, err)
		}
	}
	if source.availabilityCalls.Load() != 1 {
		t.Fatalf("uppercase UUID source calls = %d, want one warm cache fill", source.availabilityCalls.Load())
	}
}

func TestAvailabilityHintRejectsValidJSONFromDifferentJourneyScope(t *testing.T) {
	client := openCacheRedis(t)
	trainRunID := uuid.NewString()
	request := querypostgres.AvailabilityRequest{
		TrainRunID: trainRunID, OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard",
	}
	source := &cacheSourceFake{availability: availabilityFixture(trainRunID, 7)}
	store := newCacheStore(t, source, source, client)
	version, err := store.versions.GetOrCreate(context.Background(), "cache:availability:version:"+trainRunID)
	if err != nil {
		t.Fatalf("GetOrCreate() error = %v", err)
	}
	key, err := AvailabilityDataKey(version, trainRunID, "TPE", "KHH", "standard")
	if err != nil {
		t.Fatalf("AvailabilityDataKey() error = %v", err)
	}
	payload, valid := availabilityCachePayload(
		availabilityFixture(trainRunID, 99), request,
		time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
	)
	if !valid {
		t.Fatal("availabilityCachePayload() rejected valid fixture")
	}
	payload.OriginCode = "TXG"
	if err := store.writeJSON(context.Background(), key, payload, store.availabilityTTL); err != nil {
		t.Fatalf("seed cross-scope cache payload: %v", err)
	}
	availability, err := store.Availability(context.Background(), request)
	if err != nil || availability.AvailableSeats != 7 {
		t.Fatalf("Availability(cross-scope cache) = %+v, %v", availability, err)
	}
	if source.availabilityCalls.Load() != 1 {
		t.Fatalf("cross-scope cache source calls = %d, want 1", source.availabilityCalls.Load())
	}
}

func TestAvailabilityBatchCoalescesConcurrentIdenticalMisses(t *testing.T) {
	client := openCacheRedis(t)
	requests := []querypostgres.AvailabilityRequest{
		{TrainRunID: uuid.NewString(), OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard"},
		{TrainRunID: uuid.NewString(), OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard"},
	}
	source := &cacheBatchSourceFake{entered: make(chan struct{}), release: make(chan struct{})}
	store := newCacheStore(t, source, source, client)
	const callers = 32
	ready := make(chan struct{}, callers)
	start := make(chan struct{})
	errorsSeen := make(chan error, callers)
	var wait sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ready <- struct{}{}
			<-start
			results, err := store.AvailabilityBatch(context.Background(), requests)
			if err == nil && (len(results) != 2 || results[0].TrainRunID != requests[0].TrainRunID ||
				results[1].TrainRunID != requests[1].TrainRunID) {
				err = querypostgres.ErrPersistence
			}
			errorsSeen <- err
		}()
	}
	for caller := 0; caller < callers; caller++ {
		<-ready
	}
	close(start)
	<-source.entered
	close(source.release)
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("AvailabilityBatch(concurrent) error = %v", err)
		}
	}
	if source.batchCalls.Load() != 1 {
		t.Fatalf("concurrent availability batch source calls = %d, want 1", source.batchCalls.Load())
	}
}

func TestCachePolicyDisablesRedisPathsWithoutChangingSourceAuthority(t *testing.T) {
	client := openCacheRedis(t)
	trainRunID := uuid.NewString()
	source := &cacheSourceFake{
		stations:     stationFixture(t),
		search:       []querypostgres.SearchResult{searchResultFixture(1300)},
		availability: availabilityFixture(trainRunID, 4),
	}
	projection := &cacheSourceFake{search: []querypostgres.SearchResult{searchResultFixture(1200)}}
	store := newCacheStore(t, source, projection, client)
	store, err := store.WithPolicy(Policy{
		StationEnabled: false, SearchEnabled: false, SearchFallbackEnabled: false,
		AvailabilityEnabled: false, AvailabilityMaxStale: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("WithPolicy() error = %v", err)
	}
	if _, err := store.ListStations(context.Background()); err != nil {
		t.Fatalf("ListStations(disabled) error = %v", err)
	}
	search, err := store.SearchTrainRuns(context.Background(), searchFixture())
	if err != nil || len(search) != 1 || search[0].FareAmountMinor != 1200 {
		t.Fatalf("SearchTrainRuns(disabled cache) = %+v, %v", search, err)
	}
	availability, err := store.Availability(context.Background(), querypostgres.AvailabilityRequest{
		TrainRunID: trainRunID, OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard",
	})
	if err != nil || availability.AvailableSeats != 4 {
		t.Fatalf("Availability(disabled) = %+v, %v", availability, err)
	}
	if source.stationCalls.Load() != 1 || source.availabilityCalls.Load() != 1 || source.searchCalls.Load() != 0 ||
		projection.searchCalls.Load() != 1 {
		t.Fatalf(
			"disabled policy source/projection calls = station %d availability %d source-search %d projection-search %d",
			source.stationCalls.Load(), source.availabilityCalls.Load(), source.searchCalls.Load(), projection.searchCalls.Load(),
		)
	}
	if size, err := client.DBSize(context.Background()).Result(); err != nil || size != 0 {
		t.Fatalf("disabled policy Redis size = %d, %v, want 0", size, err)
	}
}

func TestAvailabilityMaxStaleRefreshesBeforeRedisTTLExpires(t *testing.T) {
	client := openCacheRedis(t)
	trainRunID := uuid.NewString()
	request := querypostgres.AvailabilityRequest{
		TrainRunID: trainRunID, OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard",
	}
	source := &cacheSourceFake{availability: availabilityFixture(trainRunID, 7)}
	testClock := clock.NewDeterministic(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	store := newCacheStoreWithClock(t, source, source, client, testClock)
	store, err := store.WithPolicy(Policy{
		StationEnabled: true, SearchEnabled: true, SearchFallbackEnabled: true,
		AvailabilityEnabled: true, AvailabilityMaxStale: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("WithPolicy() error = %v", err)
	}
	if _, err := store.Availability(context.Background(), request); err != nil {
		t.Fatalf("Availability(first) error = %v", err)
	}
	source.mu.Lock()
	source.availability = availabilityFixture(trainRunID, 2)
	source.mu.Unlock()
	testClock.Advance(6 * time.Second)
	refreshed, err := store.Availability(context.Background(), request)
	if err != nil || refreshed.AvailableSeats != 2 {
		t.Fatalf("Availability(max stale) = %+v, %v", refreshed, err)
	}
	if source.availabilityCalls.Load() != 2 {
		t.Fatalf("max-stale source calls = %d, want 2", source.availabilityCalls.Load())
	}
}

func TestCacheStoreSkipsOversizedPayloadWithoutFailingSourceRead(t *testing.T) {
	client := openCacheRedis(t)
	stationCode, err := domain.NewStationCode("TPE")
	if err != nil {
		t.Fatalf("NewStationCode() error = %v", err)
	}
	source := &cacheSourceFake{stations: []querypostgres.Station{{
		ID: uuid.NewString(), Code: stationCode,
		Name: strings.Repeat("x", MaxCachePayloadBytes), Timezone: "Asia/Taipei",
	}}}
	store := newCacheStore(t, source, source, client)
	stations, err := store.ListStations(context.Background())
	if err != nil || len(stations) != 1 {
		t.Fatalf("ListStations(oversized) = %d, %v", len(stations), err)
	}
	if size, err := client.DBSize(context.Background()).Result(); err != nil || size != 1 {
		t.Fatalf("oversized cache DB size = %d, %v, want version key only", size, err)
	}
}

func openCacheRedis(t *testing.T) *redis.Client {
	t.Helper()
	address := os.Getenv("TEST_REDIS_ADDR")
	if address == "" {
		address = "127.0.0.1:56379"
	}
	client := redis.NewClient(&redis.Options{Addr: address, DB: 12})
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		t.Skipf("Redis integration dependency unavailable: %v", err)
	}
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		client.Close()
		t.Fatalf("flush Redis test database: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newCacheStore(t *testing.T, source SourceStore, projection ProjectionSearch, client *redis.Client) *Store {
	return newCacheStoreWithClock(
		t,
		source,
		projection,
		client,
		clock.NewDeterministic(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)),
	)
}

func newCacheStoreWithClock(
	t *testing.T,
	source SourceStore,
	projection ProjectionSearch,
	client *redis.Client,
	testClock *clock.DeterministicClock,
) *Store {
	t.Helper()
	versions, err := NewSecureVersionManager(client)
	if err != nil {
		t.Fatalf("NewSecureVersionManager() error = %v", err)
	}
	stationTTL, _ := NewTTLPolicy(time.Minute, time.Second, rand.Reader)
	searchTTL, _ := NewTTLPolicy(time.Minute, time.Second, rand.Reader)
	availabilityTTL, _ := NewTTLPolicy(10*time.Second, time.Second, rand.Reader)
	store, err := NewStore(
		source, projection, client, versions,
		testClock,
		stationTTL, searchTTL, availabilityTTL,
	)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store
}

type cacheSourceFake struct {
	mu                sync.Mutex
	stations          []querypostgres.Station
	search            []querypostgres.SearchResult
	availability      querypostgres.Availability
	stationCalls      atomic.Int64
	searchCalls       atomic.Int64
	availabilityCalls atomic.Int64
}

type cacheBatchSourceFake struct {
	batchCalls atomic.Int64
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
}

func (source *cacheBatchSourceFake) ListStations(context.Context) ([]querypostgres.Station, error) {
	return nil, nil
}

func (source *cacheBatchSourceFake) SearchTrainRuns(
	context.Context,
	querypostgres.SearchRequest,
) ([]querypostgres.SearchResult, error) {
	return nil, nil
}

func (source *cacheBatchSourceFake) Availability(
	_ context.Context,
	request querypostgres.AvailabilityRequest,
) (querypostgres.Availability, error) {
	return availabilityFixture(request.TrainRunID, 7), nil
}

func (source *cacheBatchSourceFake) AvailabilityBatch(
	_ context.Context,
	requests []querypostgres.AvailabilityRequest,
) ([]querypostgres.Availability, error) {
	source.batchCalls.Add(1)
	source.once.Do(func() { close(source.entered) })
	<-source.release
	results := make([]querypostgres.Availability, 0, len(requests))
	for _, request := range requests {
		results = append(results, availabilityFixture(request.TrainRunID, 7))
	}
	return results, nil
}

func (source *cacheSourceFake) ListStations(context.Context) ([]querypostgres.Station, error) {
	source.stationCalls.Add(1)
	source.mu.Lock()
	defer source.mu.Unlock()
	return append([]querypostgres.Station(nil), source.stations...), nil
}

func (source *cacheSourceFake) SearchTrainRuns(context.Context, querypostgres.SearchRequest) ([]querypostgres.SearchResult, error) {
	source.searchCalls.Add(1)
	source.mu.Lock()
	defer source.mu.Unlock()
	return append([]querypostgres.SearchResult(nil), source.search...), nil
}

func (source *cacheSourceFake) Availability(context.Context, querypostgres.AvailabilityRequest) (querypostgres.Availability, error) {
	source.availabilityCalls.Add(1)
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.availability, nil
}

func stationFixture(t *testing.T) []querypostgres.Station {
	t.Helper()
	code, err := domain.NewStationCode("TPE")
	if err != nil {
		t.Fatalf("NewStationCode() error = %v", err)
	}
	return []querypostgres.Station{{ID: uuid.NewString(), Code: code, Name: "Taipei", Timezone: "Asia/Taipei"}}
}

func searchFixture() querypostgres.SearchRequest {
	return querypostgres.SearchRequest{
		OriginCode: "TPE", DestinationCode: "KHH", ServiceDate: time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		SeatClass: "standard", Page: 1, PageSize: 20, Sort: "departure_asc",
	}
}

func searchResultFixture(fare int64) querypostgres.SearchResult {
	return querypostgres.SearchResult{
		TrainRunID: uuid.NewString(), TrainCode: "TR200", DepartureAt: time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC),
		ArrivalAt: time.Date(2026, 7, 22, 3, 0, 0, 0, time.UTC), SeatClass: domain.SeatClassStandard,
		FareAmountMinor: fare, Currency: "TWD",
	}
}

func availabilityFixture(trainRunID string, seats int64) querypostgres.Availability {
	return querypostgres.Availability{
		TrainRunID: trainRunID, TrainCode: "TR200", FromStopIndex: 0, ToStopIndex: 2, SegmentCount: 2,
		DepartureAt: time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC),
		ArrivalAt:   time.Date(2026, 7, 22, 3, 0, 0, 0, time.UTC),
		SeatClass:   domain.SeatClassStandard, AvailableSeats: seats, FareAmountMinor: 1200, Currency: "TWD",
	}
}
