package cache

import (
	"context"
	"crypto/rand"
	"os"
	"runtime"
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
			}
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
		clock.NewDeterministic(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)),
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
		TrainRunID: trainRunID, TrainCode: "TR200", FromStopIndex: 0, ToStopIndex: 2,
		DepartureAt: time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC),
		ArrivalAt:   time.Date(2026, 7, 22, 3, 0, 0, 0, time.UTC),
		SeatClass:   domain.SeatClassStandard, AvailableSeats: seats, FareAmountMinor: 1200, Currency: "TWD",
	}
}
