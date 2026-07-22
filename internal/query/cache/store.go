package cache

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/redis/go-redis/v9"
)

type SourceStore interface {
	ListStations(context.Context) ([]querypostgres.Station, error)
	SearchTrainRuns(context.Context, querypostgres.SearchRequest) ([]querypostgres.SearchResult, error)
	Availability(context.Context, querypostgres.AvailabilityRequest) (querypostgres.Availability, error)
}

type ProjectionSearch interface {
	SearchTrainRuns(context.Context, querypostgres.SearchRequest) ([]querypostgres.SearchResult, error)
}

type availabilityBatchSource interface {
	AvailabilityBatch(context.Context, []querypostgres.AvailabilityRequest) ([]querypostgres.Availability, error)
}

type timeSource interface {
	Now() time.Time
}

type Metrics interface {
	RecordCacheRequest(cacheType, operation, result, reason string)
	RecordCacheFill(cacheType, result, reason string, duration time.Duration, shared bool)
	RecordFallback(reason string)
}

type Store struct {
	source          SourceStore
	projection      ProjectionSearch
	client          redis.UniversalClient
	versions        *VersionManager
	clock           timeSource
	stationTTL      *TTLPolicy
	searchTTL       *TTLPolicy
	availabilityTTL *TTLPolicy
	coalescer       Coalescer
	metrics         Metrics
}

func (store *Store) WithMetrics(metrics Metrics) *Store {
	if store != nil {
		store.metrics = metrics
	}
	return store
}

type cachedStation struct {
	ID       string `json:"id"`
	Code     string `json:"code"`
	Name     string `json:"name"`
	Timezone string `json:"timezone"`
}

type cachedAvailability struct {
	Value      querypostgres.Availability `json:"value"`
	ObservedAt time.Time                  `json:"observed_at"`
	Source     string                     `json:"source"`
}

type fillOutcome struct {
	value    any
	writeErr error
}

func NewStore(
	source SourceStore,
	projection ProjectionSearch,
	client redis.UniversalClient,
	versions *VersionManager,
	clock timeSource,
	stationTTL *TTLPolicy,
	searchTTL *TTLPolicy,
	availabilityTTL *TTLPolicy,
) (*Store, error) {
	if source == nil || projection == nil || client == nil || versions == nil || clock == nil ||
		stationTTL == nil || searchTTL == nil || availabilityTTL == nil {
		return nil, errors.New("read cache store configuration invalid")
	}
	return &Store{
		source: source, projection: projection, client: client, versions: versions, clock: clock,
		stationTTL: stationTTL, searchTTL: searchTTL, availabilityTTL: availabilityTTL,
	}, nil
}

func (store *Store) ListStations(ctx context.Context) ([]querypostgres.Station, error) {
	version, err := store.versions.GetOrCreate(ctx, StationVersionKey())
	if err != nil {
		store.recordRequest("stations", "version_get", "failure", "redis")
		store.recordFallback("redis")
		return store.source.ListStations(ctx)
	}
	key, err := StationDataKey(version)
	if err != nil {
		return nil, err
	}
	if stations, hit := store.readStations(ctx, key); hit {
		store.recordRequest("stations", "read", "hit", "none")
		return stations, nil
	}
	store.recordRequest("stations", "read", "miss", "none")
	started := time.Now()
	value, err, shared := store.coalescer.Do(ctx, key, func(fillContext context.Context) (any, error) {
		if stations, hit := store.readStations(fillContext, key); hit {
			return fillOutcome{value: stations}, nil
		}
		stations, sourceErr := store.source.ListStations(fillContext)
		if sourceErr != nil {
			return nil, sourceErr
		}
		sort.Slice(stations, func(i, j int) bool { return stations[i].Code.String() < stations[j].Code.String() })
		payload := make([]cachedStation, 0, len(stations))
		for _, station := range stations {
			payload = append(payload, cachedStation{
				ID: station.ID, Code: station.Code.String(), Name: station.Name, Timezone: station.Timezone,
			})
		}
		return fillOutcome{
			value: stations, writeErr: store.writeJSON(fillContext, key, payload, store.stationTTL),
		}, nil
	})
	if err != nil {
		store.recordFill("stations", "failure", "database", time.Since(started), shared)
		return nil, err
	}
	outcome := value.(fillOutcome)
	if outcome.writeErr != nil {
		store.recordFill("stations", "failure", "redis", time.Since(started), shared)
	} else {
		store.recordFill("stations", "success", "none", time.Since(started), shared)
	}
	return outcome.value.([]querypostgres.Station), nil
}

func (store *Store) SearchTrainRuns(
	ctx context.Context,
	request querypostgres.SearchRequest,
) ([]querypostgres.SearchResult, error) {
	normalized, err := querypostgres.NormalizeSearch(request)
	if err != nil {
		return nil, err
	}
	version, err := store.versions.GetOrCreate(ctx, SearchVersionKey())
	if err != nil {
		store.recordRequest("train_search", "version_get", "failure", "redis")
		store.recordFallback("redis")
		return store.source.SearchTrainRuns(ctx, request)
	}
	key, err := SearchDataKey(version, SearchQueryHash(normalized))
	if err != nil {
		return nil, err
	}
	if results, hit := store.readSearch(ctx, key); hit {
		store.recordRequest("train_search", "read", "hit", "none")
		return results, nil
	}
	store.recordRequest("train_search", "read", "miss", "none")
	started := time.Now()
	value, err, shared := store.coalescer.Do(ctx, key, func(fillContext context.Context) (any, error) {
		if results, hit := store.readSearch(fillContext, key); hit {
			return fillOutcome{value: results}, nil
		}
		results, projectionErr := store.projection.SearchTrainRuns(fillContext, request)
		if projectionErr != nil || len(results) == 0 {
			store.recordFallback("projection")
			var sourceErr error
			results, sourceErr = store.source.SearchTrainRuns(fillContext, request)
			if sourceErr != nil {
				return nil, sourceErr
			}
		}
		return fillOutcome{
			value: results, writeErr: store.writeJSON(fillContext, key, results, store.searchTTL),
		}, nil
	})
	if err != nil {
		store.recordFill("train_search", "failure", "database", time.Since(started), shared)
		return nil, err
	}
	outcome := value.(fillOutcome)
	if outcome.writeErr != nil {
		store.recordFill("train_search", "failure", "redis", time.Since(started), shared)
	} else {
		store.recordFill("train_search", "success", "none", time.Since(started), shared)
	}
	return outcome.value.([]querypostgres.SearchResult), nil
}

func (store *Store) Availability(
	ctx context.Context,
	request querypostgres.AvailabilityRequest,
) (querypostgres.Availability, error) {
	versionKey, err := AvailabilityVersionKey(request.TrainRunID)
	if err != nil {
		return querypostgres.Availability{}, querypostgres.ErrInvalidQuery
	}
	version, err := store.versions.GetOrCreate(ctx, versionKey)
	if err != nil {
		store.recordRequest("availability", "version_get", "failure", "redis")
		store.recordFallback("redis")
		return store.source.Availability(ctx, request)
	}
	key, err := AvailabilityDataKey(
		version,
		request.TrainRunID,
		request.OriginCode,
		request.DestinationCode,
		request.SeatClass,
	)
	if err != nil {
		return querypostgres.Availability{}, querypostgres.ErrInvalidJourney
	}
	if availability, hit := store.readAvailability(ctx, key, request); hit {
		store.recordRequest("availability", "read", "hit", "none")
		return availability, nil
	}
	store.recordRequest("availability", "read", "miss", "none")
	started := time.Now()
	value, err, shared := store.coalescer.Do(ctx, key, func(fillContext context.Context) (any, error) {
		if availability, hit := store.readAvailability(fillContext, key, request); hit {
			return fillOutcome{value: availability}, nil
		}
		availability, sourceErr := store.source.Availability(fillContext, request)
		if sourceErr != nil {
			return nil, sourceErr
		}
		if availability.AvailableSeats < 0 {
			return nil, querypostgres.ErrPersistence
		}
		payload := cachedAvailability{Value: availability, ObservedAt: store.clock.Now().UTC(), Source: "postgres"}
		return fillOutcome{
			value: availability, writeErr: store.writeJSON(fillContext, key, payload, store.availabilityTTL),
		}, nil
	})
	if err != nil {
		store.recordFill("availability", "failure", "database", time.Since(started), shared)
		return querypostgres.Availability{}, err
	}
	outcome := value.(fillOutcome)
	if outcome.writeErr != nil {
		store.recordFill("availability", "failure", "redis", time.Since(started), shared)
	} else {
		store.recordFill("availability", "success", "none", time.Since(started), shared)
	}
	return outcome.value.(querypostgres.Availability), nil
}

func (store *Store) AvailabilityBatch(
	ctx context.Context,
	requests []querypostgres.AvailabilityRequest,
) ([]querypostgres.Availability, error) {
	if len(requests) == 0 || len(requests) > querypostgres.MaxPageSize {
		return nil, querypostgres.ErrInvalidQuery
	}
	results := make([]querypostgres.Availability, len(requests))
	keys := make([]string, len(requests))
	misses := make([]querypostgres.AvailabilityRequest, 0)
	missIndexes := make([]int, 0)
	for index, request := range requests {
		versionKey, err := AvailabilityVersionKey(request.TrainRunID)
		if err != nil {
			return nil, querypostgres.ErrInvalidQuery
		}
		version, versionErr := store.versions.GetOrCreate(ctx, versionKey)
		if versionErr == nil {
			keys[index], err = AvailabilityDataKey(
				version, request.TrainRunID, request.OriginCode, request.DestinationCode, request.SeatClass,
			)
			if err != nil {
				return nil, querypostgres.ErrInvalidJourney
			}
			if availability, hit := store.readAvailability(ctx, keys[index], request); hit {
				store.recordRequest("availability", "read", "hit", "none")
				results[index] = availability
				continue
			}
			store.recordRequest("availability", "read", "miss", "none")
		} else {
			store.recordRequest("availability", "version_get", "failure", "redis")
			store.recordFallback("redis")
		}
		misses = append(misses, request)
		missIndexes = append(missIndexes, index)
	}
	if len(misses) == 0 {
		return results, nil
	}
	loaded := make([]querypostgres.Availability, 0, len(misses))
	if batch, ok := store.source.(availabilityBatchSource); ok {
		var err error
		loaded, err = batch.AvailabilityBatch(ctx, misses)
		if err != nil {
			return nil, err
		}
	} else {
		for _, request := range misses {
			availability, err := store.source.Availability(ctx, request)
			if err != nil {
				return nil, err
			}
			loaded = append(loaded, availability)
		}
	}
	if len(loaded) != len(misses) {
		return nil, querypostgres.ErrPersistence
	}
	for offset, availability := range loaded {
		index := missIndexes[offset]
		if availability.AvailableSeats < 0 {
			return nil, querypostgres.ErrPersistence
		}
		results[index] = availability
		if keys[index] != "" {
			payload := cachedAvailability{Value: availability, ObservedAt: store.clock.Now().UTC(), Source: "postgres"}
			started := time.Now()
			if err := store.writeJSON(ctx, keys[index], payload, store.availabilityTTL); err != nil {
				store.recordFill("availability", "failure", "redis", time.Since(started), false)
			} else {
				store.recordFill("availability", "success", "none", time.Since(started), false)
			}
		}
	}
	return results, nil
}

func (store *Store) readStations(ctx context.Context, key string) ([]querypostgres.Station, bool) {
	var cached []cachedStation
	if !store.readJSON(ctx, key, &cached) {
		return nil, false
	}
	stations := make([]querypostgres.Station, 0, len(cached))
	for _, item := range cached {
		code, err := domain.NewStationCode(item.Code)
		if err != nil || item.ID == "" || item.Name == "" || item.Timezone == "" {
			return nil, false
		}
		stations = append(stations, querypostgres.Station{
			ID: item.ID, Code: code, Name: item.Name, Timezone: item.Timezone,
		})
	}
	return stations, true
}

func (store *Store) readSearch(ctx context.Context, key string) ([]querypostgres.SearchResult, bool) {
	var results []querypostgres.SearchResult
	if !store.readJSON(ctx, key, &results) || results == nil {
		return nil, false
	}
	return results, true
}

func (store *Store) readAvailability(
	ctx context.Context,
	key string,
	request querypostgres.AvailabilityRequest,
) (querypostgres.Availability, bool) {
	var cached cachedAvailability
	if !store.readJSON(ctx, key, &cached) || cached.Source != "postgres" || cached.ObservedAt.IsZero() ||
		cached.Value.AvailableSeats < 0 || cached.Value.TrainRunID != request.TrainRunID {
		return querypostgres.Availability{}, false
	}
	return cached.Value, true
}

func (store *Store) readJSON(ctx context.Context, key string, target any) bool {
	encoded, err := store.client.Get(ctx, key).Bytes()
	return err == nil && json.Unmarshal(encoded, target) == nil
}

func (store *Store) writeJSON(ctx context.Context, key string, value any, policy *TTLPolicy) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	ttl, err := policy.Next()
	if err != nil {
		return err
	}
	return store.client.Set(ctx, key, encoded, ttl).Err()
}

func (store *Store) recordRequest(cacheType, operation, result, reason string) {
	if store.metrics != nil {
		store.metrics.RecordCacheRequest(cacheType, operation, result, reason)
	}
}

func (store *Store) recordFill(cacheType, result, reason string, duration time.Duration, shared bool) {
	if store.metrics != nil {
		store.metrics.RecordCacheFill(cacheType, result, reason, duration, shared)
	}
}

func (store *Store) recordFallback(reason string) {
	if store.metrics != nil {
		store.metrics.RecordFallback(reason)
	}
}

var _ SourceStore = (*Store)(nil)
