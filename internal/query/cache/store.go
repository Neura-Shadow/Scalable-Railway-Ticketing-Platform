package cache

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/google/uuid"
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
	source                SourceStore
	projection            ProjectionSearch
	client                redis.UniversalClient
	versions              *VersionManager
	clock                 timeSource
	stationTTL            *TTLPolicy
	searchTTL             *TTLPolicy
	availabilityTTL       *TTLPolicy
	coalescer             Coalescer
	metrics               Metrics
	stationEnabled        bool
	searchEnabled         bool
	searchFallbackEnabled bool
	availabilityEnabled   bool
	availabilityMaxStale  time.Duration
}

type Policy struct {
	StationEnabled        bool
	SearchEnabled         bool
	SearchFallbackEnabled bool
	AvailabilityEnabled   bool
	AvailabilityMaxStale  time.Duration
}

func (store *Store) WithMetrics(metrics Metrics) *Store {
	if store != nil {
		store.metrics = metrics
	}
	return store
}

func (store *Store) WithPolicy(policy Policy) (*Store, error) {
	if store == nil || policy.AvailabilityMaxStale <= 0 || policy.AvailabilityMaxStale > MaxCacheTTL {
		return nil, errors.New("read cache policy invalid")
	}
	store.stationEnabled = policy.StationEnabled
	store.searchEnabled = policy.SearchEnabled
	store.searchFallbackEnabled = policy.SearchFallbackEnabled
	store.availabilityEnabled = policy.AvailabilityEnabled
	store.availabilityMaxStale = policy.AvailabilityMaxStale
	return store, nil
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

type cacheReadStatus string

const (
	cacheReadHit          cacheReadStatus = "hit"
	cacheReadMiss         cacheReadStatus = "miss"
	cacheReadRedisFailure cacheReadStatus = "redis_failure"
	cacheReadInvalid      cacheReadStatus = "invalid"
)

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
		stationEnabled: true, searchEnabled: true, searchFallbackEnabled: true,
		availabilityEnabled: true, availabilityMaxStale: availabilityTTL.base,
	}, nil
}

func (store *Store) ListStations(ctx context.Context) ([]querypostgres.Station, error) {
	if !store.stationEnabled {
		return store.source.ListStations(ctx)
	}
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
	if stations, status := store.readStations(ctx, key); status == cacheReadHit {
		store.recordReadStatus("stations", status)
		return stations, nil
	} else {
		store.recordReadStatus("stations", status)
	}
	started := time.Now()
	value, err, shared := store.coalescer.Do(ctx, key, func(fillContext context.Context) (any, error) {
		if stations, status := store.readStations(fillContext, key); status == cacheReadHit {
			return fillOutcome{value: stations}, nil
		} else if status == cacheReadRedisFailure || status == cacheReadInvalid {
			store.recordReadStatus("stations", status)
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
	if !store.searchEnabled {
		return store.searchProjectionOrSource(ctx, request)
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
	if results, status := store.readSearch(ctx, key); status == cacheReadHit {
		store.recordReadStatus("train_search", status)
		return results, nil
	} else {
		store.recordReadStatus("train_search", status)
	}
	started := time.Now()
	value, err, shared := store.coalescer.Do(ctx, key, func(fillContext context.Context) (any, error) {
		if results, status := store.readSearch(fillContext, key); status == cacheReadHit {
			return fillOutcome{value: results}, nil
		} else if status == cacheReadRedisFailure || status == cacheReadInvalid {
			store.recordReadStatus("train_search", status)
		}
		results, projectionErr := store.searchProjectionOrSource(fillContext, request)
		if projectionErr != nil {
			return nil, projectionErr
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
	if !store.availabilityEnabled {
		return store.source.Availability(ctx, request)
	}
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
	if availability, status := store.readAvailability(ctx, key, request); status == cacheReadHit {
		store.recordReadStatus("availability", status)
		return availability, nil
	} else {
		store.recordReadStatus("availability", status)
	}
	started := time.Now()
	value, err, shared := store.coalescer.Do(ctx, key, func(fillContext context.Context) (any, error) {
		if availability, status := store.readAvailability(fillContext, key, request); status == cacheReadHit {
			return fillOutcome{value: availability}, nil
		} else if status == cacheReadRedisFailure || status == cacheReadInvalid {
			store.recordReadStatus("availability", status)
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
	if !store.availabilityEnabled {
		if batch, ok := store.source.(availabilityBatchSource); ok {
			return batch.AvailabilityBatch(ctx, requests)
		}
		results := make([]querypostgres.Availability, 0, len(requests))
		for _, request := range requests {
			availability, err := store.source.Availability(ctx, request)
			if err != nil {
				return nil, err
			}
			results = append(results, availability)
		}
		return results, nil
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
			if availability, status := store.readAvailability(ctx, keys[index], request); status == cacheReadHit {
				store.recordReadStatus("availability", status)
				results[index] = availability
				continue
			} else {
				store.recordReadStatus("availability", status)
			}
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

func (store *Store) readStations(ctx context.Context, key string) ([]querypostgres.Station, cacheReadStatus) {
	var cached []cachedStation
	status := store.readJSON(ctx, key, &cached)
	if status != cacheReadHit {
		return nil, status
	}
	if cached == nil {
		return nil, cacheReadInvalid
	}
	stations := make([]querypostgres.Station, 0, len(cached))
	for _, item := range cached {
		code, err := domain.NewStationCode(item.Code)
		if err != nil || item.ID == "" || item.Name == "" || item.Timezone == "" {
			return nil, cacheReadInvalid
		}
		stations = append(stations, querypostgres.Station{
			ID: item.ID, Code: code, Name: item.Name, Timezone: item.Timezone,
		})
	}
	return stations, cacheReadHit
}

func (store *Store) readSearch(ctx context.Context, key string) ([]querypostgres.SearchResult, cacheReadStatus) {
	var results []querypostgres.SearchResult
	status := store.readJSON(ctx, key, &results)
	if status != cacheReadHit {
		return nil, status
	}
	if results == nil {
		return nil, cacheReadInvalid
	}
	return results, cacheReadHit
}

func (store *Store) readAvailability(
	ctx context.Context,
	key string,
	request querypostgres.AvailabilityRequest,
) (querypostgres.Availability, cacheReadStatus) {
	var cached cachedAvailability
	status := store.readJSON(ctx, key, &cached)
	if status != cacheReadHit {
		return querypostgres.Availability{}, status
	}
	requestTrainRunID, requestErr := uuid.Parse(request.TrainRunID)
	cachedTrainRunID, cachedErr := uuid.Parse(cached.Value.TrainRunID)
	if cached.Source != "postgres" || cached.ObservedAt.IsZero() ||
		cached.Value.AvailableSeats < 0 || requestErr != nil || cachedErr != nil ||
		requestTrainRunID == uuid.Nil || cachedTrainRunID != requestTrainRunID {
		return querypostgres.Availability{}, cacheReadInvalid
	}
	age := store.clock.Now().UTC().Sub(cached.ObservedAt.UTC())
	if age < 0 || age > store.availabilityMaxStale {
		return querypostgres.Availability{}, cacheReadMiss
	}
	return cached.Value, cacheReadHit
}

func (store *Store) searchProjectionOrSource(
	ctx context.Context,
	request querypostgres.SearchRequest,
) ([]querypostgres.SearchResult, error) {
	results, projectionErr := store.projection.SearchTrainRuns(ctx, request)
	if projectionErr == nil && (len(results) > 0 || !store.searchFallbackEnabled) {
		return results, nil
	}
	if !store.searchFallbackEnabled {
		return nil, projectionErr
	}
	store.recordFallback("projection")
	return store.source.SearchTrainRuns(ctx, request)
}

func (store *Store) readJSON(ctx context.Context, key string, target any) cacheReadStatus {
	encoded, err := store.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return cacheReadMiss
	}
	if err != nil {
		return cacheReadRedisFailure
	}
	if json.Unmarshal(encoded, target) != nil {
		return cacheReadInvalid
	}
	return cacheReadHit
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

func (store *Store) recordReadStatus(cacheType string, status cacheReadStatus) {
	switch status {
	case cacheReadHit:
		store.recordRequest(cacheType, "read", "hit", "none")
	case cacheReadMiss:
		store.recordRequest(cacheType, "read", "miss", "none")
	case cacheReadInvalid:
		store.recordRequest(cacheType, "read", "failure", "invalid")
		store.recordFallback("redis")
	default:
		store.recordRequest(cacheType, "read", "failure", "redis")
		store.recordFallback("redis")
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
