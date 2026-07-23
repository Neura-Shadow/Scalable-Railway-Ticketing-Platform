package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

type availabilityAssignmentSource interface {
	AvailabilityAssignmentGeneration(context.Context, string) (int64, error)
}

type timeSource interface {
	Now() time.Time
}

type Metrics interface {
	RecordCacheRequest(cacheType, operation, result, reason string)
	RecordCacheFill(cacheType, result, reason string, duration time.Duration, shared bool)
	RecordCacheSingleflightShared(cacheType string)
	RecordCacheSourceQuery(cacheType string)
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
	Value                querypostgres.Availability `json:"value"`
	AssignmentGeneration int64                      `json:"assignment_generation"`
	OriginCode           string                     `json:"origin_code"`
	DestinationCode      string                     `json:"destination_code"`
	SeatClass            string                     `json:"seat_class"`
	ObservedAt           time.Time                  `json:"observed_at"`
	Source               string                     `json:"source"`
}

type cachedSearch struct {
	Schema          string                       `json:"schema"`
	OriginCode      string                       `json:"origin_code"`
	DestinationCode string                       `json:"destination_code"`
	ServiceDate     string                       `json:"service_date"`
	SeatClass       string                       `json:"seat_class"`
	Page            int                          `json:"page"`
	PageSize        int                          `json:"page_size"`
	Sort            string                       `json:"sort"`
	Results         []querypostgres.SearchResult `json:"results"`
}

type fillOutcome struct {
	value    any
	writeErr error
}

type cacheReadStatus string

const (
	MaxCachePayloadBytes                  = 1 << 20
	cacheReadHit          cacheReadStatus = "hit"
	cacheReadMiss         cacheReadStatus = "miss"
	cacheReadRedisFailure cacheReadStatus = "redis_failure"
	cacheReadInvalid      cacheReadStatus = "invalid"
)

var boundedCacheGetScript = redis.NewScript(`
local value = redis.call('GET', KEYS[1])
if not value then
  return {0, ''}
end
if string.len(value) > tonumber(ARGV[1]) then
  return {2, ''}
end
return {1, value}
`)

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
		store.recordSourceQuery("stations")
		return store.source.ListStations(ctx)
	}
	version, err := store.versions.GetOrCreate(ctx, StationVersionKey())
	if err != nil {
		store.recordRequest("stations", "version_get", "failure", "redis")
		store.recordFallback("redis")
		store.recordSourceQuery("stations")
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
	value, err, shared := store.coalescer.Do(ctx, key, func(fillContext context.Context) (any, error) {
		if stations, status := store.readStations(fillContext, key); status == cacheReadHit {
			return fillOutcome{value: stations}, nil
		} else if status == cacheReadRedisFailure || status == cacheReadInvalid {
			store.recordReadStatus("stations", status)
		}
		started := time.Now()
		store.recordSourceQuery("stations")
		stations, sourceErr := store.source.ListStations(fillContext)
		if sourceErr != nil {
			store.recordFill("stations", "failure", "database", time.Since(started), false)
			return nil, sourceErr
		}
		sort.Slice(stations, func(i, j int) bool { return stations[i].Code.String() < stations[j].Code.String() })
		payload := make([]cachedStation, 0, len(stations))
		for _, station := range stations {
			payload = append(payload, cachedStation{
				ID: station.ID, Code: station.Code.String(), Name: station.Name, Timezone: station.Timezone,
			})
		}
		outcome := fillOutcome{value: stations, writeErr: store.writeJSON(fillContext, key, payload, store.stationTTL)}
		if outcome.writeErr != nil {
			store.recordFill("stations", "failure", "redis", time.Since(started), false)
		} else {
			store.recordFill("stations", "success", "none", time.Since(started), false)
		}
		return outcome, nil
	})
	if shared {
		store.recordSingleflightShared("stations")
	}
	if err != nil {
		return nil, err
	}
	outcome := value.(fillOutcome)
	return append([]querypostgres.Station(nil), outcome.value.([]querypostgres.Station)...), nil
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
		store.recordSourceQuery("train_search")
		return store.source.SearchTrainRuns(ctx, request)
	}
	key, err := SearchDataKey(version, SearchQueryHash(normalized))
	if err != nil {
		return nil, err
	}
	if results, status := store.readSearch(ctx, key, normalized); status == cacheReadHit {
		store.recordReadStatus("train_search", status)
		return results, nil
	} else {
		store.recordReadStatus("train_search", status)
	}
	value, err, shared := store.coalescer.Do(ctx, key, func(fillContext context.Context) (any, error) {
		if results, status := store.readSearch(fillContext, key, normalized); status == cacheReadHit {
			return fillOutcome{value: results}, nil
		} else if status == cacheReadRedisFailure || status == cacheReadInvalid {
			store.recordReadStatus("train_search", status)
		}
		started := time.Now()
		results, projectionErr := store.searchProjectionOrSource(fillContext, request)
		if projectionErr != nil {
			store.recordFill("train_search", "failure", "database", time.Since(started), false)
			return nil, projectionErr
		}
		outcome := fillOutcome{
			value: results,
			writeErr: store.writeJSON(
				fillContext, key, searchCachePayload(results, normalized), store.searchTTL,
			),
		}
		if outcome.writeErr != nil {
			store.recordFill("train_search", "failure", "redis", time.Since(started), false)
		} else {
			store.recordFill("train_search", "success", "none", time.Since(started), false)
		}
		return outcome, nil
	})
	if shared {
		store.recordSingleflightShared("train_search")
	}
	if err != nil {
		return nil, err
	}
	outcome := value.(fillOutcome)
	return outcome.value.([]querypostgres.SearchResult), nil
}

func (store *Store) Availability(
	ctx context.Context,
	request querypostgres.AvailabilityRequest,
) (querypostgres.Availability, error) {
	if !store.availabilityEnabled {
		store.recordSourceQuery("availability")
		return store.source.Availability(ctx, request)
	}
	versionKey, err := AvailabilityVersionKey(request.TrainRunID)
	if err != nil {
		return querypostgres.Availability{}, querypostgres.ErrInvalidQuery
	}
	version, found, err := store.versions.Get(ctx, versionKey)
	if err != nil {
		store.recordRequest("availability", "version_get", "failure", "redis")
		store.recordFallback("redis")
		store.recordSourceQuery("availability")
		return store.source.Availability(ctx, request)
	}
	key := ""
	if found {
		key, err = AvailabilityDataKey(
			version,
			request.TrainRunID,
			request.OriginCode,
			request.DestinationCode,
			request.SeatClass,
		)
		if err != nil {
			return querypostgres.Availability{}, querypostgres.ErrInvalidJourney
		}
		generation, enforceGeneration, generationErr := store.availabilityAssignmentGeneration(ctx, request.TrainRunID)
		if generationErr != nil {
			return querypostgres.Availability{}, generationErr
		}
		if availability, status := store.readAvailability(ctx, key, request, generation, enforceGeneration); status == cacheReadHit {
			store.recordReadStatus("availability", status)
			return availability, nil
		} else {
			store.recordReadStatus("availability", status)
		}
	}
	flightKey, err := availabilityRequestFlightKey(request)
	if err != nil {
		return querypostgres.Availability{}, err
	}
	value, err, shared := store.coalescer.Do(ctx, flightKey, func(fillContext context.Context) (any, error) {
		fillVersion, fillFound, versionErr := store.versions.Get(fillContext, versionKey)
		fillKey := ""
		if versionErr == nil && fillFound {
			fillKey, versionErr = AvailabilityDataKey(
				fillVersion, request.TrainRunID, request.OriginCode, request.DestinationCode, request.SeatClass,
			)
			if versionErr == nil {
				generation, enforceGeneration, generationErr := store.availabilityAssignmentGeneration(
					fillContext, request.TrainRunID,
				)
				if generationErr != nil {
					return nil, generationErr
				}
				if availability, status := store.readAvailability(
					fillContext, fillKey, request, generation, enforceGeneration,
				); status == cacheReadHit {
					return fillOutcome{value: availability}, nil
				} else if status == cacheReadRedisFailure || status == cacheReadInvalid {
					store.recordReadStatus("availability", status)
				}
			}
		}
		started := time.Now()
		store.recordSourceQuery("availability")
		availability, sourceErr := store.source.Availability(fillContext, request)
		if sourceErr != nil {
			store.recordFill("availability", "failure", "database", time.Since(started), false)
			return nil, sourceErr
		}
		if availability.AvailableSeats < 0 {
			store.recordFill("availability", "failure", "database", time.Since(started), false)
			return nil, querypostgres.ErrPersistence
		}
		payload, valid := availabilityCachePayload(availability, request, store.clock.Now().UTC())
		if !valid {
			store.recordFill("availability", "failure", "database", time.Since(started), false)
			return nil, querypostgres.ErrPersistence
		}
		if fillKey == "" {
			var created bool
			fillVersion, created, versionErr = store.versions.GetOrCreateWithStatus(fillContext, versionKey)
			if versionErr == nil {
				fillKey, versionErr = AvailabilityDataKey(
					fillVersion, request.TrainRunID, request.OriginCode, request.DestinationCode, request.SeatClass,
				)
			}
			if versionErr == nil && !created {
				store.recordSourceQuery("availability")
				availability, sourceErr = store.source.Availability(fillContext, request)
				if sourceErr != nil {
					store.recordFill("availability", "failure", "database", time.Since(started), false)
					return nil, sourceErr
				}
				if availability.AvailableSeats < 0 {
					store.recordFill("availability", "failure", "database", time.Since(started), false)
					return nil, querypostgres.ErrPersistence
				}
				payload, valid = availabilityCachePayload(availability, request, store.clock.Now().UTC())
				if !valid {
					store.recordFill("availability", "failure", "database", time.Since(started), false)
					return nil, querypostgres.ErrPersistence
				}
			}
		}
		if versionErr != nil {
			store.recordFill("availability", "failure", "redis", time.Since(started), false)
			return fillOutcome{value: availability, writeErr: versionErr}, nil
		}
		outcome := fillOutcome{
			value: availability, writeErr: store.writeJSON(fillContext, fillKey, payload, store.availabilityTTL),
		}
		if outcome.writeErr != nil {
			store.recordFill("availability", "failure", "redis", time.Since(started), false)
		} else {
			store.recordFill("availability", "success", "none", time.Since(started), false)
		}
		return outcome, nil
	})
	if shared {
		store.recordSingleflightShared("availability")
	}
	if err != nil {
		return querypostgres.Availability{}, err
	}
	outcome := value.(fillOutcome)
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
			store.recordSourceQuery("availability")
			return batch.AvailabilityBatch(ctx, requests)
		}
		results := make([]querypostgres.Availability, 0, len(requests))
		for _, request := range requests {
			store.recordSourceQuery("availability")
			availability, err := store.source.Availability(ctx, request)
			if err != nil {
				return nil, err
			}
			results = append(results, availability)
		}
		return results, nil
	}
	results, misses, _, _, err := store.readAvailabilityBatch(ctx, requests)
	if err != nil {
		return nil, err
	}
	if len(misses) == 0 {
		return results, nil
	}
	flightKey, err := availabilityBatchFlightKey(requests)
	if err != nil {
		return nil, err
	}
	value, err, shared := store.coalescer.Do(ctx, flightKey, func(fillContext context.Context) (any, error) {
		return store.loadAvailabilityBatch(fillContext, requests)
	})
	if shared {
		store.recordSingleflightShared("availability")
	}
	if err != nil {
		return nil, err
	}
	return value.([]querypostgres.Availability), nil
}

func (store *Store) readAvailabilityBatch(
	ctx context.Context,
	requests []querypostgres.AvailabilityRequest,
) (
	[]querypostgres.Availability,
	[]querypostgres.AvailabilityRequest,
	[]int,
	[]string,
	error,
) {
	results := make([]querypostgres.Availability, len(requests))
	keys := make([]string, len(requests))
	misses := make([]querypostgres.AvailabilityRequest, 0)
	missIndexes := make([]int, 0)
	missKeys := make([]string, 0)
	for index, request := range requests {
		versionKey, err := AvailabilityVersionKey(request.TrainRunID)
		if err != nil {
			return nil, nil, nil, nil, querypostgres.ErrInvalidQuery
		}
		version, found, versionErr := store.versions.Get(ctx, versionKey)
		if versionErr == nil && found {
			keys[index], err = AvailabilityDataKey(
				version, request.TrainRunID, request.OriginCode, request.DestinationCode, request.SeatClass,
			)
			if err != nil {
				return nil, nil, nil, nil, querypostgres.ErrInvalidJourney
			}
			generation, enforceGeneration, generationErr := store.availabilityAssignmentGeneration(
				ctx, request.TrainRunID,
			)
			if generationErr != nil {
				return nil, nil, nil, nil, generationErr
			}
			if availability, status := store.readAvailability(
				ctx, keys[index], request, generation, enforceGeneration,
			); status == cacheReadHit {
				store.recordReadStatus("availability", status)
				results[index] = availability
				continue
			} else {
				store.recordReadStatus("availability", status)
			}
		} else if versionErr != nil {
			store.recordRequest("availability", "version_get", "failure", "redis")
			store.recordFallback("redis")
		}
		misses = append(misses, request)
		missIndexes = append(missIndexes, index)
		missKeys = append(missKeys, keys[index])
	}
	return results, misses, missIndexes, missKeys, nil
}

func (store *Store) loadAvailabilityBatch(
	ctx context.Context,
	requests []querypostgres.AvailabilityRequest,
) ([]querypostgres.Availability, error) {
	results, misses, missIndexes, missKeys, err := store.readAvailabilityBatch(ctx, requests)
	if err != nil {
		return nil, err
	}
	if len(misses) == 0 {
		return results, nil
	}
	loaded, err := store.fillAvailabilityMisses(ctx, misses, missKeys)
	if err != nil {
		return nil, err
	}
	if len(loaded) != len(missIndexes) {
		return nil, querypostgres.ErrPersistence
	}
	for offset, availability := range loaded {
		results[missIndexes[offset]] = availability
	}
	return results, nil
}

func (store *Store) fillAvailabilityMisses(
	ctx context.Context,
	requests []querypostgres.AvailabilityRequest,
	keys []string,
) ([]querypostgres.Availability, error) {
	results := make([]querypostgres.Availability, len(requests))
	sourceRequests := make([]querypostgres.AvailabilityRequest, 0, len(requests))
	sourceIndexes := make([]int, 0, len(requests))
	for index, request := range requests {
		if keys[index] != "" {
			generation, enforceGeneration, generationErr := store.availabilityAssignmentGeneration(
				ctx, request.TrainRunID,
			)
			if generationErr != nil {
				return nil, generationErr
			}
			if availability, status := store.readAvailability(
				ctx, keys[index], request, generation, enforceGeneration,
			); status == cacheReadHit {
				results[index] = availability
				continue
			} else if status == cacheReadRedisFailure || status == cacheReadInvalid {
				store.recordReadStatus("availability", status)
			}
		}
		sourceRequests = append(sourceRequests, request)
		sourceIndexes = append(sourceIndexes, index)
	}
	if len(sourceRequests) == 0 {
		return results, nil
	}
	loaded := make([]querypostgres.Availability, 0, len(sourceRequests))
	if batch, ok := store.source.(availabilityBatchSource); ok {
		var err error
		store.recordSourceQuery("availability")
		loaded, err = batch.AvailabilityBatch(ctx, sourceRequests)
		if err != nil {
			return nil, err
		}
	} else {
		for _, request := range sourceRequests {
			store.recordSourceQuery("availability")
			availability, err := store.source.Availability(ctx, request)
			if err != nil {
				return nil, err
			}
			loaded = append(loaded, availability)
		}
	}
	if len(loaded) != len(sourceRequests) {
		return nil, querypostgres.ErrPersistence
	}
	for offset, availability := range loaded {
		index := sourceIndexes[offset]
		if availability.AvailableSeats < 0 {
			return nil, querypostgres.ErrPersistence
		}
		results[index] = availability
		if keys[index] == "" {
			versionKey, keyErr := AvailabilityVersionKey(requests[index].TrainRunID)
			created := false
			if keyErr == nil {
				version, installed, versionErr := store.versions.GetOrCreateWithStatus(ctx, versionKey)
				created = installed
				if versionErr == nil {
					keys[index], keyErr = AvailabilityDataKey(
						version,
						requests[index].TrainRunID,
						requests[index].OriginCode,
						requests[index].DestinationCode,
						requests[index].SeatClass,
					)
				} else {
					keyErr = versionErr
				}
			}
			if keyErr != nil {
				store.recordFill("availability", "failure", "redis", 0, false)
				continue
			}
			if !created {
				store.recordSourceQuery("availability")
				refreshed, sourceErr := store.source.Availability(ctx, requests[index])
				if sourceErr != nil {
					return nil, sourceErr
				}
				if refreshed.AvailableSeats < 0 {
					return nil, querypostgres.ErrPersistence
				}
				availability = refreshed
				results[index] = refreshed
			}
		}
		payload, valid := availabilityCachePayload(availability, requests[index], store.clock.Now().UTC())
		if !valid {
			return nil, querypostgres.ErrPersistence
		}
		started := time.Now()
		if err := store.writeJSON(ctx, keys[index], payload, store.availabilityTTL); err != nil {
			store.recordFill("availability", "failure", "redis", time.Since(started), false)
		} else {
			store.recordFill("availability", "success", "none", time.Since(started), false)
		}
	}
	return results, nil
}

func availabilityRequestFlightKey(request querypostgres.AvailabilityRequest) (string, error) {
	trainRunID, origin, destination, seatClass, valid := normalizedAvailabilityScope(request)
	if !valid {
		return "", querypostgres.ErrInvalidJourney
	}
	sum := sha256.Sum256([]byte(trainRunID + "\x00" + origin + "\x00" + destination + "\x00" + seatClass))
	return "availability:" + hex.EncodeToString(sum[:]), nil
}

func availabilityBatchFlightKey(requests []querypostgres.AvailabilityRequest) (string, error) {
	if len(requests) == 0 {
		return "", querypostgres.ErrInvalidQuery
	}
	hash := sha256.New()
	for _, request := range requests {
		trainRunID, origin, destination, seatClass, valid := normalizedAvailabilityScope(request)
		if !valid {
			return "", querypostgres.ErrInvalidJourney
		}
		_, _ = hash.Write([]byte(trainRunID + "\x00" + origin + "\x00" + destination + "\x00" + seatClass + "\n"))
	}
	return "availability-batch:" + hex.EncodeToString(hash.Sum(nil)), nil
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

func (store *Store) readSearch(
	ctx context.Context,
	key string,
	search querypostgres.NormalizedSearch,
) ([]querypostgres.SearchResult, cacheReadStatus) {
	var cached cachedSearch
	status := store.readJSON(ctx, key, &cached)
	if status != cacheReadHit {
		return nil, status
	}
	if cached.Schema != searchSchema || cached.OriginCode != search.OriginCode.String() ||
		cached.DestinationCode != search.DestinationCode.String() ||
		cached.ServiceDate != search.ServiceDate.Format(time.DateOnly) ||
		cached.SeatClass != search.SeatClass.String() || cached.Page != search.Page ||
		cached.PageSize != search.PageSize || cached.Sort != string(search.Sort) ||
		cached.Results == nil || len(cached.Results) > search.PageSize {
		return nil, cacheReadInvalid
	}
	seen := make(map[string]struct{}, len(cached.Results))
	for _, result := range cached.Results {
		trainRunID, trainRunErr := uuid.Parse(result.TrainRunID)
		trainID, trainErr := uuid.Parse(result.TrainID)
		routeID, routeErr := uuid.Parse(result.RouteID)
		serviceDate := time.Date(
			result.ServiceDate.Year(), result.ServiceDate.Month(), result.ServiceDate.Day(), 0, 0, 0, 0, time.UTC,
		)
		if trainRunErr != nil || trainErr != nil || routeErr != nil ||
			trainRunID == uuid.Nil || trainID == uuid.Nil || routeID == uuid.Nil ||
			trainRunID.String() != result.TrainRunID || trainID.String() != result.TrainID || routeID.String() != result.RouteID ||
			len(result.TrainCode) < 1 || len(result.TrainCode) > 32 ||
			serviceDate != search.ServiceDate || result.Status != domain.TrainRunStatusScheduled ||
			result.FromStopIndex < 0 || result.ToStopIndex <= result.FromStopIndex ||
			result.DepartureAt.IsZero() || !result.ArrivalAt.After(result.DepartureAt) ||
			result.SeatClass != search.SeatClass || result.FareAmountMinor < 0 || !validCurrency(result.Currency) {
			return nil, cacheReadInvalid
		}
		identity := trainRunID.String() + "|" + result.SeatClass.String()
		if _, duplicate := seen[identity]; duplicate {
			return nil, cacheReadInvalid
		}
		seen[identity] = struct{}{}
	}
	return cached.Results, cacheReadHit
}

func searchCachePayload(
	results []querypostgres.SearchResult,
	search querypostgres.NormalizedSearch,
) cachedSearch {
	return cachedSearch{
		Schema: searchSchema, OriginCode: search.OriginCode.String(), DestinationCode: search.DestinationCode.String(),
		ServiceDate: search.ServiceDate.Format(time.DateOnly), SeatClass: search.SeatClass.String(),
		Page: search.Page, PageSize: search.PageSize, Sort: string(search.Sort), Results: results,
	}
}

func validCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func (store *Store) readAvailability(
	ctx context.Context,
	key string,
	request querypostgres.AvailabilityRequest,
	expectedGeneration int64,
	enforceGeneration bool,
) (querypostgres.Availability, cacheReadStatus) {
	var cached cachedAvailability
	status := store.readJSON(ctx, key, &cached)
	if status != cacheReadHit {
		return querypostgres.Availability{}, status
	}
	requestTrainRunID, requestErr := uuid.Parse(request.TrainRunID)
	cachedTrainRunID, cachedErr := uuid.Parse(cached.Value.TrainRunID)
	_, origin, destination, seatClass, scopeValid := normalizedAvailabilityScope(request)
	if cached.Source != "postgres" || cached.ObservedAt.IsZero() ||
		(enforceGeneration && cached.AssignmentGeneration != expectedGeneration) ||
		cached.Value.AvailableSeats < 0 || requestErr != nil || cachedErr != nil ||
		requestTrainRunID == uuid.Nil || cachedTrainRunID != requestTrainRunID || !scopeValid ||
		cached.OriginCode != origin || cached.DestinationCode != destination || cached.SeatClass != seatClass ||
		cached.Value.SeatClass.String() != seatClass || cached.Value.FromStopIndex < 0 ||
		cached.Value.ToStopIndex <= cached.Value.FromStopIndex || cached.Value.SegmentCount < cached.Value.ToStopIndex {
		return querypostgres.Availability{}, cacheReadInvalid
	}
	age := store.clock.Now().UTC().Sub(cached.ObservedAt.UTC())
	if age < 0 || age > store.availabilityMaxStale {
		return querypostgres.Availability{}, cacheReadMiss
	}
	return cached.Value, cacheReadHit
}

func availabilityCachePayload(
	value querypostgres.Availability,
	request querypostgres.AvailabilityRequest,
	observedAt time.Time,
) (cachedAvailability, bool) {
	_, origin, destination, seatClass, valid := normalizedAvailabilityScope(request)
	if !valid || observedAt.IsZero() {
		return cachedAvailability{}, false
	}
	return cachedAvailability{
		Value: value, AssignmentGeneration: value.AssignmentGeneration,
		OriginCode: origin, DestinationCode: destination, SeatClass: seatClass,
		ObservedAt: observedAt, Source: "postgres",
	}, true
}

func (store *Store) availabilityAssignmentGeneration(
	ctx context.Context,
	rawTrainRunID string,
) (int64, bool, error) {
	source, ok := store.source.(availabilityAssignmentSource)
	if !ok {
		return 0, false, nil
	}
	generation, err := source.AvailabilityAssignmentGeneration(ctx, rawTrainRunID)
	if err != nil {
		return 0, true, err
	}
	if generation < 0 {
		return 0, true, querypostgres.ErrPersistence
	}
	return generation, true, nil
}

func normalizedAvailabilityScope(
	request querypostgres.AvailabilityRequest,
) (string, string, string, string, bool) {
	trainRunID, err := uuid.Parse(request.TrainRunID)
	if err != nil || trainRunID == uuid.Nil {
		return "", "", "", "", false
	}
	origin, err := domain.NewStationCode(request.OriginCode)
	if err != nil {
		return "", "", "", "", false
	}
	destination, err := domain.NewStationCode(request.DestinationCode)
	if err != nil || origin == destination {
		return "", "", "", "", false
	}
	seatClass, err := domain.ParseSeatClass(request.SeatClass)
	if err != nil {
		return "", "", "", "", false
	}
	return trainRunID.String(), origin.String(), destination.String(), seatClass.String(), true
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
	store.recordSourceQuery("train_search")
	return store.source.SearchTrainRuns(ctx, request)
}

func (store *Store) readJSON(ctx context.Context, key string, target any) cacheReadStatus {
	result, err := boundedCacheGetScript.Run(ctx, store.client, []string{key}, MaxCachePayloadBytes).Slice()
	if err != nil {
		return cacheReadRedisFailure
	}
	if len(result) != 2 {
		return cacheReadRedisFailure
	}
	status, statusOK := result[0].(int64)
	encoded, encodedOK := result[1].(string)
	if !statusOK || !encodedOK {
		return cacheReadRedisFailure
	}
	if status == 0 {
		return cacheReadMiss
	}
	if status != 1 || len(encoded) > MaxCachePayloadBytes {
		return cacheReadInvalid
	}
	if json.Unmarshal([]byte(encoded), target) != nil {
		return cacheReadInvalid
	}
	return cacheReadHit
}

func (store *Store) writeJSON(ctx context.Context, key string, value any, policy *TTLPolicy) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) > MaxCachePayloadBytes {
		return errors.New("read cache payload exceeds bounded size")
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

func (store *Store) recordSingleflightShared(cacheType string) {
	if store.metrics != nil {
		store.metrics.RecordCacheSingleflightShared(cacheType)
	}
}

func (store *Store) recordSourceQuery(cacheType string) {
	if store.metrics != nil {
		store.metrics.RecordCacheSourceQuery(cacheType)
	}
}

func (store *Store) recordFallback(reason string) {
	if store.metrics != nil {
		store.metrics.RecordFallback(reason)
	}
}

var _ SourceStore = (*Store)(nil)
