package cache

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/google/uuid"
)

const (
	versionTokenBytes = 18
	searchSchema      = "journey-search-v1"
)

var (
	ErrInvalidCacheKey  = errors.New("invalid read cache key")
	versionTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{24}$`)
)

func NewVersionToken(random io.Reader) (string, error) {
	if random == nil {
		return "", ErrInvalidCacheKey
	}
	bytes := make([]byte, versionTokenBytes)
	if _, err := io.ReadFull(random, bytes); err != nil {
		return "", fmt.Errorf("generate cache version token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func ValidVersionToken(token string) bool {
	return versionTokenPattern.MatchString(token)
}

func StationVersionKey() string { return "cache:stations:version" }

func StationDataKey(token string) (string, error) {
	if !ValidVersionToken(token) {
		return "", ErrInvalidCacheKey
	}
	return "cache:stations:" + token, nil
}

func SearchVersionKey() string { return "cache:train-search:version" }

func SearchQueryHash(search querypostgres.NormalizedSearch) string {
	field, direction := searchSortIdentity(search.Sort)
	canonical := strings.Join([]string{
		"schema=" + searchSchema,
		"origin=" + search.OriginCode.String(),
		"destination=" + search.DestinationCode.String(),
		"date=" + search.ServiceDate.UTC().Format("2006-01-02"),
		"class=" + search.SeatClass.String(),
		"page=" + strconv.Itoa(search.Page),
		"limit=" + strconv.Itoa(search.PageSize),
		"sort_field=" + field,
		"sort_direction=" + direction,
	}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

func SearchDataKey(token, queryHash string) (string, error) {
	if !ValidVersionToken(token) || len(queryHash) != sha256.Size*2 {
		return "", ErrInvalidCacheKey
	}
	if _, err := hex.DecodeString(queryHash); err != nil {
		return "", ErrInvalidCacheKey
	}
	return "cache:train-search:" + token + ":" + queryHash, nil
}

func AvailabilityVersionKey(rawTrainRunID string) (string, error) {
	trainRunID, err := uuid.Parse(rawTrainRunID)
	if err != nil || trainRunID == uuid.Nil {
		return "", ErrInvalidCacheKey
	}
	return "cache:availability:version:" + trainRunID.String(), nil
}

func AvailabilityDataKey(
	token string,
	rawTrainRunID string,
	rawFromStationCode string,
	rawToStationCode string,
	rawSeatClass string,
) (string, error) {
	if !ValidVersionToken(token) {
		return "", ErrInvalidCacheKey
	}
	trainRunID, err := uuid.Parse(rawTrainRunID)
	if err != nil || trainRunID == uuid.Nil {
		return "", ErrInvalidCacheKey
	}
	fromStation, err := domain.NewStationCode(rawFromStationCode)
	if err != nil {
		return "", ErrInvalidCacheKey
	}
	toStation, err := domain.NewStationCode(rawToStationCode)
	if err != nil || fromStation == toStation {
		return "", ErrInvalidCacheKey
	}
	seatClass, err := domain.ParseSeatClass(rawSeatClass)
	if err != nil {
		return "", ErrInvalidCacheKey
	}
	return "cache:availability:" + token + ":" + trainRunID.String() + ":" +
		fromStation.String() + ":" + toStation.String() + ":" + seatClass.String(), nil
}

func searchSortIdentity(sort querypostgres.SortOrder) (string, string) {
	switch sort {
	case querypostgres.SortDepartureDesc:
		return "departure", "desc"
	case querypostgres.SortFareAsc:
		return "fare", "asc"
	case querypostgres.SortFareDesc:
		return "fare", "desc"
	default:
		return "departure", "asc"
	}
}
