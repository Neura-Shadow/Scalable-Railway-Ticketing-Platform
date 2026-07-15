package postgres

import (
	"errors"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
)

const (
	DefaultPageSize = 20
	MaxPageSize     = 100
	MaxPage         = 10_000
)

var (
	ErrInvalidQuery   = errors.New("invalid railway query")
	ErrInvalidJourney = errors.New("invalid journey")
	ErrInvalidSort    = errors.New("invalid search sort")
	ErrNotFound       = errors.New("query resource not found")
	ErrPersistence    = errors.New("query persistence failure")
)

type SortOrder string

const (
	SortDepartureAsc  SortOrder = "departure_asc"
	SortDepartureDesc SortOrder = "departure_desc"
	SortFareAsc       SortOrder = "fare_asc"
	SortFareDesc      SortOrder = "fare_desc"
)

type SearchRequest struct {
	OriginCode      string
	DestinationCode string
	ServiceDate     time.Time
	SeatClass       string
	Page            int
	PageSize        int
	Sort            string
}

type NormalizedSearch struct {
	OriginCode      domain.StationCode
	DestinationCode domain.StationCode
	ServiceDate     time.Time
	SeatClass       domain.SeatClass
	Page            int
	PageSize        int
	Offset          int
	Sort            SortOrder
}

func NormalizeSearch(request SearchRequest) (NormalizedSearch, error) {
	origin, err := domain.NewStationCode(request.OriginCode)
	if err != nil {
		return NormalizedSearch{}, ErrInvalidJourney
	}
	destination, err := domain.NewStationCode(request.DestinationCode)
	if err != nil || origin == destination {
		return NormalizedSearch{}, ErrInvalidJourney
	}
	if request.ServiceDate.IsZero() {
		return NormalizedSearch{}, ErrInvalidQuery
	}
	seatClass, err := domain.ParseSeatClass(request.SeatClass)
	if err != nil {
		return NormalizedSearch{}, err
	}
	sortOrder, err := normalizeSort(request.Sort)
	if err != nil {
		return NormalizedSearch{}, err
	}
	page := request.Page
	if page < 1 {
		page = 1
	} else if page > MaxPage {
		page = MaxPage
	}
	pageSize := request.PageSize
	if pageSize < 1 {
		pageSize = DefaultPageSize
	} else if pageSize > MaxPageSize {
		pageSize = MaxPageSize
	}
	serviceDate := time.Date(request.ServiceDate.Year(), request.ServiceDate.Month(), request.ServiceDate.Day(), 0, 0, 0, 0, time.UTC)
	return NormalizedSearch{
		OriginCode: origin, DestinationCode: destination, ServiceDate: serviceDate, SeatClass: seatClass,
		Page: page, PageSize: pageSize, Offset: (page - 1) * pageSize, Sort: sortOrder,
	}, nil
}

func normalizeSort(raw string) (SortOrder, error) {
	switch SortOrder(strings.ToLower(strings.TrimSpace(raw))) {
	case "", SortDepartureAsc:
		return SortDepartureAsc, nil
	case SortDepartureDesc:
		return SortDepartureDesc, nil
	case SortFareAsc:
		return SortFareAsc, nil
	case SortFareDesc:
		return SortFareDesc, nil
	default:
		return "", ErrInvalidSort
	}
}
