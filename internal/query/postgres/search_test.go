package postgres_test

import (
	"errors"
	"testing"
	"time"

	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
)

func TestNormalizeSearchBoundsPaginationAndAllowsOnlyFixedSorts(t *testing.T) {
	t.Parallel()

	serviceDate := time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		page         int
		pageSize     int
		sort         string
		wantPage     int
		wantPageSize int
		wantSort     querypostgres.SortOrder
		wantErr      error
	}{
		{name: "defaults", wantPage: 1, wantPageSize: 20, wantSort: querypostgres.SortDepartureAsc},
		{name: "lower bounds", page: -5, pageSize: -1, sort: "departure_desc", wantPage: 1, wantPageSize: 20, wantSort: querypostgres.SortDepartureDesc},
		{name: "upper bounds", page: 50_000, pageSize: 500, sort: "fare_asc", wantPage: 10_000, wantPageSize: 100, wantSort: querypostgres.SortFareAsc},
		{name: "fare descending", page: 2, pageSize: 10, sort: "fare_desc", wantPage: 2, wantPageSize: 10, wantSort: querypostgres.SortFareDesc},
		{name: "reject SQL ordering", sort: "departure_asc; DROP TABLE train_runs", wantErr: querypostgres.ErrInvalidSort},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			normalized, err := querypostgres.NormalizeSearch(querypostgres.SearchRequest{
				OriginCode: " tpe ", DestinationCode: "khh", ServiceDate: serviceDate,
				SeatClass: " STANDARD ", Page: tt.page, PageSize: tt.pageSize, Sort: tt.sort,
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NormalizeSearch() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if normalized.Page != tt.wantPage || normalized.PageSize != tt.wantPageSize || normalized.Sort != tt.wantSort {
				t.Fatalf("NormalizeSearch() pagination/sort = %d/%d/%q", normalized.Page, normalized.PageSize, normalized.Sort)
			}
			if normalized.OriginCode.String() != "TPE" || normalized.DestinationCode.String() != "KHH" || normalized.SeatClass.String() != "standard" {
				t.Fatalf("NormalizeSearch() identifiers = %#v", normalized)
			}
		})
	}
}

func TestNormalizeSearchRejectsIncompleteJourney(t *testing.T) {
	t.Parallel()

	validDate := time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC)
	tests := []querypostgres.SearchRequest{
		{OriginCode: "TPE", DestinationCode: "TPE", ServiceDate: validDate, SeatClass: "standard"},
		{OriginCode: "bad!", DestinationCode: "KHH", ServiceDate: validDate, SeatClass: "standard"},
		{OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard"},
		{OriginCode: "TPE", DestinationCode: "KHH", ServiceDate: validDate, SeatClass: "premium"},
	}
	for _, request := range tests {
		if _, err := querypostgres.NormalizeSearch(request); err == nil {
			t.Fatalf("NormalizeSearch(%#v) error = nil", request)
		}
	}
}
