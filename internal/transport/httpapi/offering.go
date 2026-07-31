package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

func registerOfferingRoutes(group *gin.RouterGroup, dependencies Dependencies) {
	group.GET("/stations", stationsHandler(dependencies))
	group.GET("/train-runs/search", withPhysicalQueryTimeout(dependencies, trainRunSearchHandler(dependencies))...)
	group.GET("/train-runs/:id/availability", withPhysicalQueryTimeout(dependencies, availabilityHandler(dependencies))...)
}

func stationsHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Offering == nil {
			writeError(c, ErrUnavailable)
			return
		}
		page, ok := parsePageRequest(c, "code", "code", "name")
		if !ok {
			writeError(c, ErrInvalidInput)
			return
		}
		result, err := dependencies.Offering.ListStations(c.Request.Context(), page)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func trainRunSearchHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Offering == nil {
			writeError(c, ErrUnavailable)
			return
		}
		page, ok := parsePageRequest(c, "departure_at", "departure_at", "fare_minor")
		origin := strings.TrimSpace(c.Query("origin_station_code"))
		destination := strings.TrimSpace(c.Query("destination_station_code"))
		serviceDate, err := time.Parse("2006-01-02", c.Query("service_date"))
		if !ok || err != nil || origin == "" || destination == "" || origin == destination {
			writeError(c, ErrInvalidInput)
			return
		}
		result, err := dependencies.Offering.SearchTrainRuns(c.Request.Context(), TrainRunSearch{
			OriginStationCode:      origin,
			DestinationStationCode: destination,
			ServiceDate:            serviceDate,
			SeatClass:              strings.TrimSpace(c.Query("seat_class")),
			Page:                   page,
		})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func availabilityHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Offering == nil {
			writeError(c, ErrUnavailable)
			return
		}
		query := AvailabilityQuery{
			TrainRunID:             c.Param("id"),
			OriginStationCode:      strings.TrimSpace(c.Query("origin_station_code")),
			DestinationStationCode: strings.TrimSpace(c.Query("destination_station_code")),
			SeatClass:              strings.TrimSpace(c.Query("seat_class")),
		}
		if query.TrainRunID == "" || query.OriginStationCode == "" || query.DestinationStationCode == "" || query.OriginStationCode == query.DestinationStationCode || query.SeatClass == "" {
			writeError(c, ErrInvalidInput)
			return
		}
		result, err := dependencies.Offering.GetAvailability(c.Request.Context(), query)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func parsePageRequest(c *gin.Context, defaultSort string, allowedSorts ...string) (PageRequest, bool) {
	page, ok := normalizedPositiveInt(c.Query("page"), 1)
	if !ok {
		return PageRequest{}, false
	}
	limit, ok := normalizedPositiveInt(c.Query("limit"), defaultPageSize)
	if !ok {
		return PageRequest{}, false
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	sort := strings.TrimSpace(c.Query("sort"))
	if sort == "" {
		sort = defaultSort
	}
	baseSort := strings.TrimPrefix(sort, "-")
	allowed := false
	for _, candidate := range allowedSorts {
		if baseSort == candidate {
			allowed = true
			break
		}
	}
	if !allowed || strings.Count(sort, "-") > 1 || (strings.Contains(sort, "-") && !strings.HasPrefix(sort, "-")) {
		return PageRequest{}, false
	}
	return PageRequest{Page: page, Limit: limit, Sort: sort}, true
}

func normalizedPositiveInt(raw string, fallback int) (int, bool) {
	if raw == "" {
		return fallback, true
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	if parsed <= 0 {
		return fallback, true
	}
	return parsed, true
}
