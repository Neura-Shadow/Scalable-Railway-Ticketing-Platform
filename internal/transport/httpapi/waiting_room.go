package httpapi

import (
	"errors"
	"net/http"
	"strings"

	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

var errInvalidAdmissionTokenOutput = errors.New("invalid admission token output")

type joinWaitingRoomRequest struct {
	TrainRunID             string `json:"train_run_id"`
	OriginStationCode      string `json:"origin_station_code"`
	DestinationStationCode string `json:"destination_station_code"`
	SeatClass              string `json:"seat_class"`
	PassengerCount         int    `json:"passenger_count"`
}

func registerWaitingRoomRoutes(group *gin.RouterGroup, dependencies Dependencies) {
	entries := group.Group(
		"/waiting-room/entries",
		waitingRoomCacheMiddleware(),
		authenticate(dependencies.TokenParser),
		authorize(RoleCustomer),
	)
	entries.POST("", joinWaitingRoomHandler(dependencies))
	entries.GET("/:id", getWaitingRoomEntryHandler(dependencies))
	entries.DELETE("/:id", cancelWaitingRoomEntryHandler(dependencies))
}

func joinWaitingRoomHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		setWaitingRoomCachePolicy(c)
		if dependencies.WaitingRoom == nil {
			writeError(c, ErrUnavailable)
			return
		}
		var request joinWaitingRoomRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		normalizeWaitingRoomJoin(&request)
		if !validWaitingRoomJoin(request, dependencies.MaxPassengers) {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.WaitingRoom.JoinWaitingRoom(c.Request.Context(), JoinWaitingRoomCommand{
			OwnerID:                identity.Subject,
			TrainRunID:             request.TrainRunID,
			OriginStationCode:      request.OriginStationCode,
			DestinationStationCode: request.DestinationStationCode,
			SeatClass:              request.SeatClass,
			PassengerCount:         request.PassengerCount,
		})
		if err != nil {
			writeError(c, err)
			return
		}
		writeWaitingRoomView(c, http.StatusCreated, result)
	}
}

func getWaitingRoomEntryHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		setWaitingRoomCachePolicy(c)
		if dependencies.WaitingRoom == nil {
			writeError(c, ErrUnavailable)
			return
		}
		entryID := strings.TrimSpace(c.Param("id"))
		if !validCanonicalUUID(entryID) {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.WaitingRoom.GetWaitingRoomEntry(c.Request.Context(), identity.Subject, entryID)
		if err != nil {
			writeError(c, err)
			return
		}
		writeWaitingRoomView(c, http.StatusOK, result)
	}
}

func cancelWaitingRoomEntryHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		setWaitingRoomCachePolicy(c)
		if dependencies.WaitingRoom == nil {
			writeError(c, ErrUnavailable)
			return
		}
		entryID := strings.TrimSpace(c.Param("id"))
		if !validCanonicalUUID(entryID) {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.WaitingRoom.CancelWaitingRoomEntry(c.Request.Context(), identity.Subject, entryID)
		if err != nil {
			writeError(c, err)
			return
		}
		writeWaitingRoomView(c, http.StatusOK, result)
	}
}

func normalizeWaitingRoomJoin(request *joinWaitingRoomRequest) {
	request.TrainRunID = strings.TrimSpace(request.TrainRunID)
	request.OriginStationCode = strings.ToUpper(strings.TrimSpace(request.OriginStationCode))
	request.DestinationStationCode = strings.ToUpper(strings.TrimSpace(request.DestinationStationCode))
	request.SeatClass = strings.ToLower(strings.TrimSpace(request.SeatClass))
}

func validWaitingRoomJoin(request joinWaitingRoomRequest, maxPassengers int) bool {
	if !validCanonicalUUID(request.TrainRunID) {
		return false
	}
	if _, err := offeringdomain.NewStationCode(request.OriginStationCode); err != nil {
		return false
	}
	if _, err := offeringdomain.NewStationCode(request.DestinationStationCode); err != nil {
		return false
	}
	if request.OriginStationCode == request.DestinationStationCode {
		return false
	}
	if _, err := offeringdomain.ParseSeatClass(request.SeatClass); err != nil {
		return false
	}
	if maxPassengers <= 0 {
		maxPassengers = 8
	}
	return request.PassengerCount >= 1 && request.PassengerCount <= maxPassengers
}

func validCanonicalUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}

func normalizeWaitingRoomView(view *WaitingRoomEntryView) {
	if view.RetryAfterSeconds < 0 {
		view.RetryAfterSeconds = 0
	}
	if view.RetryAfterSeconds > 60 {
		view.RetryAfterSeconds = 60
	}
}

func writeWaitingRoomView(c *gin.Context, status int, view WaitingRoomEntryView) {
	normalizeWaitingRoomView(&view)
	if view.AdmissionToken != "" {
		if view.Status != "admitted" || !validAdmissionTokenHeader(view.AdmissionToken) {
			writeError(c, errInvalidAdmissionTokenOutput)
			return
		}
		c.Header("X-Admission-Token", view.AdmissionToken)
	}
	view.AdmissionToken = ""
	c.JSON(status, view)
}

func setWaitingRoomCachePolicy(c *gin.Context) {
	c.Header("Cache-Control", "no-store, private")
}

func waitingRoomCacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		setWaitingRoomCachePolicy(c)
		c.Next()
	}
}
