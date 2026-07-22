package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type createReservationRequest struct {
	TrainRunID             string   `json:"train_run_id"`
	OriginStationCode      string   `json:"origin_station_code"`
	DestinationStationCode string   `json:"destination_station_code"`
	SeatClass              string   `json:"seat_class"`
	PassengerIDs           []string `json:"passenger_ids"`
}

func registerReservationRoutes(group *gin.RouterGroup, dependencies Dependencies) {
	reservations := group.Group("/reservations", authenticate(dependencies.TokenParser), authorize(RoleCustomer))
	reservations.POST("", createReservationHandler(dependencies))
	reservations.GET("/:id", reservationActionHandler(dependencies, "get"))
	reservations.POST("/:id/confirm", reservationActionHandler(dependencies, "confirm"))
	reservations.POST("/:id/cancel", reservationActionHandler(dependencies, "cancel"))
}

func createReservationHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Reservations == nil {
			writeError(c, ErrUnavailable)
			return
		}
		identity, _ := identityFromContext(c)
		// Reservation correctness remains in the authoritative application/DB
		// transaction, so a rate-limit backend outage degrades open here.
		if !enforceRateLimit(c, dependencies.RateLimiter, RateLimitRequest{Scope: RateLimitReservationCreate, Key: identity.Subject}, true) {
			return
		}
		var request createReservationRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		normalizeCreateReservationRequest(&request)
		idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
		admissionToken := strings.TrimSpace(c.GetHeader("X-Admission-Token"))
		if !validCreateReservationRequest(request, idempotencyKey, dependencies.MaxPassengers) {
			writeError(c, ErrInvalidInput)
			return
		}
		if admissionToken != "" && !validAdmissionTokenHeader(admissionToken) {
			writeError(c, ErrAdmissionInvalid)
			return
		}
		result, err := dependencies.Reservations.CreateHold(c.Request.Context(), CreateReservationCommand{
			OwnerID:                identity.Subject,
			IdempotencyKey:         idempotencyKey,
			AdmissionToken:         admissionToken,
			TrainRunID:             request.TrainRunID,
			OriginStationCode:      request.OriginStationCode,
			DestinationStationCode: request.DestinationStationCode,
			SeatClass:              request.SeatClass,
			PassengerIDs:           append([]string(nil), request.PassengerIDs...),
		})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusCreated, result)
	}
}

func reservationActionHandler(dependencies Dependencies, action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Reservations == nil {
			writeError(c, ErrUnavailable)
			return
		}
		identity, _ := identityFromContext(c)
		reservationID := c.Param("id")
		idempotencyKey := ""
		if action == "confirm" || action == "cancel" {
			idempotencyKey = strings.TrimSpace(c.GetHeader("Idempotency-Key"))
			if !validIdempotencyKey(idempotencyKey) {
				writeError(c, ErrInvalidInput)
				return
			}
		}
		var result ReservationView
		var err error
		switch action {
		case "get":
			result, err = dependencies.Reservations.GetReservation(c.Request.Context(), identity.Subject, reservationID)
		case "confirm":
			result, err = dependencies.Reservations.ConfirmReservation(c.Request.Context(), ReservationMutationCommand{OwnerID: identity.Subject, ReservationID: reservationID, IdempotencyKey: idempotencyKey})
		case "cancel":
			result, err = dependencies.Reservations.CancelReservation(c.Request.Context(), ReservationMutationCommand{OwnerID: identity.Subject, ReservationID: reservationID, IdempotencyKey: idempotencyKey})
		default:
			err = errors.New("unsupported reservation action")
		}
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func validCreateReservationRequest(request createReservationRequest, idempotencyKey string, maxPassengers int) bool {
	if strings.TrimSpace(request.TrainRunID) == "" || strings.TrimSpace(request.OriginStationCode) == "" || strings.TrimSpace(request.DestinationStationCode) == "" || strings.TrimSpace(request.SeatClass) == "" {
		return false
	}
	if request.OriginStationCode == request.DestinationStationCode || len(request.PassengerIDs) == 0 {
		return false
	}
	if !validIdempotencyKey(idempotencyKey) {
		return false
	}
	if maxPassengers <= 0 {
		maxPassengers = 8
	}
	if len(request.PassengerIDs) > maxPassengers {
		return false
	}
	seen := make(map[string]struct{}, len(request.PassengerIDs))
	for _, passengerID := range request.PassengerIDs {
		passengerID = strings.TrimSpace(passengerID)
		if passengerID == "" {
			return false
		}
		if _, exists := seen[passengerID]; exists {
			return false
		}
		seen[passengerID] = struct{}{}
	}
	return true
}

func normalizeCreateReservationRequest(request *createReservationRequest) {
	request.TrainRunID = strings.TrimSpace(request.TrainRunID)
	request.OriginStationCode = strings.ToUpper(strings.TrimSpace(request.OriginStationCode))
	request.DestinationStationCode = strings.ToUpper(strings.TrimSpace(request.DestinationStationCode))
	request.SeatClass = strings.ToLower(strings.TrimSpace(request.SeatClass))
	for index := range request.PassengerIDs {
		request.PassengerIDs[index] = strings.TrimSpace(request.PassengerIDs[index])
	}
}

func validIdempotencyKey(value string) bool {
	if len(value) < 8 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}
