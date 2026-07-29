package httpapi

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

func registerManagementRoutes(group *gin.RouterGroup, dependencies Dependencies) {
	admin := group.Group("/admin", authenticate(dependencies.TokenParser), authorize(RoleAdmin))
	admin.POST("/stations", adminHandler[StationWrite](dependencies, AdminCreateStation, func(command *AdminCommand, value *StationWrite) { command.Station = value }))
	admin.POST("/routes", adminHandler[RouteWrite](dependencies, AdminCreateRoute, func(command *AdminCommand, value *RouteWrite) { command.Route = value }))
	admin.POST("/trains", adminHandler[TrainWrite](dependencies, AdminCreateTrain, func(command *AdminCommand, value *TrainWrite) { command.Train = value }))
	admin.POST("/coaches", adminHandler[CoachWrite](dependencies, AdminCreateCoach, func(command *AdminCommand, value *CoachWrite) { command.Coach = value }))
	admin.POST("/seats", adminHandler[SeatWrite](dependencies, AdminCreateSeat, func(command *AdminCommand, value *SeatWrite) { command.Seat = value }))
	admin.POST("/fares", adminHandler[FareWrite](dependencies, AdminCreateFare, func(command *AdminCommand, value *FareWrite) { command.Fare = value }))

	operator := group.Group("/operator", authenticate(dependencies.TokenParser), authorize(RoleOperator))
	operator.POST("/train-runs", createTrainRunHandler(dependencies))
	operator.POST("/train-runs/:id/inventory", operatorInventoryHandler(dependencies))
	operator.PATCH("/train-runs/:id/status", updateTrainRunStatusHandler(dependencies))
	operator.PATCH("/train-runs/:id/fares/:resource_id", operatorFareSnapshotHandler(dependencies))
	operator.PATCH("/train-runs/:id/seats/:resource_id/booking-state", operatorSeatBookingStateHandler(dependencies))
	operator.PATCH("/train-runs/:id/booking-policy-version", operatorBookingPolicyHandler(dependencies))
	operator.GET("/train-runs/:id/fares/:resource_id", operatorBookingStateHandler(dependencies, OperatorBookingFareState))
	operator.GET("/train-runs/:id/seats/:resource_id/booking-state", operatorBookingStateHandler(dependencies, OperatorBookingSeatState))
	operator.GET("/train-runs/:id/booking-policy-version", operatorBookingStateHandler(dependencies, OperatorBookingPolicyState))
}

func operatorBookingStateHandler(dependencies Dependencies, kind OperatorBookingStateKind) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.OperatorBookingState == nil {
			writeError(c, ErrUnavailable)
			return
		}
		result, err := dependencies.OperatorBookingState.GetOperatorBookingState(c.Request.Context(), OperatorBookingStateQuery{
			Kind: kind, TrainRunID: c.Param("id"), ResourceID: c.Param("resource_id"),
		})
		if err != nil {
			writeError(c, err)
			return
		}
		c.Header("ETag", `"source-`+strconv.FormatInt(result.SourceVersion, 10)+`"`)
		c.JSON(http.StatusOK, result)
	}
}

func operatorFareSnapshotHandler(dependencies Dependencies) gin.HandlerFunc {
	return operatorSnapshotHandler[OperatorFareSnapshotWrite](dependencies, OperatorInstallFareSnapshot,
		func(command *OperatorCommand, value *OperatorFareSnapshotWrite) { command.FareSnapshot = value })
}

func operatorSeatBookingStateHandler(dependencies Dependencies) gin.HandlerFunc {
	return operatorSnapshotHandler[OperatorSeatBookingStateWrite](dependencies, OperatorSetSeatBookingState,
		func(command *OperatorCommand, value *OperatorSeatBookingStateWrite) { command.SeatState = value })
}

func operatorBookingPolicyHandler(dependencies Dependencies) gin.HandlerFunc {
	return operatorSnapshotHandler[OperatorBookingPolicyWrite](dependencies, OperatorBumpBookingPolicy,
		func(command *OperatorCommand, value *OperatorBookingPolicyWrite) { command.BookingPolicy = value })
}

func operatorSnapshotHandler[T any](dependencies Dependencies, action OperatorAction, assign func(*OperatorCommand, *T)) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Operator == nil {
			writeError(c, ErrUnavailable)
			return
		}
		key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
		if !validIdempotencyKey(key) {
			writeError(c, ErrInvalidInput)
			return
		}
		var request T
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		identity, _ := identityFromContext(c)
		if !enforceRateLimit(c, dependencies.RateLimiter, RateLimitRequest{
			Scope: RateLimitOperatorBooking, Key: identity.Subject,
		}, false) {
			return
		}
		command := OperatorCommand{ActorID: identity.Subject, Action: action, TrainRunID: c.Param("id"),
			ResourceID: c.Param("resource_id"), IdempotencyKey: key}
		assign(&command, &request)
		result, err := dependencies.Operator.ExecuteOperator(c.Request.Context(), command)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func adminHandler[T any](dependencies Dependencies, action AdminAction, assign func(*AdminCommand, *T)) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Admin == nil {
			writeError(c, ErrUnavailable)
			return
		}
		var request T
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		identity, _ := identityFromContext(c)
		command := AdminCommand{ActorID: identity.Subject, Action: action}
		assign(&command, &request)
		result, err := dependencies.Admin.ExecuteAdmin(c.Request.Context(), command)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusCreated, result)
	}
}

func createTrainRunHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Operator == nil {
			writeError(c, ErrUnavailable)
			return
		}
		var request TrainRunWrite
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.Operator.ExecuteOperator(c.Request.Context(), OperatorCommand{ActorID: identity.Subject, Action: OperatorCreateTrainRun, TrainRun: &request})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusCreated, result)
	}
}

func operatorInventoryHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Operator == nil {
			writeError(c, ErrUnavailable)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.Operator.ExecuteOperator(c.Request.Context(), OperatorCommand{ActorID: identity.Subject, Action: OperatorInitializeInventory, TrainRunID: c.Param("id")})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func updateTrainRunStatusHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Operator == nil {
			writeError(c, ErrUnavailable)
			return
		}
		var request TrainRunStatusWrite
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.Operator.ExecuteOperator(c.Request.Context(), OperatorCommand{ActorID: identity.Subject, Action: OperatorUpdateTrainRunStatus, TrainRunID: c.Param("id"), Status: &request})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}
