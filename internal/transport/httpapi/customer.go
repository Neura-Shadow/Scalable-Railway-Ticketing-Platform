package httpapi

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type passengerRequest struct {
	DisplayName string `json:"display_name"`
}

func registerCustomerRoutes(group *gin.RouterGroup, dependencies Dependencies) {
	passengers := group.Group("/passengers", authenticate(dependencies.TokenParser), authorize(RoleCustomer))
	passengers.GET("", listPassengersHandler(dependencies))
	passengers.POST("", createPassengerHandler(dependencies))
	passengers.GET("/:id", passengerItemHandler(dependencies, http.MethodGet))
	passengers.PATCH("/:id", passengerItemHandler(dependencies, http.MethodPatch))
	passengers.DELETE("/:id", passengerItemHandler(dependencies, http.MethodDelete))

	tickets := group.Group("/ticket-orders", authenticate(dependencies.TokenParser), authorize(RoleCustomer))
	tickets.GET("", listTicketOrdersHandler(dependencies))
	tickets.GET("/:id", getTicketOrderHandler(dependencies))
}

func listPassengersHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Passengers == nil {
			writeError(c, ErrUnavailable)
			return
		}
		page, ok := parsePageRequest(c, "created_at", "created_at", "display_name")
		if !ok {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.Passengers.ListPassengers(c.Request.Context(), identity.Subject, page)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func createPassengerHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Passengers == nil {
			writeError(c, ErrUnavailable)
			return
		}
		identity, _ := identityFromContext(c)
		if !enforceRateLimit(c, dependencies.RateLimiter, RateLimitRequest{Scope: RateLimitPassengerCreate, Key: identity.Subject}, false) {
			return
		}
		var request passengerRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		request.DisplayName = strings.TrimSpace(request.DisplayName)
		if request.DisplayName == "" || len(request.DisplayName) > 200 {
			writeError(c, ErrInvalidInput)
			return
		}
		result, err := dependencies.Passengers.CreatePassenger(c.Request.Context(), identity.Subject, request.DisplayName)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusCreated, result)
	}
}

func passengerItemHandler(dependencies Dependencies, method string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Passengers == nil {
			writeError(c, ErrUnavailable)
			return
		}
		identity, _ := identityFromContext(c)
		id := c.Param("id")
		switch method {
		case http.MethodGet:
			result, err := dependencies.Passengers.GetPassenger(c.Request.Context(), identity.Subject, id)
			if err != nil {
				writeError(c, err)
				return
			}
			c.JSON(http.StatusOK, result)
		case http.MethodPatch:
			var request passengerRequest
			if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
				writeDecodeError(c, err)
				return
			}
			request.DisplayName = strings.TrimSpace(request.DisplayName)
			if request.DisplayName == "" || len(request.DisplayName) > 200 {
				writeError(c, ErrInvalidInput)
				return
			}
			result, err := dependencies.Passengers.UpdatePassenger(c.Request.Context(), identity.Subject, id, request.DisplayName)
			if err != nil {
				writeError(c, err)
				return
			}
			c.JSON(http.StatusOK, result)
		case http.MethodDelete:
			if err := dependencies.Passengers.DeletePassenger(c.Request.Context(), identity.Subject, id); err != nil {
				writeError(c, err)
				return
			}
			c.Status(http.StatusNoContent)
		}
	}
}

func listTicketOrdersHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Tickets == nil {
			writeError(c, ErrUnavailable)
			return
		}
		page, ok := parsePageRequest(c, "-created_at", "created_at", "status")
		if !ok {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.Tickets.ListTicketOrders(c.Request.Context(), identity.Subject, page)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func getTicketOrderHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Tickets == nil {
			writeError(c, ErrUnavailable)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.Tickets.GetTicketOrder(c.Request.Context(), identity.Subject, c.Param("id"))
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}
