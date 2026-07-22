// Package httpapi adapts HTTP requests to application-facing ports.
package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Dependencies contains transport-owned consumer interfaces. Nil dependencies
// fail closed at the individual endpoint rather than weakening authentication.
type Dependencies struct {
	Readiness           ReadinessChecker
	ReadinessTimeout    time.Duration
	TokenParser         BearerTokenParser
	Reservations        ReservationService
	WaitingRoom         WaitingRoomService
	HotTrainPolicies    HotTrainPolicyService
	MaxRequestBodyBytes int64
	MaxPassengers       int
	HTTPMetrics         HTTPMetrics
	MetricsHandler      http.Handler
	Offering            OfferingQueries
	Auth                AuthService
	RateLimiter         RateLimiter
	Passengers          PassengerService
	Tickets             TicketQueries
	Admin               AdminCommands
	Operator            OperatorCommands
}

// New builds the HTTP router.
func New(dependencies Dependencies) *gin.Engine {
	router := gin.New()
	router.Use(httpMetricsMiddleware(dependencies.HTTPMetrics), safeRecovery())
	router.NoRoute(func(c *gin.Context) { writeError(c, ErrNotFound) })
	router.NoMethod(func(c *gin.Context) { writeError(c, ErrNotFound) })
	router.GET("/livez", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", readyHandler(dependencies))
	metricsHandler := dependencies.MetricsHandler
	if metricsHandler == nil {
		metricsHandler = promhttp.Handler()
	}
	router.GET("/metrics", gin.WrapH(metricsHandler))
	api := router.Group("/api/v1")
	registerAuthRoutes(api, dependencies)
	registerCustomerRoutes(api, dependencies)
	registerManagementRoutes(api, dependencies)
	registerOfferingRoutes(api, dependencies)
	registerReservationRoutes(api, dependencies)
	registerWaitingRoomRoutes(api, dependencies)
	registerHotTrainPolicyRoutes(api, dependencies)
	return router
}

func httpMetricsMiddleware(recorder HTTPMetrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		if recorder == nil {
			c.Next()
			return
		}
		startedAt := time.Now()
		c.Next()
		recorder.ObserveHTTP(c.Request.Method, c.FullPath(), c.Writer.Status(), time.Since(startedAt))
	}
}

func safeRecovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recover() != nil {
				if !c.Writer.Written() {
					writeError(c, errors.New("panic"))
				}
				c.Abort()
			}
		}()
		c.Next()
	}
}

func readyHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		timeout := dependencies.ReadinessTimeout
		if timeout <= 0 || timeout > 10*time.Second {
			timeout = 2 * time.Second
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		components := map[string]string{
			"postgres":      "down",
			"redis":         "down",
			"migrations":    "down",
			"configuration": "down",
		}
		ready := dependencies.Readiness != nil
		seen := map[string]bool{}
		if dependencies.Readiness != nil {
			checks, err := dependencies.Readiness.CheckReadiness(ctx)
			if err != nil {
				ready = false
			}
			for _, check := range checks {
				name := normalizeReadinessComponent(check.Name)
				status := "up"
				if !check.Ready {
					status = "down"
					if check.Optional {
						status = "degraded"
					} else {
						ready = false
					}
				}
				if seen[name] && components[name] == "down" {
					continue
				}
				components[name] = status
				seen[name] = true
			}
		}
		for name, status := range components {
			if status != "up" && !(name == "redis" && status == "degraded") {
				ready = false
				break
			}
		}

		statusCode := http.StatusOK
		status := "ready"
		if !ready {
			statusCode = http.StatusServiceUnavailable
			status = "unready"
		}
		c.JSON(statusCode, gin.H{"status": status, "components": components})
	}
}

func normalizeReadinessComponent(value string) string {
	switch value {
	case "postgres", "redis", "migrations", "configuration":
		return value
	default:
		return "unknown"
	}
}
