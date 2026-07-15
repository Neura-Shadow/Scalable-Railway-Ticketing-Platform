package httpapi

import (
	"net"

	"github.com/gin-gonic/gin"
)

func enforceRateLimit(c *gin.Context, limiter RateLimiter, request RateLimitRequest, failOpen bool) bool {
	if limiter == nil {
		return true
	}
	allowed, err := limiter.Allow(c.Request.Context(), request)
	if err != nil {
		if failOpen {
			return true
		}
		writeError(c, ErrUnavailable)
		return false
	}
	if !allowed {
		writeError(c, ErrTooManyRequests)
		return false
	}
	return true
}

func directClientKey(c *gin.Context) string {
	clientIP := c.ClientIP()
	if net.ParseIP(clientIP) != nil {
		return clientIP
	}
	return "unknown"
}
