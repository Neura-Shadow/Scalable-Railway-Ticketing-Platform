package httpapi

import (
	"errors"
	"net/http"
	"strings"

	accountsdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	"github.com/gin-gonic/gin"
)

type registerRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name"`
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type registrationAcceptedResponse struct {
	Message string `json:"message"`
}

const registrationAcceptedMessage = "If the registration request can be processed, the account workflow will continue."

func registerAuthRoutes(group *gin.RouterGroup, dependencies Dependencies) {
	auth := group.Group("/auth")
	auth.POST("/register", registerHandler(dependencies))
	auth.POST("/login", loginHandler(dependencies))
	auth.POST("/refresh", refreshHandler(dependencies))
	auth.POST("/logout", authenticate(dependencies.TokenParser), logoutHandler(dependencies))
}

func registerHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !enforceRateLimit(c, dependencies.RateLimiter, RateLimitRequest{Scope: RateLimitRegister, Key: directClientKey(c)}, false) {
			return
		}
		if dependencies.Auth == nil {
			writeError(c, ErrUnavailable)
			return
		}
		var request registerRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		request.Email = strings.ToLower(strings.TrimSpace(request.Email))
		request.DisplayName = strings.TrimSpace(request.DisplayName)
		if !accountsdomain.ValidRegistrationEmail(request.Email) ||
			!accountsdomain.ValidRegistrationPassword(request.Password) ||
			!accountsdomain.ValidPassengerDisplayName(request.DisplayName) {
			writeError(c, ErrInvalidInput)
			return
		}
		err := dependencies.Auth.Register(c.Request.Context(), RegisterCommand(request))
		if err != nil && !errors.Is(err, ErrConflict) {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusAccepted, registrationAcceptedResponse{Message: registrationAcceptedMessage})
	}
}

func loginHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !enforceRateLimit(c, dependencies.RateLimiter, RateLimitRequest{Scope: RateLimitLogin, Key: directClientKey(c)}, false) {
			return
		}
		if dependencies.Auth == nil {
			writeError(c, ErrUnavailable)
			return
		}
		var request loginRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		request.Email = strings.ToLower(strings.TrimSpace(request.Email))
		if !validEmail(request.Email) || request.Password == "" {
			writeError(c, ErrInvalidInput)
			return
		}
		result, err := dependencies.Auth.Login(c.Request.Context(), LoginCommand(request))
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func refreshHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Auth == nil {
			writeError(c, ErrUnavailable)
			return
		}
		var request refreshRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		if strings.TrimSpace(request.RefreshToken) == "" {
			writeError(c, ErrInvalidInput)
			return
		}
		result, err := dependencies.Auth.Refresh(c.Request.Context(), request.RefreshToken)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func logoutHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.Auth == nil {
			writeError(c, ErrUnavailable)
			return
		}
		identity, _ := identityFromContext(c)
		var request refreshRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		if strings.TrimSpace(request.RefreshToken) == "" {
			writeError(c, ErrInvalidInput)
			return
		}
		if err := dependencies.Auth.Logout(c.Request.Context(), identity.Subject, request.RefreshToken); err != nil {
			writeError(c, err)
			return
		}
		c.Status(http.StatusNoContent)
	}
}

func validEmail(value string) bool {
	at := strings.LastIndexByte(value, '@')
	return at > 0 && at < len(value)-1 && !strings.ContainsAny(value, " \t\r\n")
}
