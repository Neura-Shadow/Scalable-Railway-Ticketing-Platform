package httpapi

import (
	"net/http"
	"strings"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const maxCorrelationIDBytes = 128

type createHotTrainPolicyRequest struct {
	TrainRunID string `json:"train_run_id"`
	SeatClass  string `json:"seat_class"`
	HotTrainPolicyLimits
}

type updateHotTrainPolicyRequest struct {
	ExpectedVersion int64 `json:"expected_version"`
	Enabled         *bool `json:"enabled,omitempty"`
	HotTrainPolicyLimits
}

type disableHotTrainPolicyRequest struct {
	ExpectedVersion int64 `json:"expected_version"`
}

func registerHotTrainPolicyRoutes(group *gin.RouterGroup, dependencies Dependencies) {
	policies := group.Group(
		"/operator/hot-train-policies",
		authenticate(dependencies.TokenParser),
		authorize(RoleOperator, RoleAdmin),
	)
	policies.GET("", listHotTrainPoliciesHandler(dependencies))
	policies.POST("", createHotTrainPolicyHandler(dependencies))
	policies.GET("/:id", getHotTrainPolicyHandler(dependencies))
	policies.PUT("/:id", updateHotTrainPolicyHandler(dependencies))
	policies.DELETE("/:id", disableHotTrainPolicyHandler(dependencies))
}

func listHotTrainPoliciesHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.HotTrainPolicies == nil {
			writeError(c, ErrUnavailable)
			return
		}
		page, ok := parsePageRequest(c, "train_run_id", "train_run_id", "seat_class", "updated_at")
		if !ok {
			writeError(c, ErrInvalidInput)
			return
		}
		result, err := dependencies.HotTrainPolicies.ListHotTrainPolicies(c.Request.Context(), page)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func getHotTrainPolicyHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.HotTrainPolicies == nil {
			writeError(c, ErrUnavailable)
			return
		}
		policyID := strings.TrimSpace(c.Param("id"))
		if !validCanonicalUUID(policyID) {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		result, err := dependencies.HotTrainPolicies.GetHotTrainPolicy(c.Request.Context(), identity.Subject, policyID)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func updateHotTrainPolicyHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.HotTrainPolicies == nil {
			writeError(c, ErrUnavailable)
			return
		}
		policyID := strings.TrimSpace(c.Param("id"))
		var request updateHotTrainPolicyRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		correlationID, ok := policyCorrelationID(c)
		if !ok || !validCanonicalUUID(policyID) || request.ExpectedVersion < 1 ||
			(request.Enabled != nil && !*request.Enabled) ||
			!validHotTrainPolicyLimits(request.HotTrainPolicyLimits) {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		if !enforcePolicyMutationRateLimit(c, dependencies.RateLimiter, identity.Subject) {
			return
		}
		result, err := dependencies.HotTrainPolicies.UpdateHotTrainPolicy(c.Request.Context(), UpdateHotTrainPolicyCommand{
			ActorID:              identity.Subject,
			CorrelationID:        correlationID,
			PolicyID:             policyID,
			ExpectedVersion:      request.ExpectedVersion,
			Enabled:              request.Enabled,
			HotTrainPolicyLimits: request.HotTrainPolicyLimits,
		})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func disableHotTrainPolicyHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.HotTrainPolicies == nil {
			writeError(c, ErrUnavailable)
			return
		}
		policyID := strings.TrimSpace(c.Param("id"))
		var request disableHotTrainPolicyRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		correlationID, ok := policyCorrelationID(c)
		if !ok || !validCanonicalUUID(policyID) || request.ExpectedVersion < 1 {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		if !enforcePolicyMutationRateLimit(c, dependencies.RateLimiter, identity.Subject) {
			return
		}
		result, err := dependencies.HotTrainPolicies.DisableHotTrainPolicy(c.Request.Context(), DisableHotTrainPolicyCommand{
			ActorID:         identity.Subject,
			CorrelationID:   correlationID,
			PolicyID:        policyID,
			ExpectedVersion: request.ExpectedVersion,
		})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func createHotTrainPolicyHandler(dependencies Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dependencies.HotTrainPolicies == nil {
			writeError(c, ErrUnavailable)
			return
		}
		var request createHotTrainPolicyRequest
		if err := decodeJSON(c, dependencies.MaxRequestBodyBytes, &request); err != nil {
			writeDecodeError(c, err)
			return
		}
		request.TrainRunID = strings.TrimSpace(request.TrainRunID)
		request.SeatClass = strings.ToLower(strings.TrimSpace(request.SeatClass))
		correlationID, ok := policyCorrelationID(c)
		if !ok || !validCanonicalUUID(request.TrainRunID) || !validPolicySeatClass(request.SeatClass) ||
			!validHotTrainPolicyLimits(request.HotTrainPolicyLimits) {
			writeError(c, ErrInvalidInput)
			return
		}
		identity, _ := identityFromContext(c)
		if !enforcePolicyMutationRateLimit(c, dependencies.RateLimiter, identity.Subject) {
			return
		}
		result, err := dependencies.HotTrainPolicies.CreateHotTrainPolicy(c.Request.Context(), CreateHotTrainPolicyCommand{
			ActorID:              identity.Subject,
			CorrelationID:        correlationID,
			TrainRunID:           request.TrainRunID,
			SeatClass:            request.SeatClass,
			HotTrainPolicyLimits: request.HotTrainPolicyLimits,
		})
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusCreated, result)
	}
}

func enforcePolicyMutationRateLimit(c *gin.Context, limiter RateLimiter, actorID string) bool {
	if limiter == nil {
		writeError(c, ErrUnavailable)
		return false
	}
	return enforceRateLimit(c, limiter, RateLimitRequest{
		Scope: RateLimitPolicyMutation,
		Key:   actorID,
	}, false)
}

func validPolicySeatClass(value string) bool {
	_, err := offeringdomain.ParseSeatClass(value)
	return err == nil
}

func validHotTrainPolicyLimits(value HotTrainPolicyLimits) bool {
	_, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput{
		MaxQueueSize:           value.MaxQueueSize,
		AdmissionRatePerSecond: value.AdmissionRatePerSecond,
		MaxInflightAdmissions:  value.MaxInflightAdmissions,
		AdmissionTokenTTL:      time.Duration(value.AdmissionTokenTTLSeconds) * time.Second,
		ProcessingLease:        time.Duration(value.ProcessingLeaseSeconds) * time.Second,
		QueueEntryTTL:          time.Duration(value.QueueEntryTTLSeconds) * time.Second,
	})
	return err == nil
}

func policyCorrelationID(c *gin.Context) (string, bool) {
	value := c.GetHeader("X-Correlation-ID")
	if value == "" {
		return uuid.NewString(), true
	}
	if len(value) > maxCorrelationIDBytes || strings.TrimSpace(value) != value {
		return "", false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return "", false
		}
	}
	return value, true
}
