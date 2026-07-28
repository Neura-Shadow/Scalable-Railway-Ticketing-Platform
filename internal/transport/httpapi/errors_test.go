package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestAdmissionErrorsUseSafeStatusAndBoundedRetryAfter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		wantRetry  int
	}{
		{name: "required", err: WithRetryAfter(ErrAdmissionRequired, 3), wantStatus: http.StatusPreconditionRequired, wantCode: "admission_required", wantRetry: 3},
		{name: "expired", err: ErrAdmissionExpired, wantStatus: http.StatusGone, wantCode: "admission_expired"},
		{name: "mismatch", err: ErrAdmissionInvalid, wantStatus: http.StatusForbidden, wantCode: "admission_invalid"},
		{name: "queue full", err: WithRetryAfter(ErrQueueFull, 8), wantStatus: http.StatusTooManyRequests, wantCode: "queue_full", wantRetry: 8},
		{name: "quota", err: WithRetryAfter(ErrReservationQuotaExceeded, 12), wantStatus: http.StatusTooManyRequests, wantCode: "reservation_quota_exceeded", wantRetry: 12},
		{name: "backpressure bounded", err: WithRetryAfter(ErrReservationBackpressure, 600), wantStatus: http.StatusServiceUnavailable, wantCode: "reservation_backpressure", wantRetry: 60},
		{name: "rebalancing", err: WithRetryAfter(ErrServiceTemporarilyRebalancing, 2), wantStatus: http.StatusServiceUnavailable, wantCode: "service_temporarily_rebalancing", wantRetry: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			writeError(context, test.err)

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			if test.wantRetry == 0 {
				if got := recorder.Header().Get("Retry-After"); got != "" {
					t.Fatalf("Retry-After = %q, want empty", got)
				}
			} else if got := recorder.Header().Get("Retry-After"); got != strconv.Itoa(test.wantRetry) {
				t.Fatalf("Retry-After = %q, want %d", got, test.wantRetry)
			}
			var body errorEnvelope
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if body.Error.Code != test.wantCode {
				t.Fatalf("error code = %q, want %q", body.Error.Code, test.wantCode)
			}
		})
	}
}
