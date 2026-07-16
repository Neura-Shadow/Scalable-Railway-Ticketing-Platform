package workerhttp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/prometheus/client_golang/prometheus"
)

func TestWorkerHealthAndMetricsSurface(t *testing.T) {
	ready := true
	server, err := workerhttp.New(":9090", prometheus.NewRegistry(), func(context.Context) error {
		if !ready {
			return errors.New("database secret")
		}
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		path string
		want int
	}{{"/livez", http.StatusOK}, {"/readyz", http.StatusOK}, {"/metrics", http.StatusOK}} {
		response := httptest.NewRecorder()
		server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
		if response.Code != test.want {
			t.Fatalf("GET %s = %d, want %d", test.path, response.Code, test.want)
		}
	}
	ready = false
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "database secret") {
		t.Fatalf("unready response = %d %s", response.Code, response.Body.String())
	}
}
