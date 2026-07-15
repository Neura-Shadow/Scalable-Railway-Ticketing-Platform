// Package workerhttp provides the private health and metrics surface shared by
// background workers. It intentionally has no application command endpoints.
package workerhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ReadinessCheck func(context.Context) error

func New(address string, registry *prometheus.Registry, readiness ReadinessCheck, timeout time.Duration) (*http.Server, error) {
	if address == "" || registry == nil || readiness == nil || timeout <= 0 {
		return nil, errors.New("worker HTTP configuration invalid")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		writeStatus(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := readiness(ctx); err != nil {
			writeStatus(w, http.StatusServiceUnavailable, "unready")
			return
		}
		writeStatus(w, http.StatusOK, "ready")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	return &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}, nil
}

func writeStatus(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": value})
}
