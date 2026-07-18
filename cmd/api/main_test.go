package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestPublicReasonRedactsUnexpectedStartupErrors(t *testing.T) {
	t.Parallel()

	const secret = "sentinel-m11-database-password"
	if got := publicReason(errors.New("failed to parse postgres://user:" + secret + "@db/railway")); got != "startup failure" {
		t.Fatalf("publicReason() = %q, want bounded fallback", got)
	}
	if got := publicReason(errors.New("configuration invalid")); got != "configuration invalid" {
		t.Fatalf("publicReason() = %q, want known category", got)
	}
}

func TestServeUntilShutdownDrainsInFlightRequestBeforeCancellingBaseContext(t *testing.T) {
	baseContext, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	started := make(chan struct{})
	release := make(chan struct{})
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		close(started)
		select {
		case <-release:
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte("drained"))
		case <-request.Context().Done():
			writer.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	server := &http.Server{
		Handler: handler,
		BaseContext: func(net.Listener) context.Context {
			return baseContext
		},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	shutdown := make(chan struct{})
	serveResult := make(chan error, 1)
	go func() { serveResult <- serveUntilShutdown(server, listener, shutdown, 3*time.Second) }()

	responseResult := make(chan int, 1)
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if requestErr != nil {
			responseResult <- 0
			return
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		responseResult <- response.StatusCode
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not reach handler")
	}

	close(shutdown)
	select {
	case err := <-serveResult:
		t.Fatalf("server returned before request drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-baseContext.Done():
		t.Fatal("base request context was cancelled by shutdown signal")
	default:
	}
	close(release)

	select {
	case status := <-responseResult:
		if status != http.StatusOK {
			t.Fatalf("response status = %d, want 200", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not finish")
	}
	select {
	case err := <-serveResult:
		if err != nil {
			t.Fatalf("serveUntilShutdown() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not finish shutdown")
	}
}
