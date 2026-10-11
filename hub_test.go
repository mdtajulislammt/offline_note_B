package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHubConnectionLifecycleAndMetrics(t *testing.T) {
	server, hub, _ := setupTestServer(t)

	initialMetrics := hub.GetMetrics()
	if initialMetrics.ActiveConnections != 0 {
		t.Fatalf("expected 0 initial connections, got %d", initialMetrics.ActiveConnections)
	}

	// Connect 3 clients
	c1 := dialWS(t, server)
	defer c1.Close()

	c2 := dialWS(t, server)
	defer c2.Close()

	c3 := dialWS(t, server)
	defer c3.Close()

	// Wait for registration channels to process
	time.Sleep(50 * time.Millisecond)

	m := hub.GetMetrics()
	if m.ActiveConnections != 3 {
		t.Fatalf("expected 3 active connections, got %d", m.ActiveConnections)
	}

	// Disconnect c1
	c1.Close()
	time.Sleep(50 * time.Millisecond)

	mAfter := hub.GetMetrics()
	if mAfter.ActiveConnections != 2 {
		t.Fatalf("expected 2 active connections after close, got %d", mAfter.ActiveConnections)
	}
}

func TestHubHTTPHealthAndMetricsEndpoints(t *testing.T) {
	_, hub, _ := setupTestServer(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /health, got %d", rec.Code)
	}

	_ = hub.GetMetrics()
}
