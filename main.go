package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// Permissive check origin for real-time signaling engine (can be configured via env)
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	// Initialize structured JSON logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	port := getEnv("PORT", "8080")
	redisURL := getEnv("REDIS_URL", "localhost:6379")

	slog.Info("initializing supernote real-time signaling engine",
		"port", port,
		"redis_url", redisURL,
	)

	// Initialize Redis Store
	redisStore, err := NewRedisStore(redisURL)
	if err != nil {
		slog.Warn("redis connection failed; running in single-node in-memory mode", "err", err)
		redisStore = nil
	} else {
		slog.Info("connected to redis store successfully")
		defer redisStore.Close()
	}

	// Initialize Hub Connection Manager
	hub := NewHub(redisStore)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Hub coordinator loop
	go hub.Run(ctx)

	// Setup HTTP Handlers
	mux := http.NewServeMux()

	// WebSocket Signaling Endpoint
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	// Health Check Endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "healthy",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	})

	// Public Real-Time Metrics Endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hub.GetMetrics())
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Graceful shutdown management
	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		slog.Info("signaling server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()

	sig := <-shutdownCh
	slog.Info("shutdown signal received", "signal", sig.String())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("server forced to shutdown", "err", err)
	}

	hub.Close()
	slog.Info("signaling engine shut down cleanly")
}

// serveWs upgrades the HTTP connection to WebSocket, registers the client,
// and starts the read and write pumps.
func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("failed to upgrade websocket connection", "err", err)
		return
	}

	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		clientID = uuid.NewString()
	}
	client := NewClient(clientID, hub, conn)

	// Query params: optional initial alias ?alias=Alice
	if alias := r.URL.Query().Get("alias"); alias != "" {
		client.SetAlias(alias)
	}

	hub.Register(client)

	// Start client lifecycle pumps in separate goroutines
	go client.writePump()
	go client.readPump()

	slog.Debug("client connected and pumps started", "client_id", clientID)
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
