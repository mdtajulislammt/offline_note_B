package main

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer (512 KB: sufficient for large SDPs / note bundles).
	maxMessageSize = 512 * 1024

	// Buffer size for outgoing message channel.
	sendBufferSize = 256
)

// Client represents a single active WebSocket connection.
type Client struct {
	ID   string
	hub  *Hub
	conn *websocket.Conn

	// Outgoing buffered channel
	send chan []byte

	// Thread-safe state
	mu          sync.RWMutex
	alias       string
	p2pRoomCode string
	groupRooms  map[string]struct{}

	isClosed atomic.Bool
	closeOnce sync.Once

	ctx    context.Context
	cancel context.CancelFunc
}

// NewClient initializes a new Client instance.
func NewClient(id string, hub *Hub, conn *websocket.Conn) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		ID:         id,
		hub:        hub,
		conn:       conn,
		send:       make(chan []byte, sendBufferSize),
		groupRooms: make(map[string]struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}
}

