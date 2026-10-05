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

// SetAlias sets the user's display alias safely.
func (c *Client) SetAlias(alias string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alias = alias
}

// GetAlias returns the user's display alias safely.
func (c *Client) GetAlias() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.alias
}

// SetP2PRoom sets the active P2P room code for this client.
func (c *Client) SetP2PRoom(code string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.p2pRoomCode = code
}

// GetP2PRoom gets the active P2P room code for this client.
func (c *Client) GetP2PRoom() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.p2pRoomCode
}

// AddGroupRoom tracks that the client joined a group room.
func (c *Client) AddGroupRoom(room string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groupRooms[room] = struct{}{}
}

// RemoveGroupRoom removes tracking for a group room.
func (c *Client) RemoveGroupRoom(room string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.groupRooms, room)
}

// GetGroupRooms returns a slice of all group rooms the client has joined.
func (c *Client) GetGroupRooms() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rooms := make([]string, 0, len(c.groupRooms))
	for r := range c.groupRooms {
		rooms = append(rooms, r)
	}
	return rooms
}

// Send non-blockingly enqueues a message to the client's write channel.
// Returns false if client is closed or buffer is full (preventing slow client stalls).
func (c *Client) Send(msg []byte) bool {
	if c.isClosed.Load() {
		return false
	}

	select {
	case c.send <- msg:
		return true
	default:
		slog.Warn("client buffer full, dropped frame",
			"client_id", c.ID,
			"alias", c.GetAlias(),
		)
		return false
	}
}

// Close gracefully closes the client connection and releases resources.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.isClosed.Store(true)
		c.cancel()
		_ = c.conn.Close()
	})
}

